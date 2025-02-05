package goscs

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rabbitmq/amqp091-go"
	"github.com/rs/zerolog"
	"github.com/shirou/gopsutil/process"
)

// Get worker's PID
var workerPID = os.Getpid()

func collectWorkerMetrics(
	workerCPUUsage, workerMemUsage, workerIOReadBytes, workerIOWriteBytes prometheus.Gauge,
) {
	proc, err := process.NewProcess(int32(workerPID))
	if err != nil {
		log.Fatalf("Failed to get worker process: %v", err)
	}

	for {
		// Get CPU usage
		cpuPercent, err := proc.CPUPercent()
		if err == nil {
			workerCPUUsage.Set(cpuPercent)
		}

		// Get memory usage
		memInfo, err := proc.MemoryInfo()
		if err == nil {
			workerMemUsage.Set(float64(memInfo.RSS)) // Resident Set Size (actual memory usage)
		}

		// Get I/O stats
		ioStat, err := proc.IOCounters()
		if err == nil {
			workerIOReadBytes.Set(float64(ioStat.ReadBytes))
			workerIOWriteBytes.Set(float64(ioStat.WriteBytes))
		}

		// Wait before collecting metrics again
		time.Sleep(5 * time.Second)
	}
}

type ConsumerConfig struct {
	Ctx           context.Context
	CtxCancel     context.CancelFunc
	PrefetchCount int
	Channel       *amqp091.Channel
	Exchange      string
	ExchangeType  string
	Queue         string
	RoutingKey    string
	Handler       RbmqBaseHandler
}

type RabbitMQWorker struct {
	Logger zerolog.Logger
	URLS   []string
	conn   *amqp091.Connection

	workerContext               context.Context
	workerCancel                context.CancelFunc
	workerRestSvr               *http.Server
	WorekrRestSvrPort           int
	WorkerRestSvrHealthCheckUrl string

	PrometheusEnable    bool
	PrometheusMetricUrl string

	osSignalChan chan os.Signal

	connectFailedCount int                 // use for retry when connect rabbitmq server
	connectionErrChan  chan *amqp091.Error // use for listen if rbmq connection is closed

	setupDone bool
	consumers map[string]ConsumerConfig // use for recreate consumers when reconnect rbmq
}

func (rw *RabbitMQWorker) runWithRestServer() {
	if rw.workerRestSvr != nil {
		return
	}
	if rw.WorkerRestSvrHealthCheckUrl == "" {
		rw.WorkerRestSvrHealthCheckUrl = "/health"
	}
	if rw.WorekrRestSvrPort == 0 {
		rw.WorekrRestSvrPort = 8080
	}

	if rw.PrometheusEnable {
		if rw.PrometheusMetricUrl == "" {
			rw.PrometheusMetricUrl = "/metrics"
		}

		// declare prometheus
		messagesProcessed := prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "rabbitmq_messages_processed_total",
				Help: "Total number of messages processed from RabbitMQ",
			},
		)

		messageProcessingTime := prometheus.NewHistogram(
			prometheus.HistogramOpts{
				Name:    "rabbitmq_message_processing_seconds",
				Help:    "Time taken to process each RabbitMQ message",
				Buckets: prometheus.DefBuckets,
			},
		)

		cpuUsage := prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "worker_cpu_usage_percent",
				Help: "Current CPU usage percentage of the worker",
			},
		)

		memUsage := prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "worker_memory_usage_bytes",
				Help: "Current memory usage of the worker in bytes",
			},
		)

		ioReadBytes := prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "worker_io_read_bytes",
				Help: "Bytes read by the worker",
			},
		)

		ioWriteBytes := prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "worker_io_write_bytes",
				Help: "Bytes written by the worker",
			},
		)

		// collect system info with interval
		go collectWorkerMetrics(cpuUsage, memUsage, ioReadBytes, ioWriteBytes)

		// register
		prometheus.MustRegister(messagesProcessed)
		prometheus.MustRegister(messageProcessingTime)
		prometheus.MustRegister(cpuUsage)
		prometheus.MustRegister(memUsage)
		prometheus.MustRegister(ioReadBytes)
		prometheus.MustRegister(ioWriteBytes)

	}

	mux := http.NewServeMux()
	mux.HandleFunc(rw.WorkerRestSvrHealthCheckUrl, func(w http.ResponseWriter, r *http.Request) {
		response := `{"status":"ok"}`
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, response)
	})
	mux.Handle(rw.PrometheusMetricUrl, promhttp.Handler())
	rw.workerRestSvr = &http.Server{
		Addr:    fmt.Sprintf(":%d", rw.WorekrRestSvrPort),
		Handler: mux,
	}

	// add goroutine if you need
	func() {
		if err := rw.workerRestSvr.ListenAndServe(); err != http.ErrServerClosed {
			rw.Logger.Error().Err(err).Msg("HTTP server error, shutdown")
		}
	}()
}

func (rw *RabbitMQWorker) setup() {
	if rw.setupDone {
		return
	}

	// create context for this worker
	rw.workerContext, rw.workerCancel = context.WithCancel(context.Background())

	// create map consumer
	rw.consumers = make(map[string]ConsumerConfig, 0)

	// setup goroutine wait os terminated signal to stop worker
	rw.osSignalChan = make(chan os.Signal, 1)
	signal.Notify(rw.osSignalChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-rw.osSignalChan
		rw.workerCancel()
		rw.Logger.Info().Msg("worker canceled, waiting .....!")

		// shutdown healthcheck server
		wRestSvrCtx, _ := context.WithTimeout(context.Background(), 5*time.Second)
		rw.workerRestSvr.Shutdown(wRestSvrCtx)

		// shutdown all consumers
		for _, consumerConf := range rw.consumers {
			if consumerConf.CtxCancel != nil {
				consumerConf.CtxCancel()
			}
		}
	}()

	// setup reconnect the lost connection
	go func() {
		rw.Logger.Info().Msg("create reconnect when connection is lost or broker down")
		for {
			select {
			case <-rw.workerContext.Done():
				return

			case err, ok := <-rw.connectionErrChan:
				rw.Logger.Info().Msg("got rw.connectionErrChan err")
				if !ok {
					rw.Logger.Error().Msg("connectionErrChan is closed, worker force quit unexpected")
					// rw.connectionErrChan = make(chan *amqp091.Error)
				} else {
					rw.Logger.Error().Err(err).Msg("RabbitMQ connection lost, Reconnecting...")
					// reconnect and init consumers
					rw.connect()
					rw.initConsumers()
				}
			}
		}
	}()

	rw.setupDone = true
	rw.Logger.Info().Msg("setup done")
}

func (rw *RabbitMQWorker) connect() {
	// recreate error chan here
	// rw.connectionErrChan = make(chan *amqp091.Error)
	if rw.conn != nil && !rw.conn.IsClosed() {
		return
	}

	rw.connectionErrChan = make(chan *amqp091.Error)
	rw.connectFailedCount = 0
	for {
		var conn *amqp091.Connection
		var err error
		for _, url := range rw.URLS {
			conn, err = amqp091.Dial(url)
			if err != nil {
				rw.connectFailedCount++
				rw.Logger.Warn().Str("rbmqUrl", url).Err(err).Msg("Connect rabbitmq failed")
				continue
			} else {
				rw.conn = conn
				rw.conn.NotifyClose(rw.connectionErrChan)
				rw.Logger.Info().Str("rbmqUrl", url).Msg("Connected rabbitmq")
				return
			}
		}
		if rw.conn == nil || (rw.conn != nil && rw.conn.IsClosed()) {
			rw.Logger.Warn().Msg("Connection failed. Retrying in 5 seconds...")
			time.Sleep(5 * time.Second)
		}
	}
}

func (rw *RabbitMQWorker) checkConfig() {
	if len(rw.URLS) == 0 {
		panic("URLS is empty")
	}
	// if len(rw.consumers) == 0 {
	// 	panic("not found any config for consumer, use Register Consumer before Run")
	// }
}

// -----------------------------------------
func (rw *RabbitMQWorker) createConsumerConfigUniqueKey(exchange, queue, routingKey string) string {
	return fmt.Sprintf("%s+%s+%s", exchange, queue, routingKey)
}

func (rw *RabbitMQWorker) initConsumers() {
	for _, consumerConf := range rw.consumers {
		if consumerConf.ExchangeType == "topic" {
			rw.initConsumersTypeTopic(&consumerConf)
		} else {
			rw.Logger.Warn().Msg(fmt.Sprintf("init consumers not support exchange type %s", consumerConf.ExchangeType))
		}
	}
}

func (rw *RabbitMQWorker) initConsumersTypeTopic(consumerConf *ConsumerConfig) {
	var err error
	// create new channel
	consumerConf.Channel, err = rw.conn.Channel()
	if err != nil {
		rw.Logger.Error().Err(err).Interface("consumerConfig", consumerConf).Msg("Create channel for consumer failed")
		panic("Create channel for consumer failed")
	} else {
		rw.Logger.Info().Msg("created new channel")
	}
	err = RbmqDeclareExchangeTopicWithDLXPattern(
		consumerConf.Channel,
		consumerConf.Exchange, consumerConf.Queue, consumerConf.RoutingKey,
	)
	if err != nil {
		rw.Logger.Error().Err(err).Interface("consumerConfig", consumerConf).Msg("Create exchange, queue with DLX pattern failed")
		panic("Create exchange, queue with DLX pattern failed")
	} else {
		rw.Logger.Info().Msg("Created exchange, queue with DLX pattern")
	}
	RbmqConsumeWithWorker(consumerConf.Ctx, consumerConf.PrefetchCount, consumerConf.Handler, consumerConf.Channel, consumerConf.Queue)
	rw.Logger.Info().
		Interface("consumerConfig", consumerConf).
		Msg("Started Consumer")
}

func (rw *RabbitMQWorker) RegisterConsumerTypeTopic(
	exchange, queue, routingKey string,
	PrefetchCount int,
	handler RbmqBaseHandler,
) error {
	// run setup first
	rw.setup()

	if PrefetchCount == 0 {
		PrefetchCount = 3
	}
	if exchange == "" || queue == "" || routingKey == "" {
		rw.Logger.Error().
			Str("rbmqExchange", exchange).
			Str("rbmqQueue", queue).
			Str("rbmqRoutingKey", routingKey).
			Msg("Register failed because exchange or queue or routingKey is empty")
		return fmt.Errorf("register failed because exchange or queue or routingKey is empty")
	}
	key := rw.createConsumerConfigUniqueKey(exchange, queue, routingKey)
	consumerCtx, consumerCtxCancel := context.WithCancel(context.Background())

	if _, exists := rw.consumers[key]; !exists {
		rw.consumers[key] = ConsumerConfig{
			Ctx:           consumerCtx,
			CtxCancel:     consumerCtxCancel,
			PrefetchCount: PrefetchCount,
			Exchange:      exchange,
			ExchangeType:  "topic",
			Queue:         queue,
			RoutingKey:    routingKey,
			Handler:       handler,
		}
	}
	return nil
}

// -----------------------------------------
func (rw *RabbitMQWorker) Run() {
	//
	rw.setup()

	// run with auto failover, reconnect
	rw.checkConfig()
	rw.connect()
	rw.initConsumers()

	rw.runWithRestServer()
}
