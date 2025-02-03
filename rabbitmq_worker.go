package goscs

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rabbitmq/amqp091-go"
	"github.com/rs/zerolog"
)

type ConsumerConfig struct {
	Ctx           context.Context
	CtxCancel     context.CancelFunc
	PrefetchCount int
	Channel       *amqp091.Channel
	Exchange      string
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
	worekrRestSvrPort           int
	workerRestSvrHealthCheckUrl string

	osSignalChan chan os.Signal

	connectFailedCount int // use for retry when connect rabbitmq server
	// connectionErrChan  chan *amqp091.Error // use for listen if rbmq connection is closed

	setupDone bool
	consumers map[string]ConsumerConfig // use for recreate consumers when reconnect rbmq
}

func (rw *RabbitMQWorker) runWithRestHealthCheck() {
	if rw.workerRestSvr != nil {
		return
	}
	if rw.workerRestSvrHealthCheckUrl == "" {
		rw.workerRestSvrHealthCheckUrl = "/health"
	}
	if rw.worekrRestSvrPort == 0 {
		rw.worekrRestSvrPort = 8080
	}

	mux := http.NewServeMux()
	mux.HandleFunc(rw.workerRestSvrHealthCheckUrl, func(w http.ResponseWriter, r *http.Request) {
		response := `{"status":"ok"}`
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, response)
	})
	rw.workerRestSvr = &http.Server{
		Addr:    fmt.Sprintf(":%d", rw.worekrRestSvrPort),
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

	rw.setupDone = true
	rw.Logger.Info().Msg("setup done")
}

func (rw *RabbitMQWorker) connect() {
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
				rw.Logger.Info().Str("rbmqUrl", url).Msg("Connected rabbitmq")
				return
			}
		}
		if rw.conn == nil {
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

func (rw *RabbitMQWorker) initConsumersTypeTopic() {
	for _, consumerConf := range rw.consumers {
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
	rw.initConsumersTypeTopic()

	rw.runWithRestHealthCheck()
}
