package goscs

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/rabbitmq/amqp091-go"
	"github.com/rs/zerolog"
)

type ConsumerConfig struct {
	Name          string
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

func (c *ConsumerConfig) MarshalZerologObject(e *zerolog.Event) {
	e.Int("PrefetchCount", c.PrefetchCount).
		Str("Name", c.Name).
		Str("Exchange", c.Exchange).
		Str("ExchangeType", c.ExchangeType).
		Str("Queue", c.Queue).
		Str("RoutingKey", c.RoutingKey)
}

type RabbitMQWorker struct {
	Logger zerolog.Logger
	URLS   []string
	Name   string

	conn *amqp091.Connection

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

func (rw *RabbitMQWorker) MarshalZerologObject(e *zerolog.Event) {
	e.Strs("URLS", rw.URLS).
		Str("Name", rw.Name).
		Int("WorekrRestSvrPort", rw.WorekrRestSvrPort).
		Str("WorkerRestSvrHealthCheckUrl", rw.WorkerRestSvrHealthCheckUrl).
		Bool("PrometheusEnable", rw.PrometheusEnable).
		Str("PrometheusMetricUrl", rw.PrometheusMetricUrl).
		Int("connectFailedCount", rw.connectFailedCount).
		Bool("setupDone", rw.setupDone).
		Int("num_consumers", len(rw.consumers))
}

func (rw *RabbitMQWorker) setupDefaults() {
	if rw.WorkerRestSvrHealthCheckUrl == "" {
		rw.WorkerRestSvrHealthCheckUrl = "/health"
	}
	if rw.WorekrRestSvrPort == 0 {
		rw.WorekrRestSvrPort = 8080
	}
	if rw.Name == "" {
		hostname, err := os.Hostname()
		if err != nil {
			hostname = uuid.New().String()
		}
		rw.Name = fmt.Sprintf("%s[%s]", hostname, runtime.GOOS)
	}

	// create context for this worker
	rw.workerContext, rw.workerCancel = context.WithCancel(context.Background())

	// create map consumer
	rw.consumers = make(map[string]ConsumerConfig, 0)

}

func (rw *RabbitMQWorker) setupTerminatedSignals() {
	// setup goroutine wait os terminated signal to stop worker
	rw.osSignalChan = make(chan os.Signal, 1)
	signal.Notify(rw.osSignalChan, syscall.SIGINT, syscall.SIGTERM)

	// create goroutine listen signal
	go func() {
		<-rw.osSignalChan
		// cancel context
		rw.workerCancel()
		rw.Logger.Info().Msg("worker canceled, shutting down .....!")

		// shutdown all consumers
		for _, consumerConf := range rw.consumers {
			if consumerConf.CtxCancel != nil {
				consumerConf.CtxCancel()
			}
		}

		// shutdown healthcheck server
		wRestSvrCtx, wRestSvrCtxCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer wRestSvrCtxCancel()
		err := rw.workerRestSvr.Shutdown(wRestSvrCtx)
		if err != nil {
			time.Sleep(5 * time.Second)
		}
	}()
}

func (rw *RabbitMQWorker) setupConsumerFailoverAndReconnect() {
	// setup reconnect the lost connection
	// by create a listener event connection error
	go func() {
		rw.Logger.Info().Msg("setup Failover and Reconnect -> done")
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
}

func (rw *RabbitMQWorker) beforeSetup() {
	// let embeded struct add more actions
}

func (rw *RabbitMQWorker) afterSetup() {
	// let embeded struct add more actions
}

func (rw *RabbitMQWorker) setup() {
	if rw.setupDone {
		return
	}

	rw.beforeSetup()

	rw.setupDefaults()
	rw.setupTerminatedSignals()
	rw.setupConsumerFailoverAndReconnect()

	rw.afterSetup()

	rw.setupDone = true
	rw.Logger.Info().Msg("worker setup -> done")
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
	if len(rw.consumers) == 0 {
		panic("not found any configs for consumer, use Register Consumer before Run")
	}
}

// -----------------------------------------
func (rw *RabbitMQWorker) Run() {
	//
	rw.setup()

	// run with auto failover, reconnect
	rw.checkConfig()
	rw.connect()
	rw.initConsumers()

	rw.runWithMonitorAgent()
}
