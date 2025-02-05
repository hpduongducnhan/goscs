package goscs

import (
	"context"
	"fmt"
	"os"
)

// -----------------------------------------
func (rw *RabbitMQWorker) createConsumerConfigUniqueKey(exchange, queue, routingKey string) string {
	return fmt.Sprintf("%s+%s+%s", exchange, queue, routingKey)
}

func (rw *RabbitMQWorker) initConsumersTypeTopic(consumerConf *ConsumerConfig) {
	var err error
	// create new channel
	consumerConf.Channel, err = rw.conn.Channel()
	if err != nil {
		rw.Logger.Error().Err(err).Interface("consumerConfig", consumerConf).Msg("Create channel for consumer failed")
		panic("Create channel for consumer failed")
	}

	err = RbmqDeclareExchangeTopicWithDLXPattern(
		consumerConf.Channel,
		consumerConf.Exchange, consumerConf.Queue, consumerConf.RoutingKey,
	)
	if err != nil {
		rw.Logger.Error().Err(err).Interface("consumerConfig", consumerConf).Msg("Create exchange, queue with DLX pattern failed")
		panic("Create exchange, queue with DLX pattern failed")
	}

	RbmqConsumeWithWorker(consumerConf.Ctx, consumerConf.PrefetchCount, consumerConf.Handler, consumerConf.Channel, consumerConf.Queue, consumerConf.Name)
	rw.Logger.Info().
		Object("consumerConfig", consumerConf).
		Msg("Started Consumer")
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
			Name:          fmt.Sprintf("%s/%d/%s/%s", rw.Name, os.Getpid(), queue, routingKey),
			Ctx:           consumerCtx,
			CtxCancel:     consumerCtxCancel,
			PrefetchCount: PrefetchCount,
			Exchange:      exchange,
			ExchangeType:  "topic",
			Queue:         queue,
			RoutingKey:    routingKey,
			Handler:       handler,
		}
	} else {
		consumerCtxCancel()
	}
	return nil
}
