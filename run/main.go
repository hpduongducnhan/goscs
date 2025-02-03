package main

import (
	"context"
	"fmt"
	"os"

	"github.com/hpduongducnhan/goscs"
	"github.com/rabbitmq/amqp091-go"
	"github.com/rs/zerolog"
)

var EXCHANGE_NAME = "exampleExchange"
var QUEUE_NAME = "exampleQueue"
var ROUTING_KEY = "chat.facebook"

func consumeMessage(channel *amqp091.Channel, queueName string) {
	_ = channel.Qos(
		1,     // Prefetch count (one message at a time)
		0,     // Prefetch size
		false, // Apply to this consumer only
	)
	msgs, err := channel.Consume(
		queueName, // queue
		"",        // consumer
		false,     // auto ack
		false,     // exclusive
		false,     // no local
		false,     // no wait
		nil,       // args
	)
	if err != nil {
		fmt.Printf("consume message rbmq get error %s", err.Error())
		return
	}
	for d := range msgs {
		fmt.Printf(" [x] %s\n", d.Body)
		xDeadInfo := goscs.RbmqXDeadInfo{}
		xDeadInfo.ParseXDead(d.Headers)

		fmt.Printf("xdead info %+v\n", xDeadInfo)
		if xDeadInfo.Success {
			if xDeadInfo.Count > 10 {
				goscs.RbmqPublish(channel, xDeadInfo.Exchange, xDeadInfo.RoutingKeyDead, d.Body)
				d.Ack(false)
			}
		} else {
			d.Nack(false, false)
		}
	}
}

func runRabbitMQWithWorker() {
	fmt.Printf("hello\n")
	rabbitmqConn, err := goscs.RbmqConnect("amqp://guest:guest@192.168.1.6:5672//")
	if err != nil {
		fmt.Printf("connect rbmq get error %s", err.Error())
		return
	}
	channel, err := goscs.RbmqGetChannel(rabbitmqConn)
	if err != nil {
		fmt.Printf("create channel get error %s", err.Error())
		return
	}
	err = goscs.RbmqDeclareExchangeTopicWithDLXPattern(channel, "exampleExchange", "exampleQueue", "chat.facebook")
	if err != nil {
		fmt.Printf("declare queue with dlx get error %s", err.Error())
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	goscs.RbmqConsumeWithWorker(ctx, 2, func(msg *amqp091.Delivery) error {
		return nil
	}, channel, QUEUE_NAME)

	// time.Sleep(30 * time.Second)
	// cancel()
}

func runRbmqWorker() {
	logger := zerolog.New(zerolog.ConsoleWriter{Out: os.Stdout, NoColor: true})
	worker := &goscs.RabbitMQWorker{
		URLS: []string{
			"amqp://guest:guest@192.168.1.6:5672//",
		},
		Logger: logger,
	}
	worker.RegisterConsumerTypeTopic(
		"exampleExchange", "exampleQueue", "chat.facebook", 2,
		func(msg *amqp091.Delivery) error {
			logger.Info().Interface("rbmqMessage", msg).Msg("get rabbitmq message")
			return nil
		},
	)
	worker.RegisterConsumerTypeTopic(
		"testExchange", "testQueue", "chat.zalo", 2,
		func(msg *amqp091.Delivery) error {
			logger.Info().Interface("rbmqMessage", msg).Msg("get rabbitmq message")
			return nil
		},
	)
	logger.Info().Msg("register done")
	worker.Run()
}

func main() {
	runRbmqWorker()
}
