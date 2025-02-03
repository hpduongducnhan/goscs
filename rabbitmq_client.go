package goscs

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/rabbitmq/amqp091-go"
)

func RbmqConnect(url string) (conn *amqp091.Connection, err error) {
	// url amqp://user:password@node1:5672,node2:5672,node3:5672/vhost
	conn, err = amqp091.Dial(url)
	if err != nil {
		return nil, err
	}
	return
}

func RbmqGetChannel(conn *amqp091.Connection) (ch *amqp091.Channel, err error) {
	ch, err = conn.Channel()
	if err != nil {
		return nil, err
	}
	return
}

func RbmqDeclareQueue(ch *amqp091.Channel, name string) (*amqp091.Queue, error) {
	queue, err := ch.QueueDeclare(name, true, false, false, false, nil)
	if err != nil {
		return nil, err
	}
	return &queue, nil
}

func RbmqDeclareExchange(ch *amqp091.Channel, name, kind string) error {
	// kind should be one of []string{"direct", "fanout", "topic", "headers"}
	err := ch.ExchangeDeclare(name, kind, true, false, false, false, nil)
	return err
}

func RbmqDeclareExchangeTopicWithDLXPattern(ch *amqp091.Channel, exchangeName, queueName, routingKey string) error {
	err := ch.ExchangeDeclare(exchangeName, "topic", true, false, false, false, nil)
	if err != nil {
		return err
	}

	var delayQueueName string = fmt.Sprintf("%s-delay", queueName)
	var deadQueueName string = fmt.Sprintf("%s-dead", queueName)

	// declare main-queue
	_, err = ch.QueueDeclare(
		queueName,
		true, false, false, false,
		amqp091.Table{
			"x-dead-letter-exchange":    exchangeName,
			"x-dead-letter-routing-key": fmt.Sprintf("%s.retry", routingKey),
			"x-queue-type":              "quorum",
		},
	)
	if err != nil {
		return err
	}
	// bind main-queue to exchange
	err = ch.QueueBind(queueName, routingKey, exchangeName, false, nil)
	if err != nil {
		return err
	}

	// declare delay-queue
	_, err = ch.QueueDeclare(
		delayQueueName,
		true, false, false, false,
		amqp091.Table{
			"x-message-ttl":             10 * 1000, // 10 seconds
			"x-dead-letter-exchange":    exchangeName,
			"x-dead-letter-routing-key": routingKey,
			"x-queue-type":              "quorum",
		},
	)
	if err != nil {
		return err
	}
	// bind delay-queue to exchange
	err = ch.QueueBind(delayQueueName, fmt.Sprintf("%s.retry", routingKey), exchangeName, false, nil)
	if err != nil {
		return nil
	}

	// declare dead-queue
	_, err = ch.QueueDeclare(
		deadQueueName,
		true, false, false, false,
		amqp091.Table{"x-queue-type": "quorum"},
	)
	if err != nil {
		return err
	}
	// bind dead-queue to exchange
	err = ch.QueueBind(deadQueueName, fmt.Sprintf("%s.dead", routingKey), exchangeName, false, nil)
	if err != nil {
		return err
	}
	return nil
}

func RbmqPublish(ch *amqp091.Channel, exchangeName, routingKey string, body []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := ch.PublishWithContext(
		ctx,
		exchangeName, routingKey,
		false, false,
		amqp091.Publishing{
			Headers: amqp091.Table{
				"hello": "world",
				"w":     1,
			},
			ContentType: "application/json",
			Body:        body,
		},
	)
	return err
}

func RbmqAck(rbmqChannel *amqp091.Channel, msg *amqp091.Delivery, err error) {
	if err == nil {
		msg.Ack(false)
		return
	}
	xDeadInfo := RbmqXDeadInfo{}
	xDeadInfo.ParseXDead(msg.Headers)
	if xDeadInfo.Success {
		if xDeadInfo.Count > 10 {
			RbmqPublish(rbmqChannel, xDeadInfo.Exchange, xDeadInfo.RoutingKeyDead, msg.Body)
			msg.Ack(false)
			return
		}
	}
	msg.Nack(false, false)
}

type RbmqBaseHandler func(msg *amqp091.Delivery) error

func rbmqConsumer(
	rbmqChannel *amqp091.Channel,
	handler RbmqBaseHandler,
	internalMsgChan <-chan *amqp091.Delivery,
	wg *sync.WaitGroup, // WaitGroup to signal completion

) {
	defer wg.Done()
	for msg := range internalMsgChan {
		err := handler(msg)
		RbmqAck(rbmqChannel, msg, err)
	}
}

func RbmqConsumeWithWorker(
	ctx context.Context, // Pass the context for cancellation
	totalWorker int,
	handler RbmqBaseHandler,
	channel *amqp091.Channel,
	queueName string,
) {
	// make sure it run in a new go routine
	go rbmqConsumeWithWorker(ctx, totalWorker, handler, channel, queueName)
}

func rbmqConsumeWithWorker(
	ctx context.Context, // Pass the context for cancellation
	totalWorker int,
	handler RbmqBaseHandler,
	channel *amqp091.Channel,
	queueName string,
) {
	if totalWorker <= 0 {
		totalWorker = 1
	}
	channel.Qos(
		totalWorker, // Prefetch count (one message at a time)
		0,           // Prefetch size
		false,       // Apply to this consumer only
	)
	internalMsgChan := make(chan *amqp091.Delivery)
	var wg sync.WaitGroup
	for i := 0; i < totalWorker; i++ {
		wg.Add(1)
		go rbmqConsumer(channel, handler, internalMsgChan, &wg)
	}

	rbmqMessages, err := channel.Consume(
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
	for msg := range rbmqMessages {
		select {
		case internalMsgChan <- &msg:
		case <-ctx.Done():
			close(internalMsgChan)
			wg.Wait()
			return
		}
	}
	// Wait for all workers to finish
	wg.Wait()
}
