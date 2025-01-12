package goscs

import (
	"context"
	"runtime"
	"time"

	"github.com/rs/zerolog/log"

	redis "github.com/redis/go-redis/v9"
)

func ConnectRedis(ctx context.Context, url string) (*redis.Client, error) {
	// url = "redis://user:password@localhost:6379/0?protocol=3"
	opts, err := redis.ParseURL(url)
	if err != nil {
		return nil, err
	}
	client := redis.NewClient(opts)
	res, err := client.Ping(ctx).Result()
	if err != nil {
		return nil, err
	}
	log.Info().Interface("resPing", res).Msg("Connected to Redis")
	return client, nil
}

func Subscribe(ctx context.Context, client *redis.Client, channel string) (*redis.PubSub, <-chan *redis.Message) {
	pubsub := client.Subscribe(ctx, channel)
	_, err := pubsub.Receive(ctx)
	if err != nil {
		panic(err)
	}

	subChan := pubsub.Channel()
	return pubsub, subChan
}

func Publish(ctx context.Context, client *redis.Client, channel string, message string) error {
	err := client.Publish(ctx, channel, message).Err()
	if err != nil {
		log.Error().Err(err).Msg("Error publishing message")
		return err
	}
	return nil
}

func subscribeWorker(wID int, ctx context.Context, buffChan chan *redis.Message, messageHandler func(msg *redis.Message)) {
	log.Info().Int("workerID", wID).Msg("Worker started")
	for {
		select {
		case msg := <-buffChan:
			messageHandler(msg)
		case <-ctx.Done():
			return
		}
	}
}

func SubscribeWithReconnect(
	ctx context.Context,
	client *redis.Client,
	channelName string,
	messageHandler func(msg *redis.Message),
	workerCount int,
	bufferSize int,
) error {
	// calculate worker count based on CPU count
	if workerCount <= 0 {
		workerCount := int(float64(runtime.NumCPU()) * 0.8)
		if workerCount < 1 {
			workerCount = 1
		}
	}
	// set default buffer size
	if bufferSize <= 0 {
		bufferSize = 8888
	}

	// init workers
	buffedChannel := make(chan *redis.Message, bufferSize)
	for i := 0; i < workerCount; i++ {
		go subscribeWorker(i, ctx, buffedChannel, messageHandler)
	}

	// Subscribe to the channel
	for {
		log.Info().Str("redisChannel", channelName).Msg("subscribed to redis channel with reconnect")
		sub := client.Subscribe(ctx, channelName)
		ch := sub.Channel()
		// Monitor messages and errors
		for {
			select {
			case msg, ok := <-ch:
				if !ok {
					log.Warn().Str("redisChannel", channelName).Msg("Channel closed, reconnecting...")
					goto Reconnect // Break out of the message loop and reconnect
				}
				buffedChannel <- msg

			case <-ctx.Done():
				log.Info().Str("redisChannel", channelName).Msg("Context canceled, exiting subscriber...")
				sub.Close()
				return nil
			}
		}

	Reconnect:
		// Close the subscription before reconnecting
		if err := sub.Close(); err != nil {
			log.Error().Err(err).Str("redisChannel", channelName).Msg("Error closing subscription")
		}
		// Wait before reconnecting to avoid tight loops in case of persistent issues
		time.Sleep(2 * time.Second)
	}
}
