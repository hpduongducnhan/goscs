package goscs

import (
	"context"

	redis "github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
)

const (
	EVENT        = 2
	BINARY_EVENT = 5
)

type SioEmitterOpts struct {
	RedisUrl string // Host means hostname like localhost
	Key      string // Key means redis subscribe key
	Name     string // Name
}

type SioEmitter struct {
	Redis  *redis.Client
	Config *SioEmitterOpts
}

func (e *SioEmitter) Close() error {
	return e.Redis.Close()
}

func (e *SioEmitter) Publish(message WSMessage) (*redis.IntCmd, error) {
	message.SetName(e.Config.Name)
	packed, err := message.Pack()
	if err != nil {
		log.Error().Err(err).Msg("Failed to pack message")
		return nil, err
	}
	ctx := context.Background()

	if len(message.Rooms) == 0 {
		channel := e.Config.Key + "#" + message.Namespace + "#"
		log.Info().Str("channel", channel).Msg("Publishing message")
		return e.Redis.Publish(ctx, channel, packed), nil
	} else {
		for _, room := range message.Rooms {
			channel := e.Config.Key + "#" + message.Namespace + "#" + room + "#"
			log.Info().Str("channel", channel).Msg("Publishing message")
			_, err := e.Redis.Publish(ctx, channel, packed).Result()
			if err != nil {
				log.Error().Err(err).Msg("Failed to publish message")
				return nil, err
			}
		}
		return nil, nil
	}
}

func NewEmitter(redisUrl string, key string) (*SioEmitter, error) {
	// set default key
	if key == "" {
		key = "socket.io"
	}

	eName, err := generateRandomName(6)
	if err != nil {
		log.Error().Err(err).Msg("Failed to generate random name")
		return nil, err
	}

	// save SioEmitter options
	emitterOptions := &SioEmitterOpts{
		RedisUrl: redisUrl,
		Key:      key,
		Name:     eName,
	}

	// connect to redis
	redisOpts, err := redis.ParseURL(redisUrl)
	if err != nil {
		log.Error().Err(err).Msg("Failed to parse redis url")
		return nil, err
	}
	redisClient := redis.NewClient(redisOpts)

	socketIoEmitter := &SioEmitter{
		Redis:  redisClient,
		Config: emitterOptions,
	}
	return socketIoEmitter, nil
}
