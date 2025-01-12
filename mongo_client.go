package goscs

import (
	"context"
	"time"

	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
)

type MongoDbClient struct {
	Uri    string
	client *mongo.Client
}

func (mc *MongoDbClient) GetClient() *mongo.Client {
	return mc.client
}

func (mc *MongoDbClient) getDefaultConfig() *options.ClientOptions {
	bsonOpts := &options.BSONOptions{
		UseJSONStructTags: true,
		NilMapAsEmpty:     true,
		NilSliceAsEmpty:   true,
	}

	return options.Client().ApplyURI(mc.Uri).SetBSONOptions(bsonOpts)
}

func (mc *MongoDbClient) Connect(config *options.ClientOptions) error {
	if len(mc.Uri) == 0 {
		return nil
	}
	if config == nil {
		config = mc.getDefaultConfig()
	}
	var err error
	mc.client, err = mongo.Connect(config)
	if err != nil {
		return err
	}
	mc.client.Ping(context.Background(), nil)
	return nil
}

func (mc *MongoDbClient) Disconnect() error {
	return mc.client.Disconnect(context.TODO())
}

func (mc *MongoDbClient) Ping() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	rp := readpref.Primary() // Read from the primary node
	if err := mc.client.Ping(ctx, rp); err != nil {
		return err
	}
	log.Info().Msg("MongoDB connection is successful!")
	return nil
}

func (mc *MongoDbClient) GetCollection(dbName string, collectionName string) *mongo.Collection {
	return mc.client.Database(dbName).Collection(collectionName)
}

func NewMongoClient(uri string, config *options.ClientOptions) (*MongoDbClient, error) {
	client := &MongoDbClient{Uri: uri}
	err := client.Connect(nil)
	if err != nil {
		return nil, err
	}

	return client, nil
}
