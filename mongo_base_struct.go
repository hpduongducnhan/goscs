package goscs

import (
	"context"
	"errors"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

type EmbededMongoBaseModel interface {
	SetObjectId(objID bson.ObjectID)
}

type MongoDbBaseModel[T EmbededMongoBaseModel] struct {
	ObjectID bson.ObjectID `bson:"_id,omitempty"`
}

func (base *MongoDbBaseModel[T]) SetObjectId(objectID bson.ObjectID) {
	base.ObjectID = objectID
}

func (base *MongoDbBaseModel[T]) FindByID(
	ctx context.Context,
	collection *mongo.Collection,
	objID string,
) (*T, error) {
	objectID, err := bson.ObjectIDFromHex(objID)
	if err != nil {
		return nil, err
	}
	filter := bson.M{
		"_id": objectID,
	}
	var res T
	err = collection.FindOne(ctx, filter).Decode(&res)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, err
	}
	return &res, nil
}

func (base *MongoDbBaseModel[T]) FindMany(
	ctx context.Context,
	collection *mongo.Collection,
	filter bson.M,
) ([]T, error) {
	cursor, err := collection.Find(ctx, filter)
	if err != nil {
		return []T{}, err
	}
	var res []T
	if err = cursor.All(ctx, &res); err != nil {
		return []T{}, err
	}
	return res, nil
}
