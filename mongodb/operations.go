package mongodb

import (
	"fmt"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"

	"github.com/n-h-n/go-lib/log"
	"github.com/n-h-n/go-lib/utils"
)

func (client *Client) Aggregate(
	database Database,
	collection Collection,
	pipeline mongo.Pipeline,
	opts ...*options.AggregateOptions,
) (*mongo.Cursor, error) {
	var db *mongo.Database
	if hasSearchStage(pipeline) {
		db = client.mongoClient.Database(database.String(), options.Database().SetReadConcern(readconcern.Local()))
	} else {
		db = client.mongoClient.Database(database.String())
	}
	mongoCollection := db.Collection(collection.String())
	o := []*options.AggregateOptions{options.Aggregate().SetMaxTime(5 * time.Second)}
	o = append(o, opts...)
	cursor, err := mongoCollection.Aggregate(client.ctx, pipeline, o...)

	if err != nil {
		log.Log.Error(client.ctx, err.Error())
		return nil, err
	}
	return cursor, nil
}

func (client *Client) CountDocuments(
	database Database,
	collection Collection,
	filter interface{},
) (int64, error) {
	return client.mongoClient.Database(database.String()).Collection(collection.String()).CountDocuments(client.ctx, filter)
}

func (client *Client) DeleteMany(
	database Database,
	collection Collection,
	filter interface{},
) (*mongo.DeleteResult, error) {
	mongoCollection := client.mongoClient.Database(database.String()).Collection(collection.String())
	return mongoCollection.DeleteMany(client.ctx, filter)
}

func (client *Client) DeleteOne(
	database Database,
	collection Collection,
	filter interface{},
) (*mongo.DeleteResult, error) {
	mongoCollection := client.mongoClient.Database(database.String()).Collection(collection.String())
	return mongoCollection.DeleteOne(client.ctx, filter)
}

func (client *Client) DeleteOneDocument(
	document Document,
) (*mongo.DeleteResult, error) {
	mongoCollection := client.mongoClient.Database(document.DatabaseName()).Collection(document.CollectionName())
	filter := bson.M{"_id": document.GetID()}
	return mongoCollection.DeleteOne(client.ctx, filter)
}

func (client *Client) Disconnect() error {
	if client.verboseMode {
		log.Log.Debugf(client.ctx, "disconnecting MongoDB client from %s", client.uri)
	}
	return client.mongoClient.Disconnect(client.ctx)
}

func (client *Client) BulkWrite(
	database Database,
	collection Collection,
	models []mongo.WriteModel,
	opts ...*options.BulkWriteOptions,
) (*mongo.BulkWriteResult, error) {
	mongoCollection := client.mongoClient.Database(database.String()).Collection(collection.String())
	return mongoCollection.BulkWrite(client.ctx, models, opts...)
}

// FindOne finds the first database record matching the document _id provided
func (client *Client) FindOne(
	database Database,
	collection Collection,
	filter,
	result interface{},
	opts ...*options.FindOneOptions,
) error {
	mongoCollection := client.mongoClient.Database(database.String()).Collection(collection.String())
	return mongoCollection.FindOne(client.ctx, filter, opts...).Decode(result)
}

func (client *Client) FindOneDocument(
	document Document,
	filter interface{},
	opts ...*options.FindOneOptions,
) error {
	mongoCollection := client.mongoClient.Database(document.DatabaseName()).Collection(document.CollectionName())
	return mongoCollection.FindOne(client.ctx, filter, opts...).Decode(document)
}

func (client *Client) Find(
	database Database,
	collection Collection,
	filter interface{},
	opts ...*options.FindOptions,
) (*mongo.Cursor, error) {
	mongoCollection := client.mongoClient.Database(database.String()).Collection(collection.String())
	return mongoCollection.Find(client.ctx, filter, opts...)
}

func (client *Client) InsertOne(
	database Database,
	collection Collection,
	document interface{},
) (*mongo.InsertOneResult, error) {
	mongoCollection := client.mongoClient.Database(database.String()).Collection(collection.String())
	return mongoCollection.InsertOne(client.ctx, document)
}

func (client *Client) InsertOneDocument(
	document Document,
) (*mongo.InsertOneResult, error) {
	document.SetMetadata()
	mongoCollection := client.mongoClient.Database(document.DatabaseName()).Collection(document.CollectionName())
	return mongoCollection.InsertOne(client.ctx, document)
}

func (client *Client) ReplaceOne(
	database Database,
	document Document,
	filter interface{},
) (*mongo.UpdateResult, error) {
	mongoCollection := client.mongoClient.Database(database.String()).Collection(document.CollectionName())
	return mongoCollection.ReplaceOne(client.ctx, filter, document)
}

func (client *Client) StartSession(opts ...*options.SessionOptions) (*mongo.Session, error) {
	session, err := client.mongoClient.StartSession(opts...)
	if err != nil {
		return nil, err
	}
	return &session, nil
}

func (client *Client) UpdateMany(
	database Database,
	collection Collection,
	filter,
	update interface{},
	upsert bool,
) (*mongo.UpdateResult, error) {
	mongoCollection := client.mongoClient.Database(database.String()).Collection(collection.String())
	opts := options.Update()

	return mongoCollection.UpdateMany(client.ctx, filter, update, opts)
}

func (client *Client) UpdateOne(
	database Database,
	collection Collection,
	filter,
	update interface{},
	opts ...*options.UpdateOptions,
) (*mongo.UpdateResult, error) {
	mongoCollection := client.mongoClient.Database(database.String()).Collection(collection.String())

	return mongoCollection.UpdateOne(client.ctx, filter, update, opts...)
}

func (client *Client) UpdateOneDocument(
	document Document,
	upsert bool,
	opt ...commandOpt,
) (*mongo.UpdateResult, error) {
	if document.GetID() == "" {
		return nil, fmt.Errorf("document must have an ID: %v", document)
	}

	document.SetMetadata()

	filter := bson.D{
		{Key: "_id", Value: document.GetID()},
		{Key: document.TimestampField(), Value: bson.M{"$lt": time.Now()}},
	}

	ignoreFields := []string{}

	if !upsert {
		ignoreFields = append(ignoreFields, "_id")
	}

	customOperators := bson.M{}
	for _, o := range opt {
		output, action := o()
		switch action {
		case "filter":
			filter = output.(bson.D)
		case "ignoreFields":
			ignoreFields = append(ignoreFields, output.([]string)...)
		case "customOperators":
			customOperators = output.(bson.M)
		}
	}

	ignoreFields = utils.RemoveDuplicatesStr(ignoreFields)
	update, err := utils.IndividualizeSetFields(document, ignoreFields, customOperators)
	if err != nil {
		return nil, err
	}

	mongoCollection := client.mongoClient.Database(document.DatabaseName()).Collection(document.CollectionName())
	opts := options.Update()
	opts.SetUpsert(upsert)

	if upsert {
		res, err := mongoCollection.UpdateOne(client.ctx, filter, update, opts)
		if err != nil && strings.Contains(err.Error(), "E11000 duplicate key error") {
			ignoreFields = append(ignoreFields, "_id")
			update, errOther := utils.IndividualizeSetFields(document, ignoreFields, customOperators)
			if errOther != nil {
				return nil, errOther
			}
			opts.SetUpsert(false)
			res, err = mongoCollection.UpdateOne(client.ctx, filter, update, opts)
		}
		return res, err
	}
	return mongoCollection.UpdateOne(client.ctx, filter, update, opts)
}

func (client *Client) ListDatabaseNames(
	filter interface{},
) ([]string, error) {
	if filter == nil {
		filter = bson.M{}
	}
	return client.mongoClient.ListDatabaseNames(client.ctx, filter)
}

func (client *Client) ListCollectionNames(
	database Database,
	filter interface{},
) ([]string, error) {
	if filter == nil {
		filter = bson.M{}
	}
	return client.mongoClient.Database(database.String()).ListCollectionNames(client.ctx, filter)
}

func (client *Client) CreateCollection(
	databaseName string,
	collectionName string,
	opts ...*options.CreateCollectionOptions,
) error {
	return client.mongoClient.Database(databaseName).CreateCollection(client.ctx, collectionName, opts...)
}
