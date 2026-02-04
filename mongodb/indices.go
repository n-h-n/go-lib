package mongodb

import (
	"fmt"
	"regexp"
	"strings"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"golang.org/x/exp/slices"

	"github.com/n-h-n/go-lib/log"
	"github.com/n-h-n/go-lib/utils"
)

var (
	MongoCollectionIndices = map[Document][]mongo.IndexModel{}
)

func (client *Client) EnsureIndicesExist(
	database Database,
	reindex bool,
	indicesSpecs ...map[Document][]mongo.IndexModel) error {
	availableCollections, err := client.ListCollectionNames(database, nil)
	if err != nil {
		return err
	}

	collections := []string{}
	indices := make(map[string][]mongo.IndexModel)
	for _, spec := range indicesSpecs {
		formatted := getDesiredIndices(&spec)
		for k, v := range formatted {
			collections = append(collections, k)
			if _, exists := indices[k]; exists {
				return fmt.Errorf("duplicate indices specified for collection name: %s", k)
			}
			indices[k] = v
		}
	}

	for _, coll := range availableCollections {
		collection := client.mongoClient.Database(database.String()).Collection(coll)
		var cursor *mongo.Cursor
		var err error
		var results []bson.M
		var indexNames []string
		if slices.Contains(collections, coll) {
			cursor, err = collection.Indexes().List(client.ctx)
			if err != nil {
				return err
			}
			defer cursor.Close(client.ctx)
			cursor.All(client.ctx, &results)

			for _, index := range results {
				indexNames = append(indexNames, index["name"].(string))
			}

			accountedFor := []string{}
			for _, index := range indices[coll] {
				indexName := *index.Options.Name
				accountedFor = append(accountedFor, indexName)
				indexExists := slices.Contains(indexNames, indexName)

				// If reindex is enabled and index exists, drop it first so we can recreate with new definition
				if indexExists && reindex {
					if client.verboseMode {
						log.Log.Debugf(client.ctx, "reindex enabled, dropping existing index %s on collection %s.%s to recreate", indexName, database.String(), coll)
					}
					_, err = collection.Indexes().DropOne(client.ctx, indexName)
					if err != nil {
						log.Log.Warnf(client.ctx, "failed to drop index %s for reindex: %v", indexName, err)
					} else {
						indexExists = false
					}
				}

				// Create index if it doesn't exist (or was just dropped for reindex)
				if !indexExists {
					if client.verboseMode {
						log.Log.Debugf(client.ctx, "creating index %s on collection %s.%s", indexName, database.String(), coll)
					}
					_, err = collection.Indexes().CreateOne(client.ctx, index)
					if err != nil {
						log.Log.Errorf(client.ctx, "failed to create index %s on collection %s: %v", indexName, coll, err)
						return err
					}
				}
			}

			if reindex {
				unaccountedFor := utils.GetSliceDifference(indexNames, accountedFor)
				for _, index := range unaccountedFor {
					if index != "_id_" {
						if client.verboseMode {
							log.Log.Debugf(client.ctx, "reindex option on and remnant index %s found, dropping index on collection %s", index, coll)
						}
						collection.Indexes().DropOne(client.ctx, index)
					}
				}
			}
		}
	}

	return nil
}

func getDesiredIndices(m *map[Document][]mongo.IndexModel) map[string][]mongo.IndexModel {
	out := make(map[string][]mongo.IndexModel)
	for k, v := range *m {
		v = append(v, mongo.IndexModel{
			Keys: bson.D{
				{Key: "_id", Value: 1},
				{Key: k.TimestampField(), Value: -1},
			},
			Options: options.Index().SetName("_id_timestampField"),
		})
		out[k.CollectionName()] = v
	}
	return out
}

// updates an index (not search index) on a collection by dropping the old index and creating the new one
func (client *Client) UpdateIndex(
	database Database,
	collection string,
	oldIndexName string,
	newIndex mongo.IndexModel,
) error {
	coll := client.mongoClient.Database(database.String()).Collection(collection)

	// Drop the old index
	if oldIndexName != "" {
		if client.verboseMode {
			log.Log.Debugf(client.ctx, "dropping index %s on collection %s.%s",
				oldIndexName, database.String(), collection)
		}
		_, err := coll.Indexes().DropOne(client.ctx, oldIndexName)
		if err != nil {
			return fmt.Errorf("failed to drop index %s: %v", oldIndexName, err)
		}
	}

	// Create the new index
	if client.verboseMode {
		log.Log.Debugf(client.ctx, "creating new index %s on collection %s.%s",
			*newIndex.Options.Name, database.String(), collection)
	}
	_, err := coll.Indexes().CreateOne(client.ctx, newIndex)
	if err != nil {
		return fmt.Errorf("failed to create new index: %v", err)
	}

	return nil
}

func (c *Client) EnsureSearchIndicesExist(
	database Database,
	indicesSpecs ...map[Collection]map[string]*SearchIndexModel,
) error {
	availableCollections, err := c.ListCollectionNames(database, nil)
	if err != nil {
		return err
	}

	collections := []Collection{}
	indices := make(map[Collection]map[string]*SearchIndexModel)
	for _, spec := range indicesSpecs {
		for k, v := range spec {
			collections = append(collections, k)
			if _, exists := indices[k]; exists {
				return fmt.Errorf("duplicate indices specified for collection name: %s", k)
			}
			indices[k] = v
		}
	}

	collectionNames := []string{}
	for _, coll := range collections {
		collectionNames = append(collectionNames, coll.String())
	}

	for _, coll := range availableCollections {
		collection := c.mongoClient.Database(database.String()).Collection(coll)
		var cursor *mongo.Cursor
		var err error
		var results []bson.M
		var indexNames []string
		if slices.Contains(collectionNames, coll) {
			cursor, err = collection.SearchIndexes().List(c.ctx, nil)
			if err != nil {
				return err
			}
			defer cursor.Close(c.ctx)
			cursor.All(c.ctx, &results)

			for _, index := range results {
				indexNames = append(indexNames, index["name"].(string))
			}

			accountedFor := []string{}
			for _, index := range indices[Collection(coll)] {
				accountedFor = append(accountedFor, *index.SearchIndex.Options.Name)
				if !slices.Contains(indexNames, *index.SearchIndex.Options.Name) {
					if c.verboseMode {
						log.Log.Debugf(c.ctx, "index %s not found, creating index on collection %s.%s", *index.SearchIndex.Options.Name, database.String(), coll)
					}
					_, err = collection.SearchIndexes().CreateOne(c.ctx, index.SearchIndex)

					if err != nil {
						if index.Reindex && strings.Contains(err.Error(), "already exists") {
							if c.verboseMode {
								log.Log.Debugf(c.ctx, "client reindexing enabled, dropping search index %s on collection %s and recreating", *index.SearchIndex.Options.Name, coll)
							}
							re := regexp.MustCompile(`name:\s*(\S+)`)
							match := re.FindStringSubmatch(err.Error())
							if len(match) > 1 {
								c.UpdateSearchIndex(database, Collection(coll), index)
							}
						} else {
							return err
						}
					}
				} else {
					if index.Reindex {
						c.UpdateSearchIndex(database, Collection(coll), index)
					}
				}
			}

			unaccountedFor := utils.GetSliceDifference(indexNames, accountedFor)
			for _, index := range unaccountedFor {
				if c.verboseMode {
					log.Log.Debugf(c.ctx, "remnant index %s found, dropping search index on collection %s", index, coll)
				}
				collection.SearchIndexes().DropOne(c.ctx, index)
			}
		}
	}

	return nil
}

func (c *Client) UpdateSearchIndex(
	database Database,
	collection Collection,
	index *SearchIndexModel,
) error {
	updateCmd := bson.D{
		{Key: "updateSearchIndex", Value: collection.String()},
		{Key: "name", Value: *index.SearchIndex.Options.Name},
		{Key: "definition", Value: index.SearchIndex.Definition},
	}

	result := c.mongoClient.Database(database.String()).RunCommand(c.ctx, updateCmd)
	if result.Err() != nil {
		return fmt.Errorf("failed to update search index: %v", result.Err())
	}

	return nil
}
