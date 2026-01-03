package mongodb

import (
	"context"
	"math/rand"
	"os"
	"sync/atomic"
	"time"
	"unsafe"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
	"golang.org/x/exp/slices"

	"github.com/n-h-n/go-lib/aws/iam"
	"github.com/n-h-n/go-lib/log"
)

type Client struct {
	mongoClient        *mongo.Client
	ctx                context.Context
	iamClient          iam.IAMClient
	appName            string
	reindexCollections bool
	uri                string
	verboseMode        bool
}

// Credential stores the username and password needed to connect to MongoDB.
type Credential struct {
	Username string
	Password string
}

// NewClient creates a new Client.
func NewClient(
	ctx context.Context,
	uri string,
	clientOpts ...mongoClientOpt,
) (*Client, error) {

	c := &Client{
		ctx: ctx,
		uri: uri,
	}

	if c.appName == "" {
		c.appName = os.Getenv("HOSTNAME")
	}

	// Set the default Mongo client options.
	mongoClientOptions := createMongoClientOptions(ctx, uri, c.appName)

	// Variadic options
	for _, clientOpt := range clientOpts {
		err := clientOpt(c)
		if err != nil {
			return nil, err
		}
	}

	// Mongo IAM Mode
	if c.verboseMode {
		log.Log.Debugf(ctx, "using IAM mode for MongoDB client")
	}
	if c.iamClient == nil {
		defaultIAMClient, err := iam.NewIAMClient(ctx, iam.WithVerboseMode(c.verboseMode))
		if err != nil {
			return nil, err
		}
		c.iamClient = defaultIAMClient
	}

	mongoCredentials := c.getAssumedRoleMongoCredentials(ctx)
	mongoClientOptions.SetAuth(*mongoCredentials)
	mongoClient, err := connectMongoClient(ctx, c.verboseMode, mongoClientOptions)
	if err != nil {
		return nil, err
	}

	c.mongoClient = mongoClient

	go c.runPeriodicAuthRefresh()
	return c, nil
}

func checkDatabaseExistence(dbNames []string, dbName string) bool {
	for _, name := range dbNames {
		if name == dbName {
			return true
		}
	}
	return false
}

// Creates default set of mongo client options.
func createMongoClientOptions(
	ctx context.Context,
	uri string,
	appName string,
) *options.ClientOptions {
	mongoClientOptions := options.Client()

	// Apply the URI after the default options are set, to allow overriding them if specified already in the URI.
	mongoClientOptions.ApplyURI(uri).SetAppName(appName)

	// Set read and write concern to majority.
	// By default, these are set to nil. In that case, the value which is set on
	// server is honored. In MongoDB 5.0+ server, write concern is set to majority
	// and read concern is set to "local".
	mongoClientOptions.SetWriteConcern(writeconcern.Majority())
	mongoClientOptions.SetReadConcern(readconcern.Majority())

	return mongoClientOptions
}

// Connect to mongo with client options
func connectMongoClient(ctx context.Context, verbose bool, mongoClientOptions *options.ClientOptions) (*mongo.Client, error) {
	if verbose {
		log.Log.Debug(ctx, "connecting to MongoDB: ", mongoClientOptions.GetURI())
	}
	mongoClient, err := mongo.Connect(ctx, mongoClientOptions)
	if err != nil {
		return nil, err
	}

	if err = mongoClient.Ping(ctx, nil); err != nil {
		return nil, err
	}
	if verbose {
		log.Log.Debug(ctx, "successfully connected MongoDB")
	}

	return mongoClient, nil
}

func (client *Client) EnsureCollectionsExist(
	database Database,
	requiredCollections []Collection,
) error {
	availableCollections, err := client.ListCollectionNames(database, nil)
	if err != nil {
		return err
	}

	if client.verboseMode {
		log.Log.Debugf(client.ctx, "ensuring these collections exist in the %s database: %v", database, requiredCollections)
	}
	for _, coll := range requiredCollections {
		if !slices.Contains(availableCollections, coll.String()) {
			if client.verboseMode {
				log.Log.Debugf(client.ctx, "creating %s ....", coll)
			}
			err = client.CreateCollection(database.String(), coll.String())
			if err != nil {
				log.Log.Error(client.ctx, err.Error())
			}
		}
	}
	if client.verboseMode {
		log.Log.Debug(client.ctx, "collections initialized")
	}

	return nil
}

func (client *Client) InitDB(dbMap map[Database][]Collection) error {
	// Initiate Db Client
	// dbName Database, collections []Collection
	// check if the database exists
	dbNames, err := client.ListDatabaseNames(bson.M{})
	if err != nil {
		return err
	}

	for db, collections := range dbMap {
		if client.verboseMode {
			log.Log.Debug(client.ctx, "checking database against available databases")
		}
		if !checkDatabaseExistence(dbNames, db.String()) {
			log.Log.Warnf(client.ctx, "database %s does not exist and will be created", db.String())
		}

		err = client.EnsureCollectionsExist(db, collections)
		if err != nil {
			log.Log.Fatalf(client.ctx, "failed to ensure collections exist: %v", err.Error())
			return err
		}

		err = client.EnsureIndicesExist(
			db,
			client.reindexCollections,
			MongoCollectionIndices,
		)
		if err != nil {
			log.Log.Fatalf(client.ctx, "failed to ensure indices exist: %v", err.Error())
			return err
		}
	}

	if client.verboseMode {
		log.Log.Debug(client.ctx, "mongodb databases initialized")
	}

	return nil
}

func (c *Client) getAssumedRoleMongoCredentials(ctx context.Context) *options.Credential {
	assumedRole := c.iamClient.GetAssumedRole()
	return &options.Credential{
		AuthMechanism: "MONGODB-AWS",
		AuthMechanismProperties: map[string]string{
			"AWS_SESSION_TOKEN": *assumedRole.Credentials.SessionToken,
		},
		AuthSource: "$external",
		Username:   *assumedRole.Credentials.AccessKeyId,
		Password:   *assumedRole.Credentials.SecretAccessKey,
	}
}

func (c *Client) refreshMongoClient() error {
	if c.iamClient.GetSessionTimeRemaining().Seconds() > c.iamClient.GetSessionDuration().Seconds()*c.iamClient.GetRefreshPercentage() {
		// Greater than the refreshAtPercentageRemaining of the session duration remaining, no need to refresh
		if c.verboseMode {
			log.Log.Debugf(
				c.ctx,
				"skipping MongoDB connection refresh, session token time remaining: %v, time remaining until refresh: %v",
				c.iamClient.GetSessionTimeRemaining(),
				c.iamClient.GetSessionTimeRemaining()-time.Duration(c.iamClient.GetSessionDuration().Seconds()*c.iamClient.GetRefreshPercentage())*time.Second,
			)
		}
		return nil
	}

	if c.verboseMode {
		log.Log.Debugf(c.ctx, "refreshing MongoDB connection")
	}

	if err := c.iamClient.RefreshAWSCreds(c.ctx); err != nil {
		return err
	}

	mongoClientOptions := createMongoClientOptions(c.ctx, c.uri, c.appName)
	mongoCredentials := c.getAssumedRoleMongoCredentials(c.ctx)
	mongoClientOptions.SetAuth(*mongoCredentials)
	newMongoClient, err := connectMongoClient(c.ctx, c.verboseMode, mongoClientOptions)
	if err != nil {
		return err
	}

	ptrNewClient := unsafe.Pointer(newMongoClient)
	// Swap out the old client and gracefully disconnect with a time buffer
	oldClient := atomic.SwapPointer((*unsafe.Pointer)(unsafe.Pointer(&c.mongoClient)), ptrNewClient)

	go func() {
		if c.verboseMode {
			log.Log.Debugf(c.ctx, "closing old MongoDB client after 2 minutes")
		}
		time.Sleep(2 * time.Minute)
		if err := (*mongo.Client)(oldClient).Disconnect(c.ctx); err != nil {
			log.Log.Errorf(c.ctx, "failed to disconnect from MongoDB: %v", err)
		}
	}()

	if c.verboseMode {
		log.Log.Debugf(c.ctx, "successfully refreshed MongoDB connection")
	}

	return nil
}

func (c *Client) runPeriodicAuthRefresh() {
	// Refresh check every minute, which is arbitrary but enough given the session duration and refresh percentage threshold
	minMilliseconds := 58000
	maxMilliSeconds := 62000
	interval := rand.Intn(maxMilliSeconds-minMilliseconds) + minMilliseconds

	ticker := time.NewTicker(time.Duration(interval) * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := c.refreshMongoClient(); err != nil {
				log.Log.Errorf(c.ctx, "failed to refresh MongoDB connection and authenticate with IAM: %v", err)
			}
		case <-c.ctx.Done():
			return
		}
	}
}

func (c *Client) Close() error {
	return c.mongoClient.Disconnect(c.ctx)
}
