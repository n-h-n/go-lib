package rds_postgres

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/rds/auth"
	_ "github.com/lib/pq"

	"github.com/n-h-n/go-lib/aws/iam"
	"github.com/n-h-n/go-lib/log"
)

type Client struct {
	ctx             context.Context
	dbClient        *sql.DB
	dbName          string
	hostURI         string
	iamClient       iam.IAMClient
	port            int
	region          string
	sslCertFilePath string
	sslMode         string
	user            string
	verboseMode     bool
}

func NewClient(
	ctx context.Context,
	hostURI string,
	opts ...clientOpt,
) (*Client, error) {
	c := Client{
		ctx:         ctx,
		hostURI:     hostURI,
		port:        5432,
		verboseMode: false,
		sslMode:     "disable",
	}

	for _, opt := range opts {
		err := opt(&c)
		if err != nil {
			return nil, err
		}
	}

	if c.iamClient == nil {
		// RDS auth tokens are limited to 15 minutes so set session to 15 minutes
		iamClient, err := iam.NewIAMClient(ctx, iam.WithVerboseMode(c.verboseMode), iam.WithSessionDuration(15*time.Minute))
		if err != nil {
			return nil, err
		}
		c.iamClient = iamClient
	}

	if c.user == "" {
		c.user = c.iamClient.GetServiceName()
	}

	if c.region == "" {
		c.region = c.iamClient.GetAWSRegion()
	}

	if c.dbName == "" {
		c.dbName = c.user
	}

	stsCreds := c.iamClient.GetAssumedRole().Credentials
	awsCredsProvider := credentials.NewStaticCredentialsProvider(
		*stsCreds.AccessKeyId,
		*stsCreds.SecretAccessKey,
		*stsCreds.SessionToken,
	)
	authToken, err := auth.BuildAuthToken(
		ctx,
		fmt.Sprintf("%s:%d", c.hostURI, c.port),
		c.region,
		c.user,
		awsCredsProvider,
	)
	if err != nil {
		return nil, err
	}

	dsn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s", c.hostURI, c.port, c.user, authToken, c.dbName, c.sslMode)

	if c.sslMode != "disable" {
		if c.sslCertFilePath == "" {
			if c.verboseMode {
				log.Log.Debugf(ctx, "sslMode set to %s but no cert filepath specified; downloading SSL root cert from AWS....", c.sslMode)
			}
			// download the cert from AWS
			certFilePath, err := downloadSSLRootCert(c.region)
			if err != nil {
				return nil, err
			}
			c.sslCertFilePath = certFilePath
		}

		dsn += fmt.Sprintf(" sslrootcert=%s", c.sslCertFilePath)
	}

	if c.verboseMode {
		log.Log.Debugf(ctx, "connecting to RDS DB: %s:%d, db=%s, with sslmode=%s and sslCertFilePath=%s", c.hostURI, c.port, c.dbName, c.sslMode, c.sslCertFilePath)
	}

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}

	err = db.Ping()
	if err != nil {
		return nil, err
	}

	c.dbClient = db
	if c.verboseMode {
		log.Log.Debugf(ctx, "successfully connected to RDS DB")
	}

	go c.runPeriodicRefresh()

	return &c, nil
}

func downloadSSLRootCert(region string) (string, error) {
	currentDir, err := os.Getwd()
	if err != nil {
		return "", err
	}

	filepath := fmt.Sprintf("%s/%s-bundle.pem", currentDir, region)

	// Create the file
	out, err := os.Create(filepath)
	if err != nil {
		return "", err
	}
	defer out.Close()

	url := fmt.Sprintf("https://truststore.pki.rds.amazonaws.com/%s/%s-bundle.pem", region, region)

	// Get the data
	resp, err := http.Get(url)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	// Write the body to file
	_, err = io.Copy(out, resp.Body)
	if err != nil {
		return "", err
	}

	return filepath, nil
}

func (c *Client) refreshDBClient() error {
	if c.iamClient.GetSessionTimeRemaining().Seconds() > c.iamClient.GetSessionDuration().Seconds()*c.iamClient.GetRefreshPercentage() {
		// Greater than the refreshAtPercentageRemaining of the session duration remaining, no need to refresh
		if c.verboseMode {
			log.Log.Debugf(
				c.ctx,
				"skipping RDS DB connection refresh, session token time remaining: %v, time remaining until refresh: %v",
				c.iamClient.GetSessionTimeRemaining(),
				c.iamClient.GetSessionTimeRemaining()-time.Duration(c.iamClient.GetSessionDuration().Seconds()*c.iamClient.GetRefreshPercentage())*time.Second,
			)
		}
		return nil
	}

	if c.verboseMode {
		log.Log.Debugf(c.ctx, "refreshing RDS DB connection")
	}

	if err := c.iamClient.RefreshAWSCreds(c.ctx); err != nil {
		return err
	}

	stsCreds := c.iamClient.GetAssumedRole().Credentials
	awsCredsProvider := credentials.NewStaticCredentialsProvider(
		*stsCreds.AccessKeyId,
		*stsCreds.SecretAccessKey,
		*stsCreds.SessionToken,
	)

	authToken, err := auth.BuildAuthToken(
		c.ctx,
		fmt.Sprintf("%s:%d", c.hostURI, c.port),
		c.region,
		c.user,
		awsCredsProvider,
	)
	if err != nil {
		return err
	}

	dsn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s", c.hostURI, c.port, c.user, authToken, c.dbName, c.sslMode)

	if c.sslMode != "disable" {
		dsn += fmt.Sprintf(" sslrootcert=%s", c.sslCertFilePath)
	}

	newDB, err := sql.Open("postgres", dsn)
	if err != nil {
		return err
	}

	err = newDB.Ping()
	if err != nil {
		return err
	}

	oldDB := c.dbClient
	c.dbClient = newDB

	if oldDB != nil {
		if c.verboseMode {
			log.Log.Debugf(c.ctx, "closing old RDS DB connection after 2 minutes")
		}
		time.AfterFunc(2*time.Minute, func() {
			if err := oldDB.Close(); err != nil {
				log.Log.Errorf(c.ctx, "failed to close old RDS DB connection: %v", err)
			}
		})
	}

	if c.verboseMode {
		log.Log.Debugf(c.ctx, "successfully refreshed RDS DB connection")
	}

	return nil
}

func (c *Client) runPeriodicRefresh() {
	minMilliseconds := 58000
	maxMilliSeconds := 62000
	interval := rand.Intn(maxMilliSeconds-minMilliseconds) + minMilliseconds

	ticker := time.NewTicker(time.Duration(interval) * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := c.refreshDBClient(); err != nil {
				log.Log.Errorf(c.ctx, "failed to refresh RDS client: %v", err)
			}
		case <-c.ctx.Done():
			return
		}
	}
}

func (c *Client) Close() error {
	return c.dbClient.Close()
}
