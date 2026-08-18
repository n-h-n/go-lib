package rds_postgres

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/rds/auth"
	"github.com/lib/pq"

	"github.com/n-h-n/go-lib/aws/iam"
	"github.com/n-h-n/go-lib/log"
)

type Client struct {
	ctx             context.Context
	dbClient        *sql.DB
	dbName          string
	hostURI         string // Canonical RDS hostname (IAM auth token signing)
	dialHost        string // Optional TCP dial host override (e.g. 127.0.0.1 for SSM tunnel)
	dialPort        int    // Optional TCP dial port override (e.g. local forward port)
	iamClient       iam.IAMClient
	password        string // Used for password-based authentication (e.g., GCP Cloud SQL)
	port            int    // Canonical RDS port (IAM auth token signing)
	region          string
	sslCertFilePath string
	sslMode         string
	user            string
	verboseMode     bool
	mu              sync.Mutex // mintAuthToken vs STS refresh
}

func NewClient(
	ctx context.Context,
	hostURI string,
	opts ...ClientOpt,
) (*Client, error) {
	c := &Client{
		ctx:         ctx,
		hostURI:     hostURI,
		port:        5432,
		verboseMode: false,
		sslMode:     "disable",
	}

	for _, opt := range opts {
		if err := opt(c); err != nil {
			return nil, err
		}
	}

	if c.password != "" {
		if c.user == "" {
			return nil, fmt.Errorf("user is required when using password authentication")
		}
		if c.dbName == "" {
			c.dbName = c.user
		}
	} else {
		if c.iamClient == nil {
			// STS session matches RDS IAM token lifetime (15 minutes).
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
	}

	if c.sslMode != "disable" && c.sslCertFilePath == "" && c.region != "" {
		if c.verboseMode {
			log.Log.Debugf(ctx, "sslMode set to %s but no cert filepath specified; downloading SSL root cert from AWS....", c.sslMode)
		}
		certFilePath, err := downloadSSLRootCert(c.region)
		if err != nil {
			return nil, err
		}
		c.sslCertFilePath = certFilePath
	}

	if c.verboseMode {
		dialHost, dialPort := c.dialTarget()
		log.Log.Debugf(ctx, "connecting to DB: dial=%s:%d (canonical=%s:%d), db=%s, sslmode=%s", dialHost, dialPort, c.hostURI, c.port, c.dbName, c.sslMode)
	}

	var db *sql.DB
	if c.password != "" {
		opened, err := sql.Open("postgres", c.dsnWithSSL(c.password))
		if err != nil {
			return nil, err
		}
		db = opened
	} else {
		// Mint a fresh IAM token on each new physical connection. Do not replace
		// the *sql.DB — swapping the pool and closing the old one aborts in-flight
		// COPY/transactions with "sql: database is closed".
		db = sql.OpenDB(&iamTokenConnector{c: c})
	}

	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}

	c.dbClient = db
	if c.verboseMode {
		log.Log.Debugf(ctx, "successfully connected to DB")
	}

	if c.password == "" {
		go c.runPeriodicRefresh()
	}

	return c, nil
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
	if c.password != "" || c.iamClient == nil {
		return nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.iamClient.GetSessionTimeRemaining().Seconds() > c.iamClient.GetSessionDuration().Seconds()*c.iamClient.GetRefreshPercentage() {
		return nil
	}
	if c.verboseMode {
		log.Log.Debugf(c.ctx, "refreshing AWS credentials for RDS IAM (sql pool unchanged)")
	}
	return c.iamClient.RefreshAWSCreds(c.ctx)
}

func (c *Client) mintAuthToken(ctx context.Context) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.iamClient.GetSessionTimeRemaining() < 2*time.Minute {
		if err := c.iamClient.RefreshAWSCreds(ctx); err != nil {
			return "", err
		}
	}
	stsCreds := c.iamClient.GetAssumedRole().Credentials
	awsCredsProvider := credentials.NewStaticCredentialsProvider(
		*stsCreds.AccessKeyId,
		*stsCreds.SecretAccessKey,
		*stsCreds.SessionToken,
	)
	return auth.BuildAuthToken(
		ctx,
		fmt.Sprintf("%s:%d", c.hostURI, c.port),
		c.region,
		c.user,
		awsCredsProvider,
	)
}

func (c *Client) dsnWithSSL(password string) string {
	dsn := c.buildDSN(password)
	if c.sslMode != "disable" && c.sslCertFilePath != "" {
		dsn += fmt.Sprintf(" sslrootcert=%s", c.sslCertFilePath)
	}
	return dsn
}

// iamTokenConnector opens each new physical Postgres connection with a freshly
// minted RDS IAM token. Authenticated sessions stay valid after the token
// expires; only new dials need a new token.
type iamTokenConnector struct {
	c *Client
}

func (ic *iamTokenConnector) Connect(ctx context.Context) (driver.Conn, error) {
	token, err := ic.c.mintAuthToken(ctx)
	if err != nil {
		return nil, err
	}
	inner, err := pq.NewConnector(ic.c.dsnWithSSL(token))
	if err != nil {
		return nil, err
	}
	return inner.Connect(ctx)
}

func (ic *iamTokenConnector) Driver() driver.Driver {
	return pq.Driver{}
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

func (c *Client) dialTarget() (host string, port int) {
	host = c.hostURI
	port = c.port
	if c.dialHost != "" {
		host = c.dialHost
	}
	if c.dialPort > 0 {
		port = c.dialPort
	}
	return host, port
}

// buildDSN uses dialHost/dialPort for TCP when set (SSM tunnel), while IAM
// tokens are always signed against the canonical hostURI:port.
func (c *Client) buildDSN(password string) string {
	host, port := c.dialTarget()
	return fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s", host, port, c.user, password, c.dbName, c.sslMode)
}

func (c *Client) Close() error {
	return c.dbClient.Close()
}

// DB returns the underlying *sql.DB for callers that need SELECT/custom SQL.
// Prefer UpsertRows/DeleteRows/AlignTableSchema for schema-aligned writes.
func (c *Client) DB() *sql.DB {
	if c == nil {
		return nil
	}
	return c.dbClient
}

// ExecContext runs a statement on the underlying DB.
func (c *Client) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return c.dbClient.ExecContext(ctx, query, args...)
}

// QueryContext runs a query on the underlying DB.
func (c *Client) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return c.dbClient.QueryContext(ctx, query, args...)
}

// QueryRowContext runs a query that returns at most one row.
func (c *Client) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	return c.dbClient.QueryRowContext(ctx, query, args...)
}
