package bigquery

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/n-h-n/go-lib/gcp/iam"
	"github.com/n-h-n/go-lib/log"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/option"
)

type Client struct {
	ctx               context.Context
	client            *bigquery.Client
	projectID         string
	datasetID         string
	location          string
	iamClient         *iam.IdentityFederationClient
	credentialsJSON   string
	verboseMode       bool
	jobTimeout        int
	maxRetries        int
	useLegacySQL      bool
	dryRun            bool
	createDisposition string
	writeDisposition  string
}

func NewClient(
	ctx context.Context,
	projectID string,
	opts ...clientOpt,
) (*Client, error) {
	c := Client{
		ctx:               ctx,
		projectID:         projectID,
		location:          "US",
		verboseMode:       false,
		jobTimeout:        300, // 5 minutes default
		maxRetries:        3,
		useLegacySQL:      false,
		dryRun:            false,
		createDisposition: "CREATE_IF_NEEDED",
		writeDisposition:  "WRITE_APPEND",
	}

	for _, opt := range opts {
		err := opt(&c)
		if err != nil {
			return nil, err
		}
	}

	if c.datasetID == "" {
		return nil, fmt.Errorf("dataset ID is required")
	}

	var clientOptions []option.ClientOption

	// Handle authentication
	if c.credentialsJSON != "" {
		// Use credentials JSON
		creds, err := google.CredentialsFromJSON(ctx, []byte(c.credentialsJSON), bigquery.Scope)
		if err != nil {
			return nil, fmt.Errorf("failed to create credentials from JSON: %w", err)
		}
		clientOptions = append(clientOptions, option.WithCredentials(creds))
	} else if c.iamClient != nil {
		// Use IAM federation client
		tokenSource, err := c.iamClient.GetTokenSource(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to get token source: %w", err)
		}
		clientOptions = append(clientOptions, option.WithTokenSource(tokenSource))
	} else {
		// Use default credentials (Application Default Credentials)
		if c.verboseMode {
			log.Log.Debugf(ctx, "using default credentials for BigQuery authentication")
		}
	}

	// Create BigQuery client
	client, err := bigquery.NewClient(ctx, c.projectID, clientOptions...)
	if err != nil {
		return nil, fmt.Errorf("failed to create BigQuery client: %w", err)
	}

	c.client = client

	if c.verboseMode {
		log.Log.Debugf(ctx, "successfully connected to BigQuery project: %s, dataset: %s", c.projectID, c.datasetID)
	}

	return &c, nil
}

// GetClient returns the underlying BigQuery client
func (c *Client) GetClient() *bigquery.Client {
	return c.client
}

// GetProjectID returns the project ID
func (c *Client) GetProjectID() string {
	return c.projectID
}

// GetDatasetID returns the dataset ID
func (c *Client) GetDatasetID() string {
	return c.datasetID
}

// GetLocation returns the location
func (c *Client) GetLocation() string {
	return c.location
}

// IsVerbose returns whether verbose mode is enabled
func (c *Client) IsVerbose() bool {
	return c.verboseMode
}

// SetDatasetID sets the dataset ID
func (c *Client) SetDatasetID(datasetID string) {
	c.datasetID = datasetID
}

// SetLocation sets the location
func (c *Client) SetLocation(location string) {
	c.location = location
}

// RefreshAuth refreshes the authentication token
func (c *Client) RefreshAuth(ctx context.Context) error {
	if c.verboseMode {
		log.Log.Debugf(ctx, "refreshing BigQuery authentication")
	}

	var clientOptions []option.ClientOption

	// Handle authentication refresh
	if c.credentialsJSON != "" {
		// Credentials JSON doesn't need refresh
		if c.verboseMode {
			log.Log.Debugf(ctx, "credentials JSON authentication doesn't require refresh")
		}
		return nil
	} else if c.iamClient != nil {
		// Get fresh token source from IAM federation client
		tokenSource, err := c.iamClient.GetTokenSource(ctx)
		if err != nil {
			return fmt.Errorf("failed to refresh token source: %w", err)
		}
		clientOptions = append(clientOptions, option.WithTokenSource(tokenSource))
	} else {
		// Use default credentials (Application Default Credentials)
		if c.verboseMode {
			log.Log.Debugf(ctx, "using default credentials for BigQuery authentication refresh")
		}
	}

	// Create new client with fresh authentication
	client, err := bigquery.NewClient(ctx, c.projectID, clientOptions...)
	if err != nil {
		return fmt.Errorf("failed to refresh BigQuery client: %w", err)
	}

	// Close old client
	if c.client != nil {
		c.client.Close()
	}

	c.client = client

	if c.verboseMode {
		log.Log.Debugf(ctx, "successfully refreshed BigQuery authentication")
	}

	return nil
}

// Close closes the BigQuery client
func (c *Client) Close() error {
	if c.client != nil {
		return c.client.Close()
	}
	return nil
}

// GetTableReference returns a table reference for the given table name
func (c *Client) GetTableReference(tableName string) *bigquery.Table {
	return c.client.Dataset(c.datasetID).Table(tableName)
}

// GetDatasetReference returns a dataset reference
func (c *Client) GetDatasetReference() *bigquery.Dataset {
	return c.client.Dataset(c.datasetID)
}

// WaitForJob waits for a BigQuery job to complete
func (c *Client) WaitForJob(ctx context.Context, job *bigquery.Job) error {
	if c.verboseMode {
		log.Log.Debugf(ctx, "waiting for job %s to complete", job.ID())
	}

	// Set timeout if specified
	if c.jobTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(c.jobTimeout)*time.Second)
		defer cancel()
	}

	status, err := job.Wait(ctx)
	if err != nil {
		return fmt.Errorf("job failed: %w", err)
	}

	if status.Err() != nil {
		return fmt.Errorf("job completed with error: %w", status.Err())
	}

	if c.verboseMode {
		log.Log.Debugf(ctx, "job %s completed successfully", job.ID())
	}

	return nil
}

// ExecuteQuery executes a BigQuery query and returns the iterator
func (c *Client) ExecuteQuery(ctx context.Context, query string) (*bigquery.RowIterator, error) {
	if c.verboseMode {
		log.Log.Debugf(ctx, "executing query: %s", query)
	}

	q := c.client.Query(query)
	q.UseLegacySQL = c.useLegacySQL
	q.DryRun = c.dryRun

	job, err := q.Run(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to run query: %w", err)
	}

	if !c.dryRun {
		err = c.WaitForJob(ctx, job)
		if err != nil {
			return nil, err
		}

		it, err := job.Read(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to read query results: %w", err)
		}

		return it, nil
	}

	return nil, nil
}

// ExecuteDML executes a DML (Data Manipulation Language) query
func (c *Client) ExecuteDML(ctx context.Context, query string) error {
	if c.verboseMode {
		log.Log.Debugf(ctx, "executing DML query: %s", query)
	}

	q := c.client.Query(query)
	q.UseLegacySQL = c.useLegacySQL
	q.DryRun = c.dryRun

	job, err := q.Run(ctx)
	if err != nil {
		return fmt.Errorf("failed to run DML query: %w", err)
	}

	if !c.dryRun {
		err = c.WaitForJob(ctx, job)
		if err != nil {
			return err
		}
	}

	if c.verboseMode {
		log.Log.Debugf(ctx, "DML query executed successfully")
	}

	return nil
}

// GetJob retrieves a BigQuery job by ID
func (c *Client) GetJob(ctx context.Context, jobID string) (*bigquery.Job, error) {
	job, err := c.client.JobFromID(ctx, jobID)
	if err != nil {
		return nil, fmt.Errorf("failed to get job: %w", err)
	}

	// Get job status
	status, err := job.Status(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get job status: %w", err)
	}

	if c.verboseMode {
		log.Log.Debugf(ctx, "retrieved job %s with state: %s", jobID, status.State)
	}

	return job, nil
}

// ListJobs lists BigQuery jobs
func (c *Client) ListJobs(ctx context.Context, maxResults int) ([]*bigquery.Job, error) {
	if maxResults <= 0 {
		maxResults = 100
	}

	it := c.client.Jobs(ctx)
	it.State = bigquery.Done

	var jobs []*bigquery.Job
	count := 0
	for {
		if count >= maxResults {
			break
		}
		job, err := it.Next()
		if err != nil {
			return nil, fmt.Errorf("failed to list jobs: %w", err)
		}
		if job == nil {
			break
		}
		jobs = append(jobs, job)
		count++
	}

	if c.verboseMode {
		log.Log.Debugf(ctx, "listed %d jobs", len(jobs))
	}

	return jobs, nil
}
