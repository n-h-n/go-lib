package bigquery

import (
	"fmt"

	"github.com/n-h-n/go-lib/gcp/iam"
)

type clientOpt func(*Client) error

func WithProjectID(projectID string) clientOpt {
	return func(c *Client) error {
		c.projectID = projectID
		return nil
	}
}

func WithDatasetID(datasetID string) clientOpt {
	return func(c *Client) error {
		c.datasetID = datasetID
		return nil
	}
}

func WithLocation(location string) clientOpt {
	return func(c *Client) error {
		c.location = location
		return nil
	}
}

func WithVerbose(v bool) clientOpt {
	return func(c *Client) error {
		c.verboseMode = v
		return nil
	}
}

func WithJobTimeout(timeout int) clientOpt {
	return func(c *Client) error {
		c.jobTimeout = timeout
		return nil
	}
}

func WithMaxRetries(maxRetries int) clientOpt {
	return func(c *Client) error {
		c.maxRetries = maxRetries
		return nil
	}
}

// WithRetryConfig provides convenient retry configuration options
func WithRetryConfig(maxRetries int) clientOpt {
	return func(c *Client) error {
		if maxRetries < 0 {
			return fmt.Errorf("maxRetries must be non-negative, got %d", maxRetries)
		}
		c.maxRetries = maxRetries
		return nil
	}
}

func WithUseLegacySQL(useLegacySQL bool) clientOpt {
	return func(c *Client) error {
		c.useLegacySQL = useLegacySQL
		return nil
	}
}

func WithDryRun(dryRun bool) clientOpt {
	return func(c *Client) error {
		c.dryRun = dryRun
		return nil
	}
}

func WithCreateDisposition(disposition string) clientOpt {
	return func(c *Client) error {
		c.createDisposition = disposition
		return nil
	}
}

func WithWriteDisposition(disposition string) clientOpt {
	return func(c *Client) error {
		c.writeDisposition = disposition
		return nil
	}
}

func WithIAMClient(iamClient *iam.IdentityFederationClient) clientOpt {
	return func(c *Client) error {
		c.iamClient = iamClient
		return nil
	}
}

func WithAutoIAMFederation(gcpProjectID, gcpProjectNum string, opts ...iam.Option) clientOpt {
	return func(c *Client) error {
		// Create IAM federation client with provided options
		iamClient, err := iam.NewIdentityFederationClient(c.ctx, gcpProjectID, gcpProjectNum, opts...)
		if err != nil {
			return fmt.Errorf("failed to create IAM federation client: %w", err)
		}
		c.iamClient = iamClient
		return nil
	}
}

func WithCredentialsJSON(credentialsJSON string) clientOpt {
	return func(c *Client) error {
		c.credentialsJSON = credentialsJSON
		return nil
	}
}
