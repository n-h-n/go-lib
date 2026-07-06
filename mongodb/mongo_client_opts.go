package mongodb

import (
	"errors"

	"github.com/n-h-n/go-lib/aws/iam"
)

type mongoClientOpt func(*Client) error

// WithCustomIAMClient sets the mongo client to use a custom IAM client, allowing the client to pass
// in a custom instantation of the IAM client.
func WithCustomIAMClient(iamClient iam.IAMClient) mongoClientOpt {
	return func(c *Client) error {
		if iamClient == nil {
			return errors.New("iamClient in WithCustomIAMClient option cannot be nil")
		}
		if c.iamClient == nil {
			c.iamClient = iamClient
		}
		return nil
	}
}

// WithVerboseMode sets debug logging verbosity
func WithVerboseMode(v bool) mongoClientOpt {
	return func(c *Client) error {
		c.verboseMode = v
		return nil
	}
}

// WithReindexCollections sets the mongo client to reindex collections on startup
func WithReindexCollections(v bool) mongoClientOpt {
	return func(c *Client) error {
		c.reindexCollections = v
		return nil
	}
}

// WithSCRAMCredentials authenticates with a static username/password instead of AWS IAM.
// Used on Atlas tiers that do not support MONGODB-AWS auth (e.g. M0 dev clusters).
func WithSCRAMCredentials(username, password string) mongoClientOpt {
	return func(c *Client) error {
		if username == "" || password == "" {
			return errors.New("username and password in WithSCRAMCredentials cannot be empty")
		}
		c.useSCRAMAuth = true
		c.scramUsername = username
		c.scramPassword = password
		return nil
	}
}
