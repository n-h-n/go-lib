package iam

import (
	"fmt"
	"strings"

	"github.com/n-h-n/go-lib/env"
)

var (
	GCPProjects = struct {
		EdsciProdOpsProjectID  string
		EdsciProdOpsProjectNum string
	}{
		EdsciProdOpsProjectID:  "edsci-prod-ops",
		EdsciProdOpsProjectNum: "1040772709881",
	}
)

// Constants that are just the environment variable names, in case of reuse
const (
	ACTIVE_ENV            string = "ACTIVE_ENV"
	AWS_ACCESS_KEY_ID     string = "AWS_ACCESS_KEY_ID"
	AWS_REGION            string = "AWS_REGION"
	AWS_ROLE_ARN          string = "AWS_ROLE_ARN"
	AWS_SECRET_ACCESS_KEY string = "AWS_SECRET_ACCESS_KEY"
	AWS_SESSION_TOKEN     string = "AWS_SESSION_TOKEN"
	SERVICE_NAME          string = "SERVICE_NAME"
	GCP_PROJECT_ID        string = "GCP_PROJECT_ID"
	GCP_PROJECT_NUMBER    string = "GCP_PROJECT_NUMBER"
)

type Option = func(*IdentityFederationClient) error

// Specifies the desired duration for all AWS and GCP tokens. It is recommended
// that you keep this value somewhere in the range of: 900 <= seconds <= 3600.
// Values outside of this range may result in error messages returned by AWS
// and GCP servers.
func WithTokenDuration(seconds int64) Option {
	return func(c *IdentityFederationClient) error {
		c.opt.tokenDurationSeconds = seconds
		return nil
	}
}

// Specifies the Environment "env"
func WithEnvironment(env env.Environment) Option {
	return func(c *IdentityFederationClient) error {
		c.env = strings.ToLower(string(env))
		return nil
	}
}

// Refreshes all tokens for AWS assumed role, AWS session, GCP federation,
// and GCP service account on-demand, meaning they will only be checked
// and refreshed if needed at call time with the IdentityFederationClient.GetTokenSource() call.
// refreshPercentage is a fractional multiplier on c.opt.tokenDurationSeconds to let
// the on-demand refresh logic know if it should refresh or not.
// Cannot be specified alongside WithPeriodicAuthRefresh().
func WithOnDemandAuthRefresh(refreshPercentage float64) Option {
	return func(c *IdentityFederationClient) error {
		if refreshPercentage < float64(0.20) || refreshPercentage > float64(0.50) {
			return fmt.Errorf(`you must specify a percentageTimeRemaining between 0.20 and 0.50 to give
			 the refreshing ample buffer room and to not get rate limited with token exchange reqests
			 `)
		}
		if c.opt.flagRefreshAlreadySet {
			return fmt.Errorf("cannot set on-demand auth refresh; auth refresh preference already set")
		}
		c.opt.periodicAuthRefresh = false
		c.opt.refreshPercentage = refreshPercentage
		c.opt.flagRefreshAlreadySet = true
		return nil
	}
}

// Refreshes all tokens for AWS assumed role, AWS session, GCP federation,
// and GCP service account periodically with a goroutine. Cannot be specified alongside
// WithOnDemandAuthRefresh(). refreshPercentage is a fractional multiplier on
// c.opt.tokenDurationSeconds to let the periodic refresh checks know if it should refresh or not.
func WithPeriodicAuthRefresh(refreshPercentage float64) Option {
	return func(c *IdentityFederationClient) error {
		if refreshPercentage < float64(0.20) || refreshPercentage > float64(0.50) {
			return fmt.Errorf(`you must specify a percentageTimeRemaining between 0.20 and 0.50 to give
			 the refreshing ample buffer room and to not get rate limited with token exchange reqests
			 `)
		}
		if c.opt.flagRefreshAlreadySet {
			return fmt.Errorf("cannot set periodic auth refresh; auth refresh preference already set")
		}
		c.opt.refreshPercentage = refreshPercentage
		c.opt.periodicAuthRefresh = true
		c.opt.flagRefreshAlreadySet = true
		return nil
	}
}

// Logs more information during calls. Only use when debugging; do not run in stable
// deployment as you'll add unnecessary info logs to Datadog.
func WithVerbose(v bool) Option {
	return func(c *IdentityFederationClient) error {
		c.opt.verbose = v
		return nil
	}
}

// Enables a custom specification for the GCP service account name. GCP enforces a
// 6-character minimum and 30 character maximum for service account names, and so for some
// services, this may be necessary.
func WithCustomGCPServiceAccountName(name string) Option {
	return func(c *IdentityFederationClient) error {
		c.gcp.serviceAccountName = name
		return nil
	}
}

// Specify the custom project ID and number to override with.
func WithCustomGCPProject(id, number string) Option {
	return func(c *IdentityFederationClient) error {
		c.gcp.projectID = id
		c.gcp.projectNumber = number
		return nil
	}
}

// Specify the custom AWS session name to override with
func WithCustomAWSSessionName(name string) Option {
	return func(c *IdentityFederationClient) error {
		c.aws.sessionName = name
		return nil
	}
}

// Set ADC
func WithSetADC(v bool) Option {
	return func(c *IdentityFederationClient) error {
		c.opt.setADC = true
		return nil
	}
}

// Set added scopes for access token
func WithAddedGCPScopes(scopes []string) Option {
	return func(c *IdentityFederationClient) error {
		c.gcp.addedScopes = scopes
		return nil
	}
}
