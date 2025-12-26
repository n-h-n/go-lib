package iam

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sts"

	"github.com/n-h-n/go-lib/log"
)

const (
	awsSessionDuration           = 3600 * time.Second // 3600s is max due to role chaining limits
	refreshAtPercentageRemaining = 0.20
	EmptyBodySHA256              = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855" // the hex encoded SHA-256 of an empty string: https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html
)

type iamClient struct {
	assumedRole         *sts.AssumeRoleOutput
	awsConfig           *aws.Config
	awsEnvConfig        *config.EnvConfig
	originalCredentials aws.Credentials
	refreshPercentage   float64
	serviceName         string
	sessionDuration     time.Duration
	sessionName         string
	verboseMode         bool
	tokenUpdateTime     time.Time
}

type IAMClient interface {
	GetAssumedRole() *sts.AssumeRoleOutput
	GetAWSConfig() *aws.Config
	GetOriginalCredentials() aws.Credentials
	GetAWSRegion() string
	GetRefreshPercentage() float64
	GetRoleARN() string
	GetServiceName() string
	GetSessionDuration() time.Duration
	GetSessionTimeRemaining() time.Duration
	GetTokenUpdateTime() time.Time
	RefreshAWSCreds(context.Context, ...func(*RefreshOpts)) error
}

func NewIAMClient(
	ctx context.Context,
	clientOptions ...iamClientOpt,
) (*iamClient, error) {
	c := &iamClient{
		refreshPercentage: refreshAtPercentageRemaining,
		sessionDuration:   awsSessionDuration,
	}

	// options
	for _, option := range clientOptions {
		err := option(c)
		if err != nil {
			return nil, err
		}
	}

	if err := c.loadDefaultConfig(ctx); err != nil {
		return nil, err
	}

	// Store original credentials before assuming role (needed for ElastiCache IAM auth)
	originalCreds, err := c.awsConfig.Credentials.Retrieve(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve original AWS credentials: %w", err)
	}
	c.originalCredentials = originalCreds

	if err := c.assumeRole(ctx); err != nil {
		return nil, err
	}

	if c.sessionDuration.Seconds() > 3600 || c.sessionDuration.Seconds() < 900 {
		return nil, fmt.Errorf("session duration must be between 900 and 3600 seconds")
	}

	if c.refreshPercentage <= 0.0 || c.refreshPercentage > 1.0 {
		return nil, fmt.Errorf("refresh percentage must be between 0.0 and 1.0")
	}

	// take the trailing string of the role ARN as the service name
	// e.g. arn:aws:iam::123456789012:role/MyRoleName
	// servicename := MyRoleName
	if c.serviceName == "" {
		arnParts := strings.Split(c.awsEnvConfig.RoleARN, "/")
		c.serviceName = arnParts[len(arnParts)-1]
	}

	return c, nil
}

func (c *iamClient) loadDefaultConfig(ctx context.Context) error {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return err
	}
	c.awsConfig = &cfg

	awsEnvConfig, err := config.NewEnvConfig()
	if err != nil {
		return err
	}
	c.awsEnvConfig = &awsEnvConfig

	return nil
}

func (c *iamClient) assumeRole(ctx context.Context) error {
	stsService := sts.NewFromConfig(*c.awsConfig)

	if c.sessionName == "" {
		c.sessionName = strconv.FormatInt(time.Now().Unix(), 10)
	}
	// Assumed role for temporary credentials
	assumeRoleInput := &sts.AssumeRoleInput{
		RoleArn:         aws.String(c.awsEnvConfig.RoleARN),
		RoleSessionName: aws.String(c.sessionName),
		DurationSeconds: aws.Int32(int32(c.sessionDuration.Seconds())),
	}
	assumedRole, err := stsService.AssumeRole(ctx, assumeRoleInput)
	if err != nil {
		log.Log.Errorf(ctx, "failed to assume AWS role with input: %v", assumeRoleInput)
		return err
	}

	c.assumedRole = assumedRole
	c.tokenUpdateTime = time.Now()
	return nil
}

func (c *iamClient) runPeriodicCredsRefresh(ctx context.Context) {
	// Refresh check every minute, which is arbitrary but enough given the session duration and refresh percentage threshold
	ticker := time.NewTicker(45 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := c.RefreshAWSCreds(ctx); err != nil {
				log.Log.Errorf(ctx, "failed to refresh AWS IAM credentials: %s", err)
			}
		case <-ctx.Done():
			return
		}
	}
}

func (c *iamClient) RefreshAWSCreds(ctx context.Context, opts ...func(*RefreshOpts)) error {
	options := RefreshOpts{}

	for _, opt := range opts {
		opt(&options)
	}

	if !options.force {
		timeRemaining := time.Until(*c.assumedRole.Credentials.Expiration)
		if timeRemaining.Seconds() > c.sessionDuration.Seconds()*c.refreshPercentage {
			if c.verboseMode {
				log.Log.Debugf(ctx, "AWS credentials are still valid for %v seconds, skipping refresh; refreshing at threshold in %v",
					int(timeRemaining.Seconds()),
					timeRemaining-time.Duration(c.sessionDuration.Seconds()*c.refreshPercentage)*time.Second,
				)
			}
			return nil
		}
	}

	if c.verboseMode {
		log.Log.Debugf(ctx, "refreshing AWS IAM credentials....")
	}

	if err := c.loadDefaultConfig(ctx); err != nil {
		return err
	}

	// Refresh original credentials (pod identity credentials are managed by pod identity agent)
	// These are needed for ElastiCache IAM auth which requires credentials matching the ElastiCache user
	originalCreds, err := c.awsConfig.Credentials.Retrieve(ctx)
	if err != nil {
		return fmt.Errorf("failed to retrieve original AWS credentials: %w", err)
	}
	c.originalCredentials = originalCreds

	if err := c.assumeRole(ctx); err != nil {
		return err
	}

	if c.verboseMode {
		log.Log.Debugf(ctx, "AWS IAM credentials refreshed")
	}

	return nil
}

func (c *iamClient) GetAssumedRole() *sts.AssumeRoleOutput {
	return c.assumedRole
}

func (c *iamClient) GetAWSConfig() *aws.Config {
	return c.awsConfig
}

func (c *iamClient) GetOriginalCredentials() aws.Credentials {
	return c.originalCredentials
}

func (c *iamClient) GetAWSRegion() string {
	return c.awsEnvConfig.Region
}

func (c *iamClient) GetRefreshPercentage() float64 {
	return c.refreshPercentage
}

func (c *iamClient) GetRoleARN() string {
	return c.awsEnvConfig.RoleARN
}

func (c *iamClient) GetServiceName() string {
	return c.serviceName
}

func (c *iamClient) GetSessionDuration() time.Duration {
	return c.sessionDuration
}

func (c *iamClient) GetSessionTimeRemaining() time.Duration {
	return time.Until(*c.assumedRole.Credentials.Expiration)
}

func (c *iamClient) GetTokenUpdateTime() time.Time {
	return c.tokenUpdateTime
}
