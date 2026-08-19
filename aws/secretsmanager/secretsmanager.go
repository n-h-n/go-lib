package secretsmanager

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awsSecretsmanager "github.com/aws/aws-sdk-go-v2/service/secretsmanager"

	"github.com/n-h-n/go-lib/aws/awslimit"
	"github.com/n-h-n/go-lib/aws/iam"
)

// SecretFetcher defines the interface for fetching secrets
type SecretFetcher interface {
	FetchSecret(ctx context.Context, secretKey string) ([]byte, error)
}

// AWSSecretFetcher implements SecretFetcher using AWS Secrets Manager
type AWSSecretFetcher struct{}

// FetchSecret retrieves a secret from AWS Secrets Manager.
//
// When AWS_ROLE_ARN is set (k3s: node IMDS → service role; EKS: often paired
// with a web-identity token), this assumes that role before calling
// GetSecretValue — matching elasticache/sqs/rds_postgres. LoadDefaultConfig
// alone does not assume AWS_ROLE_ARN without a web-identity token file, so
// omitting this step leaves k3s pods authenticated as the node instance
// profile, which is not granted secretsmanager:GetSecretValue.
func (f *AWSSecretFetcher) FetchSecret(ctx context.Context, secretKey string) ([]byte, error) {
	cfg, err := loadAWSConfig(ctx)
	if err != nil {
		return nil, err
	}

	client := awsSecretsmanager.NewFromConfig(cfg, func(o *awsSecretsmanager.Options) {
		o.APIOptions = append(o.APIOptions, awslimit.StackOption(awslimit.SecretsManager))
	})

	input := &awsSecretsmanager.GetSecretValueInput{
		SecretId: aws.String(secretKey),
	}

	result, err := client.GetSecretValue(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("failed to get secret %s from AWS Secrets Manager: %w", secretKey, err)
	}

	if result.SecretString == nil {
		return nil, fmt.Errorf("secret %s has no value", secretKey)
	}

	return []byte(*result.SecretString), nil
}

func loadAWSConfig(ctx context.Context) (aws.Config, error) {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return aws.Config{}, fmt.Errorf("failed to load AWS config: %w", err)
	}

	envCfg, err := config.NewEnvConfig()
	if err != nil {
		return aws.Config{}, fmt.Errorf("failed to load AWS env config: %w", err)
	}
	// IRSA: default chain already assumes via AWS_WEB_IDENTITY_TOKEN_FILE +
	// AWS_ROLE_ARN. Re-assuming would be role-chaining and usually fails.
	// Local/dev with only AWS_PROFILE: RoleARN is empty — use default creds.
	// k3s: RoleARN set, no web-identity token — IMDS is the node profile, so
	// assume the service role explicitly (same as elasticache/sqs).
	if envCfg.RoleARN == "" || envCfg.WebIdentityTokenFilePath != "" {
		return cfg, nil
	}

	iamClient, err := iam.NewIAMClient(ctx, iam.WithSessionDuration(1*time.Hour))
	if err != nil {
		return aws.Config{}, fmt.Errorf("failed to assume AWS_ROLE_ARN %s: %w", envCfg.RoleARN, err)
	}
	stsCreds := iamClient.GetAssumedRole().Credentials
	cfg.Credentials = credentials.NewStaticCredentialsProvider(
		*stsCreds.AccessKeyId,
		*stsCreds.SecretAccessKey,
		*stsCreds.SessionToken,
	)
	return cfg, nil
}

// FetchSecretsWithFetcher is a generic function that can unmarshal secrets into any struct type
func FetchSecretsWithFetcher[T any](ctx context.Context, secretKey string, fetcher SecretFetcher) (*T, error) {
	secretData, err := fetcher.FetchSecret(ctx, secretKey)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch secret %s: %w", secretKey, err)
	}

	var result T
	if err := json.Unmarshal(secretData, &result); err != nil {
		return nil, fmt.Errorf("failed to unmarshal secrets for key %s: %w", secretKey, err)
	}

	return &result, nil
}
