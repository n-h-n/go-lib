package secretsmanager

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
)

// SecretFetcher defines the interface for fetching secrets
type SecretFetcher interface {
	FetchSecret(ctx context.Context, secretKey string) ([]byte, error)
}

// AWSSecretFetcher implements SecretFetcher using AWS Secrets Manager
type AWSSecretFetcher struct{}

// FetchSecret retrieves a secret from AWS Secrets Manager
func (f *AWSSecretFetcher) FetchSecret(ctx context.Context, secretKey string) ([]byte, error) {
	// Load AWS configuration
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	// Create Secrets Manager client
	client := secretsmanager.NewFromConfig(cfg)

	// Fetch the secret from AWS Secrets Manager
	input := &secretsmanager.GetSecretValueInput{
		SecretId: aws.String(secretKey),
	}

	result, err := client.GetSecretValue(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("failed to get secret %s from AWS Secrets Manager: %w", secretKey, err)
	}

	// Check if the secret has a value
	if result.SecretString == nil {
		return nil, fmt.Errorf("secret %s has no value", secretKey)
	}

	return []byte(*result.SecretString), nil
}

// FetchSecretsWithFetcher is a generic function that can unmarshal secrets into any struct type
func FetchSecretsWithFetcher[T any](ctx context.Context, secretKey string, fetcher SecretFetcher) (*T, error) {
	// Fetch the secret data
	secretData, err := fetcher.FetchSecret(ctx, secretKey)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch secret %s: %w", secretKey, err)
	}

	// Unmarshal the JSON secret into the target type
	var result T
	if err := json.Unmarshal(secretData, &result); err != nil {
		return nil, fmt.Errorf("failed to unmarshal secrets for key %s: %w", secretKey, err)
	}

	return &result, nil
}
