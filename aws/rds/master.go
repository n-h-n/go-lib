// Package rds provides small AWS RDS helpers used by services that bootstrap
// Postgres IAM users against RDS instances with managed master passwords.
package rds

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	awsrds "github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/aws/aws-sdk-go-v2/service/sts"

	"github.com/n-h-n/go-lib/aws/awslimit"
)

// MasterCredentials are the RDS master login details plus endpoint metadata.
type MasterCredentials struct {
	Host      string
	Port      int32
	DBName    string
	Username  string
	Password  string
	SecretARN string
}

// loadConfig resolves the identity these calls are made as.
//
// AWS_ROLE_ARN is the service's own role, and every other AWS call this library
// makes on behalf of a service is made as that role — rds_postgres builds its
// IAM auth token from it, so the database sees the service role. This resolved
// to a different principal, because LoadDefaultConfig only acts on AWS_ROLE_ARN
// when the web-identity token file is there to exchange.
//
// On EKS with IRSA that file exists, so the two agreed and nothing looked
// wrong. On plain EC2 or k3s it does not: AWS_ROLE_ARN is inert, the chain
// falls through to IMDS, and the call goes out as the *node instance role*
// while the same process talks to the database as the service role. The
// symptom is an AccessDenied naming a role nobody granted the permission to,
// against a policy correctly attached to the role that was supposed to be
// asking.
//
// So the role is assumed explicitly, but only when the default chain will not
// already have done it. Assuming on top of IRSA would be redundant at best and
// a self-assumption failure at worst, and IRSA is the common deployment.
func loadConfig(ctx context.Context) (aws.Config, error) {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return aws.Config{}, fmt.Errorf("rds: load aws config: %w", err)
	}

	roleARN := os.Getenv("AWS_ROLE_ARN")
	if roleARN == "" || os.Getenv("AWS_WEB_IDENTITY_TOKEN_FILE") != "" {
		return cfg, nil
	}

	// Cached so the credentials refresh on expiry rather than pinning one
	// STS response for the process lifetime.
	cfg.Credentials = aws.NewCredentialsCache(
		stscreds.NewAssumeRoleProvider(
			sts.NewFromConfig(cfg, func(o *sts.Options) {
				o.APIOptions = append(o.APIOptions, awslimit.StackOption(awslimit.STS))
			}),
			roleARN,
		),
	)
	return cfg, nil
}

// FetchMasterCredentials describes an RDS instance and reads its AWS-managed
// master secret (manage_master_user_password = true).
//
// Made as the service role named by AWS_ROLE_ARN where one is set — see
// loadConfig. The permissions this needs (rds:DescribeDBInstances and
// secretsmanager:GetSecretValue on the instance's managed master secret)
// therefore belong on that role, not on whatever the instance profile happens
// to be.
func FetchMasterCredentials(ctx context.Context, dbInstanceIdentifier string) (*MasterCredentials, error) {
	if dbInstanceIdentifier == "" {
		return nil, fmt.Errorf("rds: db instance identifier is required")
	}

	cfg, err := loadConfig(ctx)
	if err != nil {
		return nil, err
	}

	rdsClient := awsrds.NewFromConfig(cfg, func(o *awsrds.Options) {
		o.APIOptions = append(o.APIOptions, awslimit.StackOption(awslimit.RDS))
	})
	out, err := rdsClient.DescribeDBInstances(ctx, &awsrds.DescribeDBInstancesInput{
		DBInstanceIdentifier: aws.String(dbInstanceIdentifier),
	})
	if err != nil {
		return nil, fmt.Errorf("rds: describe db instance %s: %w", dbInstanceIdentifier, err)
	}
	if len(out.DBInstances) == 0 {
		return nil, fmt.Errorf("rds: db instance %s not found", dbInstanceIdentifier)
	}

	inst := out.DBInstances[0]
	if inst.Endpoint == nil || inst.Endpoint.Address == nil {
		return nil, fmt.Errorf("rds: db instance %s has no endpoint", dbInstanceIdentifier)
	}
	if inst.MasterUserSecret == nil || inst.MasterUserSecret.SecretArn == nil {
		return nil, fmt.Errorf("rds: db instance %s has no managed master secret", dbInstanceIdentifier)
	}

	secretARN := aws.ToString(inst.MasterUserSecret.SecretArn)
	sm := secretsmanager.NewFromConfig(cfg, func(o *secretsmanager.Options) {
		o.APIOptions = append(o.APIOptions, awslimit.StackOption(awslimit.SecretsManager))
	})
	sec, err := sm.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{
		SecretId: aws.String(secretARN),
	})
	if err != nil {
		return nil, fmt.Errorf("rds: get master secret %s: %w", secretARN, err)
	}
	if sec.SecretString == nil {
		return nil, fmt.Errorf("rds: master secret %s has no string value", secretARN)
	}

	var payload struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}
	if err := json.Unmarshal([]byte(*sec.SecretString), &payload); err != nil {
		return nil, fmt.Errorf("rds: unmarshal master secret: %w", err)
	}
	if payload.Username == "" || payload.Password == "" {
		return nil, fmt.Errorf("rds: master secret missing username/password")
	}

	port := int32(5432)
	if inst.Endpoint.Port != nil {
		port = *inst.Endpoint.Port
	}

	return &MasterCredentials{
		Host:      aws.ToString(inst.Endpoint.Address),
		Port:      port,
		DBName:    aws.ToString(inst.DBName),
		Username:  payload.Username,
		Password:  payload.Password,
		SecretARN: secretARN,
	}, nil
}
