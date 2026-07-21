// Package rds provides small AWS RDS helpers used by services that bootstrap
// Postgres IAM users against RDS instances with managed master passwords.
package rds

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	awsrds "github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
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

// FetchMasterCredentials describes an RDS instance and reads its AWS-managed
// master secret (manage_master_user_password = true).
func FetchMasterCredentials(ctx context.Context, dbInstanceIdentifier string) (*MasterCredentials, error) {
	if dbInstanceIdentifier == "" {
		return nil, fmt.Errorf("rds: db instance identifier is required")
	}

	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("rds: load aws config: %w", err)
	}

	rdsClient := awsrds.NewFromConfig(cfg)
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
	sm := secretsmanager.NewFromConfig(cfg)
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
