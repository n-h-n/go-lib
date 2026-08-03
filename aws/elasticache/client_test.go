package elasticache

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	ststypes "github.com/aws/aws-sdk-go-v2/service/sts/types"

	"github.com/n-h-n/go-lib/aws/iam"
)

type fakeIAMClient struct {
	serviceName       string
	region            string
	originalCreds     aws.Credentials
	assumedCreds      *ststypes.Credentials
	sessionDuration   time.Duration
	refreshPercentage float64
	refreshForces     []bool
}

func (f *fakeIAMClient) GetAssumedRole() *sts.AssumeRoleOutput {
	creds := f.assumedCreds
	if creds == nil {
		exp := time.Now().Add(f.sessionDuration)
		if f.sessionDuration == 0 {
			exp = time.Now().Add(15 * time.Minute)
		}
		creds = &ststypes.Credentials{
			AccessKeyId:     aws.String(f.originalCreds.AccessKeyID),
			SecretAccessKey: aws.String(f.originalCreds.SecretAccessKey),
			SessionToken:    aws.String(f.originalCreds.SessionToken),
			Expiration:      &exp,
		}
	}
	return &sts.AssumeRoleOutput{Credentials: creds}
}

func (f *fakeIAMClient) GetAWSConfig() *aws.Config {
	return nil
}

func (f *fakeIAMClient) GetOriginalCredentials() aws.Credentials {
	return f.originalCreds
}

func (f *fakeIAMClient) GetAWSRegion() string {
	return f.region
}

func (f *fakeIAMClient) GetRefreshPercentage() float64 {
	return f.refreshPercentage
}

func (f *fakeIAMClient) GetRoleARN() string {
	return ""
}

func (f *fakeIAMClient) GetServiceName() string {
	return f.serviceName
}

func (f *fakeIAMClient) GetSessionDuration() time.Duration {
	return f.sessionDuration
}

func (f *fakeIAMClient) GetSessionTimeRemaining() time.Duration {
	return f.sessionDuration
}

func (f *fakeIAMClient) GetTokenUpdateTime() time.Time {
	return time.Time{}
}

func (f *fakeIAMClient) RefreshAWSCreds(_ context.Context, opts ...func(*iam.RefreshOpts)) error {
	f.refreshForces = append(f.refreshForces, len(opts) > 0)
	return nil
}

func TestPrepareAuthTokenForcesRefreshWhenRequested(t *testing.T) {
	t.Parallel()

	iamClient := &fakeIAMClient{
		serviceName:       "ops-daemon",
		region:            "us-east-1",
		originalCreds:     aws.Credentials{AccessKeyID: "akid", SecretAccessKey: "secret", SessionToken: "session"},
		sessionDuration:   15 * time.Minute,
		refreshPercentage: 0.2,
	}
	client := &Client{
		ctx:                context.Background(),
		iamClient:          iamClient,
		replicationGroupID: "rg.example.cache.amazonaws.com",
	}

	token, err := client.prepareAuthToken(context.Background(), true)
	if err != nil {
		t.Fatalf("prepareAuthToken returned error: %v", err)
	}
	if token == "" {
		t.Fatal("prepareAuthToken returned empty token")
	}
	if len(iamClient.refreshForces) != 1 || !iamClient.refreshForces[0] {
		t.Fatalf("expected forced refresh, got %#v", iamClient.refreshForces)
	}
	if client.token != token {
		t.Fatalf("expected client token to be stored")
	}
}

func TestRedisCredentialsProviderUsesNonForcedRefresh(t *testing.T) {
	t.Parallel()

	iamClient := &fakeIAMClient{
		serviceName:       "ops-daemon",
		region:            "us-east-1",
		originalCreds:     aws.Credentials{AccessKeyID: "akid", SecretAccessKey: "secret", SessionToken: "session"},
		sessionDuration:   15 * time.Minute,
		refreshPercentage: 0.2,
	}
	client := &Client{
		ctx:                context.Background(),
		iamClient:          iamClient,
		replicationGroupID: "rg.example.cache.amazonaws.com",
	}

	username, token, err := client.redisCredentialsProvider(context.Background())
	if err != nil {
		t.Fatalf("redisCredentialsProvider returned error: %v", err)
	}
	if username != iamClient.serviceName {
		t.Fatalf("expected username %q, got %q", iamClient.serviceName, username)
	}
	if token == "" {
		t.Fatal("redisCredentialsProvider returned empty token")
	}
	if len(iamClient.refreshForces) != 1 || iamClient.refreshForces[0] {
		t.Fatalf("expected non-forced refresh, got %#v", iamClient.refreshForces)
	}
	if client.token != token {
		t.Fatal("expected redisCredentialsProvider to persist token on client")
	}
}

func TestSplitRedisAddr(t *testing.T) {
	t.Parallel()

	host, port, err := splitRedisAddr("master.valkey.example.cache.amazonaws.com:6379")
	if err != nil {
		t.Fatal(err)
	}
	if host != "master.valkey.example.cache.amazonaws.com" || port != 6379 {
		t.Fatalf("got %s:%d", host, port)
	}

	host, port, err = splitRedisAddr("rediss://master.valkey.example.cache.amazonaws.com:6379/0")
	if err != nil {
		t.Fatal(err)
	}
	if host != "master.valkey.example.cache.amazonaws.com" || port != 6379 {
		t.Fatalf("got %s:%d", host, port)
	}

	if _, _, err := splitRedisAddr(""); err == nil {
		t.Fatal("expected empty URI to fail")
	}
}

func TestRedisDialUsesCanonicalTLSServerName(t *testing.T) {
	t.Parallel()

	client := &Client{dialHost: "127.0.0.1", dialPort: 16379}
	addr, tlsConfig, err := client.redisDial("master.valkey.example.cache.amazonaws.com:6379")
	if err != nil {
		t.Fatal(err)
	}
	if addr != "127.0.0.1:16379" {
		t.Fatalf("addr=%q", addr)
	}
	if tlsConfig == nil || tlsConfig.ServerName != "master.valkey.example.cache.amazonaws.com" {
		t.Fatalf("ServerName=%q", tlsConfig.ServerName)
	}

	direct := &Client{}
	addr, tlsConfig, err = direct.redisDial("master.valkey.example.cache.amazonaws.com:6379")
	if err != nil {
		t.Fatal(err)
	}
	if addr != "master.valkey.example.cache.amazonaws.com:6379" {
		t.Fatalf("addr=%q", addr)
	}
	if tlsConfig.ServerName != "master.valkey.example.cache.amazonaws.com" {
		t.Fatalf("ServerName=%q", tlsConfig.ServerName)
	}
}
