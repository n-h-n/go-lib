package rds

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
)

// assumesRole reports whether the resolved config will exchange for the role in
// AWS_ROLE_ARN rather than using the ambient identity.
func assumesRole(t *testing.T, cfg aws.Config) bool {
	t.Helper()
	cache, ok := cfg.Credentials.(*aws.CredentialsCache)
	if !ok {
		return false
	}
	return cache.IsCredentialsProvider(&stscreds.AssumeRoleProvider{})
}

func loadForTest(t *testing.T) aws.Config {
	t.Helper()
	// Keep resolution off the network: without this the SDK probes IMDS for a
	// region and the test inherits a several-second timeout.
	t.Setenv("AWS_REGION", "us-west-2")
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")

	cfg, err := loadConfig(context.Background())
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	return cfg
}

// The bug this fixes: on k3s and plain EC2, AWS_ROLE_ARN is inert in the
// default chain, so the call went out as the node instance role while the same
// process reached Postgres as the service role. The permissions live on the
// service role, so the result was an AccessDenied naming a principal nobody
// had granted anything to.
func TestAssumesTheServiceRoleWithoutWebIdentity(t *testing.T) {
	t.Setenv("AWS_ROLE_ARN", "arn:aws:iam::123456789012:role/daemon")
	t.Setenv("AWS_WEB_IDENTITY_TOKEN_FILE", "")

	if !assumesRole(t, loadForTest(t)) {
		t.Fatal("AWS_ROLE_ARN set with no web identity token must assume the role")
	}
}

// With IRSA the default chain already exchanges the token for AWS_ROLE_ARN.
// Assuming again would be redundant, and self-assumption fails unless the trust
// policy happens to allow it — so the common deployment must be left alone.
func TestLeavesWebIdentityResolutionAlone(t *testing.T) {
	t.Setenv("AWS_ROLE_ARN", "arn:aws:iam::123456789012:role/daemon")
	t.Setenv("AWS_WEB_IDENTITY_TOKEN_FILE", "/var/run/secrets/eks.amazonaws.com/serviceaccount/token")

	if assumesRole(t, loadForTest(t)) {
		t.Fatal("assumed on top of IRSA; the default chain already resolves the role")
	}
}

// No role configured means the ambient identity is the intended one, which is
// how this behaved everywhere before and must keep behaving.
func TestUsesAmbientIdentityWithoutARoleARN(t *testing.T) {
	t.Setenv("AWS_ROLE_ARN", "")
	t.Setenv("AWS_WEB_IDENTITY_TOKEN_FILE", "")

	if assumesRole(t, loadForTest(t)) {
		t.Fatal("assumed a role with none configured")
	}
}
