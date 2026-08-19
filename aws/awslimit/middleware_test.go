package awslimit

import (
	"testing"

	"github.com/aws/smithy-go/middleware"
)

func TestSharedLocalLimiterSameKeyspace(t *testing.T) {
	a := NewLimiter(STS)
	b := NewLimiter(STS)
	if a != b {
		t.Fatal("same keyspace should share one process-local limiter")
	}
}

func TestPrefixedKeyspaceDoesNotCollide(t *testing.T) {
	a := NewLimiter(S3)
	b := NewLimiter(S3, WithKeyPrefix("daemon"))
	if a == b {
		t.Fatal("prefixed keyspace must not share the unprefixed bucket")
	}
}

func TestStackOptionAddsMiddleware(t *testing.T) {
	stack := middleware.NewStack("test", func() interface{} { return nil })
	if err := StackOption(STS)(stack); err != nil {
		t.Fatal(err)
	}
}
