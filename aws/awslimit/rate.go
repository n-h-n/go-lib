// Package awslimit rate-limits AWS SDK v2 API calls with the same local
// (llim) / distributed (rlim) token-bucket used by mailgun, places, and
// ops-daemon outbound clients.
//
// Attach StackOption to a service client's APIOptions so every operation
// (PutObject, AssumeRole, GetSecretValue, …) waits on the bucket before the
// request is signed. Do not attach a limiter to a shared aws.Config used by
// multiple services — each service has its own quota.
package awslimit

import (
	"context"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/n-h-n/go-lib/local/llim"
	"github.com/n-h-n/go-lib/redis/rlim"
)

// limiter is the Allow/Wait surface of llim and rlim. Defined locally so this
// package does not import utils (utils → elasticache → iam → awslimit cycle).
type limiter interface {
	Allow(ctx context.Context) bool
	Wait(ctx context.Context) error
}

// RateConfig is a proactive token-bucket for one AWS service API.
type RateConfig struct {
	RequestsPerSecond int
	BurstSize         int
	Keyspace          string
}

// Defaults stay under published account-level API quotas so multi-pod callers
// sharing a Redis key do not trip AWS throttling. S3 prefix rates are
// thousands/s; Secrets Manager GetSecretValue is 5k–10k TPS; STS AssumeRole
// is 600 TPS; RDS Describe is ~40–100 TPS; SQS SendMessage is 300 TPS.
var (
	S3 = RateConfig{
		RequestsPerSecond: 80,
		BurstSize:         80,
		Keyspace:          "{aws:s3:api}",
	}
	STS = RateConfig{
		RequestsPerSecond: 10,
		BurstSize:         10,
		Keyspace:          "{aws:sts:api}",
	}
	SecretsManager = RateConfig{
		RequestsPerSecond: 20,
		BurstSize:         20,
		Keyspace:          "{aws:secretsmanager:api}",
	}
	RDS = RateConfig{
		RequestsPerSecond: 10,
		BurstSize:         10,
		Keyspace:          "{aws:rds:api}",
	}
	SQS = RateConfig{
		RequestsPerSecond: 50,
		BurstSize:         50,
		Keyspace:          "{aws:sqs:api}",
	}
)

type options struct {
	prefix  string
	redis   redis.UniversalClient
	refresh func() redis.UniversalClient
}

// Opt configures limiter construction.
type Opt func(*options)

// WithKeyPrefix prepends a service name so Redis keys are daemon:{aws:s3:api}.
func WithKeyPrefix(prefix string) Opt {
	return func(o *options) {
		o.prefix = prefix
	}
}

// WithRedis uses Valkey for a cluster-wide budget. When redis is nil the
// process-local limiter for this keyspace is used instead.
func WithRedis(redisClient redis.UniversalClient, refresh func() redis.UniversalClient) Opt {
	return func(o *options) {
		o.redis = redisClient
		o.refresh = refresh
	}
}

func (r RateConfig) withDefaults(def RateConfig) RateConfig {
	out := r
	if out.RequestsPerSecond <= 0 {
		out.RequestsPerSecond = def.RequestsPerSecond
	}
	if out.BurstSize <= 0 {
		out.BurstSize = out.RequestsPerSecond
	}
	if out.Keyspace == "" {
		out.Keyspace = def.Keyspace
	}
	return out
}

func (r RateConfig) prefixed(prefix string) RateConfig {
	if prefix == "" {
		return r
	}
	r.Keyspace = prefix + ":" + r.Keyspace
	return r
}

var (
	localMu sync.Mutex
	local   = map[string]limiter{}
)

// NewLimiter returns a Redis limiter when WithRedis is set, otherwise a
// process-wide local limiter shared by keyspace so one-off SDK clients in the
// same process share one bucket.
func NewLimiter(rate RateConfig, opts ...Opt) limiter {
	var o options
	for _, opt := range opts {
		opt(&o)
	}
	rate = rate.withDefaults(rate).prefixed(o.prefix)
	if o.redis != nil {
		rlimOpts := []rlim.LimiterOpt{
			rlim.WithDistributedLimiter(
				o.redis,
				rlim.PerSecond(rate.RequestsPerSecond, rate.BurstSize),
				rate.Keyspace,
			),
		}
		if o.refresh != nil {
			rlimOpts = append(rlimOpts, rlim.WithClientRefresh(o.refresh))
		}
		return rlimWait{rlim.NewLimiter(rlimOpts...)}
	}
	return sharedLocal(rate)
}

func sharedLocal(rate RateConfig) limiter {
	localMu.Lock()
	defer localMu.Unlock()
	if lim, ok := local[rate.Keyspace]; ok {
		return lim
	}
	lim := llimWait{llim.NewLimiter(
		llim.WithLocalLimiter(
			rate.Keyspace,
			rate.RequestsPerSecond,
			time.Second,
			rate.BurstSize,
		),
	)}
	local[rate.Keyspace] = lim
	return lim
}

type llimWait struct{ l llim.Limiter }

func (w llimWait) Allow(ctx context.Context) bool { return w.l.Allow(ctx) }
func (w llimWait) Wait(ctx context.Context) error { return w.l.Wait(ctx) }

type rlimWait struct{ l rlim.Limiter }

func (w rlimWait) Allow(ctx context.Context) bool { return w.l.Allow(ctx) }
func (w rlimWait) Wait(ctx context.Context) error { return w.l.Wait(ctx) }
