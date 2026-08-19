package bigquery

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/n-h-n/go-lib/aws/elasticache"
	"github.com/n-h-n/go-lib/local/llim"
	"github.com/n-h-n/go-lib/redis/rlim"
	"github.com/n-h-n/go-lib/utils"
)

// RateConfig is the proactive token-bucket budget for outbound BigQuery API
// calls (jobs.query, tables.get, tables.insert, …).
//
// Google's default "Maximum number of API requests per second per user" is
// 100 for most methods (jobs.get is 1000/s; tables.insert is 10/s). Defaults
// stay under the 100/s user cap so multi-pod callers sharing a Redis key do
// not trip quota. See:
// https://docs.cloud.google.com/bigquery/docs/troubleshoot-quotas#ts-maximum-api-request-limit
type RateConfig struct {
	RequestsPerSecond int
	BurstSize         int
	Keyspace          string // Redis hash-tag key, e.g. daemon:{bigquery:api}
}

// DefaultRateConfig stays under BigQuery's 100 req/s per-user API cap.
var DefaultRateConfig = RateConfig{
	RequestsPerSecond: 50,
	BurstSize:         50,
	Keyspace:          "{bigquery:api}",
}

func (r RateConfig) withDefaults() RateConfig {
	out := r
	if out.RequestsPerSecond <= 0 {
		out.RequestsPerSecond = DefaultRateConfig.RequestsPerSecond
	}
	if out.BurstSize <= 0 {
		out.BurstSize = out.RequestsPerSecond
	}
	if out.Keyspace == "" {
		out.Keyspace = DefaultRateConfig.Keyspace
	}
	return out
}

func newRateLimiter(
	rate RateConfig,
	useRedis bool,
	redisClient redis.UniversalClient,
	ec *elasticache.Client,
) utils.RateLimiter {
	rate = rate.withDefaults()
	if useRedis && redisClient != nil {
		opts := []rlim.LimiterOpt{
			rlim.WithDistributedLimiter(
				redisClient,
				rlim.PerSecond(rate.RequestsPerSecond, rate.BurstSize),
				rate.Keyspace,
			),
		}
		if ec != nil {
			opts = append(opts, rlim.WithClientRefresh(ec.RefreshAndGetRedisClient))
		}
		return utils.NewRedisRateLimiterWrapper(rlim.NewLimiter(opts...), false)
	}
	local := llim.NewLimiter(
		llim.WithLocalLimiter(
			rate.Keyspace,
			rate.RequestsPerSecond,
			time.Second,
			rate.BurstSize,
		),
	)
	return utils.NewLocalRateLimiterWrapper(local, false)
}

func (c *Client) waitRateLimit(ctx context.Context) error {
	if c == nil || c.rateLimiter == nil {
		return nil
	}
	if ctx == nil {
		ctx = c.ctx
	}
	if c.rateLimiter.Allow(ctx) {
		return nil
	}
	if err := c.rateLimiter.Wait(ctx); err != nil {
		return fmt.Errorf("bigquery: rate limit: %w", err)
	}
	return nil
}
