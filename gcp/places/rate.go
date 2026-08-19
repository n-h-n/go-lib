package places

import (
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/n-h-n/go-lib/aws/elasticache"
	"github.com/n-h-n/go-lib/local/llim"
	"github.com/n-h-n/go-lib/redis/rlim"
	"github.com/n-h-n/go-lib/utils"
)

// RateConfig is a proactive token-bucket for one Places SKU.
type RateConfig struct {
	RequestsPerSecond int
	BurstSize         int
	Keyspace          string
}

// DefaultAutocompleteRate stays far under Places Autocomplete QPS and the
// 10k/month free Autocomplete Requests cap when sessions are abandoned.
var DefaultAutocompleteRate = RateConfig{
	RequestsPerSecond: 10,
	BurstSize:         10,
	Keyspace:          "{places:autocomplete:api}",
}

// DefaultDetailsRate covers Place Details Essentials (10k/month free).
var DefaultDetailsRate = RateConfig{
	RequestsPerSecond: 5,
	BurstSize:         5,
	Keyspace:          "{places:details:api}",
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

func newRateLimiter(
	rate RateConfig,
	useRedis bool,
	redisClient redis.UniversalClient,
	ec *elasticache.Client,
) utils.RateLimiter {
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
