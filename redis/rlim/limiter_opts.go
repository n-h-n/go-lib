package rlim

import (
	"fmt"

	"github.com/go-redis/redis_rate/v10"
	"golang.org/x/time/rate"

	"github.com/redis/go-redis/v9"
)

type LimiterOpt func(*limiterOptions) error

// WithLocalLimiter sets the limiter type to local and sets the rate.
func WithLocalLimiter(
	rate rate.Limit,
	burst int,
) LimiterOpt {
	return func(o *limiterOptions) error {
		if o.limiterType != "" {
			return fmt.Errorf("limiter type already set to %s", o.limiterType)
		}
		o.limiterType = limiterTypeLocal
		o.local.rate = rate
		o.local.burst = burst
		return nil
	}
}

func WithDistributedLimiter(
	redisClient redis.UniversalClient,
	limit redis_rate.Limit,
	key string,
) LimiterOpt {
	return func(o *limiterOptions) error {
		if o.limiterType != "" {
			return fmt.Errorf("limiter type already set to %s", o.limiterType)
		}
		o.limiterType = limiterTypeDistributed
		o.distributed.redisClient = &redisClient
		o.distributed.keyspace = key
		o.distributed.limit = limit
		return nil
	}
}

// WithClientRefresh sets a function that returns a fresh redis.UniversalClient.
// On auth errors (WRONGPASS/NOAUTH), the limiter calls this to swap in a
// re-authenticated client and treats the current request as rate-limited.
func WithClientRefresh(fn func() redis.UniversalClient) LimiterOpt {
	return func(o *limiterOptions) error {
		o.distributed.clientRefresh = fn
		return nil
	}
}

type waitOpt func(w *wait) error

func WithWaitingChannel(ch chan bool) waitOpt {
	return func(w *wait) error {
		w.waitingChan = ch
		w.waitForSignal = true
		return nil
	}
}

func WithRemaining(rem int) waitOpt {
	return func(w *wait) error {
		w.rem = rem
		return nil
	}
}

func WithRealign(realign bool) waitOpt {
	return func(w *wait) error {
		w.realign = realign
		return nil
	}
}
