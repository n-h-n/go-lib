package rlim

import (
	"context"
	"fmt"
	"time"

	"golang.org/x/time/rate"

	"github.com/go-redis/redis_rate/v10"
	"github.com/redis/go-redis/v9"
)

const (
	limiterTypeLocal       limiterType = "local"
	limiterTypeDistributed limiterType = "distributed"
)

type Limiter interface {
	Allow(ctx context.Context) bool
	Wait(ctx context.Context, opts ...waitOpt) error
	WaitN(ctx context.Context, n int, opts ...waitOpt) error
}

type localLimiter struct {
	limiter *rate.Limiter
}

type distributedLimiter struct {
	limiter  *redis_rate.Limiter
	keyspace string
	limit    redis_rate.Limit
}

type wait struct {
	realign       bool
	rem           int
	waitingChan   chan bool
	waitForSignal bool
}
type limiterOptions struct {
	limiterType limiterType
	distributed struct {
		redisClient *redis.UniversalClient
		keyspace    string
		limit       redis_rate.Limit
	}
	local struct {
		rate  rate.Limit
		burst int
	}
}

type limiterType string

func NewLimiter(opts ...limiterOpt) Limiter {

	limiterOpts := &limiterOptions{}
	for _, opt := range opts {
		if err := opt(limiterOpts); err != nil {
			panic(err)
		}
	}

	if limiterOpts.limiterType != limiterTypeDistributed {
		panic(fmt.Errorf("only distributed limiter type is supported"))
	}

	return &distributedLimiter{
		limiter:  redis_rate.NewLimiter(*limiterOpts.distributed.redisClient),
		keyspace: limiterOpts.distributed.keyspace,
		limit:    limiterOpts.distributed.limit,
	}
}

func (l *distributedLimiter) Wait(ctx context.Context, opt ...waitOpt) error {
	return l.WaitN(ctx, 1, opt...)
}

func (l *distributedLimiter) WaitN(ctx context.Context, n int, opts ...waitOpt) error {
	done := make(chan struct{})
	errChan := make(chan error)

	w := &wait{
		realign:       false,
		rem:           0,
		waitingChan:   make(chan bool),
		waitForSignal: false,
	}
	for _, o := range opts {
		if err := o(w); err != nil {
			return err
		}
	}

	go func() {
		for {
			res, err := l.limiter.AllowN(ctx, l.keyspace, l.limit, n)
			if err != nil {
				errChan <- err
				break
			}

			if res == nil {
				errChan <- fmt.Errorf("distributed limiter returned nil result")
				break
			}

			if n != 0 && res.Allowed == 0 {
				if w.waitForSignal {
					w.waitingChan <- true
				}
				time.Sleep(res.RetryAfter)
				continue
			}

			if w.realign && res.Remaining > w.rem {
				if err := l.WaitN(ctx, res.Remaining-w.rem, opts...); err != nil {
					errChan <- err
					break
				}
			} else if w.realign && res.Remaining < w.rem {
				w.realign = false
			}

			if w.waitForSignal {
				close(w.waitingChan)
			}
			close(done)
			break
		}
	}()

	select {
	case <-done:
		return nil
	case err := <-errChan:
		return err
	}
}

func (l *distributedLimiter) Allow(ctx context.Context) bool {
	res, err := l.limiter.Allow(ctx, l.keyspace, l.limit)
	if err != nil {
		panic(err)
	}

	if res.Remaining == 0 {
		return false
	}

	return true
}

func (l *localLimiter) Allow(ctx context.Context) bool {
	return l.limiter.Allow()
}

func (l *localLimiter) Wait(ctx context.Context) error {
	return l.limiter.Wait(ctx)
}

func (l *localLimiter) WaitN(ctx context.Context, n int) error {
	return l.limiter.WaitN(ctx, n)
}
