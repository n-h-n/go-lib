package rlim

import (
	"context"
	"fmt"
	"strings"
	"time"

	"golang.org/x/time/rate"

	"github.com/go-redis/redis_rate/v10"
	"github.com/n-h-n/go-lib/log"
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
	Remaining(ctx context.Context) int
}

type localLimiter struct {
	limiter *rate.Limiter
}

type distributedLimiter struct {
	limiter       *redis_rate.Limiter
	keyspace      string
	limit         redis_rate.Limit
	clientRefresh func() redis.UniversalClient
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
		redisClient   *redis.UniversalClient
		keyspace      string
		limit         redis_rate.Limit
		clientRefresh func() redis.UniversalClient
	}
	local struct {
		rate  rate.Limit
		burst int
	}
}

type limiterType string

func NewLimiter(opts ...LimiterOpt) Limiter {

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
		limiter:       redis_rate.NewLimiter(*limiterOpts.distributed.redisClient),
		keyspace:      limiterOpts.distributed.keyspace,
		limit:         limiterOpts.distributed.limit,
		clientRefresh: limiterOpts.distributed.clientRefresh,
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
			res, err := l.allowN(ctx, n, false)
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
	return l.allow(ctx, false)
}

func (l *distributedLimiter) allow(ctx context.Context, retried bool) bool {
	res, err := l.limiter.Allow(ctx, l.keyspace, l.limit)
	if err != nil {
		if !retried && l.clientRefresh != nil && isAuthError(err) {
			log.Log.Warnf(ctx, "redis auth failure detected, refreshing client for keyspace %s", l.keyspace)
			l.limiter = redis_rate.NewLimiter(l.clientRefresh())
			return l.allow(ctx, true)
		}
		log.Log.Errorf(ctx, "distributed rate limiter error: %v", err)
		return false
	}

	if res.Remaining == 0 {
		return false
	}

	return true
}

func (l *distributedLimiter) allowN(
	ctx context.Context,
	n int,
	retried bool,
) (*redis_rate.Result, error) {
	res, err := l.limiter.AllowN(ctx, l.keyspace, l.limit, n)
	if err != nil {
		if !retried && l.clientRefresh != nil && isAuthError(err) {
			log.Log.Warnf(ctx, "redis auth failure detected, refreshing client for keyspace %s", l.keyspace)
			l.limiter = redis_rate.NewLimiter(l.clientRefresh())
			return l.allowN(ctx, n, true)
		}
		return nil, err
	}
	return res, nil
}

func isAuthError(err error) bool {
	return strings.Contains(err.Error(), "WRONGPASS") || strings.Contains(err.Error(), "NOAUTH")
}

func (l *distributedLimiter) Remaining(ctx context.Context) int {
	res, err := l.allowN(ctx, 0, false)
	if err != nil || res == nil {
		return -1
	}
	return res.Remaining
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

func (l *localLimiter) Remaining(ctx context.Context) int {
	return int(l.limiter.Tokens())
}
