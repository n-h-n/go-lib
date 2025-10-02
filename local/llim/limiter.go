package llim

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type Limiter interface {
	Allow(ctx context.Context) bool
	Wait(ctx context.Context, opts ...waitOpt) error
	WaitN(ctx context.Context, n int, opts ...waitOpt) error
}

type limiter struct {
	keyspace string
	rate     int           // requests per period
	period   time.Duration // time period
	burst    int           // burst capacity

	// Local state
	mu           sync.Mutex
	tokens       int             // current available tokens
	lastRefill   time.Time       // last time tokens were refilled
	waitingQueue []chan struct{} // queue of waiting requests
}

type wait struct {
	realign       bool
	rem           int
	waitingChan   chan bool
	waitForSignal bool
}

type Result struct {
	Allowed    int
	Remaining  int
	RetryAfter time.Duration
}

// Global limiter registry for local development
var (
	globalMutex     sync.RWMutex
	limiterRegistry = make(map[string]*limiter)
)

type LimiterOpt func(*limiterOptions) error

type limiterOptions struct {
	keyspace string
	rate     int
	period   time.Duration
	burst    int
}

func NewLimiter(opts ...LimiterOpt) Limiter {
	limiterOpts := &limiterOptions{}
	for _, opt := range opts {
		if err := opt(limiterOpts); err != nil {
			panic(err)
		}
	}

	if limiterOpts.keyspace == "" {
		panic(fmt.Errorf("keyspace is required"))
	}
	if limiterOpts.rate <= 0 {
		panic(fmt.Errorf("rate must be positive"))
	}
	if limiterOpts.period <= 0 {
		panic(fmt.Errorf("period must be positive"))
	}
	if limiterOpts.burst <= 0 {
		limiterOpts.burst = limiterOpts.rate // Default burst to rate
	}

	globalMutex.Lock()
	defer globalMutex.Unlock()

	// Get or create limiter for this keyspace
	l, exists := limiterRegistry[limiterOpts.keyspace]
	if !exists {
		l = &limiter{
			keyspace:     limiterOpts.keyspace,
			rate:         limiterOpts.rate,
			period:       limiterOpts.period,
			burst:        limiterOpts.burst,
			tokens:       limiterOpts.burst, // Start with full burst
			lastRefill:   time.Now(),
			waitingQueue: make([]chan struct{}, 0),
		}
		limiterRegistry[limiterOpts.keyspace] = l
	}

	return l
}

func (l *limiter) refillTokens() {
	now := time.Now()
	elapsed := now.Sub(l.lastRefill)

	if elapsed >= l.period {
		// Calculate how many tokens to add based on elapsed time
		periods := int(elapsed / l.period)
		tokensToAdd := periods * l.rate

		l.tokens += tokensToAdd
		if l.tokens > l.burst {
			l.tokens = l.burst
		}

		l.lastRefill = now
	}
}

func (l *limiter) allowN(n int) *Result {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.refillTokens()

	if l.tokens >= n {
		l.tokens -= n
		return &Result{
			Allowed:    n,
			Remaining:  l.tokens,
			RetryAfter: 0,
		}
	}

	// Calculate retry after time
	tokensNeeded := n - l.tokens
	periodsNeeded := (tokensNeeded + l.rate - 1) / l.rate // Ceiling division
	retryAfter := time.Duration(periodsNeeded) * l.period

	return &Result{
		Allowed:    0,
		Remaining:  l.tokens,
		RetryAfter: retryAfter,
	}
}

func (l *limiter) Allow(ctx context.Context) bool {
	result := l.allowN(1)
	return result.Allowed > 0
}

func (l *limiter) Wait(ctx context.Context, opts ...waitOpt) error {
	return l.WaitN(ctx, 1, opts...)
}

func (l *limiter) WaitN(ctx context.Context, n int, opts ...waitOpt) error {
	w := &wait{
		realign:       false,
		rem:           0,
		waitingChan:   make(chan bool),
		waitForSignal: false,
	}

	for _, opt := range opts {
		if err := opt(w); err != nil {
			return err
		}
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		result := l.allowN(n)

		if result.Allowed >= n {
			// Handle realignment logic
			if w.realign && result.Remaining > w.rem {
				// Need to consume more tokens to realign
				extraTokens := result.Remaining - w.rem
				if err := l.WaitN(ctx, extraTokens, opts...); err != nil {
					return err
				}
			} else if w.realign && result.Remaining < w.rem {
				w.realign = false
			}

			if w.waitForSignal {
				close(w.waitingChan)
			}
			return nil
		}

		// Not enough tokens, need to wait
		if w.waitForSignal {
			w.waitingChan <- true
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(result.RetryAfter):
			continue
		}
	}
}

// Options
func WithLocalLimiter(keyspace string, rate int, period time.Duration, burst int) LimiterOpt {
	return func(o *limiterOptions) error {
		o.keyspace = keyspace
		o.rate = rate
		o.period = period
		o.burst = burst
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
