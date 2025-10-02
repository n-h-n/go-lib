package llock

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/n-h-n/go-lib/log"
)

const (
	LockTypeRead  string = "read"
	LockTypeWrite string = "write"

	UnlockTypeInstance string = "instance"
	UnlockTypeAll      string = "all"

	acquired           string = "acquired"
	blocked            string = "blocked"
	maxConcurrency     string = "max_concurrency"
	defaultConcurrency int    = 1
)

type Mutex interface {
	Lock(context.Context) error
	Unlock(context.Context) error
}

type mutex struct {
	key               string
	appID             string
	concurrency       int
	customConcurrency bool
	ttl               time.Duration
	lockType          string
	unlockType        string
	verbose           bool
	wait              bool
	waitInterval      time.Duration

	// Reference to shared state for this key
	sharedState *mutexState
}

type mutexState struct {
	mu          sync.RWMutex
	holders     map[string]time.Time // appID -> expiry time
	lastCleanup time.Time
}

type maxConcurrencyError struct {
	Key     string
	AppID   string
	TTL     int64
	Holders []string
}

func (e maxConcurrencyError) Error() string {
	return fmt.Sprintf("max concurrency reached for lock: %s, app ID: %s, ttl remaining: %v, holders: %v", e.Key, e.AppID, e.TTL, e.Holders)
}

// Implement Is interface for errors.Is support
func (e maxConcurrencyError) Is(target error) bool {
	_, ok := target.(maxConcurrencyError)
	return ok
}

var ErrMaxConcurrency = maxConcurrencyError{}

// Global lock registry for local development
var (
	globalMutex   sync.RWMutex
	stateRegistry = make(map[string]*mutexState)
)

type MutexOpt func(*mutex) error

func NewMutex(
	key string,
	appID string,
	opts ...MutexOpt,
) Mutex {
	// Normalize key (remove hash tagging for local use)
	normalizedKey := key

	globalMutex.Lock()
	defer globalMutex.Unlock()

	// Get or create shared state for this key
	state, exists := stateRegistry[normalizedKey]
	if !exists {
		state = &mutexState{
			holders:     make(map[string]time.Time),
			lastCleanup: time.Now(),
		}
		stateRegistry[normalizedKey] = state
	}

	// Create mutex instance
	m := &mutex{
		key:               normalizedKey,
		appID:             appID,
		concurrency:       defaultConcurrency,
		customConcurrency: false,
		ttl:               4 * time.Minute,
		lockType:          LockTypeWrite,
		unlockType:        UnlockTypeInstance,
		wait:              true,
		waitInterval:      1 * time.Second,
		sharedState:       state,
	}

	for _, opt := range opts {
		err := opt(m)
		if err != nil {
			panic(err)
		}
	}

	return m
}

func (m *mutex) cleanupExpired() {
	now := time.Now()
	if now.Sub(m.sharedState.lastCleanup) < time.Second {
		return // Don't cleanup too frequently
	}

	for appID, expiry := range m.sharedState.holders {
		if now.After(expiry) {
			delete(m.sharedState.holders, appID)
		}
	}
	m.sharedState.lastCleanup = now
}

func (m *mutex) Lock(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		m.sharedState.mu.Lock()
		m.cleanupExpired()

		// Check if we can acquire the lock
		currentHolders := len(m.sharedState.holders)
		canAcquire := false

		if m.lockType == LockTypeRead {
			// Read locks: unlimited concurrency unless explicitly set
			if m.concurrency == -1 {
				canAcquire = true
			} else {
				canAcquire = currentHolders < m.concurrency
			}
		} else {
			// Write locks: respect concurrency limit
			canAcquire = currentHolders < m.concurrency
		}

		if canAcquire {
			// Acquire the lock
			expiry := time.Now().Add(m.ttl)
			m.sharedState.holders[m.appID] = expiry
			m.sharedState.mu.Unlock()

			if m.verbose {
				log.Log.Debugf(ctx, "acquired lock: %s, app ID: %s, type: %s", m.key, m.appID, m.lockType)
			}
			return nil
		}

		// Lock is blocked
		if !m.wait {
			holders := make([]string, 0, len(m.sharedState.holders))
			var minTTL time.Duration = time.Hour * 24 // Large default

			for appID, expiry := range m.sharedState.holders {
				holders = append(holders, appID)
				if ttl := time.Until(expiry); ttl < minTTL {
					minTTL = ttl
				}
			}

			m.sharedState.mu.Unlock()

			return maxConcurrencyError{
				Key:     m.key,
				AppID:   m.appID,
				TTL:     int64(minTTL.Seconds()),
				Holders: holders,
			}
		}

		// Calculate minimum TTL for verbose logging
		var minTTL time.Duration = time.Hour * 24
		for _, expiry := range m.sharedState.holders {
			if ttl := time.Until(expiry); ttl < minTTL {
				minTTL = ttl
			}
		}

		m.sharedState.mu.Unlock()

		if m.verbose {
			log.Log.Debugf(ctx, "lock blocked, ttl remaining: %v, waiting....", minTTL)
		}

		// Wait before retrying
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(m.waitInterval):
			continue
		}
	}
}

func (m *mutex) Unlock(ctx context.Context) error {
	m.sharedState.mu.Lock()
	defer m.sharedState.mu.Unlock()

	if m.unlockType == UnlockTypeInstance {
		// Remove only this instance
		delete(m.sharedState.holders, m.appID)
		if m.verbose {
			log.Log.Debugf(ctx, "unlocked instance: %s, app ID: %s", m.key, m.appID)
		}
	} else if m.unlockType == UnlockTypeAll {
		// Remove all holders
		m.sharedState.holders = make(map[string]time.Time)
		if m.verbose {
			log.Log.Debugf(ctx, "unlocked all instances: %s", m.key)
		}
	}

	return nil
}

func WithTTL(ttl time.Duration) MutexOpt {
	return func(m *mutex) error {
		m.ttl = ttl
		return nil
	}
}

func WithConcurrency(concurrency int) MutexOpt {
	return func(m *mutex) error {
		m.concurrency = concurrency
		m.customConcurrency = true
		return nil
	}
}

func WithVerbose() MutexOpt {
	return func(m *mutex) error {
		m.verbose = true
		return nil
	}
}

func WithWait(v bool) MutexOpt {
	return func(m *mutex) error {
		m.wait = v
		return nil
	}
}

func WithWaitInterval(waitInterval time.Duration) MutexOpt {
	return func(m *mutex) error {
		m.waitInterval = waitInterval
		return nil
	}
}

func WithLockType(lockType string) MutexOpt {
	return func(m *mutex) error {
		if lockType != LockTypeRead && lockType != LockTypeWrite {
			return fmt.Errorf("invalid lock type: %s", lockType)
		}
		m.lockType = lockType
		if lockType == LockTypeRead && !m.customConcurrency {
			m.concurrency = -1 // Unlimited for read locks
		}
		return nil
	}
}

func WithUnlockType(unlockType string) MutexOpt {
	return func(m *mutex) error {
		if unlockType != UnlockTypeInstance && unlockType != UnlockTypeAll {
			return fmt.Errorf("invalid unlock type: %s", unlockType)
		}
		m.unlockType = unlockType
		return nil
	}
}
