package rlock

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/n-h-n/go-lib/log"
)

const (
	maxConcurrency     string = "max_concurrency"
	defaultConcurrency int    = 1
	acquired           string = "acquired"
	blocked            string = "blocked"

	LockTypeRead  string = "read"
	LockTypeWrite string = "write"

	UnlockTypeInstance string = "instance"
	UnlockTypeAll      string = "all"
)

type Mutex interface {
	Lock(context.Context) error
	Unlock(context.Context) error
}

type mutex struct {
	redisClient       *redis.UniversalClient
	key               string
	appID             string
	concurrency       int // number of concurrent requests allowed
	customConcurrency bool
	ttl               time.Duration
	lockType          string
	unlockType        string
	verbose           bool
	wait              bool
	waitInterval      time.Duration
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

type MutexOpt func(*mutex) error

func NewMutex(
	redisClient redis.UniversalClient,
	key string,
	appID string,
	opts ...MutexOpt,
) Mutex {
	// hash tagging keys if needed, find if contains curly braces
	re := regexp.MustCompile(`\{.*?\}`)
	if !re.MatchString(key) {
		firstIdx := strings.Index(key, ":")
		if firstIdx == -1 {
			panic(fmt.Sprintf("invalid key: %s, key must begin with \"service-name:\"", key))
		}

		serviceName := key[:firstIdx]
		key = serviceName + fmt.Sprintf(":{%s}", key[firstIdx+1:])
	}

	m := &mutex{
		redisClient:       &redisClient,
		key:               key,
		appID:             appID,
		concurrency:       defaultConcurrency,
		customConcurrency: false,
		ttl:               4 * time.Minute,
		lockType:          LockTypeWrite,
		unlockType:        UnlockTypeInstance,
		wait:              true,
		waitInterval:      1 * time.Second,
	}

	for _, opt := range opts {
		err := opt(m)
		if err != nil {
			panic(err)
		}
	}

	return m
}

func (m *mutex) Lock(ctx context.Context) error {
	script := redis.NewScript(concurrentLockLS)
	done := make(chan struct{})
	errChan := make(chan error)

	go func() {
		for {
			result, err := script.Run(
				ctx,
				*m.redisClient, []string{m.key},
				m.appID,
				m.concurrency,
				m.ttl.Seconds(),
				m.lockType,
			).Result()
			if err != nil {
				errChan <- err
			}

			r, ok := result.([]interface{})
			if !ok {
				errChan <- fmt.Errorf("unexpected result type from redis: %T", result)
				break
			}

			status, ok := r[0].(string)
			if !ok {
				errChan <- fmt.Errorf("unexpected status type from redis: %T", r[0])
				break
			}

			ttl, ok := r[1].(int64)
			if !ok {
				errChan <- fmt.Errorf("unexpected ttl type from redis: %T", r[1])
				break
			}

			if status == blocked {
				if m.wait {
					if m.verbose {
						log.Log.Debugf(ctx, "lock blocked, ttl remaining: %v, waiting....", ttl)
					}
					time.Sleep(m.waitInterval)
					continue
				} else {
					errChan <- fmt.Errorf("lock already held, ttl remaining: %v", ttl)
					break
				}
			}

			if status == maxConcurrency {
				if m.wait {
					if m.verbose {
						log.Log.Debugf(ctx, "max concurrency reached for lock: %s, app ID: %s, instances holding: %v, ttl remaining: %v, waiting....", m.key, m.appID, r[2], ttl)
					}
					time.Sleep(m.waitInterval)
					continue
				} else {
					holders := r[2].([]interface{})
					holdersStr := make([]string, len(holders))
					for i, holder := range holders {
						holdersStr[i] = holder.(string)
					}
					errChan <- maxConcurrencyError{
						Key:     m.key,
						AppID:   m.appID,
						TTL:     ttl,
						Holders: holdersStr,
					}
					break
				}
			}

			if status != acquired {
				errChan <- fmt.Errorf("unknown result from redis while acquiring lock: %v", result)
				break
			}

			if m.verbose {
				log.Log.Debugf(ctx, "acquired lock: %s, app ID: %s, lock type: %s, ttl: %v", m.key, m.appID, m.lockType, ttl)
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

func (m *mutex) Unlock(ctx context.Context) error {
	keySuffix := fmt.Sprintf(":%s", m.lockType)
	if m.unlockType == UnlockTypeInstance {
		_, err := (*m.redisClient).SRem(ctx, m.key+keySuffix, m.appID).Result()
		if err != nil {
			log.Log.Errorf(ctx, "error while unlocking: %v", err)
			return err
		}
	} else if m.unlockType == UnlockTypeAll {
		_, err := (*m.redisClient).Del(ctx, m.key+keySuffix).Result()
		if err != nil {
			log.Log.Errorf(ctx, "error while unlocking: %v", err)
			return err
		}
	}

	if m.verbose {
		log.Log.Debugf(ctx, "unlocked lock: %s, app ID: %s, lock type: %s, unlock type: %s", m.key, m.appID, m.lockType, m.unlockType)
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
			m.concurrency = -1
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
