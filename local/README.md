# Local Development Package

This package provides local implementations of distributed mutual exclusion locks and rate limiting for development environments where Redis is not available.

## Features

- **Local Mutex (`llock`)**: In-memory distributed mutex with same interface as Redis version
- **Local Rate Limiter (`llim`)**: In-memory rate limiting with same interface as Redis version
- **Factory Pattern**: Easy switching between Redis and local implementations
- **Full API Compatibility**: Drop-in replacement for Redis implementations

## Packages

### `llock` - Local Mutex

Provides distributed mutual exclusion locks using in-memory state management.

**Features:**
- Concurrent access control with configurable limits
- Read/Write lock types
- TTL-based automatic expiration
- Instance or all-instance unlocking
- Waiting and non-waiting modes

**Example:**
```go
import "github.com/n-h-n/go-lib/local/llock"

mutex := llock.NewMutex("service:resource", "app1",
    llock.WithVerbose(),
    llock.WithConcurrency(2),
    llock.WithTTL(10*time.Second),
)

ctx := context.Background()
if err := mutex.Lock(ctx); err != nil {
    log.Fatal(err)
}
defer mutex.Unlock(ctx)
```

### `llim` - Local Rate Limiter

Provides token bucket rate limiting using in-memory state management.

**Features:**
- Token bucket algorithm with configurable rate and burst
- Per-keyspace rate limiting
- Wait and Allow methods
- Realignment and waiting channel support

**Example:**
```go
import "github.com/n-h-n/go-lib/local/llim"

// 10 requests per second with burst of 20
limiter := llim.NewLimiter(
    llim.WithLocalLimiter("api:endpoint", 10, time.Second, 20),
)

ctx := context.Background()
if limiter.Allow(ctx) {
    // Process request
} else {
    // Rate limited
}

// Or wait for capacity
if err := limiter.Wait(ctx); err != nil {
    log.Fatal(err)
}
```

### Rate Limit Helpers

Similar to Redis version, provides convenient rate limit constructors:

```go
// Common rate limits
perSecLimit := llim.PerSecond(10, 20)
perMinLimit := llim.PerMinute(100, 150)
perHourLimit := llim.PerHour(1000, 1500)

limiter := llim.NewLimiterFromLimit("api:v2", perSecLimit)
```

### Factory Pattern

The factory allows easy switching between Redis and local implementations:

```go
import "github.com/n-h-n/go-lib/local"

// For local development
mutexFactory := local.NewMutexFactory(false) // useRedis = false
mutex := mutexFactory.NewMutex("service:resource", "app1")

limiterFactory := local.NewLimiterFactory(false) // useRedis = false
limiter := limiterFactory.NewLimiter("api:endpoint", 10, time.Second, 20)

// For production with Redis
redisClient := redis.NewClient(&redis.Options{...})
mutexFactory := local.NewMutexFactory(true, redisClient)
limiterFactory := local.NewLimiterFactory(true, redisClient)
```

## API Compatibility

Both `llock` and `llim` implement the same interfaces as their Redis counterparts:

### Mutex Interface
```go
type Mutex interface {
    Lock(context.Context) error
    Unlock(context.Context) error
}
```

### Limiter Interface
```go
type Limiter interface {
    Allow(ctx context.Context) bool
    Wait(ctx context.Context, opts ...waitOpt) error
    WaitN(ctx context.Context, n int, opts ...waitOpt) error
}
```

## Options

### Mutex Options

- `WithTTL(duration)` - Set lock expiration time
- `WithConcurrency(n)` - Set maximum concurrent holders
- `WithVerbose()` - Enable verbose logging
- `WithWait(bool)` - Enable/disable waiting for lock
- `WithWaitInterval(duration)` - Set wait retry interval
- `WithLockType(type)` - Set lock type (read/write)
- `WithUnlockType(type)` - Set unlock type (instance/all)

### Rate Limiter Options

- `WithLocalLimiter(keyspace, rate, period, burst)` - Configure rate limiter
- `WithWaitingChannel(chan)` - Set waiting notification channel
- `WithRemaining(n)` - Set remaining tokens for realignment
- `WithRealign(bool)` - Enable token realignment

## Thread Safety

Both implementations are thread-safe and can be used concurrently across multiple goroutines. State is shared appropriately:

- **Mutex**: State is shared per key across all instances
- **Rate Limiter**: State is shared per keyspace across all instances

## Use Cases

### Development Environment
```go
// Use local implementations when Redis is not available
if env.IsDevelopment() {
    mutex := llock.NewMutex("service:resource", appID)
    limiter := llim.NewLimiter(llim.WithLocalLimiter("api", 10, time.Second, 20))
}
```

### Testing
```go
func TestRateLimit(t *testing.T) {
    limiter := llim.NewLimiter(llim.WithLocalLimiter("test", 1, time.Second, 1))
    
    assert.True(t, limiter.Allow(ctx))  // First request allowed
    assert.False(t, limiter.Allow(ctx)) // Second request blocked
}
```

### Configuration-Based Switching
```go
func NewServices(config *Config) *Services {
    var mutex rlock.Mutex
    var limiter rlim.Limiter
    
    if config.UseRedis {
        mutex = rlock.NewMutex(config.RedisClient, "service:lock", appID)
        limiter = rlim.NewLimiter(rlim.WithDistributedLimiter(
            config.RedisClient, 
            rlim.PerSecond(10, 20), 
            "api",
        ))
    } else {
        mutex = llock.NewMutex("service:lock", appID)
        limiter = llim.NewLimiter(llim.WithLocalLimiter("api", 10, time.Second, 20))
    }
    
    return &Services{mutex: mutex, limiter: limiter}
}
```

## Limitations

### Local Mutex
- State is not persisted across application restarts
- Only works within a single process (not truly distributed)
- Memory usage grows with number of unique keys

### Local Rate Limiter
- State is not persisted across application restarts
- Only works within a single process (not truly distributed)
- Memory usage grows with number of unique keyspaces

These limitations make the local implementations suitable for development and testing, but not for production distributed systems.
