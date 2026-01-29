package server

import (
	"fmt"
	"net/http"
	"strings"
	"sync"

	"github.com/gin-gonic/gin"
	"github.com/go-redis/redis_rate/v10"
	"github.com/redis/go-redis/v9"

	"github.com/n-h-n/go-lib/aws/elasticache"
	"github.com/n-h-n/go-lib/local/llim"
	"github.com/n-h-n/go-lib/log"
	"github.com/n-h-n/go-lib/redis/rlim"
	"github.com/n-h-n/go-lib/utils"
)

func GinRateLimiterMiddleware(
	elasticacheClient *elasticache.Client,
	opts ...func(*rateLimiterMiddlewareOptions),
) gin.HandlerFunc {
	defaultLim := rlim.PerSecond(50, 50)
	o := rateLimiterMiddlewareOptions{
		endpointCustomRLs:      make(map[string]redis_rate.Limit),
		endpointsToSkipRLCheck: []string{},
		rateLimit:              &defaultLim,
		serviceName:            "",
		useRedisRateLimit:      elasticacheClient != nil,
		redisClient:            nil,
	}

	// Extract redisClient from elasticacheClient if available
	if elasticacheClient != nil {
		o.redisClient = elasticacheClient.GetRedisClient()
	}

	for _, opt := range opts {
		opt(&o)
	}

	// Limiter cache to avoid recreating limiters on every request
	limiterCache := &limiterCache{
		limiters: make(map[string]utils.RateLimiter),
		mu:       sync.RWMutex{},
	}

	return func(c *gin.Context) {
		// log endpoint and client ip
		log.Log.Infof(c.Request.Context(), "http request received: endpoint: %s, client ip: %s", c.Request.URL.Path, c.ClientIP())

		// Skip rate limit check for specified endpoints
		for _, endpoint := range o.endpointsToSkipRLCheck {
			if strings.HasPrefix(c.Request.URL.Path, endpoint) {
				c.Next()
				return
			}
		}

		// Get rate limit for endpoint
		var rl redis_rate.Limit
		if v, exists := o.endpointCustomRLs[c.Request.URL.Path]; exists {
			rl = v
		} else {
			rl = *o.rateLimit
		}

		// Generate rate limit keys
		ip := c.ClientIP()
		userAgent := c.Request.UserAgent()
		keys := []string{
			fmt.Sprintf("%s:{%s:%s:%s}", o.serviceName, c.Request.URL.Path, ip, userAgent),
			fmt.Sprintf("%s:{%s:%s}", o.serviceName, c.Request.URL.Path, ip),
		}

		// Check rate limits
		for _, key := range keys {
			limiter := limiterCache.getOrCreateLimiter(key, rl, o.useRedisRateLimit, o.redisClient)
			if !limiter.Allow(c.Request.Context()) {
				c.AbortWithStatusJSON(
					http.StatusTooManyRequests,
					gin.H{
						"error":      "too many requests",
						"status":     http.StatusTooManyRequests,
						"endpoint":   c.Request.URL.Path,
						"rate_limit": rl,
						"client_ip":  ip,
						"user_agent": userAgent,
					},
				)
				return
			}
		}

		c.Next()
	}
}

// limiterCache caches rate limiters per key to avoid recreating them
type limiterCache struct {
	limiters map[string]utils.RateLimiter
	mu       sync.RWMutex
}

func (lc *limiterCache) getOrCreateLimiter(
	key string,
	limit redis_rate.Limit,
	useRedis bool,
	redisClient redis.UniversalClient,
) utils.RateLimiter {
	// Try to get existing limiter
	lc.mu.RLock()
	if limiter, exists := lc.limiters[key]; exists {
		lc.mu.RUnlock()
		return limiter
	}
	lc.mu.RUnlock()

	// Create new limiter
	lc.mu.Lock()
	defer lc.mu.Unlock()

	// Double-check after acquiring write lock
	if limiter, exists := lc.limiters[key]; exists {
		return limiter
	}

	var limiter utils.RateLimiter

	if useRedis && redisClient != nil {
		// Use Redis-based distributed rate limiting
		redisLimiter := rlim.NewLimiter(
			rlim.WithDistributedLimiter(
				redisClient,
				limit,
				key,
			),
		)
		limiter = utils.NewRedisRateLimiterWrapper(redisLimiter, false)
	} else {
		// Use local in-memory rate limiting
		// Convert redis_rate.Limit to local limiter parameters
		rate := limit.Rate
		period := limit.Period
		burst := limit.Burst
		if burst == 0 {
			burst = rate // Default burst to rate if not set
		}

		localLimiter := llim.NewLimiter(
			llim.WithLocalLimiter(
				key,
				rate,
				period,
				burst,
			),
		)
		limiter = utils.NewLocalRateLimiterWrapper(localLimiter, false)
	}

	lc.limiters[key] = limiter
	return limiter
}

type rateLimiterMiddlewareOptions struct {
	endpointCustomRLs      map[string]redis_rate.Limit
	endpointsToSkipRLCheck []string
	rateLimit              *redis_rate.Limit
	serviceName            string
	useRedisRateLimit      bool
	redisClient            redis.UniversalClient
}

func WithGinRLMWEndpointCustomRLs(rls map[string]redis_rate.Limit) func(*rateLimiterMiddlewareOptions) {
	return func(o *rateLimiterMiddlewareOptions) {
		o.endpointCustomRLs = rls
	}
}

func WithGinRLMWEndpointsToSkipRLCheck(endpoints []string) func(*rateLimiterMiddlewareOptions) {
	return func(o *rateLimiterMiddlewareOptions) {
		o.endpointsToSkipRLCheck = endpoints
	}
}

func WithGinRLMWRateLimit(rl *redis_rate.Limit) func(*rateLimiterMiddlewareOptions) {
	return func(o *rateLimiterMiddlewareOptions) {
		o.rateLimit = rl
	}
}

func WithGinRLMWServiceName(serviceName string) func(*rateLimiterMiddlewareOptions) {
	return func(o *rateLimiterMiddlewareOptions) {
		o.serviceName = serviceName
	}
}

// WithGinRLMWRedisClient allows specifying a Redis client directly
// If not provided, will use elasticacheClient.GetRedisClient() if available
func WithGinRLMWRedisClient(redisClient redis.UniversalClient) func(*rateLimiterMiddlewareOptions) {
	return func(o *rateLimiterMiddlewareOptions) {
		o.redisClient = redisClient
		o.useRedisRateLimit = redisClient != nil
	}
}

// WithGinRLMWUseLocalRateLimit forces the use of local rate limiting
// even if Redis/Elasticache is available
func WithGinRLMWUseLocalRateLimit() func(*rateLimiterMiddlewareOptions) {
	return func(o *rateLimiterMiddlewareOptions) {
		o.useRedisRateLimit = false
	}
}
