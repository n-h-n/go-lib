package server

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/go-redis/redis_rate/v10"

	"github.com/n-h-n/go-lib/aws/elasticache"
	"github.com/n-h-n/go-lib/redis/rlim"
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
	}

	for _, opt := range opts {
		opt(&o)
	}

	return func(c *gin.Context) {
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
			limiter := rlim.NewLimiter(
				rlim.WithDistributedLimiter(
					elasticacheClient.GetRedisClient(),
					rl,
					key,
				),
			)
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

type rateLimiterMiddlewareOptions struct {
	endpointCustomRLs      map[string]redis_rate.Limit
	endpointsToSkipRLCheck []string
	rateLimit              *redis_rate.Limit
	serviceName            string
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
