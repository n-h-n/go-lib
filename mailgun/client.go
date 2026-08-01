package mailgun

import (
	"context"
	"fmt"
	"strings"
	"time"

	mgsdk "github.com/mailgun/mailgun-go/v5"
	"github.com/redis/go-redis/v9"

	"github.com/n-h-n/go-lib/aws/elasticache"
	"github.com/n-h-n/go-lib/local/llim"
	"github.com/n-h-n/go-lib/log"
	"github.com/n-h-n/go-lib/redis/rlim"
	"github.com/n-h-n/go-lib/utils"
)

// =============================================================================
// Mailgun Sending Client
// =============================================================================
// Wraps the official Mailgun Go SDK (github.com/mailgun/mailgun-go/v5) with
// go-lib's canonical distributed (Redis) / local rate limiting.
//
// API reference: https://documentation.mailgun.com/docs/mailgun/api-reference/send/mailgun
// Rate limits:   https://documentation.mailgun.com/docs/mailgun/api-reference/api-overview
//
// Documented API rate limit: 500 requests every 10 seconds.
// Defaults below leave ~10% headroom so multi-pod senders stay under the cap.
//
// IMPORTANT — PHI:
// This client must NEVER be used to send Protected Health Information (PHI).
// Mailgun is outside the HIPAA BAA boundary for Emrys Protocol. Outbound mail
// is limited to non-PHI transactional content (account/ops notices, magic
// links, "you have a message" pointers with deep links — never clinical detail,
// patient identifiers in free text, or chart content). Callers are responsible
// for keeping message bodies and subjects free of PHI.
// =============================================================================

// Well-known local-parts used as From addresses on the verified sending domain.
const (
	FromNoReply       = "no-reply"
	FromNotifications = "notifications"
	FromSupport       = "support"
)

// Client wraps Mailgun send with authentication and rate limiting.
//
// Never send PHI through this client — see package comment.
type Client struct {
	apiKey            string
	domain            string
	mg                mgsdk.Mailgun
	elasticacheClient *elasticache.Client
	redisClient       redis.UniversalClient
	ctx               context.Context
	rateLimiter       utils.RateLimiter
	verboseMode       bool
}

// ClientConfig holds configuration for the Mailgun client.
type ClientConfig struct {
	// APIKey is the Mailgun sending / private API key.
	APIKey string
	// Domain is the verified Mailgun sending domain
	// (e.g. emrysprotocol.dev or emrysprotocol.com).
	Domain string

	Ctx context.Context

	// Rate limiting
	UseRedisRateLimit bool
	RedisClient       redis.UniversalClient
	RateLimit         RateConfig
	ElasticacheClient *elasticache.Client

	// APIBase overrides the Mailgun API endpoint (e.g. mgsdk.APIBaseEU).
	APIBase string

	VerboseMode bool
}

// RateConfig defines the proactive token-bucket budget for outbound Mailgun
// API calls. Mailgun documents 500 requests per 10-second window.
type RateConfig struct {
	RequestsPerPeriod int
	Period            time.Duration
	BurstSize         int
	Keyspace          string // Redis hash-tag key, e.g. {mailgun:messages:api}
}

// Documented Mailgun API window.
const (
	DocumentedRequestsPerWindow = 500
	DocumentedWindow            = 10 * time.Second
)

// DefaultRateConfig stays under Mailgun's published 500 req / 10s cap.
var DefaultRateConfig = RateConfig{
	RequestsPerPeriod: 450, // ~10% headroom vs documented 500
	Period:            DocumentedWindow,
	BurstSize:         450,
	Keyspace:          "{mailgun:messages:api}",
}

// NewClient creates a Mailgun sending client with rate limiting.
//
// Never send PHI through the returned client — see package comment.
func NewClient(cfg ClientConfig) (*Client, error) {
	if strings.TrimSpace(cfg.APIKey) == "" {
		return nil, fmt.Errorf("mailgun: API key is required")
	}
	if strings.TrimSpace(cfg.Domain) == "" {
		return nil, fmt.Errorf("mailgun: sending domain is required")
	}
	if cfg.Ctx == nil {
		cfg.Ctx = context.Background()
	}

	cfg.RateLimit = cfg.RateLimit.withDefaults()

	mg := mgsdk.NewMailgun(cfg.APIKey)
	if cfg.APIBase != "" {
		if err := mg.SetAPIBase(cfg.APIBase); err != nil {
			return nil, fmt.Errorf("mailgun: set API base: %w", err)
		}
	}

	rateLimiter, err := newRateLimiter(cfg)
	if err != nil {
		return nil, err
	}

	client := &Client{
		apiKey:            cfg.APIKey,
		domain:            cfg.Domain,
		mg:                mg,
		elasticacheClient: cfg.ElasticacheClient,
		redisClient:       cfg.RedisClient,
		ctx:               cfg.Ctx,
		rateLimiter:       rateLimiter,
		verboseMode:       cfg.VerboseMode,
	}

	log.Log.Infof(cfg.Ctx,
		"Mailgun client created (domain=%s, rate=%d/%s, burst=%d, redis=%t)",
		cfg.Domain,
		cfg.RateLimit.RequestsPerPeriod,
		cfg.RateLimit.Period,
		cfg.RateLimit.BurstSize,
		cfg.UseRedisRateLimit && cfg.RedisClient != nil,
	)

	return client, nil
}

func (r RateConfig) withDefaults() RateConfig {
	out := r
	if out.RequestsPerPeriod <= 0 {
		out.RequestsPerPeriod = DefaultRateConfig.RequestsPerPeriod
	}
	if out.Period <= 0 {
		out.Period = DefaultRateConfig.Period
	}
	if out.BurstSize <= 0 {
		out.BurstSize = out.RequestsPerPeriod
	}
	if out.Keyspace == "" {
		out.Keyspace = DefaultRateConfig.Keyspace
	}
	return out
}

func newRateLimiter(cfg ClientConfig) (utils.RateLimiter, error) {
	rate := cfg.RateLimit.withDefaults()

	if cfg.UseRedisRateLimit && cfg.RedisClient != nil {
		opts := []rlim.LimiterOpt{
			rlim.WithDistributedLimiter(
				cfg.RedisClient,
				rlim.PerPeriod(rate.RequestsPerPeriod, rate.BurstSize, rate.Period),
				rate.Keyspace,
			),
		}
		if cfg.ElasticacheClient != nil {
			opts = append(opts, rlim.WithClientRefresh(cfg.ElasticacheClient.RefreshAndGetRedisClient))
		}
		return utils.NewRedisRateLimiterWrapper(rlim.NewLimiter(opts...), false), nil
	}

	local := llim.NewLimiter(
		llim.WithLocalLimiter(
			rate.Keyspace,
			rate.RequestsPerPeriod,
			rate.Period,
			rate.BurstSize,
		),
	)
	return utils.NewLocalRateLimiterWrapper(local, false), nil
}

// Domain returns the verified sending domain.
func (c *Client) Domain() string {
	if c == nil {
		return ""
	}
	return c.domain
}

// Address builds an email address on the verified sending domain
// (e.g. Address("no-reply") → "no-reply@emrysprotocol.dev").
func (c *Client) Address(localPart string) string {
	localPart = strings.TrimSpace(localPart)
	if c == nil || localPart == "" || c.domain == "" {
		return ""
	}
	return localPart + "@" + c.domain
}

// FromNoReply returns no-reply@<domain>.
func (c *Client) FromNoReply() string { return c.Address(FromNoReply) }

// FromNotifications returns notifications@<domain>.
func (c *Client) FromNotifications() string { return c.Address(FromNotifications) }

// FromSupport returns support@<domain>.
func (c *Client) FromSupport() string { return c.Address(FromSupport) }

// SDK returns the underlying official Mailgun client for advanced operations
// that this wrapper does not cover. Prefer Send for ordinary outbound mail so
// rate limiting is applied. Never send PHI.
func (c *Client) SDK() mgsdk.Mailgun {
	if c == nil {
		return nil
	}
	return c.mg
}

// Close releases rate-limiter resources.
func (c *Client) Close() error {
	if c == nil {
		return nil
	}
	if closer, ok := c.rateLimiter.(interface{ Close() error }); ok {
		if err := closer.Close(); err != nil {
			return fmt.Errorf("mailgun: close rate limiter: %w", err)
		}
	}
	return nil
}

// SetElasticacheClient sets the Elasticache client (can be called after creation).
func (c *Client) SetElasticacheClient(ec *elasticache.Client) {
	if c == nil {
		return
	}
	c.elasticacheClient = ec
}

func (c *Client) waitRateLimit(ctx context.Context) error {
	if c == nil || c.rateLimiter == nil {
		return nil
	}
	if c.rateLimiter.Allow(ctx) {
		return nil
	}
	if err := c.rateLimiter.Wait(ctx); err != nil {
		return fmt.Errorf("mailgun: rate limit: %w", err)
	}
	return nil
}
