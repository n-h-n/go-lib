package voyage

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/n-h-n/go-lib/aws/elasticache"
	"github.com/n-h-n/go-lib/log"
	"github.com/redis/go-redis/v9"

	"github.com/n-h-n/go-lib/local/llim"
	"github.com/n-h-n/go-lib/redis/rlim"
	"github.com/n-h-n/go-lib/utils"
)

// =============================================================================
// Voyage AI Embeddings Client
// =============================================================================
// Voyage AI provides embedding models for semantic search and retrieval.
// API Reference: https://docs.voyageai.com/reference/embeddings-api
// Rate Limits:  https://docs.voyageai.com/docs/rate-limits
//
// Basic rate limits (Tier 1) vary by model:
//   Model                    RPM    TPM
//   voyage-4                 2000   8M
//   voyage-4-lite            2000   16M
//   voyage-4-large           2000   3M
//   voyage-3.5               2000   8M
//   voyage-3.5-lite          2000   16M
//   voyage-3-large           2000   3M
//   voyage-multimodal-3.5    2000   2M
//   voyage-multimodal-3      2000   2M
//
// Tiers 2 and 3 multiply these limits by 2x and 3x respectively.
//
// Note: Voyage API does not return rate limit headers, so we use fixed limits.
// =============================================================================

// Client wraps the Voyage AI API with authentication and rate limiting
type Client struct {
	apiKey            string
	appID             string
	httpClient        *http.Client
	elasticacheClient *elasticache.Client
	redisClient       redis.UniversalClient
	ctx               context.Context
	rateLimiter       utils.RateLimiter
	tokenRateLimiter  utils.RateLimiter // Tracks token consumption (model-dependent TPM)
	baseURL           string
	model             string
	verboseMode       bool
}

// ClientConfig holds configuration for the Voyage client
type ClientConfig struct {
	// Authentication
	APIKey string // Voyage API key for authentication
	AppID  string

	// API configuration
	BaseURL string // Voyage API base URL (defaults to https://api.voyageai.com)
	Model   string // Embedding model (defaults to voyage-4)

	Ctx context.Context

	// Rate limiting configuration
	UseRedisRateLimit bool
	RedisClient       redis.UniversalClient
	RateLimit         RateConfig
	TokenRateLimit    TokenRateConfig // Token-based rate limiting (optional)

	// Elasticache client (optional, can be set after creation)
	ElasticacheClient *elasticache.Client
	VerboseMode       bool
}

// RateConfig defines request-based rate limiting parameters
type RateConfig struct {
	RequestsPerSecond int
	BurstSize         int
	Keyspace          string // For Redis rate limiting
}

// TokenRateConfig defines token-based rate limiting parameters.
// Voyage AI enforces model-specific TPM (tokens per minute) limits alongside
// the request limit. Token consumption is tracked using actual usage from API
// responses, with character-based estimation (~5 chars/token) for pre-request gating.
//
// If TokensPerSecond is left at 0, NewClient will auto-select the correct limit
// based on the configured model using ModelTokensPerMinute.
type TokenRateConfig struct {
	TokensPerSecond int
	BurstSize       int
	Keyspace        string // For Redis rate limiting (should differ from request keyspace)
}

// Token estimation constants
const (
	// charsPerToken is the average characters per token for Voyage models.
	// Voyage docs state ~5 chars/token on average. We use 4.5 for a conservative
	// overestimate (~11% buffer) to avoid underestimating pre-request.
	charsPerToken = 4.5

	// conservativeTPMFactor applies a 10% reduction to the published TPM limit
	// to provide headroom and avoid hitting the exact boundary.
	conservativeTPMFactor = 0.90
)

// ModelTokensPerMinute maps Voyage model names to their Tier 1 basic TPM limits.
// Source: https://docs.voyageai.com/docs/rate-limits
//
// Tiers 2 (>=100 paid) and 3 (>=$1000 paid) multiply these by 2x and 3x.
// If using a higher tier, set TokenRateConfig.TokensPerSecond explicitly.
var ModelTokensPerMinute = map[string]int{
	// 8M TPM models
	"voyage-4":    8_000_000,
	"voyage-3.5":  8_000_000,

	// 16M TPM models
	"voyage-4-lite":   16_000_000,
	"voyage-3.5-lite": 16_000_000,
	"voyage-4-nano":   16_000_000,

	// 3M TPM models
	"voyage-4-large":   3_000_000,
	"voyage-3-large":   3_000_000,
	"voyage-context-3": 3_000_000,
	"voyage-code-3":    3_000_000,
	"voyage-3":         3_000_000,
	"voyage-code-2":    3_000_000,
	"voyage-finance-2": 3_000_000,
	"voyage-law-2":     3_000_000,
	"voyage-large-2":   3_000_000,

	// 2M TPM models
	"voyage-multimodal-3.5": 2_000_000,
	"voyage-multimodal-3":   2_000_000,
}

// tokenRateConfigForModel returns a TokenRateConfig with conservative defaults
// derived from the model's published TPM limit. Falls back to 3M TPM for unknown models.
func tokenRateConfigForModel(model string) TokenRateConfig {
	tpm, ok := ModelTokensPerMinute[model]
	if !ok {
		tpm = 3_000_000 // Conservative fallback for unknown models
	}

	// Apply conservative factor and convert TPM -> tokens/second
	effectiveTPM := int(float64(tpm) * conservativeTPMFactor)
	tps := effectiveTPM / 60

	return TokenRateConfig{
		TokensPerSecond: tps,
		BurstSize:       tps * 2, // Allow burst up to 2 seconds worth
		Keyspace:        "{voyage:embeddings:tokens}",
	}
}

// Default configurations
var (
	DefaultVoyageBaseURL = "https://api.voyageai.com"
	DefaultVoyageModel   = "voyage-4" // 1024 dimensions, best for retrieval

	// Voyage rate limits: 2000 requests/min = ~33 req/sec
	// Being slightly conservative with 30 req/sec default
	DefaultRateConfig = RateConfig{
		RequestsPerSecond: 30,
		BurstSize:         60, // Allow burst up to 2 seconds worth
		Keyspace:          "{voyage:embeddings:api}",
	}
)

// Embedding model dimensions
const (
	Voyage4Dimensions       = 1024
	Voyage4LiteDimensions   = 512
	Voyage4LargeDimensions  = 1536
	Voyage3Dimensions       = 1024
	Voyage3LiteDimensions   = 512
	VoyageLargeDimensions   = 1536
	VoyageFinanceDimensions = 1024
	VoyageCodeDimensions    = 1536
)

// NewClient creates a new Voyage AI client with authentication and rate limiting
func NewClient(cfg ClientConfig) (*Client, error) {
	if cfg.APIKey == "" {
		return nil, fmt.Errorf("API key is required")
	}

	if cfg.BaseURL == "" {
		cfg.BaseURL = DefaultVoyageBaseURL
	}

	if cfg.Model == "" {
		cfg.Model = DefaultVoyageModel
	}

	if cfg.RateLimit.RequestsPerSecond == 0 {
		cfg.RateLimit = DefaultRateConfig
	}

	// Auto-select token rate limit from model if not explicitly configured
	if cfg.TokenRateLimit.TokensPerSecond == 0 {
		cfg.TokenRateLimit = tokenRateConfigForModel(cfg.Model)
	}
	if cfg.TokenRateLimit.Keyspace == "" {
		cfg.TokenRateLimit.Keyspace = "{voyage:embeddings:tokens}"
	}

	// Create HTTP client with Bearer token authentication
	httpClient := &http.Client{
		Transport: &bearerTokenTransport{
			apiKey:    cfg.APIKey,
			transport: http.DefaultTransport,
		},
		Timeout: 60 * time.Second, // Embeddings can take longer for large batches
	}

	// Setup rate limiters
	// Note: Voyage doesn't return rate limit headers, so we use fixed limiters.
	// We maintain two limiters:
	//   1. Request rate limiter: gates by requests/second (2000 req/min)
	//   2. Token rate limiter: gates by tokens/second (model-dependent TPM)
	var rateLimiter utils.RateLimiter
	var tokenRateLimiter utils.RateLimiter
	if cfg.UseRedisRateLimit && cfg.RedisClient != nil {
		requestLimiterOpts := []rlim.LimiterOpt{
			rlim.WithDistributedLimiter(
				cfg.RedisClient,
				rlim.PerSecond(cfg.RateLimit.RequestsPerSecond, cfg.RateLimit.BurstSize),
				cfg.RateLimit.Keyspace,
			),
		}
		tokenLimiterOpts := []rlim.LimiterOpt{
			rlim.WithDistributedLimiter(
				cfg.RedisClient,
				rlim.PerSecond(cfg.TokenRateLimit.TokensPerSecond, cfg.TokenRateLimit.BurstSize),
				cfg.TokenRateLimit.Keyspace,
			),
		}
		if cfg.ElasticacheClient != nil {
			clientRefresh := rlim.WithClientRefresh(cfg.ElasticacheClient.RefreshAndGetRedisClient)
			requestLimiterOpts = append(requestLimiterOpts, clientRefresh)
			tokenLimiterOpts = append(tokenLimiterOpts, clientRefresh)
		}

		// Use Redis rate limiter for distributed rate limiting
		redisLimiter := rlim.NewLimiter(requestLimiterOpts...)
		rateLimiter = utils.NewRedisRateLimiterWrapper(redisLimiter, false)

		// Token rate limiter (distributed)
		redisTokenLimiter := rlim.NewLimiter(tokenLimiterOpts...)
		tokenRateLimiter = utils.NewRedisRateLimiterWrapper(redisTokenLimiter, false)
	} else {
		// Use local rate limiter (default)
		localLimiter := llim.NewLimiter(
			llim.WithLocalLimiter(
				cfg.RateLimit.Keyspace,
				cfg.RateLimit.RequestsPerSecond,
				time.Second,
				cfg.RateLimit.BurstSize,
			),
		)
		rateLimiter = utils.NewLocalRateLimiterWrapper(localLimiter, false)

		// Token rate limiter (local)
		localTokenLimiter := llim.NewLimiter(
			llim.WithLocalLimiter(
				cfg.TokenRateLimit.Keyspace,
				cfg.TokenRateLimit.TokensPerSecond,
				time.Second,
				cfg.TokenRateLimit.BurstSize,
			),
		)
		tokenRateLimiter = utils.NewLocalRateLimiterWrapper(localTokenLimiter, false)
	}

	client := &Client{
		apiKey:            cfg.APIKey,
		appID:             cfg.AppID,
		elasticacheClient: cfg.ElasticacheClient,
		redisClient:       cfg.RedisClient,
		ctx:               cfg.Ctx,
		httpClient:        httpClient,
		rateLimiter:       rateLimiter,
		tokenRateLimiter:  tokenRateLimiter,
		baseURL:           cfg.BaseURL,
		model:             cfg.Model,
		verboseMode:       cfg.VerboseMode,
	}

	log.Log.Infof(cfg.Ctx, "Voyage client created with model: %s (token limit: %d tokens/sec, burst: %d)",
		cfg.Model, cfg.TokenRateLimit.TokensPerSecond, cfg.TokenRateLimit.BurstSize)

	return client, nil
}

// bearerTokenTransport implements http.RoundTripper to add Bearer token authentication
type bearerTokenTransport struct {
	apiKey    string
	transport http.RoundTripper
}

func (t *bearerTokenTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	// Clone the request to avoid modifying the original
	reqClone := req.Clone(req.Context())

	// Add required headers for Voyage API
	reqClone.Header.Set("Authorization", fmt.Sprintf("Bearer %s", t.apiKey))
	reqClone.Header.Set("Content-Type", "application/json")

	// Use the underlying transport
	return t.transport.RoundTrip(reqClone)
}

// makeRequest wraps API calls with rate limiting
func (c *Client) makeRequest(req *http.Request) (*http.Response, error) {
	// Apply rate limiting
	if !c.rateLimiter.Allow(c.ctx) {
		// If not allowed, wait for capacity
		if err := c.rateLimiter.Wait(c.ctx); err != nil {
			return nil, fmt.Errorf("rate limit exceeded: %w", err)
		}
	}

	// Execute the actual API call
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("API request failed: %w", err)
	}

	// Log for debugging
	if c.verboseMode {
		log.Log.Debugf(c.ctx, "Voyage API request completed with status: %d", resp.StatusCode)
	}

	return resp, nil
}

// GetHTTPClient returns the configured HTTP client for custom requests
func (c *Client) GetHTTPClient() *http.Client {
	return c.httpClient
}

// GetBaseURL returns the configured base URL
func (c *Client) GetBaseURL() string {
	return c.baseURL
}

// GetModel returns the configured embedding model
func (c *Client) GetModel() string {
	return c.model
}

// SetModel changes the embedding model
func (c *Client) SetModel(model string) {
	c.model = model
}

// Close cleans up resources used by the client
func (c *Client) Close() error {
	// Close the HTTP client's transport if it has a Close method
	if transport, ok := c.httpClient.Transport.(interface{ Close() error }); ok {
		if err := transport.Close(); err != nil {
			return fmt.Errorf("failed to close HTTP transport: %w", err)
		}
	}

	// Close the request rate limiter if it has a Close method
	if closer, ok := c.rateLimiter.(interface{ Close() error }); ok {
		if err := closer.Close(); err != nil {
			return fmt.Errorf("failed to close rate limiter: %w", err)
		}
	}

	// Close the token rate limiter if it has a Close method
	if closer, ok := c.tokenRateLimiter.(interface{ Close() error }); ok {
		if err := closer.Close(); err != nil {
			return fmt.Errorf("failed to close token rate limiter: %w", err)
		}
	}

	return nil
}

// UpdateAPIKey updates the API key for authentication
func (c *Client) UpdateAPIKey(newKey string) error {
	if newKey == "" {
		return fmt.Errorf("API key cannot be empty")
	}

	c.apiKey = newKey

	// Update the HTTP client transport with the new key
	c.httpClient.Transport = &bearerTokenTransport{
		apiKey:    newKey,
		transport: http.DefaultTransport,
	}

	return nil
}

// SetElasticacheClient sets the Elasticache client (can be called after creation)
func (c *Client) SetElasticacheClient(ec *elasticache.Client) {
	c.elasticacheClient = ec
}
