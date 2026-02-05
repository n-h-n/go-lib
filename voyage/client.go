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
//
// Rate Limits (default):
// - 2,000 requests per minute
// - 3,000,000 tokens per minute
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

	// Elasticache client (optional, can be set after creation)
	ElasticacheClient *elasticache.Client
	VerboseMode       bool
}

// RateConfig defines rate limiting parameters
type RateConfig struct {
	RequestsPerSecond int
	BurstSize         int
	Keyspace          string // For Redis rate limiting
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

	// Create HTTP client with Bearer token authentication
	httpClient := &http.Client{
		Transport: &bearerTokenTransport{
			apiKey:    cfg.APIKey,
			transport: http.DefaultTransport,
		},
		Timeout: 60 * time.Second, // Embeddings can take longer for large batches
	}

	// Setup rate limiter
	// Note: Voyage doesn't return rate limit headers, so we use a fixed limiter
	var rateLimiter utils.RateLimiter
	if cfg.UseRedisRateLimit && cfg.RedisClient != nil {
		// Use Redis rate limiter for distributed rate limiting
		redisLimiter := rlim.NewLimiter(
			rlim.WithDistributedLimiter(
				cfg.RedisClient,
				rlim.PerSecond(cfg.RateLimit.RequestsPerSecond, cfg.RateLimit.BurstSize),
				cfg.RateLimit.Keyspace,
			),
		)
		rateLimiter = utils.NewRedisRateLimiterWrapper(redisLimiter, false)
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
	}

	client := &Client{
		apiKey:            cfg.APIKey,
		appID:             cfg.AppID,
		elasticacheClient: cfg.ElasticacheClient,
		redisClient:       cfg.RedisClient,
		ctx:               cfg.Ctx,
		httpClient:        httpClient,
		rateLimiter:       rateLimiter,
		baseURL:           cfg.BaseURL,
		model:             cfg.Model,
		verboseMode:       cfg.VerboseMode,
	}

	log.Log.Infof(cfg.Ctx, "Voyage client created with model: %s", cfg.Model)

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

	// Close the rate limiter if it has a Close method
	if closer, ok := c.rateLimiter.(interface{ Close() error }); ok {
		if err := closer.Close(); err != nil {
			return fmt.Errorf("failed to close rate limiter: %w", err)
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
