// Package places is a Places API (New) client for address autocomplete.
//
// Autocomplete requests and Place Details are rate-limited with the same
// local (llim) / distributed (rlim) token-bucket used by mailgun and
// ops-daemon outbound Google clients.
package places

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/n-h-n/go-lib/aws/elasticache"
	"github.com/n-h-n/go-lib/log"
	"github.com/n-h-n/go-lib/utils"
)

const (
	autocompleteURL = "https://places.googleapis.com/v1/places:autocomplete"
	placeURLFmt     = "https://places.googleapis.com/v1/places/%s"

	autocompleteFieldMask = "suggestions.placePrediction.placeId,suggestions.placePrediction.text,suggestions.placePrediction.structuredFormat"
	detailsFieldMask      = "id,formattedAddress,addressComponents"
)

// ClientConfig holds construction options for Client.
type ClientConfig struct {
	APIKey string
	Ctx    context.Context

	UseRedisRateLimit bool
	RedisClient       redis.UniversalClient
	ElasticacheClient *elasticache.Client
	AutocompleteRate  RateConfig
	DetailsRate       RateConfig

	HTTPClient  *http.Client
	VerboseMode bool
}

// Client calls Places API (New) Autocomplete and Place Details.
type Client struct {
	apiKey            string
	http              *http.Client
	ctx               context.Context
	verboseMode       bool
	autocompleteLimit utils.RateLimiter
	detailsLimit      utils.RateLimiter
}

// NewClient builds a rate-limited Places client. APIKey is required.
func NewClient(cfg ClientConfig) (*Client, error) {
	if strings.TrimSpace(cfg.APIKey) == "" {
		return nil, fmt.Errorf("places: API key is required")
	}
	if cfg.Ctx == nil {
		cfg.Ctx = context.Background()
	}
	httpClient := cfg.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 8 * time.Second}
	}

	acRate := cfg.AutocompleteRate.withDefaults(DefaultAutocompleteRate)
	detRate := cfg.DetailsRate.withDefaults(DefaultDetailsRate)

	c := &Client{
		apiKey:            strings.TrimSpace(cfg.APIKey),
		http:              httpClient,
		ctx:               cfg.Ctx,
		verboseMode:       cfg.VerboseMode,
		autocompleteLimit: newRateLimiter(acRate, cfg.UseRedisRateLimit, cfg.RedisClient, cfg.ElasticacheClient),
		detailsLimit:      newRateLimiter(detRate, cfg.UseRedisRateLimit, cfg.RedisClient, cfg.ElasticacheClient),
	}
	if cfg.VerboseMode {
		log.Log.Debugf(cfg.Ctx, "places client created (autocomplete=%d/s details=%d/s redis=%t)",
			acRate.RequestsPerSecond, detRate.RequestsPerSecond,
			cfg.UseRedisRateLimit && cfg.RedisClient != nil)
	}
	return c, nil
}

func (c *Client) wait(ctx context.Context, lim utils.RateLimiter) error {
	if c == nil || lim == nil {
		return nil
	}
	if lim.Allow(ctx) {
		return nil
	}
	if err := lim.Wait(ctx); err != nil {
		return fmt.Errorf("places: rate limit: %w", err)
	}
	return nil
}

func (c *Client) doJSON(ctx context.Context, method, rawURL string, fieldMask string, body any, dest any) error {
	var reader io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("places: marshal: %w", err)
		}
		reader = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, rawURL, reader)
	if err != nil {
		return fmt.Errorf("places: request: %w", err)
	}
	req.Header.Set("X-Goog-Api-Key", c.apiKey)
	req.Header.Set("X-Goog-FieldMask", fieldMask)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	res, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("places: do: %w", err)
	}
	defer res.Body.Close()
	payload, err := io.ReadAll(io.LimitReader(res.Body, 1<<20))
	if err != nil {
		return fmt.Errorf("places: read: %w", err)
	}
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return fmt.Errorf("places: http %d: %s", res.StatusCode, strings.TrimSpace(string(payload)))
	}
	if dest == nil || len(payload) == 0 {
		return nil
	}
	if err := json.Unmarshal(payload, dest); err != nil {
		return fmt.Errorf("places: decode: %w", err)
	}
	return nil
}

func escapePlaceID(placeID string) string {
	return url.PathEscape(strings.TrimPrefix(strings.TrimSpace(placeID), "places/"))
}
