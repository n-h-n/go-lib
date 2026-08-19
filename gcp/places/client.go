// Package places is a Places API (New) client for address autocomplete.
//
// Autocomplete requests and Place Details are rate-limited with the same
// local (llim) / distributed (rlim) token-bucket used by mailgun and
// ops-daemon outbound Google clients.
//
// Auth is AWS→GCP workload identity federation: an oauth2.TokenSource from
// iam.IdentityFederationClient, refreshed as the impersonated service-account
// token nears expiry. Places API (New) accepts that as Authorization: Bearer.
// Do not send an API key when using OAuth.
package places

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"golang.org/x/oauth2"

	"github.com/n-h-n/go-lib/aws/elasticache"
	"github.com/n-h-n/go-lib/gcp/iam"
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
	Ctx context.Context

	// IAMClient supplies a federated GCP access token. NewClient does not
	// Close it unless it created the client itself (from GCPProjectID /
	// GCPProjectNumber or env).
	IAMClient *iam.IdentityFederationClient
	// TokenSource overrides IAMClient. Tests can pass oauth2.StaticTokenSource.
	TokenSource oauth2.TokenSource
	// GCPProjectID / GCPProjectNumber are used to construct an
	// IdentityFederationClient when IAMClient and TokenSource are both nil.
	// Empty falls back to GCP_PROJECT_ID / GCP_PROJECT_NUMBER.
	GCPProjectID     string
	GCPProjectNumber string
	IAMOptions       []iam.Option

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
	http              *http.Client
	ctx               context.Context
	verboseMode       bool
	autocompleteLimit utils.RateLimiter
	detailsLimit      utils.RateLimiter
	iamClient         *iam.IdentityFederationClient
	ownsIAM           bool
}

// NewClient builds a rate-limited Places client authenticated with a federated
// GCP OAuth token (IdentityFederationClient.GetTokenSource).
func NewClient(cfg ClientConfig) (*Client, error) {
	if cfg.Ctx == nil {
		cfg.Ctx = context.Background()
	}

	ts, iamClient, ownsIAM, err := resolveTokenSource(cfg)
	if err != nil {
		return nil, err
	}
	if _, err := ts.Token(); err != nil {
		if ownsIAM && iamClient != nil {
			_ = iamClient.Close(cfg.Ctx)
		}
		return nil, fmt.Errorf("places: token source: %w", err)
	}

	httpClient := wrapHTTPClient(cfg.HTTPClient, ts)

	acRate := cfg.AutocompleteRate.withDefaults(DefaultAutocompleteRate)
	detRate := cfg.DetailsRate.withDefaults(DefaultDetailsRate)

	c := &Client{
		http:              httpClient,
		ctx:               cfg.Ctx,
		verboseMode:       cfg.VerboseMode,
		autocompleteLimit: newRateLimiter(acRate, cfg.UseRedisRateLimit, cfg.RedisClient, cfg.ElasticacheClient),
		detailsLimit:      newRateLimiter(detRate, cfg.UseRedisRateLimit, cfg.RedisClient, cfg.ElasticacheClient),
		iamClient:         iamClient,
		ownsIAM:           ownsIAM,
	}
	if cfg.VerboseMode {
		log.Log.Debugf(cfg.Ctx, "places client created (autocomplete=%d/s details=%d/s redis=%t federation=%t)",
			acRate.RequestsPerSecond, detRate.RequestsPerSecond,
			cfg.UseRedisRateLimit && cfg.RedisClient != nil,
			iamClient != nil)
	}
	return c, nil
}

func resolveTokenSource(cfg ClientConfig) (oauth2.TokenSource, *iam.IdentityFederationClient, bool, error) {
	if cfg.TokenSource != nil {
		return cfg.TokenSource, cfg.IAMClient, false, nil
	}
	if cfg.IAMClient != nil {
		ts, err := cfg.IAMClient.GetTokenSource(cfg.Ctx)
		if err != nil {
			return nil, nil, false, fmt.Errorf("places: token source: %w", err)
		}
		return ts, cfg.IAMClient, false, nil
	}

	projectID := strings.TrimSpace(cfg.GCPProjectID)
	projectNum := strings.TrimSpace(cfg.GCPProjectNumber)
	if projectID == "" {
		projectID = strings.TrimSpace(os.Getenv(iam.GCP_PROJECT_ID))
	}
	if projectNum == "" {
		projectNum = strings.TrimSpace(os.Getenv(iam.GCP_PROJECT_NUMBER))
	}
	if projectID == "" || projectNum == "" {
		return nil, nil, false, fmt.Errorf("places: IAM federation requires GCP project ID and number (or IAMClient / TokenSource)")
	}

	opts := append([]iam.Option{iam.WithVerbose(cfg.VerboseMode)}, cfg.IAMOptions...)
	iamClient, err := iam.NewIdentityFederationClient(cfg.Ctx, projectID, projectNum, opts...)
	if err != nil {
		return nil, nil, false, fmt.Errorf("places: identity federation: %w", err)
	}
	ts, err := iamClient.GetTokenSource(cfg.Ctx)
	if err != nil {
		_ = iamClient.Close(cfg.Ctx)
		return nil, nil, false, fmt.Errorf("places: token source: %w", err)
	}
	return ts, iamClient, true, nil
}

func wrapHTTPClient(base *http.Client, ts oauth2.TokenSource) *http.Client {
	if base == nil {
		base = &http.Client{Timeout: 8 * time.Second}
	}
	clone := *base
	if clone.Timeout == 0 {
		clone.Timeout = 8 * time.Second
	}
	transport := clone.Transport
	if transport == nil {
		transport = http.DefaultTransport
	}
	clone.Transport = &oauth2.Transport{Source: ts, Base: transport}
	return &clone
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

// Close stops a federation client NewClient created. A caller-supplied
// IAMClient is left running.
func (c *Client) Close() error {
	if c == nil {
		return nil
	}
	if c.ownsIAM && c.iamClient != nil {
		return c.iamClient.Close(context.Background())
	}
	return nil
}
