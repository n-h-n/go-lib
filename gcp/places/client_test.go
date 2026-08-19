package places

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"golang.org/x/oauth2"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

func TestNewClientRequiresAuth(t *testing.T) {
	t.Parallel()
	_, err := NewClient(ClientConfig{})
	if err == nil {
		t.Fatal("expected error without federation credentials")
	}
}

func TestAutocompleteUsesBearerTokenNotAPIKey(t *testing.T) {
	t.Parallel()

	var gotAuth, gotKey, gotMask string
	rt := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		gotAuth = r.Header.Get("Authorization")
		gotKey = r.Header.Get("X-Goog-Api-Key")
		gotMask = r.Header.Get("X-Goog-FieldMask")
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader(`{"suggestions":[]}`)),
			Header:     make(http.Header),
		}, nil
	})

	c, err := NewClient(ClientConfig{
		TokenSource: oauth2.StaticTokenSource(&oauth2.Token{AccessToken: "tok-abc", TokenType: "Bearer"}),
		HTTPClient:  &http.Client{Transport: rt},
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	if _, err := c.Autocomplete(context.Background(), AutocompleteRequest{
		Query: "123 Main",
		State: "WI",
	}); err != nil {
		t.Fatalf("Autocomplete: %v", err)
	}
	if gotAuth != "Bearer tok-abc" {
		t.Errorf("Authorization = %q, want Bearer tok-abc", gotAuth)
	}
	if gotKey != "" {
		t.Errorf("X-Goog-Api-Key = %q, want empty", gotKey)
	}
	if gotMask == "" {
		t.Error("X-Goog-FieldMask missing")
	}
}
