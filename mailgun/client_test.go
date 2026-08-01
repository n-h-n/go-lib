package mailgun

import (
	"context"
	"testing"
	"time"
)

func TestNewClientRequiresKeyAndDomain(t *testing.T) {
	t.Parallel()

	if _, err := NewClient(ClientConfig{Domain: "emrysprotocol.dev"}); err == nil {
		t.Fatal("expected error for missing API key")
	}
	if _, err := NewClient(ClientConfig{APIKey: "key-test"}); err == nil {
		t.Fatal("expected error for missing domain")
	}
}

func TestNewClientDefaultsAndAddresses(t *testing.T) {
	t.Parallel()

	c, err := NewClient(ClientConfig{
		APIKey: "key-test",
		Domain: "emrysprotocol.dev",
		Ctx:    context.Background(),
		RateLimit: RateConfig{
			Keyspace: "test:{mailgun:messages:api}",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })

	if c.Domain() != "emrysprotocol.dev" {
		t.Fatalf("domain=%q", c.Domain())
	}
	if got := c.FromNoReply(); got != "no-reply@emrysprotocol.dev" {
		t.Fatalf("FromNoReply=%q", got)
	}
	if got := c.FromNotifications(); got != "notifications@emrysprotocol.dev" {
		t.Fatalf("FromNotifications=%q", got)
	}
	if got := c.FromSupport(); got != "support@emrysprotocol.dev" {
		t.Fatalf("FromSupport=%q", got)
	}
}

func TestRateConfigDefaultsMatchDocumentedWindow(t *testing.T) {
	t.Parallel()

	if DefaultRateConfig.Period != DocumentedWindow {
		t.Fatalf("period=%s want %s", DefaultRateConfig.Period, DocumentedWindow)
	}
	if DefaultRateConfig.RequestsPerPeriod >= DocumentedRequestsPerWindow {
		t.Fatalf("default rate %d should leave headroom under %d",
			DefaultRateConfig.RequestsPerPeriod, DocumentedRequestsPerWindow)
	}
	if DefaultRateConfig.Period != 10*time.Second {
		t.Fatalf("unexpected period %s", DefaultRateConfig.Period)
	}
}

func TestSendValidation(t *testing.T) {
	t.Parallel()

	c, err := NewClient(ClientConfig{
		APIKey: "key-test",
		Domain: "emrysprotocol.dev",
		RateLimit: RateConfig{
			Keyspace: "test-send:{mailgun:messages:api}",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })

	ctx := context.Background()
	if _, err := c.Send(ctx, SendRequest{
		FromLocalPart: FromNoReply,
		Subject:       "hi",
		Text:          "body",
	}); err == nil {
		t.Fatal("expected error for missing To")
	}
	if _, err := c.Send(ctx, SendRequest{
		FromLocalPart: FromNoReply,
		To:            []string{"user@example.com"},
		Text:          "body",
	}); err == nil {
		t.Fatal("expected error for missing subject")
	}
	if _, err := c.Send(ctx, SendRequest{
		From:    "other@not-our-domain.com",
		To:      []string{"user@example.com"},
		Subject: "hi",
		Text:    "body",
	}); err == nil {
		t.Fatal("expected error for foreign From domain")
	}
}

func TestResolveFrom(t *testing.T) {
	t.Parallel()

	c := &Client{domain: "emrysprotocol.com"}
	got, err := c.resolveFrom(SendRequest{
		FromLocalPart: FromSupport,
		FromName:      "Emrys Support",
	})
	if err != nil {
		t.Fatal(err)
	}
	if got != "Emrys Support <support@emrysprotocol.com>" {
		t.Fatalf("got=%q", got)
	}
}
