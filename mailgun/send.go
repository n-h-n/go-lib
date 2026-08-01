package mailgun

import (
	"context"
	"fmt"
	"strings"
	"time"

	mgsdk "github.com/mailgun/mailgun-go/v5"
	"github.com/mailgun/mailgun-go/v5/mtypes"

	"github.com/n-h-n/go-lib/log"
)

// SendRequest is a non-PHI transactional email to queue via Mailgun.
//
// Never include PHI in Subject, Text, HTML, FromName, or recipient-facing
// content. Prefer deep links / opaque tokens over identifying detail.
type SendRequest struct {
	// FromLocalPart is the mailbox on the verified domain
	// (e.g. FromNoReply, FromNotifications, FromSupport). Required unless
	// From is set to a full address.
	FromLocalPart string
	// From is an optional full From address. When set, it must be on the
	// client's verified Domain(). Prefer FromLocalPart for the common cases.
	From string
	// FromName is an optional display name ("Emrys Protocol").
	FromName string

	To       []string
	CC       []string
	BCC      []string
	ReplyTo  string
	Subject  string
	Text     string
	HTML     string
	Tags     []string
	Headers  map[string]string
	Timeout  time.Duration // defaults to 10s
}

// SendResult is Mailgun's acceptance response for a queued message.
type SendResult struct {
	ID      string
	Message string
}

// Send queues a message for delivery. Rate-limited per Mailgun's documented
// API caps. Never send PHI — see package comment and SendRequest docs.
func (c *Client) Send(ctx context.Context, req SendRequest) (*SendResult, error) {
	if c == nil {
		return nil, fmt.Errorf("mailgun: client is nil")
	}
	if ctx == nil {
		ctx = c.ctx
	}

	from, err := c.resolveFrom(req)
	if err != nil {
		return nil, err
	}
	if len(req.To) == 0 {
		return nil, fmt.Errorf("mailgun: at least one To recipient is required")
	}
	if strings.TrimSpace(req.Subject) == "" {
		return nil, fmt.Errorf("mailgun: subject is required")
	}
	if strings.TrimSpace(req.Text) == "" && strings.TrimSpace(req.HTML) == "" {
		return nil, fmt.Errorf("mailgun: text or HTML body is required")
	}

	if err := c.waitRateLimit(ctx); err != nil {
		return nil, err
	}

	msg := mgsdk.NewMessage(c.domain, from, req.Subject, req.Text, req.To...)
	if req.HTML != "" {
		msg.SetHTML(req.HTML)
	}
	for _, cc := range req.CC {
		msg.AddCC(cc)
	}
	for _, bcc := range req.BCC {
		msg.AddBCC(bcc)
	}
	if req.ReplyTo != "" {
		msg.SetReplyTo(req.ReplyTo)
	}
	if len(req.Tags) > 0 {
		if err := msg.AddTag(req.Tags...); err != nil {
			return nil, fmt.Errorf("mailgun: add tag: %w", err)
		}
	}
	for k, v := range req.Headers {
		msg.AddHeader(k, v)
	}

	timeout := req.Timeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	sendCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	resp, err := c.mg.Send(sendCtx, msg)
	if err != nil {
		return nil, fmt.Errorf("mailgun: send: %w", err)
	}

	if c.verboseMode {
		log.Log.Debugf(ctx, "Mailgun send accepted id=%s message=%s", resp.ID, resp.Message)
	}

	return sendResultFrom(resp), nil
}

func (c *Client) resolveFrom(req SendRequest) (string, error) {
	from := strings.TrimSpace(req.From)
	if from == "" {
		local := strings.TrimSpace(req.FromLocalPart)
		if local == "" {
			return "", fmt.Errorf("mailgun: FromLocalPart or From is required")
		}
		from = c.Address(local)
		if from == "" {
			return "", fmt.Errorf("mailgun: could not build From address")
		}
	} else if !strings.HasSuffix(strings.ToLower(from), "@"+strings.ToLower(c.domain)) {
		// Allow "Name <local@domain>" by checking containment.
		if !strings.Contains(strings.ToLower(from), "@"+strings.ToLower(c.domain)) {
			return "", fmt.Errorf("mailgun: From address must be on verified domain %q", c.domain)
		}
	}

	if name := strings.TrimSpace(req.FromName); name != "" {
		// Avoid double-wrapping if caller already supplied a display name.
		if !strings.Contains(from, "<") {
			from = fmt.Sprintf("%s <%s>", name, from)
		}
	}
	return from, nil
}

func sendResultFrom(resp mtypes.SendMessageResponse) *SendResult {
	return &SendResult{
		ID:      resp.ID,
		Message: resp.Message,
	}
}
