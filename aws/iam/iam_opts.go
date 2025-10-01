package iam

import (
	"fmt"
	"time"
)

type iamClientOpt func(*iamClient) error

func WithSessionDuration(dur time.Duration) iamClientOpt {
	return func(c *iamClient) error {
		if dur > 1*time.Hour || dur < 15*time.Minute {
			return fmt.Errorf("session duration must be between 15 minutes and 1 hour, inclusive")
		}
		c.sessionDuration = dur
		return nil
	}
}

func WithSessionRefreshPercentage(percentage float64) iamClientOpt {
	return func(c *iamClient) error {
		if percentage < 0.0 || percentage > 1.0 {
			return fmt.Errorf("refresh percentage must be between 0.0 and 1.0")
		}
		c.refreshPercentage = percentage
		return nil
	}
}

func WithVerboseMode(v bool) iamClientOpt {
	return func(c *iamClient) error {
		c.verboseMode = v
		return nil
	}
}

func WithSessionName(sessionName string) iamClientOpt {
	return func(c *iamClient) error {
		c.sessionName = sessionName
		return nil
	}
}

type RefreshOpts struct {
	force bool
}

func WithForceRefresh(v bool) func(*RefreshOpts) {
	return func(o *RefreshOpts) {
		o.force = v
	}
}
