package sqs

import (
	"github.com/redis/go-redis/v9"

	"github.com/n-h-n/go-lib/aws/awslimit"
)

type clientOpt func(*Client) error
type queueOpt func(*queue) error
type messageOpt func(*mOpt) error

type mOpt struct {
	MaxNumberOfMessages int32
	WaitTimeSeconds     int32
	VisibilityTimeout   int32
}

// WithVerboseMode sets the verbose mode for the client.
func WithVerboseMode(verboseMode bool) clientOpt {
	return func(c *Client) error {
		c.verboseMode = verboseMode
		return nil
	}
}

// WithRedisRateLimit uses Valkey for a cluster-wide SQS API budget.
func WithRedisRateLimit(redisClient redis.UniversalClient, refresh func() redis.UniversalClient) clientOpt {
	return func(c *Client) error {
		if redisClient != nil {
			c.rateLimitOpts = append(c.rateLimitOpts, awslimit.WithRedis(redisClient, refresh))
		}
		return nil
	}
}

// WithRateLimitKeyPrefix prepends a service name to the SQS limiter key.
func WithRateLimitKeyPrefix(prefix string) clientOpt {
	return func(c *Client) error {
		if prefix != "" {
			c.rateLimitOpts = append(c.rateLimitOpts, awslimit.WithKeyPrefix(prefix))
		}
		return nil
	}
}

// Sets max number of messages returned in a single call to ReceiveMessages.
func WithMaxNumberOfMessages(maxNumberOfMessages int32) queueOpt {
	return func(q *queue) error {
		q.MaxNumberOfMessages = maxNumberOfMessages
		return nil
	}
}

// Sets the wait time for the queue.
func WithWaitTimeSeconds(waitTimeSeconds int32) queueOpt {
	return func(q *queue) error {
		q.WaitTimeSeconds = waitTimeSeconds
		return nil
	}
}

// Sets the visibility timeout for the queue.
func WithVisibilityTimeout(visibilityTimeout int32) queueOpt {
	return func(q *queue) error {
		q.VisibilityTimeout = visibilityTimeout
		return nil
	}
}
