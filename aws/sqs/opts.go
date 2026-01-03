package sqs

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
