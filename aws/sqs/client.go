package sqs

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"reflect"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/n-h-n/go-lib/aws/iam"
	"github.com/n-h-n/go-lib/log"
)

func NewClient(ctx context.Context, clientOptions ...clientOpt) (*Client, error) {
	c := &Client{
		mtx:    &sync.RWMutex{},
		queues: &map[string]*queue{},
		ctx:    ctx,
	}

	for _, option := range clientOptions {
		err := option(c)
		if err != nil {
			return nil, err
		}
	}

	iamClient, err := iam.NewIAMClient(ctx, iam.WithVerboseMode(c.verboseMode), iam.WithSessionDuration(1*time.Hour))
	if err != nil {
		return nil, err
	}
	c.iamClient = iamClient

	if err := c.newSQSClient(); err != nil {
		return nil, err
	}

	go c.runPeriodicRefresh()

	return c, nil
}

func (c *Client) newSQSClient() error {
	stsCreds := c.iamClient.GetAssumedRole().Credentials
	awsCredsProvider := credentials.NewStaticCredentialsProvider(
		*stsCreds.AccessKeyId,
		*stsCreds.SecretAccessKey,
		*stsCreds.SessionToken,
	)

	awsConfig := c.iamClient.GetAWSConfig()

	sqsClient := sqs.New(sqs.Options{
		Credentials:        awsCredsProvider,
		Region:             awsConfig.Region,
		HTTPClient:         awsConfig.HTTPClient,
		DefaultsMode:       awsConfig.DefaultsMode,
		Logger:             awsConfig.Logger,
		RuntimeEnvironment: awsConfig.RuntimeEnvironment,
		APIOptions:         awsConfig.APIOptions,
		ClientLogMode:      awsConfig.ClientLogMode,
		AppID:              awsConfig.AppID,
	})

	c.sqsClient = sqsClient

	c.mtx.Lock()
	defer c.mtx.Unlock()
	for _, q := range *c.queues {
		q.SQSClient = sqsClient
	}

	return nil
}

func (c *Client) refreshClientCredentials() error {
	if c.iamClient.GetSessionTimeRemaining().Seconds() > c.iamClient.GetSessionDuration().Seconds()*c.iamClient.GetRefreshPercentage() {
		// Greater than the refreshAtPercentageRemaining of the session duration remaining, no need to refresh
		if c.verboseMode {
			log.Log.Debugf(
				c.ctx,
				"skipping SQS client refresh, session token time remaining: %v, time remaining until refresh: %v",
				c.iamClient.GetSessionTimeRemaining(),
				c.iamClient.GetSessionTimeRemaining()-time.Duration(c.iamClient.GetSessionDuration().Seconds()*c.iamClient.GetRefreshPercentage())*time.Second,
			)
		}
		return nil
	}

	if c.verboseMode {
		log.Log.Debugf(c.ctx, "refreshing SQS client connection")
	}

	if err := c.iamClient.RefreshAWSCreds(c.ctx); err != nil {
		return err
	}

	if err := c.newSQSClient(); err != nil {
		return err
	}

	return nil
}

func (c *Client) runPeriodicRefresh() {
	minMilliseconds := 58000
	maxMilliSeconds := 62000
	interval := rand.Intn(maxMilliSeconds-minMilliseconds) + minMilliseconds

	ticker := time.NewTicker(time.Duration(interval) * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := c.refreshClientCredentials(); err != nil {
				log.Log.Errorf(c.ctx, "failed to refresh SQS client: %v", err)
			}
		case <-c.ctx.Done():
			return
		}
	}
}

func (c *Client) AddQueue(id, url string, opts ...queueOpt) *queue {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	q := &queue{
		URL:                 url,
		MaxNumberOfMessages: 1,
		SQSClient:           c.sqsClient,
		WaitTimeSeconds:     10,
		VisibilityTimeout:   5,
	}

	for _, opt := range opts {
		opt(q)
	}

	(*c.queues)[id] = q

	return q
}

func (c *Client) GetQueue(id string) *queue {
	c.mtx.RLock()
	defer c.mtx.RUnlock()

	return (*c.queues)[id]
}

func (c *Client) RemoveQueue(id string) {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	delete(*c.queues, id)
}

func (q *queue) SendMessage(ctx context.Context, message Message) (messageID string, err error) {
	if hasRequiredFields := MessageHasValidFields(message); !hasRequiredFields {
		return "", fmt.Errorf("message structs must have fields with JSON tags 'type' and 'strategy' with corresponding types MessageType and MessageStrategy")
	}

	// marshal message to JSON
	js, err := json.Marshal(message)
	if err != nil {
		return "", fmt.Errorf("could not marshal message to JSON in SendMessage: %w", err)
	}

	messageBody := string(js)
	resp, err := q.SQSClient.SendMessage(ctx, &sqs.SendMessageInput{
		MessageBody: &messageBody,
		QueueUrl:    &q.URL,
	})
	if err != nil {
		return "", err
	}

	messageID = *resp.MessageId
	return
}

// ReceiveMessage receives messages from the queue. If the message strategy is DeleteAfterReceive, the message is deleted after receiving.
// If there's an error deleting the message, the message will still be returned if received successfully.
// If the message strategy is DeleteAfterProcess, the message is deleted after processing and must be deleted manually via DeleteMessage.
// If the message strategy is DoNotDelete, the message is not deleted.
func (q *queue) ReceiveMessages(ctx context.Context, opts ...messageOpt) (messages []MessageReceipt, err error) {
	mo := &mOpt{
		MaxNumberOfMessages: q.MaxNumberOfMessages,
		WaitTimeSeconds:     q.WaitTimeSeconds,
		VisibilityTimeout:   q.VisibilityTimeout,
	}

	for _, opt := range opts {
		opt(mo)
	}

	resp, err := q.SQSClient.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            &q.URL,
		MaxNumberOfMessages: mo.MaxNumberOfMessages,
		WaitTimeSeconds:     mo.WaitTimeSeconds,
		VisibilityTimeout:   mo.VisibilityTimeout,
	})

	if err != nil {
		return
	}

	if len(resp.Messages) == 0 {
		return
	}

	for _, m := range resp.Messages {
		protoMsg := protoMessage{}
		if err := json.Unmarshal([]byte(*m.Body), &protoMsg); err != nil {
			return messages, fmt.Errorf("could not unmarshal message body to ProtoMessage: %w", err)
		}

		message, err := newMessage(protoMsg.Type)
		if err != nil {
			return messages, err
		}
		if err := json.Unmarshal([]byte(*m.Body), &message); err != nil {
			return messages, fmt.Errorf("could not unmarshal message body to Message: %w", err)
		}

		messages = append(messages, MessageReceipt{
			ReceiptHandle: *m.ReceiptHandle,
			Message:       message,
		})

		if message.GetStrategy() == DeleteAfterReceive {
			if err := q.DeleteMessage(ctx, *m.ReceiptHandle); err != nil {
				return messages, err
			}
		}

	}

	return
}

func (q *queue) DeleteMessage(ctx context.Context, receiptHandle string) (err error) {
	_, err = q.SQSClient.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      &q.URL,
		ReceiptHandle: &receiptHandle,
	})

	return
}

func MessageHasValidFields(m Message) bool {
	val := reflect.ValueOf(m)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}

	flagType := false
	flagStrategy := false

	for i := 0; i < val.NumField(); i++ {
		typeField := val.Type().Field(i)
		jsonTag := typeField.Tag.Get("json")
		if jsonTag == "type" && typeField.Type == reflect.TypeOf(MessageType("")) {
			flagType = true
		}
		if jsonTag == "strategy" && typeField.Type == reflect.TypeOf(MessageStrategy("")) {
			flagStrategy = true
		}
		if flagType && flagStrategy {
			return true
		}
	}

	return flagType && flagStrategy
}
