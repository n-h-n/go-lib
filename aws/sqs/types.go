package sqs

import (
	"context"
	"fmt"
	"sync"

	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/n-h-n/go-lib/aws/iam"
)

type MessageType string

const (
	DeleteAfterReceive MessageStrategy = "delete_after_receive"
	DeleteAfterProcess MessageStrategy = "delete_after_process"

	OpsDaemonSlackOAuthModalEventType MessageType = "ops_daemon_slack_oauth_modal_event"
)

func newMessage(messageType MessageType) (Message, error) {
	switch messageType {
	case OpsDaemonSlackOAuthModalEventType:
		return &OpsDaemonSlackOAuthModalEventMessage{}, nil
	default:
		return nil, fmt.Errorf("unknown message type in ops-daemon: %s", messageType)
	}
}

func (mt MessageType) String() string {
	return string(mt)
}

type Client struct {
	mtx         *sync.RWMutex
	queues      *map[string]*queue
	iamClient   iam.IAMClient
	sqsClient   *sqs.Client
	verboseMode bool
	ctx         context.Context
}

type queue struct {
	URL                 string
	MaxNumberOfMessages int32
	SQSClient           *sqs.Client
	WaitTimeSeconds     int32
	VisibilityTimeout   int32
}

type Queue interface {
	SendMessage(ctx context.Context, message string) error
	ReceiveMessage(ctx context.Context) (string, error)
	DeleteMessage(ctx context.Context, receiptHandle string) error
}

type Message interface {
	GetStrategy() MessageStrategy
	GetType() MessageType
}

type MessageStrategy string

type MessageReceipt struct {
	ReceiptHandle string
	Message       Message
}

type protoMessage struct {
	Strategy MessageStrategy `json:"strategy"`
	Type     MessageType     `json:"type"`
}

type OpsDaemonSlackOAuthModalEventMessage struct {
	Strategy MessageStrategy `json:"strategy"`
	Type     MessageType     `json:"type"`
	Hash     string          `json:"hash"`
	ViewID   string          `json:"view_id"`
	Success  bool            `json:"success"`
	TeamID   string          `json:"team_id"`
	UserID   string          `json:"user_id"`
	AppID    string          `json:"app_id"`
}

func (m *OpsDaemonSlackOAuthModalEventMessage) GetStrategy() MessageStrategy {
	return m.Strategy
}

func (m *OpsDaemonSlackOAuthModalEventMessage) GetType() MessageType {
	return m.Type
}
