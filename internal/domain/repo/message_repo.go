package repo

import "github.com/artsadert/artmq/internal/domain/entities/message"

type MessageRepo interface {
	PushMessage(message *message.Message) error
	PullMessage(topicName string) (*message.Message, error)
	PeekMessage(topicName string) (*message.Message, error)

	IsEmpty(topicName string) (bool, error)

	// PushToDLQ writes the message into the dead-letter topic for origTopic.
	// The message's TopicName is rewritten to the DLQ topic.
	PushToDLQ(origTopic string, msg *message.Message) error
}
