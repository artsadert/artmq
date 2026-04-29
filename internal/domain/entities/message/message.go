package message

import (
	"fmt"

	"github.com/google/uuid"
)

const DefaultMaxAttempts = 5

type Message struct {
	Id          uuid.UUID
	TopicName   string
	Payload     []byte
	Exp         *int64
	Priority    int
	Qos         byte
	Attempts    int
	MaxAttempts int
}

func NewMessage(topicName string, payload []byte) (*Message, error) {
	if topicName == "" {
		return nil, fmt.Errorf("topicName is required")
	}

	return &Message{
		Id:          uuid.New(),
		TopicName:   topicName,
		Payload:     payload,
		Exp:         nil,
		Priority:    0,
		Qos:         0,
		Attempts:    0,
		MaxAttempts: DefaultMaxAttempts,
	}, nil
}

// DLQTopic returns the dead-letter topic name for the given origin topic.
func DLQTopic(origin string) string {
	return "$dlq/" + origin
}
