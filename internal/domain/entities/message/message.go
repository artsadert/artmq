package message

import (
	"fmt"

	"github.com/google/uuid"
)

type Message struct {
	Id        uuid.UUID
	TopicName string
	Payload   []byte
	Exp       *int64
	Priority  int
}

func NewMessage(topicName string, payload []byte) (*Message, error) {
	if topicName == "" {
		return nil, fmt.Errorf("topicName is required")
	}

	return &Message{
		Id:        uuid.New(),
		TopicName: topicName,
		Payload:   payload,
		Exp:       nil,
		Priority:  0,
	}, nil
}
