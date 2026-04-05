package message

import (
	"fmt"

	"github.com/google/uuid"
)

type Message struct {
	Id        uuid.UUID
	TopicName string
	Exp       *int64
	Priority  int64
}

func NewMessage(topicName string) (*Message, error) {
	if topicName == "" {
		return nil, fmt.Errorf("topicName is required")
	}

	return &Message{
		Id:        uuid.New(),
		TopicName: topicName,
		Exp:       nil,
		Priority:  0,
	}, nil
}
