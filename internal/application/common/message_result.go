package common

import "github.com/artsadert/artmq/internal/domain/entities/message"

type MessageResult struct {
	Message *message.Message `json:"message"`
	Error   string           `json:"error"`
}
