package command

import (
	"github.com/artsadert/artmq/internal/application/common"
)

type PushMessageCommand struct {
	TopicName string `json:"topicName"`
	TTL       *int64 `json:"ttl,omitempty"`
	Payload   []byte `json:"payload"`
	Priority  *int64 `json:"priority,omitempty"`
}

type PushMessageCommandResult struct {
	Result *common.MessageResult
}
