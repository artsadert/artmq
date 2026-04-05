package command

import "github.com/artsadert/artmq/internal/application/common"

type PeekMessageCommand struct {
	TopicName string
}

type PeekMessageCommandResult struct {
	Result *common.MessageResult
}
