package command

import "github.com/artsadert/artmq/internal/application/common"

type PullMessageCommand struct {
	TopicName string
}

type PullMessageCommandResult struct {
	Result *common.MessageResult
}
