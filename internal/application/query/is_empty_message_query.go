package command

import (
	"github.com/artsadert/artmq/internal/application/common"
)

type IsEmptyMessageQuery struct {
	TopicName string `json:"topicName"`
}

type IsEmptyMessageQueryResult struct {
	Result *common.MessageResult
}
