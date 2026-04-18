package mappers

import (
	"github.com/artsadert/artmq/internal/application/common"
	"github.com/artsadert/artmq/internal/domain/entities/message"
)

func ToMessageResult(msg *message.Message, err error) *common.MessageResult {
	result := &common.MessageResult{
		Message: msg,
	}

	if err != nil {
		result.Error = err.Error()
	}

	return result
}
