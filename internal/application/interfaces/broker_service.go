package interfaces

import (
	"github.com/artsadert/artmq/internal/application/command"
	"github.com/artsadert/artmq/internal/application/query"
)

type BrokerService interface {
	PushMessage(*command.PushMessageCommand) *command.PushMessageCommandResult
	PeekMessage(*command.PeekMessageCommand) *command.PeekMessageCommandResult
	PullMessage(*command.PullMessageCommand) *command.PullMessageCommandResult

	IsEmpty(*query.IsEmptyMessageQuery) *query.IsEmptyMessageQueryResult
}
