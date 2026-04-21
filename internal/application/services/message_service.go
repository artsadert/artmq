package services

import (
	"log"
	"time"

	"github.com/artsadert/artmq/internal/application/command"
	"github.com/artsadert/artmq/internal/application/mappers"
	"github.com/artsadert/artmq/internal/application/query"
	"github.com/artsadert/artmq/internal/domain/entities/message"
	"github.com/artsadert/artmq/internal/domain/repo"
)

type MessageService struct {
	repo repo.MessageRepo
}

func NewMessageService(repo repo.MessageRepo) *MessageService {
	return &MessageService{
		repo: repo,
	}
}

func (s *MessageService) PushMessage(cmd *command.PushMessageCommand) *command.PushMessageCommandResult {
	msg, err := message.NewMessage(cmd.TopicName, cmd.Payload)
	if err != nil {
		return &command.PushMessageCommandResult{
			Result: mappers.ToMessageResult(nil, err),
		}
	}

	// apply priority
	if cmd.Priority != nil {
		msg.Priority = int(*cmd.Priority)
	}

	// apply TTL
	if cmd.TTL != nil {
		exp := time.Now().Unix() + *cmd.TTL
		msg.Exp = &exp
	}

	err = s.repo.PushMessage(msg)

	return &command.PushMessageCommandResult{
		Result: mappers.ToMessageResult(msg, err),
	}
}

func (s *MessageService) PeekMessage(cmd *command.PeekMessageCommand) *command.PeekMessageCommandResult {
	msg, err := s.repo.PeekMessage(cmd.TopicName)

	return &command.PeekMessageCommandResult{
		Result: mappers.ToMessageResult(msg, err),
	}
}

func (s *MessageService) PullMessage(cmd *command.PullMessageCommand) *command.PullMessageCommandResult {
	msg, err := s.repo.PullMessage(cmd.TopicName)

	// log.Println(msg, "pull")
	return &command.PullMessageCommandResult{
		Result: mappers.ToMessageResult(msg, err),
	}
}

func (s *MessageService) IsEmpty(q *query.IsEmptyMessageQuery) *query.IsEmptyMessageQueryResult {
	empty, err := s.repo.IsEmpty(q.TopicName)
	if err != nil {
		log.Println("Error while trying to check if queue is empty", err)
		return &query.IsEmptyMessageQueryResult{
			IsEmpty: true,
		}
	}

	result := &query.IsEmptyMessageQueryResult{
		IsEmpty: empty,
	}

	return result
}
