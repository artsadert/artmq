package services

import (
	"errors"
	"testing"

	"github.com/artsadert/artmq/internal/application/command"
	"github.com/artsadert/artmq/internal/application/query"
	"github.com/artsadert/artmq/internal/domain/entities/message"
)

type mockRepo struct {
	pushErr   error
	pullMsg   *message.Message
	pullErr   error
	peekMsg   *message.Message
	peekErr   error
	empty     bool
	emptyErr  error
	dlqErr    error
	dlqCalls  int
	lastDLQOf string
	lastDLQ   *message.Message
}

func (m *mockRepo) PushMessage(_ *message.Message) error           { return m.pushErr }
func (m *mockRepo) PullMessage(_ string) (*message.Message, error) { return m.pullMsg, m.pullErr }
func (m *mockRepo) PeekMessage(_ string) (*message.Message, error) { return m.peekMsg, m.peekErr }
func (m *mockRepo) IsEmpty(_ string) (bool, error)                 { return m.empty, m.emptyErr }
func (m *mockRepo) PushToDLQ(orig string, msg *message.Message) error {
	m.dlqCalls++
	m.lastDLQOf = orig
	m.lastDLQ = msg
	return m.dlqErr
}

func TestPushMessage_Success(t *testing.T) {
	svc := NewMessageService(&mockRepo{})
	result := svc.PushMessage(&command.PushMessageCommand{TopicName: "test", Payload: []byte("data")})
	if result.Result.Error != "" {
		t.Errorf("unexpected error: %s", result.Result.Error)
	}
	if result.Result.Message == nil {
		t.Error("expected message in result")
	}
}

func TestPushMessage_EmptyTopic(t *testing.T) {
	svc := NewMessageService(&mockRepo{})
	result := svc.PushMessage(&command.PushMessageCommand{TopicName: "", Payload: []byte("data")})
	if result.Result.Error == "" {
		t.Error("expected error for empty topic")
	}
}

func TestPushMessage_RepoError(t *testing.T) {
	svc := NewMessageService(&mockRepo{pushErr: errors.New("db error")})
	result := svc.PushMessage(&command.PushMessageCommand{TopicName: "test", Payload: nil})
	if result.Result.Error == "" {
		t.Error("expected repo error to propagate")
	}
}

func TestPushMessage_WithPriority(t *testing.T) {
	svc := NewMessageService(&mockRepo{})
	p := int64(5)
	result := svc.PushMessage(&command.PushMessageCommand{TopicName: "test", Payload: nil, Priority: &p})
	if result.Result.Error != "" {
		t.Fatalf("unexpected error: %s", result.Result.Error)
	}
	if result.Result.Message.Priority != 5 {
		t.Errorf("expected priority 5, got %d", result.Result.Message.Priority)
	}
}

func TestPushMessage_WithTTL(t *testing.T) {
	svc := NewMessageService(&mockRepo{})
	ttl := int64(60)
	result := svc.PushMessage(&command.PushMessageCommand{TopicName: "test", Payload: nil, TTL: &ttl})
	if result.Result.Error != "" {
		t.Fatalf("unexpected error: %s", result.Result.Error)
	}
	if result.Result.Message.Exp == nil {
		t.Error("expected Exp to be set when TTL is provided")
	}
}

func TestPeekMessage_Success(t *testing.T) {
	msg, _ := message.NewMessage("topic", nil)
	svc := NewMessageService(&mockRepo{peekMsg: msg})
	result := svc.PeekMessage(&command.PeekMessageCommand{TopicName: "topic"})
	if result.Result.Message == nil {
		t.Fatal("expected message in result")
	}
	if result.Result.Message.Id != msg.Id {
		t.Error("wrong message returned")
	}
}

func TestPeekMessage_Error(t *testing.T) {
	svc := NewMessageService(&mockRepo{peekErr: errors.New("empty")})
	result := svc.PeekMessage(&command.PeekMessageCommand{TopicName: "topic"})
	if result.Result.Error == "" {
		t.Error("expected error in result")
	}
}

func TestPullMessage_Success(t *testing.T) {
	msg, _ := message.NewMessage("topic", nil)
	svc := NewMessageService(&mockRepo{pullMsg: msg})
	result := svc.PullMessage(&command.PullMessageCommand{TopicName: "topic"})
	if result.Result.Message == nil {
		t.Fatal("expected message in result")
	}
	if result.Result.Message.Id != msg.Id {
		t.Error("wrong message returned")
	}
}

func TestPullMessage_Error(t *testing.T) {
	svc := NewMessageService(&mockRepo{pullErr: errors.New("empty")})
	result := svc.PullMessage(&command.PullMessageCommand{TopicName: "topic"})
	if result.Result.Error == "" {
		t.Error("expected error in result")
	}
}

func TestIsEmpty_True(t *testing.T) {
	svc := NewMessageService(&mockRepo{empty: true})
	result := svc.IsEmpty(&query.IsEmptyMessageQuery{TopicName: "topic"})
	if !result.IsEmpty {
		t.Error("expected IsEmpty=true")
	}
}

func TestIsEmpty_False(t *testing.T) {
	svc := NewMessageService(&mockRepo{empty: false})
	result := svc.IsEmpty(&query.IsEmptyMessageQuery{TopicName: "topic"})
	if result.IsEmpty {
		t.Error("expected IsEmpty=false")
	}
}

func TestIsEmpty_ErrorDefaultsToTrue(t *testing.T) {
	svc := NewMessageService(&mockRepo{emptyErr: errors.New("db error"), empty: false})
	result := svc.IsEmpty(&query.IsEmptyMessageQuery{TopicName: "topic"})
	if !result.IsEmpty {
		t.Error("expected IsEmpty=true when repo returns error")
	}
}

func TestDeadLetter_Routes(t *testing.T) {
	repo := &mockRepo{}
	svc := NewMessageService(repo)

	msg, _ := message.NewMessage("topic", []byte("x"))
	if err := svc.DeadLetter("topic", msg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if repo.dlqCalls != 1 {
		t.Errorf("expected 1 DLQ call, got %d", repo.dlqCalls)
	}
	if repo.lastDLQOf != "topic" {
		t.Errorf("expected origTopic 'topic', got %q", repo.lastDLQOf)
	}
	if repo.lastDLQ.Id != msg.Id {
		t.Error("DLQ pushed wrong message")
	}
}

func TestDeadLetter_NilRejected(t *testing.T) {
	svc := NewMessageService(&mockRepo{})
	if err := svc.DeadLetter("topic", nil); err == nil {
		t.Error("expected error on nil message")
	}
}

func TestRequeue_PushesViaRepo(t *testing.T) {
	repo := &mockRepo{}
	svc := NewMessageService(repo)

	msg, _ := message.NewMessage("topic", nil)
	msg.Attempts = 2
	if err := svc.Requeue(msg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRequeue_PropagatesRepoError(t *testing.T) {
	svc := NewMessageService(&mockRepo{pushErr: errors.New("disk full")})
	msg, _ := message.NewMessage("topic", nil)
	if err := svc.Requeue(msg); err == nil {
		t.Error("expected error from repo to propagate")
	}
}

func TestRequeue_NilRejected(t *testing.T) {
	svc := NewMessageService(&mockRepo{})
	if err := svc.Requeue(nil); err == nil {
		t.Error("expected error on nil message")
	}
}
