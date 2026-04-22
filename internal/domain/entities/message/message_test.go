package message

import (
	"testing"

	"github.com/google/uuid"
)

func TestNewMessage_Valid(t *testing.T) {
	msg, err := NewMessage("test/topic", []byte("hello"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if msg.TopicName != "test/topic" {
		t.Errorf("expected topic 'test/topic', got %q", msg.TopicName)
	}
	if string(msg.Payload) != "hello" {
		t.Errorf("expected payload 'hello', got %q", string(msg.Payload))
	}
	if msg.Id == (uuid.UUID{}) {
		t.Error("expected non-zero UUID")
	}
	if msg.Priority != 0 {
		t.Errorf("expected default priority 0, got %d", msg.Priority)
	}
	if msg.Exp != nil {
		t.Error("expected nil Exp by default")
	}
}

func TestNewMessage_EmptyTopic(t *testing.T) {
	_, err := NewMessage("", []byte("hello"))
	if err == nil {
		t.Error("expected error for empty topic")
	}
}

func TestNewMessage_NilPayload(t *testing.T) {
	msg, err := NewMessage("topic", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if msg.Payload != nil {
		t.Error("expected nil payload")
	}
}

func TestNewMessage_UniqueIDs(t *testing.T) {
	msg1, _ := NewMessage("topic", nil)
	msg2, _ := NewMessage("topic", nil)
	if msg1.Id == msg2.Id {
		t.Error("expected unique IDs for each message")
	}
}
