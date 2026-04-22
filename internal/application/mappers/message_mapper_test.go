package mappers

import (
	"errors"
	"testing"

	"github.com/artsadert/artmq/internal/domain/entities/message"
)

func TestToMessageResult_WithMessage(t *testing.T) {
	msg, _ := message.NewMessage("topic", []byte("data"))
	result := ToMessageResult(msg, nil)
	if result.Message != msg {
		t.Error("expected message to be set")
	}
	if result.Error != "" {
		t.Errorf("expected no error, got %q", result.Error)
	}
}

func TestToMessageResult_WithError(t *testing.T) {
	result := ToMessageResult(nil, errors.New("something failed"))
	if result.Message != nil {
		t.Error("expected nil message")
	}
	if result.Error != "something failed" {
		t.Errorf("unexpected error string: %q", result.Error)
	}
}

func TestToMessageResult_NilMessageNilError(t *testing.T) {
	result := ToMessageResult(nil, nil)
	if result.Message != nil {
		t.Error("expected nil message")
	}
	if result.Error != "" {
		t.Errorf("expected empty error, got %q", result.Error)
	}
}
