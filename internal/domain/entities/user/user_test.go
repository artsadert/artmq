package user

import (
	"testing"

	"github.com/google/uuid"
)

func TestNewUser_Valid(t *testing.T) {
	u, err := NewUser("alice")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if u.Username != "alice" {
		t.Errorf("expected username 'alice', got %q", u.Username)
	}
	if u.Id == (uuid.UUID{}) {
		t.Error("expected non-zero UUID")
	}
}

func TestNewUser_EmptyUsername(t *testing.T) {
	_, err := NewUser("")
	if err == nil {
		t.Error("expected error for empty username")
	}
}

func TestNewUser_UniqueIDs(t *testing.T) {
	u1, _ := NewUser("alice")
	u2, _ := NewUser("alice")
	if u1.Id == u2.Id {
		t.Error("expected unique IDs for each user")
	}
}
