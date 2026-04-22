package bolt

import (
	"os"
	"testing"
	"time"

	"github.com/artsadert/artmq/internal/domain/entities/message"
)

func newBoltRepo(t *testing.T) (*BoltRepository, func()) {
	t.Helper()
	f, err := os.CreateTemp("", "artmq-bolt-*.db")
	if err != nil {
		t.Fatal(err)
	}
	f.Close()

	repo, err := NewBoltRepository(f.Name())
	if err != nil {
		os.Remove(f.Name())
		t.Fatal(err)
	}

	return repo, func() {
		repo.db.Close()
		os.Remove(f.Name())
	}
}

func boltMsg(topic string, payload []byte, priority int, exp *int64) *message.Message {
	msg, _ := message.NewMessage(topic, payload)
	msg.Priority = priority
	msg.Exp = exp
	return msg
}

func TestBolt_PushAndPull(t *testing.T) {
	repo, cleanup := newBoltRepo(t)
	defer cleanup()

	msg, _ := message.NewMessage("topic1", []byte("hello"))
	if err := repo.PushMessage(msg); err != nil {
		t.Fatalf("push failed: %v", err)
	}

	pulled, err := repo.PullMessage("topic1")
	if err != nil {
		t.Fatalf("pull failed: %v", err)
	}
	if pulled.Id != msg.Id {
		t.Error("pulled wrong message")
	}
}

func TestBolt_PullFromEmptyTopic(t *testing.T) {
	repo, cleanup := newBoltRepo(t)
	defer cleanup()

	_, err := repo.PullMessage("nonexistent")
	if err == nil {
		t.Error("expected error pulling from empty topic")
	}
}

func TestBolt_PullRemovesMessage(t *testing.T) {
	repo, cleanup := newBoltRepo(t)
	defer cleanup()

	msg, _ := message.NewMessage("topic1", nil)
	_ = repo.PushMessage(msg)
	_, _ = repo.PullMessage("topic1")

	_, err := repo.PullMessage("topic1")
	if err == nil {
		t.Error("expected error: message should have been removed by first pull")
	}
}

func TestBolt_PriorityOrder(t *testing.T) {
	repo, cleanup := newBoltRepo(t)
	defer cleanup()

	low := boltMsg("topic1", nil, 5, nil)
	high := boltMsg("topic1", nil, 1, nil)

	_ = repo.PushMessage(low)
	_ = repo.PushMessage(high)

	pulled, err := repo.PullMessage("topic1")
	if err != nil {
		t.Fatalf("pull failed: %v", err)
	}
	if pulled.Priority != 1 {
		t.Errorf("expected highest priority (1) first, got %d", pulled.Priority)
	}
}

func TestBolt_SkipExpiredOnPull(t *testing.T) {
	repo, cleanup := newBoltRepo(t)
	defer cleanup()

	past := time.Now().Unix() - 10
	future := time.Now().Unix() + 10

	_ = repo.PushMessage(boltMsg("topic1", nil, 1, &past))
	valid := boltMsg("topic1", nil, 2, &future)
	_ = repo.PushMessage(valid)

	pulled, err := repo.PullMessage("topic1")
	if err != nil {
		t.Fatalf("pull failed: %v", err)
	}
	if pulled.Id != valid.Id {
		t.Error("expected non-expired message to be returned")
	}
}

func TestBolt_PeekDoesNotRemove(t *testing.T) {
	repo, cleanup := newBoltRepo(t)
	defer cleanup()

	msg, _ := message.NewMessage("topic1", []byte("data"))
	_ = repo.PushMessage(msg)

	peeked, err := repo.PeekMessage("topic1")
	if err != nil {
		t.Fatalf("peek failed: %v", err)
	}
	if peeked.Id != msg.Id {
		t.Error("peek returned wrong message")
	}

	pulled, err := repo.PullMessage("topic1")
	if err != nil {
		t.Fatalf("pull after peek failed: %v", err)
	}
	if pulled.Id != msg.Id {
		t.Error("message should still be present after peek")
	}
}

func TestBolt_PeekFromEmptyTopic(t *testing.T) {
	repo, cleanup := newBoltRepo(t)
	defer cleanup()

	_, err := repo.PeekMessage("nonexistent")
	if err == nil {
		t.Error("expected error peeking from empty topic")
	}
}

func TestBolt_SkipExpiredOnPeek(t *testing.T) {
	repo, cleanup := newBoltRepo(t)
	defer cleanup()

	past := time.Now().Unix() - 10
	future := time.Now().Unix() + 10

	_ = repo.PushMessage(boltMsg("topic1", nil, 1, &past))
	valid := boltMsg("topic1", nil, 2, &future)
	_ = repo.PushMessage(valid)

	peeked, err := repo.PeekMessage("topic1")
	if err != nil {
		t.Fatalf("peek failed: %v", err)
	}
	if peeked.Id != valid.Id {
		t.Error("expected non-expired message on peek")
	}
}

func TestBolt_IsEmpty_NonexistentTopic(t *testing.T) {
	repo, cleanup := newBoltRepo(t)
	defer cleanup()

	empty, err := repo.IsEmpty("topic1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !empty {
		t.Error("expected empty for nonexistent topic")
	}
}

func TestBolt_IsEmpty_AfterPush(t *testing.T) {
	repo, cleanup := newBoltRepo(t)
	defer cleanup()

	msg, _ := message.NewMessage("topic1", nil)
	_ = repo.PushMessage(msg)

	empty, _ := repo.IsEmpty("topic1")
	if empty {
		t.Error("expected not empty after push")
	}
}

func TestBolt_IsEmpty_AfterPull(t *testing.T) {
	repo, cleanup := newBoltRepo(t)
	defer cleanup()

	msg, _ := message.NewMessage("topic1", nil)
	_ = repo.PushMessage(msg)
	_, _ = repo.PullMessage("topic1")

	empty, _ := repo.IsEmpty("topic1")
	if !empty {
		t.Error("expected empty after pulling all messages")
	}
}

func TestBolt_MultiTopicIsolation(t *testing.T) {
	repo, cleanup := newBoltRepo(t)
	defer cleanup()

	msg1, _ := message.NewMessage("topic1", []byte("one"))
	msg2, _ := message.NewMessage("topic2", []byte("two"))
	_ = repo.PushMessage(msg1)
	_ = repo.PushMessage(msg2)

	r1, _ := repo.PullMessage("topic1")
	r2, _ := repo.PullMessage("topic2")

	if r1.TopicName != "topic1" {
		t.Errorf("expected topic1, got %q", r1.TopicName)
	}
	if r2.TopicName != "topic2" {
		t.Errorf("expected topic2, got %q", r2.TopicName)
	}
}
