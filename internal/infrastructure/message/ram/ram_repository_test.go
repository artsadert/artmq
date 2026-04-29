package ram

import (
	"sync"
	"testing"
	"time"

	"github.com/artsadert/artmq/internal/domain/entities/message"
)

func newMsg(topic string, exp *int64, priority int) *message.Message {
	msg, _ := message.NewMessage(topic, nil)
	msg.Exp = exp
	msg.Priority = priority
	return msg
}

func TestPushAndPullMessage_Order(t *testing.T) {
	repo := NewRamRepository()

	now := time.Now().Unix()
	exp1 := now + 5
	exp2 := now + 10

	msg1 := newMsg("topic1", &exp1, 2)
	msg2 := newMsg("topic1", &exp2, 1)

	_ = repo.PushMessage(msg1)
	_ = repo.PushMessage(msg2)

	first, err := repo.PullMessage("topic1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if first.Id != msg2.Id {
		t.Errorf("expected msg2 first")
	}

	second, _ := repo.PullMessage("topic1")
	if second.Id != msg1.Id {
		t.Errorf("expected msg1 second")
	}
}

func TestSkipExpiredMessages(t *testing.T) {
	repo := NewRamRepository()

	past := time.Now().Unix() - 10
	future := time.Now().Unix() + 10

	expired := newMsg("topic1", &past, 1)
	valid := newMsg("topic1", &future, 1)

	_ = repo.PushMessage(expired)
	_ = repo.PushMessage(valid)

	msg, err := repo.PullMessage("topic1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if msg.Id != valid.Id {
		t.Errorf("expected non-expired message")
	}
}

func TestPeekMessage(t *testing.T) {
	repo := NewRamRepository()

	future := time.Now().Unix() + 10
	msg := newMsg("topic1", &future, 1)

	_ = repo.PushMessage(msg)

	peeked, err := repo.PeekMessage("topic1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if peeked.Id != msg.Id {
		t.Errorf("peek returned wrong message")
	}

	// ensure it was NOT removed
	pulled, _ := repo.PullMessage("topic1")
	if pulled.Id != msg.Id {
		t.Errorf("peek should not remove message")
	}
}

func TestPeekSkipsExpired(t *testing.T) {
	repo := NewRamRepository()

	past := time.Now().Unix() - 10
	future := time.Now().Unix() + 10

	expired := newMsg("topic1", &past, 1)
	valid := newMsg("topic1", &future, 1)

	_ = repo.PushMessage(expired)
	_ = repo.PushMessage(valid)

	peeked, err := repo.PeekMessage("topic1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if peeked.Id != valid.Id {
		t.Errorf("expected non-expired message")
	}
}

func TestIsEmpty(t *testing.T) {
	repo := NewRamRepository()

	empty, _ := repo.IsEmpty("topic1")
	if !empty {
		t.Errorf("expected empty")
	}

	future := time.Now().Unix() + 10
	msg := newMsg("topic1", &future, 1)

	_ = repo.PushMessage(msg)

	empty, _ = repo.IsEmpty("topic1")
	if empty {
		t.Errorf("expected not empty")
	}
}

func TestMultiTopicIsolation(t *testing.T) {
	repo := NewRamRepository()

	future := time.Now().Unix() + 10

	msg1 := newMsg("topic1", &future, 1)
	msg2 := newMsg("topic2", &future, 1)

	_ = repo.PushMessage(msg1)
	_ = repo.PushMessage(msg2)

	res1, _ := repo.PullMessage("topic1")
	res2, _ := repo.PullMessage("topic2")

	if res1.TopicName != "topic1" {
		t.Errorf("wrong topic for msg1")
	}
	if res2.TopicName != "topic2" {
		t.Errorf("wrong topic for msg2")
	}
}

func TestPullEmpty(t *testing.T) {
	repo := NewRamRepository()

	_, err := repo.PullMessage("topic1")
	if err == nil {
		t.Errorf("expected error on empty pull")
	}
}

func TestPushNilMessage(t *testing.T) {
	repo := NewRamRepository()
	err := repo.PushMessage(nil)
	if err == nil {
		t.Error("expected error when pushing nil message")
	}
}

func TestPushEmptyTopic(t *testing.T) {
	repo := NewRamRepository()
	msg, _ := message.NewMessage("topic", nil)
	msg.TopicName = ""
	err := repo.PushMessage(msg)
	if err == nil {
		t.Error("expected error when pushing message with empty topic")
	}
}

func TestPeekEmpty(t *testing.T) {
	repo := NewRamRepository()
	_, err := repo.PeekMessage("nonexistent")
	if err == nil {
		t.Error("expected error peeking from empty topic")
	}
}

func TestAllExpiredOnPull(t *testing.T) {
	repo := NewRamRepository()
	past := time.Now().Unix() - 10
	_ = repo.PushMessage(newMsg("topic1", &past, 1))
	_ = repo.PushMessage(newMsg("topic1", &past, 2))

	_, err := repo.PullMessage("topic1")
	if err == nil {
		t.Error("expected error when all messages are expired")
	}
}

func TestAllExpiredOnPeek(t *testing.T) {
	repo := NewRamRepository()
	past := time.Now().Unix() - 10
	_ = repo.PushMessage(newMsg("topic1", &past, 1))

	_, err := repo.PeekMessage("topic1")
	if err == nil {
		t.Error("expected error when all messages are expired on peek")
	}
}

func TestNoExpiry(t *testing.T) {
	repo := NewRamRepository()
	msg := newMsg("topic1", nil, 1) // nil exp = never expires

	_ = repo.PushMessage(msg)
	pulled, err := repo.PullMessage("topic1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if pulled.Id != msg.Id {
		t.Error("wrong message")
	}
}

func TestRam_PushToDLQ(t *testing.T) {
	repo := NewRamRepository()

	msg, _ := message.NewMessage("topic1", []byte("dead"))
	if err := repo.PushToDLQ("topic1", msg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	pulled, err := repo.PullMessage(message.DLQTopic("topic1"))
	if err != nil {
		t.Fatalf("expected DLQ message, got error: %v", err)
	}
	if pulled.TopicName != message.DLQTopic("topic1") {
		t.Errorf("expected topic %q, got %q", message.DLQTopic("topic1"), pulled.TopicName)
	}
	if pulled.Id != msg.Id {
		t.Error("DLQ message ID mismatch")
	}
}

func TestRam_ExpiredMessageRoutedToDLQ(t *testing.T) {
	repo := NewRamRepository()

	past := time.Now().Unix() - 10
	future := time.Now().Unix() + 60

	expired := newMsg("topic1", &past, 1)
	valid := newMsg("topic1", &future, 1)

	_ = repo.PushMessage(expired)
	_ = repo.PushMessage(valid)

	pulled, err := repo.PullMessage("topic1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if pulled.Id != valid.Id {
		t.Errorf("expected non-expired message back from pull")
	}

	// expired message should now live in DLQ
	dlq, err := repo.PullMessage(message.DLQTopic("topic1"))
	if err != nil {
		t.Fatalf("expected expired message in DLQ, got error: %v", err)
	}
	if dlq.Id != expired.Id {
		t.Error("DLQ should contain the expired message")
	}
}

func TestRam_PushToDLQNil(t *testing.T) {
	repo := NewRamRepository()
	if err := repo.PushToDLQ("topic", nil); err == nil {
		t.Error("expected error pushing nil to DLQ")
	}
}

func TestConcurrentAccess(t *testing.T) {
	repo := NewRamRepository()
	wg := sync.WaitGroup{}

	future := time.Now().Unix() + 100

	// writers
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			msg := newMsg("topic1", &future, 1)
			_ = repo.PushMessage(msg)
		}()
	}

	// readers
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = repo.PullMessage("topic1")
		}()
	}

	wg.Wait()
}
