package ram

import (
	"sync"
	"testing"
	"time"

	"github.com/artsadert/artmq/internal/domain/entities/message"
)

func newMsg(topic string, exp *int64) *message.Message {
	msg, _ := message.NewMessage(topic)
	msg.Exp = exp
	return msg
}

func TestPushAndPullMessage_Order(t *testing.T) {
	repo := NewRamRepository()

	now := time.Now().Unix()
	exp1 := now + 10
	exp2 := now + 5

	msg1 := newMsg("topic1", &exp1)
	msg2 := newMsg("topic1", &exp2)

	_ = repo.PushMessage(msg1)
	_ = repo.PushMessage(msg2)

	// msg2 should come first (smaller exp)
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

	expired := newMsg("topic1", &past)
	valid := newMsg("topic1", &future)

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
	msg := newMsg("topic1", &future)

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

	expired := newMsg("topic1", &past)
	valid := newMsg("topic1", &future)

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
	msg := newMsg("topic1", &future)

	_ = repo.PushMessage(msg)

	empty, _ = repo.IsEmpty("topic1")
	if empty {
		t.Errorf("expected not empty")
	}
}

func TestMultiTopicIsolation(t *testing.T) {
	repo := NewRamRepository()

	future := time.Now().Unix() + 10

	msg1 := newMsg("topic1", &future)
	msg2 := newMsg("topic2", &future)

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

func TestConcurrentAccess(t *testing.T) {
	repo := NewRamRepository()
	wg := sync.WaitGroup{}

	future := time.Now().Unix() + 100

	// writers
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			msg := newMsg("topic1", &future)
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
