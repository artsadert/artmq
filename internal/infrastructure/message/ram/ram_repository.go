package ram

import (
	"container/heap"
	"fmt"
	"sync"
	"time"

	"github.com/artsadert/artmq/internal/domain/entities/message"
	"github.com/artsadert/artmq/internal/domain/value_object/priority_queue"
)

type RamRepository struct {
	mu     sync.RWMutex
	queues map[string]*priority_queue.PriorityQueue
}

func NewRamRepository() *RamRepository {
	return &RamRepository{
		queues: make(map[string]*priority_queue.PriorityQueue),
	}
}

func (r *RamRepository) getOrCreateQueue(topic string) *priority_queue.PriorityQueue {
	q, ok := r.queues[topic]
	if !ok {
		q = &priority_queue.PriorityQueue{}
		heap.Init(q)
		r.queues[topic] = q
	}
	return q
}

func getPriority(msg *message.Message) int {
	return msg.Priority
}

func isExpired(msg *message.Message) bool {
	if msg.Exp == nil {
		return false
	}
	return time.Now().Unix() > *msg.Exp
}

func (r *RamRepository) PushMessage(msg *message.Message) error {
	if msg == nil {
		return fmt.Errorf("message is nil")
	}
	if msg.TopicName == "" {
		return fmt.Errorf("topic is required")
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	q := r.getOrCreateQueue(msg.TopicName)

	item := priority_queue.NewItem(msg, getPriority(msg))
	heap.Push(q, item)

	return nil
}

func (r *RamRepository) PullMessage(topicName string) (*message.Message, error) {
	r.mu.Lock()

	q, ok := r.queues[topicName]
	if !ok || q.Len() == 0 {
		r.mu.Unlock()
		return nil, fmt.Errorf("no messages in topic")
	}

	var expired []*message.Message
	var selected *message.Message

	for q.Len() > 0 {
		item := heap.Pop(q).(*priority_queue.Item)

		msg, ok := item.Value().(*message.Message)
		if !ok {
			continue
		}

		if isExpired(msg) {
			expired = append(expired, msg)
			continue
		}

		selected = msg
		break
	}
	r.mu.Unlock()

	for _, em := range expired {
		_ = r.PushToDLQ(topicName, em)
	}

	if selected == nil {
		return nil, fmt.Errorf("no non-expired messages")
	}

	return selected, nil
}

func (r *RamRepository) PeekMessage(topicName string) (*message.Message, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	q, ok := r.queues[topicName]
	if !ok || q.Len() == 0 {
		return nil, fmt.Errorf("no messages in topic")
	}

	for q.Len() > 0 {
		item := (*q)[0]

		msg, ok := item.Value().(*message.Message)
		if !ok {
			heap.Pop(q)
			continue
		}

		if isExpired(msg) {
			heap.Pop(q)
			continue
		}

		return msg, nil
	}

	return nil, fmt.Errorf("no non-expired messages")
}

func (r *RamRepository) IsEmpty(topicName string) (bool, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	q, ok := r.queues[topicName]
	if !ok {
		return true, nil
	}

	return q.Len() == 0, nil
}

func (r *RamRepository) PushToDLQ(origTopic string, msg *message.Message) error {
	if msg == nil {
		return fmt.Errorf("message is nil")
	}

	dlqTopic := message.DLQTopic(origTopic)
	dlqMsg := *msg
	dlqMsg.TopicName = dlqTopic
	dlqMsg.Exp = nil // dead-lettered messages do not expire

	r.mu.Lock()
	defer r.mu.Unlock()
	q := r.getOrCreateQueue(dlqTopic)
	item := priority_queue.NewItem(&dlqMsg, getPriority(&dlqMsg))
	heap.Push(q, item)
	return nil
}