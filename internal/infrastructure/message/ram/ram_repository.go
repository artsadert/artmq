package ram

import (
	"container/heap"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/artsadert/artmq/internal/domain/entities/message"
	"github.com/artsadert/artmq/internal/domain/repo"
	"github.com/artsadert/artmq/internal/domain/value_object/priority_queue"
)

type RamRepository struct {
	mu     sync.RWMutex
	queues map[string]*priority_queue.PriorityQueue
}

func NewRamRepository() repo.MessageRepo {
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
	if msg.Exp == nil {
		return math.MaxInt64
	}
	return int(*msg.Exp)
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
	defer r.mu.Unlock()

	q, ok := r.queues[topicName]
	if !ok || q.Len() == 0 {
		return nil, fmt.Errorf("no messages in topic")
	}

	// skip expired messages
	for q.Len() > 0 {
		item := heap.Pop(q).(*priority_queue.Item)

		msg, ok := item.Value().(*message.Message)
		if !ok {
			continue
		}

		if isExpired(msg) {
			continue
		}

		return msg, nil
	}

	return nil, fmt.Errorf("no non-expired messages")
}

func (r *RamRepository) PeekMessage(topicName string) (*message.Message, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	q, ok := r.queues[topicName]
	if !ok || q.Len() == 0 {
		return nil, fmt.Errorf("no messages in topic")
	}

	// we may need to clean expired ones from the top
	for q.Len() > 0 {
		item := (*q)[0] // peek root

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
