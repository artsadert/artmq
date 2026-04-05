package priority_queue

import (
	"container/heap"
	"testing"
)

// helper to create item
func newItem(value interface{}, priority int) *Item {
	return &Item{
		value:    value,
		priority: priority,
	}
}

func TestPriorityQueue_Len(t *testing.T) {
	pq := &PriorityQueue{}
	heap.Init(pq)

	if pq.Len() != 0 {
		t.Errorf("expected length 0, got %d", pq.Len())
	}

	heap.Push(pq, newItem("a", 2))
	heap.Push(pq, newItem("b", 1))

	if pq.Len() != 2 {
		t.Errorf("expected length 2, got %d", pq.Len())
	}
}

func TestPriorityQueue_Order(t *testing.T) {
	pq := &PriorityQueue{}
	heap.Init(pq)

	heap.Push(pq, newItem("low", 3))
	heap.Push(pq, newItem("medium", 2))
	heap.Push(pq, newItem("high", 1))

	item := heap.Pop(pq).(*Item)
	if item.value != "high" {
		t.Errorf("expected 'high', got %v", item.value)
	}

	item = heap.Pop(pq).(*Item)
	if item.value != "medium" {
		t.Errorf("expected 'medium', got %v", item.value)
	}

	item = heap.Pop(pq).(*Item)
	if item.value != "low" {
		t.Errorf("expected 'low', got %v", item.value)
	}
}

func TestPriorityQueue_PushPop(t *testing.T) {
	pq := &PriorityQueue{}
	heap.Init(pq)

	items := []*Item{
		newItem("a", 5),
		newItem("b", 1),
		newItem("c", 3),
	}

	for _, it := range items {
		heap.Push(pq, it)
	}

	if pq.Len() != 3 {
		t.Fatalf("expected length 3, got %d", pq.Len())
	}

	// Pop all and ensure sorted order
	prevPriority := -1
	for pq.Len() > 0 {
		item := heap.Pop(pq).(*Item)
		if prevPriority != -1 && item.priority < prevPriority {
			t.Errorf("heap property violated: got %d after %d", item.priority, prevPriority)
		}
		prevPriority = item.priority
	}
}

func TestPriorityQueue_IndexUpdate(t *testing.T) {
	pq := &PriorityQueue{}
	heap.Init(pq)

	a := newItem("a", 2)
	b := newItem("b", 1)

	heap.Push(pq, a)
	heap.Push(pq, b)

	if a.index == b.index {
		t.Errorf("indexes should be different")
	}

	heap.Pop(pq)

	if b.index != -1 {
		t.Errorf("expected popped item index to be -1, got %d", b.index)
	}
}

func TestPriorityQueue_EmptyPop(t *testing.T) {
	pq := &PriorityQueue{}
	heap.Init(pq)

	defer func() {
		if r := recover(); r == nil {
			t.Errorf("expected panic when popping from empty queue")
		}
	}()

	heap.Pop(pq) // should panic
}
