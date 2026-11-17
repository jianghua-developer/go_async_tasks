package pq

import (
	"testing"
	"time"
)

func TestThreadSafePriorityQueueOrder(t *testing.T) {
	q := NewThreadSafePriorityQueue(1) // 容量故意设小，覆盖动态扩容
	defer q.Close()

	q.Push(&Item{Value: "c", Priority: 3})
	q.Push(&Item{Value: "a", Priority: 1})
	q.Push(&Item{Value: "b", Priority: 2})
	q.Push(&Item{Value: "d", Priority: 0})

	// Push 是异步的，等待队列协程处理完最后一次推送
	time.Sleep(30 * time.Millisecond)

	want := []string{"d", "a", "b", "c"}
	for _, w := range want {
		item := q.Pop()
		if item == nil {
			t.Fatalf("pop returned nil, want %q", w)
		}
		if item.Value != w {
			t.Errorf("pop = %v, want %v", item.Value, w)
		}
	}

	if item := q.Pop(); item != nil {
		t.Errorf("pop on empty queue = %v, want nil", item.Value)
	}
}

func TestThreadSafePriorityQueueIsEmpty(t *testing.T) {
	q := NewThreadSafePriorityQueue(2)
	defer q.Close()

	if !q.IsEmpty() {
		t.Fatal("new queue should be empty")
	}

	q.Push(&Item{Value: 1, Priority: 1})
	time.Sleep(20 * time.Millisecond)

	if q.IsEmpty() {
		t.Fatal("queue should not be empty after push")
	}

	q.Pop()
	if !q.IsEmpty() {
		t.Fatal("queue should be empty after pop")
	}
}

func TestThreadSafeDelayQueueOrder(t *testing.T) {
	q := NewThreadSafeDelayQueue(2)
	defer q.Close()

	now := time.Now()
	q.Offer("late", now.Add(120*time.Millisecond).UnixNano())
	q.Offer("early", now.Add(20*time.Millisecond).UnixNano())

	for i, want := range []string{"early", "late"} {
		select {
		case v := <-q.Notify():
			item, ok := v.(*Item)
			if !ok {
				t.Fatalf("notify %d returned %T, want *Item", i, v)
			}
			if item.Value != want {
				t.Errorf("notify %d = %v, want %v", i, item.Value, want)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("timeout waiting for %v", want)
		}
	}
}

func TestThreadSafeDelayQueueToSlice(t *testing.T) {
	q := NewThreadSafeDelayQueue(2)
	now := time.Now()
	q.Offer("a", now.Add(time.Hour).UnixNano())
	q.Offer("b", now.Add(2*time.Hour).UnixNano())
	time.Sleep(20 * time.Millisecond)

	items := q.ToSlice()
	if len(items) != 2 {
		t.Fatalf("got %d items, want 2", len(items))
	}
	seen := map[interface{}]bool{}
	for _, item := range items {
		seen[item.Value] = true
	}
	if !seen["a"] || !seen["b"] {
		t.Errorf("unexpected items: %v", seen)
	}
}
