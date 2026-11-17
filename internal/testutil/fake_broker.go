// Package testutil provides lightweight fakes used by unit tests.
package testutil

import (
	"context"
	"errors"
	"sync"

	"github.com/jianghua-developer/go_async_tasks/iface/consumer"
	"github.com/jianghua-developer/go_async_tasks/iface/task"
	"github.com/jianghua-developer/go_async_tasks/messages"
)

// FakeBroker 只记录入队消息与已注册任务，用于不依赖 RabbitMQ 的单元测试。
type FakeBroker struct {
	mu       sync.Mutex
	tasks    map[string]task.ITask
	enqueued []*messages.Message
}

func NewFakeBroker() *FakeBroker {
	return &FakeBroker{
		tasks: make(map[string]task.ITask),
	}
}

func (f *FakeBroker) Consume(ctx context.Context, queueName string, prefetch int) (consumer.IConsumer, error) {
	return nil, errors.New("fake broker does not support consume")
}

func (f *FakeBroker) Enqueue(ctx context.Context, message *messages.Message) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.enqueued = append(f.enqueued, message)
	return nil
}

func (f *FakeBroker) RegisterTask(tasks ...task.ITask) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, t := range tasks {
		f.tasks[t.Name()] = t
	}
}

func (f *FakeBroker) GetTask(taskName string) (task.ITask, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	t, ok := f.tasks[taskName]
	if !ok {
		return nil, errors.New("task not register")
	}
	return t, nil
}

func (f *FakeBroker) HasTask(taskName string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.tasks[taskName]
	return ok
}

func (f *FakeBroker) ConsumeDelayDirect() bool {
	return false
}

func (f *FakeBroker) Close() {}

// Enqueued 返回按入队顺序记录的消息。
func (f *FakeBroker) Enqueued() []*messages.Message {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]*messages.Message, len(f.enqueued))
	copy(out, f.enqueued)
	return out
}
