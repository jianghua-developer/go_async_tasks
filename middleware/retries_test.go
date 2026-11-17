package middleware

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jianghua-developer/go_async_tasks/iface/consumer"
	"github.com/jianghua-developer/go_async_tasks/internal/testutil"
	"github.com/jianghua-developer/go_async_tasks/messages"
	tasks "github.com/jianghua-developer/go_async_tasks/task"
)

var errBoom = errors.New("boom")

func TestRetriesReenqueueWithBackoff(t *testing.T) {
	b := testutil.NewFakeBroker()
	tk := tasks.NewTask("boom", "q", func() (int, error) { return 0, errBoom }, b)
	b.RegisterTask(tk)

	m := tk.BuildMessage(nil, messages.Retries{
		MaxRetries: 3,
		MinBackoff: 10 * time.Millisecond,
		MaxBackoff: time.Second,
	})
	proxy := consumer.NewMessageProxy(m)
	proxy.WithError(errBoom)

	if err := NewRetries().AfterProcessMessage(context.Background(), b, proxy, nil); err != nil {
		t.Fatalf("AfterProcessMessage failed: %v", err)
	}

	msgs := b.Enqueued()
	if len(msgs) != 1 {
		t.Fatalf("enqueued %d messages, want 1", len(msgs))
	}
	if got := msgs[0].Options.Retries; got != 1 {
		t.Errorf("retries = %d, want 1", got)
	}
	if msgs[0].Options.Delay <= 0 {
		t.Errorf("delay = %v, want > 0", msgs[0].Options.Delay)
	}
}

func TestRetriesStopsAtMaxRetries(t *testing.T) {
	b := testutil.NewFakeBroker()
	tk := tasks.NewTask("boom", "q", func() (int, error) { return 0, errBoom }, b)
	b.RegisterTask(tk)

	m := tk.BuildMessage(nil, messages.Retries{MaxRetries: 2, MinBackoff: time.Millisecond, MaxBackoff: time.Second})
	m.Options.Retries = 2
	proxy := consumer.NewMessageProxy(m)
	proxy.WithError(errBoom)

	err := NewRetries().AfterProcessMessage(context.Background(), b, proxy, nil)
	if !errors.Is(err, retriesExceeded) {
		t.Fatalf("err = %v, want retriesExceeded", err)
	}
	if len(b.Enqueued()) != 0 {
		t.Error("exceeded retries should not re-enqueue")
	}
}

func TestRetriesEscapeError(t *testing.T) {
	b := testutil.NewFakeBroker()
	tk := tasks.NewTask("boom", "q", func() (int, error) { return 0, errBoom }, b).
		WithOptions(tasks.Retries{RetryEscape: tasks.RetryEscape{errBoom}})
	b.RegisterTask(tk)

	m := tk.BuildMessage(nil, messages.Retries{MaxRetries: 3, MinBackoff: time.Millisecond, MaxBackoff: time.Second})
	proxy := consumer.NewMessageProxy(m)
	proxy.WithError(errBoom)

	err := NewRetries().AfterProcessMessage(context.Background(), b, proxy, nil)
	if err != errBoom {
		t.Fatalf("err = %v, want %v", err, errBoom)
	}
	if len(b.Enqueued()) != 0 {
		t.Error("escaped error should not re-enqueue")
	}
}

func TestRetriesWhenReturnsFalse(t *testing.T) {
	b := testutil.NewFakeBroker()
	tk := tasks.NewTask("boom", "q", func() (int, error) { return 0, errBoom }, b).
		WithOptions(tasks.Retries{RetryWhen: func(retries int, err error) bool { return false }})
	b.RegisterTask(tk)

	m := tk.BuildMessage(nil, messages.Retries{MaxRetries: 3, MinBackoff: time.Millisecond, MaxBackoff: time.Second})
	proxy := consumer.NewMessageProxy(m)
	proxy.WithError(errBoom)

	if err := NewRetries().AfterProcessMessage(context.Background(), b, proxy, nil); !errors.Is(err, retriesExceeded) {
		t.Fatalf("err = %v, want retriesExceeded", err)
	}
	if len(b.Enqueued()) != 0 {
		t.Error("retryWhen=false should not re-enqueue")
	}
}
