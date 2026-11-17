package middleware

import (
	"context"
	"testing"
	"time"

	"github.com/jianghua-developer/go_async_tasks/iface/consumer"
	"github.com/jianghua-developer/go_async_tasks/internal/testutil"
	"github.com/jianghua-developer/go_async_tasks/messages"
)

func TestPeriodReenqueuesAfterSuccess(t *testing.T) {
	b := testutil.NewFakeBroker()
	m := messages.NewMessage("q", "tick", nil).WithOPtions(messages.Period(50 * time.Millisecond))
	proxy := consumer.NewMessageProxy(m)

	if err := NewPeriod().AfterProcessMessage(context.Background(), b, proxy, nil); err != nil {
		t.Fatalf("AfterProcessMessage failed: %v", err)
	}

	msgs := b.Enqueued()
	if len(msgs) != 1 {
		t.Fatalf("enqueued %d messages, want 1", len(msgs))
	}
	next := msgs[0]
	if next.Options.Period != 50*time.Millisecond {
		t.Errorf("period = %v, want 50ms", next.Options.Period)
	}
	if next.Options.Delay <= 0 {
		t.Errorf("delay = %v, want > 0", next.Options.Delay)
	}
	if next.ID == m.ID {
		t.Error("periodic message should have a new ID")
	}
}

func TestPeriodSkipsOnFailure(t *testing.T) {
	b := testutil.NewFakeBroker()
	m := messages.NewMessage("q", "tick", nil).WithOPtions(messages.Period(time.Second))
	proxy := consumer.NewMessageProxy(m)
	proxy.WithError(errBoom)

	if err := NewPeriod().AfterProcessMessage(context.Background(), b, proxy, nil); err != nil {
		t.Fatalf("AfterProcessMessage failed: %v", err)
	}
	if len(b.Enqueued()) != 0 {
		t.Error("failed task should not schedule next period")
	}
}

func TestPeriodWithoutOptionDoesNothing(t *testing.T) {
	b := testutil.NewFakeBroker()
	m := messages.NewMessage("q", "tick", nil)
	proxy := consumer.NewMessageProxy(m)

	if err := NewPeriod().AfterProcessMessage(context.Background(), b, proxy, nil); err != nil {
		t.Fatalf("AfterProcessMessage failed: %v", err)
	}
	if len(b.Enqueued()) != 0 {
		t.Error("message without period option should not re-enqueue")
	}
}
