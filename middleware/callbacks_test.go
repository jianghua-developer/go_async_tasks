package middleware

import (
	"context"
	"testing"

	"github.com/jianghua-developer/go_async_tasks/iface/consumer"
	itask "github.com/jianghua-developer/go_async_tasks/iface/task"
	"github.com/jianghua-developer/go_async_tasks/internal/testutil"
	tasks "github.com/jianghua-developer/go_async_tasks/task"
)

func TestFailureCallbackInvokesOnFailure(t *testing.T) {
	b := testutil.NewFakeBroker()
	main := tasks.NewTask("main", "q", func() (int, error) { return 0, errBoom }, b).
		WithOptions(tasks.FailureCallback{OnFailure: "on_failure"})
	cb := tasks.NewTask("on_failure", "q", func(errType, errMsg string) (bool, error) {
		return true, nil
	}, b)
	b.RegisterTask(main, cb)

	m := main.BuildMessage(nil)
	proxy := consumer.NewMessageProxy(m)
	proxy.WithError(errBoom)

	if err := NewCallbacks().AfterProcessMessage(context.Background(), b, proxy, nil); err != nil {
		t.Fatalf("AfterProcessMessage failed: %v", err)
	}

	msgs := b.Enqueued()
	if len(msgs) != 1 {
		t.Fatalf("enqueued %d messages, want 1", len(msgs))
	}
	if msgs[0].TaskName != "on_failure" {
		t.Errorf("callback task = %q, want on_failure", msgs[0].TaskName)
	}
	if len(msgs[0].TaskArgs) != 2 {
		t.Errorf("callback args = %d, want 2 (error type and message)", len(msgs[0].TaskArgs))
	}
}

func TestSuccessCallbackGetsTaskResult(t *testing.T) {
	b := testutil.NewFakeBroker()
	main := tasks.NewTask("main", "q", func() (int, error) { return 7, nil }, b).
		WithOptions(tasks.SuccessCallback{OnSucess: "on_success"})
	cb := tasks.NewTask("on_success", "q", func(v int) (bool, error) {
		return v == 7, nil
	}, b)
	b.RegisterTask(main, cb)

	m := main.BuildMessage(nil)
	proxy := consumer.NewMessageProxy(m)

	results := []*itask.Result{{Type: "int", Value: int64(7)}}
	if err := NewCallbacks().AfterProcessMessage(context.Background(), b, proxy, results); err != nil {
		t.Fatalf("AfterProcessMessage failed: %v", err)
	}

	msgs := b.Enqueued()
	if len(msgs) != 1 || msgs[0].TaskName != "on_success" {
		t.Fatalf("enqueued = %+v, want one on_success message", msgs)
	}
}
