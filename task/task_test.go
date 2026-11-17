package tasks

import (
	"context"
	"errors"
	"strings"
	"testing"

	itask "github.com/jianghua-developer/go_async_tasks/iface/task"
)

func mustPanic(t *testing.T, fn func()) {
	t.Helper()
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic, got none")
		}
	}()
	fn()
}

func TestNewTaskValidatesSignature(t *testing.T) {
	mustPanic(t, func() { NewTask("bad", "q", 1, nil) })
	mustPanic(t, func() { NewTask("bad", "q", func() {}, nil) })
	mustPanic(t, func() { NewTask("bad", "q", func() int { return 1 }, nil) })

	// 合法：最后一个返回值是 error
	NewTask("ok", "q", func(a int) (int, error) { return a, nil }, nil)
}

func TestTaskCall(t *testing.T) {
	task := NewTask("add", "q", func(a, b int) (int, error) {
		return a + b, nil
	}, nil)

	results, err := task.Call([]itask.Arg{
		{Type: "int", Value: int64(1)},
		{Type: "int", Value: int64(2)},
	})
	if err != nil {
		t.Fatalf("Call failed: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("got %d results, want 1", len(results))
	}
	if results[0].Type != "int" || results[0].Value != 3 {
		t.Errorf("result = %+v, want int(3)", results[0])
	}
}

func TestTaskCallInjectsContext(t *testing.T) {
	task := NewTask("greet", "q", func(ctx context.Context, name string) (string, error) {
		if ctx == nil {
			return "", errors.New("nil context")
		}
		return "hello " + name, nil
	}, nil)
	if !task.UseContext {
		t.Fatal("UseContext = false, want true")
	}

	results, err := task.Call([]itask.Arg{{Type: "string", Value: "jiang"}})
	if err != nil {
		t.Fatalf("Call failed: %v", err)
	}
	if results[0].Value != "hello jiang" {
		t.Errorf("result = %v, want hello jiang", results[0].Value)
	}
}

func TestTaskCallRecoversPanic(t *testing.T) {
	task := NewTask("boom", "q", func() (int, error) {
		panic("boom")
	}, nil)

	_, err := task.Call(nil)
	if err == nil || !strings.Contains(err.Error(), "encountered an error while call task") {
		t.Fatalf("err = %v, want panic recovered error", err)
	}
}

func TestTaskCallReturnsTaskError(t *testing.T) {
	sentinel := errors.New("task failed")
	task := NewTask("fail", "q", func() (int, error) {
		return 0, sentinel
	}, nil)

	results, err := task.Call(nil)
	if err != sentinel {
		t.Fatalf("err = %v, want %v", err, sentinel)
	}
	if results != nil {
		t.Errorf("results = %v, want nil", results)
	}
}

func TestWithRetriesOptions(t *testing.T) {
	when := func(retries int, err error) bool { return retries < 3 }
	task := NewTask("x", "q", func() (int, error) { return 0, nil }, nil).
		WithOptions(Retries{RetryWhen: when})

	if task.Options.RetryWhen == nil {
		t.Fatal("RetryWhen was not applied")
	}
	if !task.Options.RetryWhen(1, nil) {
		t.Error("RetryWhen applied with wrong function")
	}
}
