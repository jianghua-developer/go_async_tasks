package composition

import (
	"testing"

	"github.com/jianghua-developer/go_async_tasks/config"
	"github.com/jianghua-developer/go_async_tasks/iface/barrier"
	"github.com/jianghua-developer/go_async_tasks/internal/testutil"
	"github.com/jianghua-developer/go_async_tasks/messages"
)

func TestPipelineWiresTargets(t *testing.T) {
	b := testutil.NewFakeBroker()
	m1 := messages.NewMessage("q", "step1", nil)
	m2 := messages.NewMessage("q", "step2", nil)

	p := NewPipeline(b, m1, m2)
	if p.Len() != 2 {
		t.Fatalf("Len() = %d, want 2", p.Len())
	}
	if err := p.Send(); err != nil {
		t.Fatalf("Send failed: %v", err)
	}

	if m1.Options.PipeTarget != m2 {
		t.Error("first message should target the second")
	}
	if !m2.Options.PipeLast {
		t.Error("last message should be marked PipeLast")
	}
	if m1.Options.PipelineID == "" || m1.Options.PipelineID != m2.Options.PipelineID {
		t.Errorf("pipeline IDs mismatch: %q vs %q", m1.Options.PipelineID, m2.Options.PipelineID)
	}

	msgs := b.Enqueued()
	if len(msgs) != 1 || msgs[0] != m1 {
		t.Fatalf("enqueued = %v, want only the first message", msgs)
	}
}

func TestPipelineNests(t *testing.T) {
	b := testutil.NewFakeBroker()
	m1 := messages.NewMessage("q", "a", nil)
	m2 := messages.NewMessage("q", "b", nil)
	m3 := messages.NewMessage("q", "c", nil)

	inner := NewPipeline(b, m2, m3)
	outer := NewPipeline(b, m1, inner)

	if outer.Len() != 3 {
		t.Fatalf("Len() = %d, want 3", outer.Len())
	}
	if m1.Options.PipeTarget != m2 || m2.Options.PipeTarget != m3 {
		t.Error("nested pipeline should be flattened in order")
	}
	if !m3.Options.PipeLast {
		t.Error("m3 should be the last message")
	}
}

type fakeBarrier struct {
	created map[string]int
}

func (f *fakeBarrier) Wait() (bool, error)                { return true, nil }
func (f *fakeBarrier) Load(key string) barrier.IBarrier   { return f }
func (f *fakeBarrier) Create(key string, count int) error { f.created[key] = count; return nil }
func (f *fakeBarrier) Delete(key string) error            { delete(f.created, key); return nil }

// config.SetGroupBarrier 内部使用 sync.Once，整个测试进程只能设置一次，
// 因此复用同一个 fake 实例，并在每个用例开始时清空记录。
var testBarrier = &fakeBarrier{created: map[string]int{}}

func init() {
	config.SetGroupBarrier(testBarrier)
}

func TestGroupSendCreatesBarrierAndTagsChildren(t *testing.T) {
	testBarrier.created = map[string]int{}

	b := testutil.NewFakeBroker()
	m1 := messages.NewMessage("q", "a", nil)
	m2 := messages.NewMessage("q", "b", nil)

	g := NewGroup(b, m1, m2)
	if g.Len() != 2 {
		t.Fatalf("Len() = %d, want 2", g.Len())
	}
	if err := g.Send(); err != nil {
		t.Fatalf("Send failed: %v", err)
	}

	if got := testBarrier.created[g.ID]; got != 2 {
		t.Errorf("barrier count = %d, want 2", got)
	}

	msgs := b.Enqueued()
	if len(msgs) != 2 {
		t.Fatalf("enqueued %d messages, want 2", len(msgs))
	}
	for _, m := range msgs {
		if m.Options.GroupID != g.ID {
			t.Errorf("message GroupID = %q, want %q", m.Options.GroupID, g.ID)
		}
	}
}
