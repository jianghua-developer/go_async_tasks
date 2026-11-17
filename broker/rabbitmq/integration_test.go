//go:build integration

package rabbitmq_test

import (
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jianghua-developer/go_async_tasks/broker/rabbitmq"
	tasks "github.com/jianghua-developer/go_async_tasks/task"
	"github.com/jianghua-developer/go_async_tasks/worker"
)

// 需要真实 RabbitMQ，且启用 rabbitmq_delayed_message_exchange 插件。
// 运行方式见 README「集成测试」。

func amqpURL(t *testing.T) string {
	t.Helper()
	url := os.Getenv("AMQP_URL")
	if url == "" {
		t.Skip("AMQP_URL 未设置，跳过集成测试")
	}
	return url
}

func runDelayedTask(t *testing.T, usePlugin bool) {
	url := amqpURL(t)
	b := rabbitmq.NewBroker(url, 0, 0, 0)
	if usePlugin {
		b.WithOptions(rabbitmq.UsePlugin(true))
	}

	queue := fmt.Sprintf("itest-delay-%d", time.Now().UnixNano())
	var (
		mu         sync.Mutex
		executedAt time.Time
	)
	done := make(chan struct{}, 1)

	tk := tasks.NewTask("itest_delay", queue, func() (int, error) {
		mu.Lock()
		executedAt = time.Now()
		mu.Unlock()
		select {
		case done <- struct{}{}:
		default:
		}
		return 1, nil
	}, b)
	b.RegisterTask(tk)

	w := worker.NewWorker(b, 1, []string{queue}, time.Second, false)
	go w.Start()

	// 等待消费者就绪，保证延时从投递时刻开始计算
	time.Sleep(2 * time.Second)

	start := time.Now()
	if _, err := tk.Delay(nil, 2*time.Second); err != nil {
		t.Fatalf("投递延时任务失败: %v", err)
	}

	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("延时任务在 15s 内未执行")
	}

	mu.Lock()
	elapsed := executedAt.Sub(start)
	mu.Unlock()

	if elapsed < 1800*time.Millisecond {
		t.Fatalf("任务在 %v 后执行，小于预期的 2s 延时", elapsed)
	}
	if elapsed > 8*time.Second {
		t.Fatalf("任务在 %v 后执行，远超预期的 2s 延时", elapsed)
	}
	t.Logf("usePlugin=%v，任务在 %v 后执行", usePlugin, elapsed)
}

func TestDelayedTaskWithPlugin(t *testing.T) {
	runDelayedTask(t, true)
}

func TestDelayedTaskWithMemoryQueue(t *testing.T) {
	runDelayedTask(t, false)
}
