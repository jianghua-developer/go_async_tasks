package middleware

import (
	"context"
	"time"

	"github.com/jianghua-developer/go_async_tasks/config"
	"github.com/jianghua-developer/go_async_tasks/consts"
	"github.com/jianghua-developer/go_async_tasks/iface/broker"
	"github.com/jianghua-developer/go_async_tasks/iface/consumer"
	"github.com/jianghua-developer/go_async_tasks/iface/middleware"
	"github.com/jianghua-developer/go_async_tasks/iface/task"
	"github.com/jianghua-developer/go_async_tasks/messages"
	"github.com/jianghua-developer/go_async_tasks/util/deepcopy"
)

// Period 实现固定间隔的周期任务：任务成功执行后，按消息上配置的
// Period 间隔重新入队一条相同任务的消息。
type Period struct {
}

func NewPeriod() *Period {
	return &Period{}
}

func (p *Period) Phase() []int {
	return []int{
		middleware.AfterProcessMessage,
	}
}

func (p *Period) Name() string {
	return consts.PeriodMiddleware
}

func (p *Period) AfterProcessMessage(ctx context.Context, broker broker.IBroker, message consumer.IConsumerMessage, result []*task.Result) error {
	// 只有执行成功才安排下一轮周期，失败的由重试中间件负责
	if message.Err() != nil || message.IsFailed() {
		return nil
	}

	m := message.Message()
	period := m.Options.Period
	if period <= 0 {
		return nil
	}

	next := deepcopy.DeepCopy(m).(*messages.Message)
	next.ID = config.IDMaker().NewID()
	next.MessageAt = time.Now().Local().UnixNano()
	next.Options.Retries = 0
	next.WithOPtions(messages.MessageDelay(period))

	return broker.Enqueue(ctx, next)
}
