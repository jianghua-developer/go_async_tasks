package messages

import "time"

// Period 为消息设置周期执行间隔。任务成功执行后，Period 中间件会按该间隔
// 重新入队一条相同任务的消息，从而实现固定间隔的周期任务。
// 值为 0 或负数时不启用周期执行。
type Period time.Duration

func (p Period) apply(m *Message) {
	m.Options.Period = time.Duration(p)
}
