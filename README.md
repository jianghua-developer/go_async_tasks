# go_async_tasks

基于 RabbitMQ + Redis 的分布式异步任务框架，使用原生 Go 实现。

设计上参考 Celery：任务注册与反射调用、Broker / Backend 解耦、中间件扩展、Group / Pipeline 任务编排、结果查询。核心代码不依赖任何任务框架，仅使用 RabbitMQ 与 Redis 的官方客户端。

```
Producer ──► RabbitMQ ──► Consumer ──► 优先队列 / 延时队列 ──► Worker ──► Task
                 │                                              │
                 └──► 死信队列 ──► DLX Worker                    └──► 中间件链 ──► Redis Backend
```

## 特性

- **任务模型**：注册任务函数，按 `{type, value}` 自动编解码参数，支持 `context.Context` 注入与 panic 恢复
- **异步 / 延时 / 优先级**：延时消息支持两种实现——插件模式经 `delay.exchange`（`x-delay`）由 broker 延迟；无插件时由 Worker 取入进程内延时队列，到期重新投递
- **周期任务**：任务成功后按 `Period` 间隔自动重新入队
- **失败重试**：指数退避（可配置上下限）、条件重试（`RetryWhen`）、异常豁免（`RetryEscape`）
- **死信处理**：消费失败的消息进入死信队列，由独立的 DLX Worker 消费，避免毒消息反复投递
- **任务编排**：`Group` 并行扇出 + barrier 计数 + 完成回调；`Pipeline` 链式执行，上游结果自动作为下游参数，二者支持嵌套
- **结果查询**：任务结果写入 Backend，可通过 `TaskResult.Result / Completed` 异步查询
- **中间件**：8 个生命周期钩子，按阶段注册，可自行扩展
- **可插拔**：Broker、Backend、Barrier、ID 生成器均为接口，替换实现不影响内核
- **可靠性**：Publisher Confirm、手动 Ack、连接/信道断线自动重连、未确认消息在连接关闭后自动重回队列

## 快速开始

### 1. 启动依赖

```yaml
# docker-compose.yml
services:
  rabbitmq:
    image: rabbitmq:3-management
    ports:
      - "5672:5672"
      - "15672:15672"
  redis:
    image: redis:7
    ports:
      - "6379:6379"
```

```bash
docker compose up -d
```

### 2. 初始化 Broker 与 Backend

```go
rdb := goredis.NewClient(&goredis.Options{Addr: "127.0.0.1:6379"})
config.SetBackend(redisbackend.NewBackend(rdb, 24*time.Hour))
config.SetGroupBarrier(redisbarrier.NewBarrier(rdb))

b := rabbitmq.NewBroker("amqp://guest:guest@127.0.0.1:5672/", 0, 0, 0)
b.AddMiddleware(
    middleware.NewRetries(),
    middleware.NewCallbacks(),
    middleware.NewGroup(),
    middleware.NewPipeline(),
    middleware.NewResults(),
    middleware.NewPeriod(),
)
```

Broker 可选配置：

```go
b.WithOptions(
    rabbitmq.Confirm(true),     // 开启 Publisher Confirm
    rabbitmq.MaxPriority(10),   // 开启消息优先级（0-255）
    rabbitmq.UsePlugin(true),   // 使用 delayed-message-exchange 插件投递延时消息
)
```

### 3. 注册任务并发送

```go
add := tasks.NewTask("add", "default", func(a, b int) (int, error) {
    return a + b, nil
}, b)
b.RegisterTask(add)

// 异步执行，5 秒后开始，失败最多重试 3 次（1s 起，10s 封顶）
res, err := add.Delay([]task.Arg{
    {Type: "int", Value: int64(1)},
    {Type: "int", Value: int64(2)},
}, 5*time.Second, messages.Retries{
    MaxRetries: 3,
    MinBackoff: time.Second,
    MaxBackoff: 10 * time.Second,
})

// 周期任务：每 10 分钟执行一次
add.Send([]task.Arg{{Type: "int", Value: int64(1)}}, messages.Period(10*time.Minute))
```

> 参数按 JSON 兼容的类型传递，整型请使用 `int64`，与 JSON 解码结果保持一致。

### 4. 启动 Worker

```go
w := worker.NewWorker(b, 8, []string{"default"}, 5*time.Second, false)
go w.Start()

// 独立消费死信队列
dlx := worker.NewWorker(b, 2, []string{"default"}, 5*time.Second, true)
dlx.Start()
```

### 5. 查询结果

```go
results, err := res.Result(context.Background()) // 未完成时返回 errr.TaskNotComplete
done, err := res.Completed(context.Background())
```

## 任务编排

### Pipeline

上游任务的返回值自动追加到下游任务参数中：

```go
m1 := fetch.BuildMessage([]task.Arg{{Type: "int", Value: int64(1)}})
m2 := transform.BuildMessage(nil)

p := composition.NewPipeline(b, m1, m2)
p.Send()

done, _ := p.Completed(ctx) // 最后一个任务完成后为 true
```

### Group

并行执行多个任务，全部完成后触发回调：

```go
g := composition.NewGroup(b, m1, m2)
g.AddCallback(messages.NewMessage("default", "on_group_done", nil))
g.Send()

count, _ := g.CompletedCount(ctx)
done, _ := g.Completed(ctx)
```

Group 使用 Redis barrier 做计数，`Watch` 乐观事务保证并发安全。

## 延时任务的两种实现

### 插件模式（`rabbitmq.UsePlugin(true)`）

延时消息发往 `delay.exchange`（类型 `x-delayed-message`），并携带 `x-delay` 头；到点后由插件路由到 `<queue>.DQ`，Worker 消费后直接进入工作队列。

- 优点：延时状态由 broker 承担，扩展性好；Worker 重启不影响计时
- 前提：RabbitMQ 需启用 `rabbitmq_delayed_message_exchange` 插件

### 内存模式（默认）

生产者把延时消息投递到 `<queue>.DQ`，Worker **立即取走但不 ACK**，放入进程内按 ETA 排序的延时队列；到期后复制消息、清零 `delay`、重新投递回正常队列，成功后才 ACK 原消息。

- 优点：不依赖插件，精度取决于进程内定时器
- 代价：待到期消息占用 prefetch 名额（默认 `并发数 × 2`），大量长延时任务会阻塞 `.DQ` 消费；进程重启后未 ACK 的消息回到 `.DQ`，会**重新计时**；"重投成功但 ACK 失败"会导致重复投递（at-least-once）

两种模式对上层 API 完全一致，切换只需一个 Broker 选项。

## 中间件

中间件通过 `Phase()` 声明生效阶段，按注册顺序执行：

| 阶段 | 说明 |
| --- | --- |
| `BeforeEnqueue` / `AfterEnqueue` | 消息入队前后 |
| `BeforeProcessMessage` / `AfterProcessMessage` | 任务执行前后 |
| `BeforeAck` / `AfterAck` | 消息确认前后 |
| `BeforeNack` / `AfterNack` | 消息拒绝前后 |

内置中间件：

| 中间件 | 作用 |
| --- | --- |
| `Retries` | 失败重试，指数退避 + 条件重试 |
| `Callbacks` | 任务成功 / 失败回调 |
| `Results` | 结果写入 Backend |
| `Group` | Group 完成检测与回调触发 |
| `Pipeline` | 链式任务流转 |
| `Period` | 周期任务重新入队 |
| `CurrentMessage` | 在任务内获取当前消息上下文 |

自定义中间件只需实现对应阶段的接口：

```go
type MyMiddleware struct{}

func (m *MyMiddleware) Name() string { return "my-middleware" }
func (m *MyMiddleware) Phase() []int { return []int{middleware.BeforeProcessMessage} }
func (m *MyMiddleware) BeforeProcessMessage(ctx context.Context, b broker.IBroker, msg consumer.IConsumerMessage) error {
    // ...
    return nil
}
```

## 可靠性设计

- **投递确认**：开启 `rabbitmq.Confirm(true)` 后等待 Publisher Confirm，Nack 则重新入队
- **消费确认**：手动 Ack；失败 Nack 且不重回队列，直接进入死信队列
- **断线重连**：连接 / 信道关闭后按间隔自动重连，声明队列与入队均有最大重试次数
- **未确认消息重回队列**：内存延时队列中的消息始终未 ACK，连接 / 信道关闭后由 RabbitMQ 自动重新投递（会重新计时）
- **去重保护**：Consumer 维护 delivery tag 集合，重复 Ack / Nack 会被忽略

## 目录结构

```
├── broker/rabbitmq      # Broker 实现（连接管理、队列声明、投递确认）
├── consumer/rabbitmq    # 消费实现（Ack/Nack、重连）
├── backend/redis        # 结果存储（单任务 / Group / Pipeline）
├── barrier/redis        # Group 计数屏障（Watch 乐观事务）
├── middleware/          # 内置中间件
├── task/                # 任务定义与反射调用
│   └── composition/     # Group / Pipeline 编排
├── worker/              # 消费者协程与工作协程
├── messages/            # 消息模型与选项
├── util/pq              # 线程安全优先队列、延时队列
├── iface/               # 各组件的接口定义
└── internal/testutil    # 测试用 Fake Broker
```

## 与 Celery 的对应关系

| Celery | go_async_tasks |
| --- | --- |
| `chain` | `composition.Pipeline` |
| `chord` / `group` | `composition.Group` |
| `retry` / `max_retries` | `messages.Retries` + `Retries` 中间件 |
| `countdown` / `eta` | `Task.Delay` / `messages.MessageDelay` |
| `result_backend` | `backend/redis` |
| `queue` | 创建任务时指定队列名 |
| 死信队列 | `.XQ` 死信队列 + DLX Worker |

## 开发

```bash
# 单元测试（不需要外部依赖）
go test ./...

# 集成测试（需要真实 RabbitMQ，并启用延时插件）
docker run -d --name rabbitmq -p 5672:5672 \
  -e RABBITMQ_DEFAULT_USER=app -e RABBITMQ_DEFAULT_PASS=app \
  rabbitmq:3.13-management
# 将 rabbitmq_delayed_message_exchange-3.13.0.ez 放入容器 /opt/rabbitmq/plugins/ 后：
docker exec rabbitmq rabbitmq-plugins enable rabbitmq_delayed_message_exchange

AMQP_URL="amqp://app:app@127.0.0.1:5672/" \
  go test -tags integration -v ./broker/rabbitmq/ -run TestDelayedTask
```

集成测试会分别验证插件模式与内存模式的延时时长。

## 限制

- Broker 目前仅实现 RabbitMQ，Backend / Barrier 仅实现 Redis
- 消息体使用 JSON 编码，不支持任意二进制载荷
- 暂无 Web 管理界面与任务追踪面板
