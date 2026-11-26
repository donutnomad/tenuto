---
title: Signal
description: 发送与接收 Signal、循环消费、Selector 组合、Signal-With-Start 和重复消息处理
status: Stable
---

# Signal

<FeatureStatus status="Stable" />

Signal 把外部事实追加到 Event History。调用方在事件持久化后取得 Event ID，Workflow 在后续 Workflow Task 中消费它。Signal 没有返回值；需要同步结果时使用 [Update](/develop/updates)。

## 接收 Signal

`workflow.WaitSignal(ctx, name)` 返回 `workflow.SignalFuture`，`Get` 阻塞到对应的 Signal 到达：

```go
var decision map[string]any
if err := workflow.WaitSignal(ctx, "approval-result").Get(ctx, &decision); err != nil {
    return err
}
```

- `Get` 的目标是 `*map[string]any`，内容是调用端发送的 JSON object。
- Signal 可以先于等待点到达：它保存在 History 里，Workflow 走到 `WaitSignal` 时立即取得。
- 同名 Signal 是一个队列，语义与 Temporal 的 Signal Channel 相同：`SignalFuture` 第一次被消费（`Get` 返回，或 Selector 选中它并执行回调）时取走该名称下一条未消费的 Signal，之后对它 `Get` 一直返回这一条。调用 `WaitSignal` 本身不占 Signal；没被消费的 Future（例如 Selector 循环里没被选中、随后丢弃的那个）也不占。
- 多个同名 Future 按消费的先后分配 Signal：先 `Get` 的取到较早的一条，与调用 `WaitSignal` 的先后无关。消费顺序由 History 决定，重放时相同。

把 Signal 内容交给 Activity 处理：

```go
var decision map[string]any
if err := workflow.WaitSignal(ctx, "approval-result").Get(ctx, &decision); err != nil {
    return err
}
var applied map[string]any
if err := workflow.ExecuteActivity(ctx, activities.ApplyDecision, decision, workflow.ActivityOptions{StartToCloseTimeout: 30 * time.Second}).Get(ctx, &applied); err != nil {
    return err
}
```

## 循环接收

每次 `Get` 消费下一条同名 Signal：

```go
for i := 0; i < in.ExpectedApprovals; i++ {
    var approval map[string]any
    if err := workflow.WaitSignal(ctx, "approval").Get(ctx, &approval); err != nil {
        return Output{}, err
    }
    // 处理第 i 条 approval
}
```

长期接收 Signal 的 Workflow 通过 [Continue-As-New](/develop/continue-as-new) 控制 History 长度。

## 等待多个名称的 Signal

多个名称依次等待：

```go
var one map[string]any
if err := workflow.WaitSignal(ctx, "one").Get(ctx, &one); err != nil {
    return err
}
var two map[string]any
if err := workflow.WaitSignal(ctx, "two").Get(ctx, &two); err != nil {
    return err
}
```

等待最先到达的一个，用 Selector：

```go
approve := workflow.WaitSignal(ctx, "approve")
reject := workflow.WaitSignal(ctx, "reject")

outcome := ""
sel := workflow.NewSelector(ctx)
sel.AddSignalFuture(ctx, approve, func(f workflow.SignalFuture) {
    var out map[string]any
    _ = f.Get(ctx, &out)
    outcome = "approved"
})
sel.AddSignalFuture(ctx, reject, func(f workflow.SignalFuture) {
    var out map[string]any
    _ = f.Get(ctx, &out)
    outcome = "rejected"
})
sel.Select(ctx)
```

在循环里每轮新建 Selector、重新调用 `WaitSignal` 是安全的：上一轮没被选中的 Future 没有消费 Signal，之后到达的 Signal 由新一轮的 Future 收到。

```go
for !approved {
    sel := workflow.NewSelector(ctx)
    sel.AddSignalFuture(ctx, workflow.WaitSignal(ctx, "approve"), func(f workflow.SignalFuture) {
        var out map[string]any
        _ = f.Get(ctx, &out)
        approved = true
    })
    sel.AddSignalFuture(ctx, workflow.WaitSignal(ctx, "tick"), func(f workflow.SignalFuture) {
        var out map[string]any
        _ = f.Get(ctx, &out)
        ticks++
    })
    sel.Select(ctx)
}
```

Signal 与 Activity、Child Workflow、Channel 的组合见 [并发、Channel 与 Selector](/develop/concurrency)。Signal 与超时的组合见 [Timer 与 Workflow Sleep](/develop/timers)。

## 在协程里接收

`WaitSignal` 可以放进 `workflow.Go` 协程，主协程用 `workflow.Await` 等待它修改的状态：

```go
stopped := false
workflow.Go(ctx, func(ctx workflow.Context) {
    var stop map[string]any
    _ = workflow.WaitSignal(ctx, "stop").Get(ctx, &stop)
    stopped = true
})
```

## 发送 Signal

```go
err := c.SignalWorkflow(ctx, workflowID, runID, "approval-result", map[string]any{
    "message_id": messageID,
    "approved":   true,
})
```

`c` 是 [`sdk/client`](/sdk/client) 的 `Client`，Namespace 取自它。签名：`SignalWorkflow(ctx, workflowID, runID, signalName string, arg any) error`，`arg` 编码成 JSON 对象，可以为 `nil`。`runID` 为空时，Server 用 Namespace 和 Workflow ID 解析 current Run；携带 Run ID 时固定到具体 Run。向已结束的 Run 发送返回 `ErrInvalidArgument`。

HTTP 入口是 `POST /runtime/workflow/signal`：

```bash
curl --fail-with-body -sS "$TENUTO_URL/runtime/workflow/signal" \
  -H 'Content-Type: application/json' \
  -d '{"namespace":"orders","run_id":"<run-id>","name":"approval-result","input":{"approved":true}}'
```

## Signal-With-Start

`SignalWithStartWorkflow` 在同一个 Workflow ID 临界区内解析 current Run：存在进行中的 Run 时向它写入 Signal；没有 Run 或上一个 Run 已结束时，按 Workflow Type 启动新 Run，并在同一个事务里写入 Signal。

```go
run, err := c.SignalWithStartWorkflow(
    ctx,
    workflowID,                                // Workflow ID
    "approval-requested",                      // Signal Name
    map[string]any{"message_id": requestID},   // Signal 参数
    client.StartWorkflowOptions{},
    "approval-flow",                           // Workflow Type
    map[string]any{"order_id": orderID},       // 启动输入
)
```

详见 [Signal-With-Start](/sdk/signal-with-start)。

## 交付模型

Go SDK 的每次 `SignalWorkflow` / `SignalWithStartWorkflow` 带一个 SDK 生成的 request ID：连接中断、网关 502/504 等瞬时错误由 SDK 重发，同一个 Run 上同一个 request ID 只记一条 `WorkflowSignaled`（HTTP 调用方在请求体里给 `request_id` 得到同样的去重）。`SignalWithStartWorkflow` 的重发在第一次启动的 Run 已经结束时同样返回那个 Run 与第一次的事件 ID，不启动第二个 Run，见 [Signal-With-Start](/sdk/signal-with-start#重发与去重)。

Workflow 失败后按重试策略等待退避的重试 Run 收到 Signal 时，Signal 写入它的 History，不让重试提前开始，退避到期后的第一个 Workflow Task 一并处理（同 Temporal）。

跨越一次调用的重试仍是 at-least-once：调用超出 `ctx` 期限后应用代码再调用一次、或进程重启后重放消息，是新的 request ID，会形成第二条 Signal。在 payload 中携带稳定的 `message_id`，由处理该 Signal 的 Workflow 或 Activity 依据它去重。

## 行为矩阵

| 场景 | Server 行为 | 调用方策略 |
| --- | --- | --- |
| Run 正在运行 | 写入 `WorkflowSignaled` 并调度 Workflow Task | 保存 Event ID |
| Run ID 为空 | 解析 current Run | 使用 Workflow ID 跟随 Run Chain |
| 目标 Run 已终态 | 返回终态错误 | 查询 current Run 或停止投递 |
| 连接中断、网关 502/503/504 | SDK 以同一 request ID 重发，只记一条 | 无需处理 |
| 调用超出 `ctx` 期限且提交结果未知 | 事件可能已经持久化 | 使用稳定 Message ID 重试 |
| Workflow 尚未到等待点 | Signal 保存在 History | Workflow 执行到等待点时消费 |
| 同名 Signal 多次到达 | 按 Event 顺序保存 | 按调用次序逐条消费 |
| Workflow 不再等待该名称 | Signal 保留在 History，无人消费 | 发送前确认 Workflow 的等待契约 |

Signal 适合审批结果、外部状态变化和异步通知。
