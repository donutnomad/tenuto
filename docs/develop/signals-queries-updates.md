---
title: Signal、Query 与 Update
description: Workflow 外部交互的三种语义
status: Stable
---

# Signal、Query 与 Update

<FeatureStatus status="Stable" />

Signal、Query 和 Update 对应三种不同的外部交互需求。

## Signal：异步事件

Workflow 等待 Signal：

```go
var decision map[string]any
if err := workflow.WaitSignal(ctx, "approval-result").Get(ctx, &decision); err != nil {
    return err
}
```

`Get` 的目标是 `*map[string]any`，内容是调用端发送的 JSON object，见 [Signal](/develop/signals)。

调用端发送：

```go
err := c.SignalWorkflow(ctx, workflowID, runID, "approval-result", map[string]any{"approved": true})
```

`c` 是 [`sdk/client`](/sdk/client) 的 `Client`。Signal 写入 Event History 后调用返回，业务处理由后续 Workflow Task 完成。

## Query：只读现场

```go
if err := workflow.SetQueryHandler(ctx, "progress", func(
    ctx workflow.Context,
    input map[string]any,
) map[string]any {
    return map[string]any{
        "stage": stage,
        "processed": processed,
    }
}); err != nil {
    return err
}
```

Query 在重放到最新状态的只读现场执行。Query handler 适合读取 Workflow 内存变量，执行过程中产生的 command 会被隔离。

## Update：带结果的业务命令

```go
type ChangeLimitResult struct {
    Previous int64 `json:"previous"`
    Current  int64 `json:"current"`
}

err := workflow.SetUpdateHandler(ctx, "change-limit", func(
    ctx workflow.Context,
    next int64,
) (ChangeLimitResult, error) {
    previous := limit
    limit = next
    return ChangeLimitResult{Previous: previous, Current: limit}, nil
})
```

Update handler 支持两种签名：

```go
func(workflow.Context, ...Args) error
func(workflow.Context, ...Args) (Result, error)
```

Handler 可以使用 `Await`、Activity Future、Selector、Channel 和 `workflow.Go`。参数、结果和错误详情通过确定性 `json/plain-v1` Payload 持久化。

## Accepted 与 Completed

- **Accepted**：Update 已进入 Run 的历史并获得稳定 Update ID。
- **Completed**：Handler 已返回结果或业务错误。

调用端通过 `WaitForStage` 决定初次调用等待到哪个阶段。Accepted Handle 的 `Get` 会继续等待同一个 Update 的 Completed 结果。

## Handler 注册契约

Update Name 是 Workflow 代码和调用端之间的稳定契约：

```text
调用端 UpdateName = "change-limit"
Workflow SetUpdateHandler name = "change-limit"
```

Workflow 没有注册对应 Handler 时，Server 先写入 `WorkflowUpdateAccepted`，Worker 在主流程第一次阻塞时找不到同名 Handler，用 Type 为 `UnknownUpdate` 的 Failure 完成这个 Update。调用端使用 `Accepted` 可以观察到已接受状态；调用端使用 `Completed` 得到 `*apperror.Error`（`Type() == "UnknownUpdate"`）。

Workflow 注册 Handler 的时机也属于版本契约。Handler 应在主流程第一次阻塞之前注册；在那之后才注册的 Handler 接不到已经到达的 Update，这些 Update 以 `UnknownUpdate` 完成。发布新版本时保留旧 Update Name，完成存量 Run 后再移除旧名称。

## Handler 生命周期

```text
Update 请求
  → 参数校验（可选 Validator）
  → WorkflowUpdateAccepted
  → Handler 排队
  → Handler Await/Activity/并发执行
  → WorkflowUpdateCompleted(success 或 failure)
```

Handler 返回 `error` 会生成 Completed failure，客户端通过 `*apperror.Error` 读取类型、消息和 details。Handler 使用 Activity 时，Activity 的重试会延迟 Update Completed；Handler 使用 Await 时，必须有后续 Signal、Timer、Activity 或其他事件推进条件。

## 终态与并发竞态

- Update 已 Completed：重复读取同一 Update ID 会复用结果。
- Update 已 Accepted、Run 被取消或终止：Handler 可能无法完成，客户端读取到终态错误。
- Run 已 Completed 后发送新 Update：请求进入拒绝路径，调用端应检查 Run 状态。
- 多个 Update 使用不同 Update ID：按 Accepted Event ID 顺序执行，业务层负责冲突校验。
- 多次请求使用同一 Update ID：视为同一逻辑请求，服务端复用已有 Accepted/Completed 记录。

## Validator 与输入校验

Validator 是 Server 侧按 Update Name 注册的无状态输入校验器，适合参数范围等无副作用的快速拒绝；注册入口见 [Typed Update](/develop/updates)。依赖 Workflow 状态的校验、数据库查询和外部调用放入 Handler 或 Activity。Validator 拒绝会返回 `ErrUpdateRejected`，History 不产生事件。

## 选型

| 需求 | 使用 |
| --- | --- |
| 通知 Workflow 一个外部事实，调用方不等结果 | [Signal](/develop/signals) |
| 读取 Workflow 当前状态，不改变它 | [Query](/develop/queries) |
| 修改 Workflow 状态并同步取得结果或业务错误 | [Typed Update](/develop/updates) |
| Run 不存在时先启动再投递 | [Signal-With-Start](/sdk/signal-with-start)、[Update-With-Start](/sdk/update-with-start) |
