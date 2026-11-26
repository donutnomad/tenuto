---
title: Query
description: 读取 Workflow 内存状态、处理 Query 拒绝和重放错误
status: Stable
---

# Query

<FeatureStatus status="Stable" />

Query 在 Workflow 重放到最后一个已完成的 Workflow Task 后执行只读 Handler。Query 结果不会写入 Event History。

Server 为每个 Query 读一次 History 并在进程内重放。一个 Server 进程同时重放的 Query 数受 [`WORKFLOW_QUERY_CONCURRENCY`](/reference/configuration)（默认 8）限制，超出的 Query 不排队，立即应答 503 与 `Retry-After: 1`（对标 Temporal 的 `RESOURCE_EXHAUSTED`），Go SDK 按瞬时错误退避重发；`__stack_trace` 不重放，不占名额。

Query 读到的是已提交状态：最后一个已完成的 Workflow Task 没有看到的 Signal、Activity 结果等事实（它完成之后到达的，以及它执行期间到达的），在下一个 Workflow Task 完成后才体现在 Query 结果里。Workflow Task 正在执行时发起的 Query 读到的是它开始之前的状态。

## 注册 Handler

```go
err := workflow.SetQueryHandler(ctx, "progress",
    func(ctx workflow.Context, _ map[string]any) map[string]any {
        return map[string]any{
            "stage":     stage,
            "processed": processed,
            "total":     total,
        }
    },
)
if err != nil {
    return err
}
```

`SetQueryHandler` 返回 `error`：Query 名为空或 Handler 形状不对时登记失败。生成器在生成期做同样的检查并给出文件行号。

Handler 可以读取 Workflow 内存变量和闭包捕获。Query 执行期间生成的 Command 会被隔离，业务设计应保持 Handler 纯读。Handler 不能阻塞或起协程：调用 `workflow.Await`、`Sleep`、Future 的 `Get`、Channel 收发、`Selector.Select`、`WaitGroup.Wait` 或 `workflow.Go` 时，这次 Query 以 `query handler must not block or start coroutines` 被拒。Handler 也不能改变 Signal 的消费：对还没取到 Signal 的 `SignalFuture` 调用 `Get` 或把它放进 `Selector.Select`、而此时有同名 Signal 可取时，这次 Query 以 `query handler must not consume a Signal` 被拒；已经取到 Signal 的 Future 可以 `Get`，返回原值。

## 调用与结果

```go
value, err := c.QueryWorkflow(ctx, workflowID, runID, "progress")
var rejected *client.QueryFailedError
if errors.As(err, &rejected) {
    return fmt.Errorf("query rejected: %s", rejected.Message)
}
if err != nil {
    return err
}
var progress Progress
err = value.Get(&progress)
```

`c` 是 [`sdk/client`](/sdk/client) 的 `Client`。Query 至多带一个参数，编码成 JSON 对象；结果是 Handler 返回的 map，`value.Get` 解码进结构体或 map。Server 的拒绝（HTTP 应答里的 `query_rejected` 与 `query_error`）到达调用端是 `*client.QueryFailedError`。

Query 支持只提供 Workflow ID 并解析 current Run。终态 Run 仍可通过 Run ID 或 current mapping 查询其最终内存状态。

## 业务拒绝

当前 Query Handler 使用 `__query_error` 表达业务拒绝：

```go
return map[string]any{"__query_error": "progress is private"}
```

Server 将其映射为 Query 拒绝（`query_rejected=true`、`query_error`，Go 调用端是 `*client.QueryFailedError`），History 长度保持不变。

## 行为矩阵

| 场景 | 结果 |
| --- | --- |
| Handler 正常返回 | 结果包含现场值 |
| Query Name 未注册 | 拒绝，错误含 `unknown queryType` |
| Handler 返回 `__query_error` | 拒绝，错误为业务原因 |
| 重放遇到确定性失配 | 拒绝，错误含 replay failure |
| Run 已完成 | 重放终态 History 并执行 Handler |
| 首个 Workflow Task 尚未完成 | 拒绝，错误为 `workflow has not completed its first workflow task yet; query handlers are not registered` |
| Query Context 截止 | 调用返回 Context 错误 |
| Handler 尝试调度 Activity | Command 被查询隔离层丢弃 |
| Handler 调用阻塞原语或 `workflow.Go` | 拒绝：`query handler must not block or start coroutines ...` |
| Handler 让 `SignalFuture` 取走一条 Signal | 拒绝：`query handler must not consume a Signal ...` |
| 旧 Program 缺少用户 Handler | 用户 Query 返回 unknown；内置 stack trace 仍可使用 |

## 内置 Query

名称 `__stack_trace` 的 Query 由 Server 直接回答，不重放 Workflow，Workflow 不需要注册 Handler。返回值是 Run 的逻辑进度，粒度为 Activity：

- `steps`：History 中每个已调度 Activity 的 `activity_id`、`activity_type` 和 `status`（`running`、`completed`、`failed`）。
- `waiting`：Run 当前等待点的 `type`（`workflow_task`、`activity`、`timer`、`child_workflow`）以及适用的 `activity_id`、`child_id`、`activity_type`、`timer_id`；没有这四类等待点时为空对象。
- `summary`：一句话描述终态或当前等待点。

结果不包含协程或源码行。

## 约束

- Handler 签名固定为 `func(ctx workflow.Context, input map[string]any) map[string]any`，写成内联闭包；Query 名非空。
- 同名重复注册时后一次覆盖前一次。
- Handler 在主流程执行到 `SetQueryHandler` 且该 Workflow Task 完成之后才可用，在 Workflow 开头注册。Run 刚启动、首个 Workflow Task 完成之前的 Query 被拒绝，调用端稍后重试。

Query 适合进度、当前阶段和只读业务快照。需要改变状态或取得持久化命令结果时使用 [Update](/develop/updates)。
