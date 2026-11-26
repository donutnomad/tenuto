---
title: 取消、超时与终止
description: Workflow 和 Activity 的停止语义、超时边界与传播路径
status: Stable
---

# 取消、超时与终止

<FeatureStatus status="Stable" />

取消、超时和终止都会改变 Run 的执行路径，触发方式和清理语义各有不同。取消的语义与 Temporal Go SDK 相同。

| 操作 | 触发方 | Workflow 代码 | 运行中的 Activity |
| --- | --- | --- | --- |
| 取消 | 调用端 `CancelWorkflow` | 继续运行：根 Context 被取消，代码决定清理与终态 | 代码取消它所在的作用域时被请求取消，经 Heartbeat 得知 |
| 终止 | 调用端 `TerminateWorkflow` | 立即关闭 Run | 后续回报被判定为 stale |
| Activity 超时 | Server 计时 | 按 Retry Policy 重试，最终失败时 `Get` 返回错误 | 当前 Attempt 结束 |

## 取消

```go
err := c.CancelWorkflow(ctx, workflowID, runID)

// Workflow 返回取消错误时，Get 的错误满足 IsCanceledError
if err := c.GetWorkflow(ctx, workflowID, runID).Get(ctx, nil); workflow.IsCanceledError(err) {
    // Run 以 Cancelled 结束
}
```

`c` 是 [`sdk/client`](/sdk/client) 的 `Client`。`runID` 为空时按 Namespace 和 Workflow ID 解析 current Run。对已经请求过取消的 Run 重复调用是幂等的，History 里只有一条 `WorkflowCancellationRequested`；对已关闭的 Run 调用返回错误。

### 取消送达 Workflow 代码

1. Server 在 History 写入 `WorkflowCancellationRequested` 并调度 Workflow Task。
2. Worker 处理这个 Workflow Task 时，在协程推进之前取消 Workflow 的根 Context。送达点是这条事件，重放时在同一位置送达。
3. Workflow 代码继续运行。在已取消的 Context 上：
   - `ExecuteActivity` 的 Activity 被请求取消（`RequestCancelActivity`），Future 返回取消错误；
   - `Sleep` 的 timer 被取消（`CancelTimer` → `TimerCancelled`），`Sleep` 返回取消错误；
   - `ExecuteChildWorkflow` 的子 Workflow 收到取消请求（`RequestCancelChildWorkflow`），Future 返回取消错误；
   - `Await` 返回取消错误；`ctx.Done()` 返回的 Channel 已关闭，`ctx.Err()` 返回 `workflow.ErrCanceled`；
   - `WaitSignal` 的 `Get`、Channel 的 `Receive` 与 `Selector.Select` 不受取消影响，需要同时等待取消时把 `ctx.Done()` 放进 Selector。
4. Workflow 的返回值决定终态：

| Workflow 返回 | Run 终态 |
| --- | --- |
| 取消错误（`workflow.IsCanceledError(err)` 为 true，包括 cause 链上有取消错误） | `cancelled`，`WorkflowCancelled.details` 是该取消错误的 details |
| `nil` | `completed` |
| 其他错误 | `failed`（按 Workflow 重试策略处理） |

没有取消请求时返回取消错误同样以 `cancelled` 结束，与 Temporal 相同：Server 不检查取消请求。Workflow 不理会取消、继续等待其他事件时，Run 保持运行，直到代码返回。

### 取消后清理

在已取消的 Context 上发起的操作会立即被取消，清理与补偿用 `workflow.NewDisconnectedContext` 返回的 Context：它不随父 Context 取消。

```go
var payment map[string]any
err := workflow.ExecuteActivity(ctx, AwaitPayment, input, workflow.ActivityOptions{
    StartToCloseTimeout: time.Hour,
    HeartbeatTimeout:    10 * time.Second,
}).Get(ctx, &payment)
if workflow.IsCanceledError(err) {
    cleanupCtx, _ := workflow.NewDisconnectedContext(ctx)
    var released map[string]any
    if releaseErr := workflow.ExecuteActivity(cleanupCtx, ReleaseSeat, input,
        workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(cleanupCtx, &released); releaseErr != nil {
        return releaseErr
    }
    return err // Run 以 cancelled 结束
}
```

完整可运行的示例与测试见 `docs/examples/cancellation`。

### 取消作用域

`workflow.WithCancel(ctx)` 返回 ctx 的可取消副本与它的 `cancel`。调用 `cancel` 或父 Context 被取消时，这个作用域里发起、尚未结束的 Activity、timer 与子 Workflow 依次被取消：先按发起顺序取消本作用域的操作，再按创建顺序取消派生的作用域。只取消一部分工作不影响 Workflow 其余部分：

```go
cctx, cancel := workflow.WithCancel(ctx)
defer cancel()
slow := workflow.ExecuteActivity(cctx, Search, input, opts)
var fast map[string]any
if err := workflow.ExecuteActivity(ctx, Lookup, input, opts).Get(ctx, &fast); err != nil {
    return err
}
cancel() // 只取消 Search
```

`workflow.Go` 的协程沿用传入的 Context；Update handler 与 Query handler 拿到的是根 Context。

### 操作被取消时的结果

| 操作 | 取消时的状态 | 写入的事件 | Workflow 看到 |
| --- | --- | --- | --- |
| Activity | 调度命令还没提交（同一个 Workflow Task 里发起又取消） | 无，命令被撤回 | 取消错误 |
| Activity | 尚未开始（排队中或重试退避中） | `ActivityCancelRequested`，同事务 `ActivityCancelled` | `WaitForCancellation: false`：取消请求发出即返回取消错误；`true`：`ActivityCancelled` 之后返回取消错误 |
| Activity | 已开始 | `ActivityCancelRequested`；Worker 确认后 `ActivityCancelled`（带 details） | `false`：立即返回取消错误；`true`：等 Activity 结束，返回取消错误、结果或失败 |
| Timer | 未触发 | `TimerCancelled`（命令还没提交时不写） | `Sleep` 立即返回取消错误 |
| 子 Workflow | 已发起 | `ChildWorkflowCancelRequested`，子 Run 写入 `WorkflowCancellationRequested` | `ChildWorkflowOptions.WaitForCancellation: false`：立即返回取消错误；`true`：等子 Workflow 结束，子 Run 以 cancelled 结束时返回取消错误 |

取消请求之后，已开始的 Activity 的结局按 Server 收到的第一个结果决定：

- Activity 返回取消错误或 `context.Canceled`：Worker 上报取消确认，记 `ActivityCancelled`；
- Activity 仍返回成功：记 `ActivityCompleted`，完成优先于取消；
- Activity 返回其他错误或 Attempt 超时：不再重试，记 `ActivityFailed` / `ActivityTimedOut`，`retryState` 为 `CancelRequested`；
- Worker 停止时交还的 Attempt 不再派发，记 `ActivityCancelled`。

### Activity 如何得知取消

取消只经 Heartbeat 送达：**Activity 要可取消就必须 Heartbeat**（并配置 `HeartbeatTimeout`）。Workflow 请求取消某个 Activity 后，下一次心跳请求的响应带回取消（心跳由 Worker 节流，见 [Heartbeat](/develop/activity-execution#heartbeat)），Worker 取消 Activity 的 `ctx`。Activity 在 `ctx.Done()` 后释放数据库事务、锁、临时文件和第三方会话，然后返回 `ctx.Err()` 或 `apperror.NewCanceled(details...)`（details 写进 `ActivityCancelled`）确认取消；不 Heartbeat 的 Activity 收不到取消，只在自己返回或超时时结束。

Workflow 的取消请求本身不取消任何 Activity：Run 收到取消请求后 Activity 照常开始与执行，直到 Workflow 代码取消它们所在的作用域。清理 Activity 因此能在取消之后调度并运行。

## 终止

```go
err := c.TerminateWorkflow(ctx, workflowID, runID, "manual intervention")
// 之后 Get 返回的错误 errors.As 到 *client.TerminatedError
```

终止立即结束 Run，不等待 Workflow 代码，也不依赖 Activity 的协作式响应。终止适用于泄漏、死循环、错误版本和人工止损。外部系统在终止后通过幂等补偿任务处理已经发出的副作用。

## Activity 超时

Activity 支持四类超时，通过 `workflow.ActivityOptions` 配置：

| 超时 | 检查对象 | 常见原因 |
| --- | --- | --- |
| `ScheduleToStartTimeout` | 每个 Attempt 在队列中的等待，从可派发时起算 | Worker 容量不足或队列错误 |
| `StartToCloseTimeout` | 单次 Attempt | Activity 执行卡住 |
| `ScheduleToCloseTimeout` | 整个 Call，含重试 | 外部依赖持续失败 |
| `HeartbeatTimeout` | 两次 Heartbeat 的间隔 | Worker 崩溃或网络中断 |

超时产生 Activity 失败事件。`ScheduleToStartTimeout` 与 `ScheduleToCloseTimeout` 到期后 Call 直接失败；`StartToCloseTimeout` 与 `HeartbeatTimeout` 按 Retry Policy 决定下一次 Attempt。配置了 `StartToCloseTimeout` 时，Worker 给 Activity 的 `ctx` 设置相同的 deadline。详见 [Activity 执行生命周期](/develop/activity-execution)。

## Workflow 级的等待上限

Workflow 内的业务时限用 Timer 表达：在一条协程里 `workflow.Sleep`，在另一条协程里等待业务事件，用 `workflow.Await` 等待两者之一。示例见 [Timer 与 Workflow Sleep](/develop/timers) 的「并发中的 Timer」。

## Workflow Task 超时

单个 Workflow Task 的执行超时由 `worker.Options.WorkflowTaskTimeout` 控制，缺省 10 秒。Worker 领取 Workflow Task 时把这个值报给 Server，Server 记在 `WorkflowTaskStarted` 的 `timeoutMs` 里并从领取时刻开始计时。

超时由两侧共同执行：

- Worker 在超时内没有执行完时上报超时。Worker 优雅停止的宽限期到期时放弃仍未完成的 Workflow Task、不上报，由 Server 的计时重新调度。
- Worker 领取后崩溃或失联、没有在超时内关闭 Workflow Task 时，Server 到期自己记 `WorkflowTaskTimedOut`。

两侧对同一个 Workflow Task 只写一次 `WorkflowTaskTimedOut`。超时后 Server 立即调度新的 Workflow Task，并结束该 Run 的 sticky 会话：新的 Workflow Task 进普通队列，任何 Worker 都能领取。Server 重启后按持久化的截止时间继续计时；多副本部署中，持有计时的副本下线后，其他副本的到期扫描在宽限期（30 秒）之后接管。Run 的 History 只多出超时事件，业务状态不受影响。

## 组合场景

- 取消中的 Activity 仍可能完成一次外部调用，完成结果优先于取消；Activity 以幂等键保护写入。
- 超时后的旧 Attempt 回报通过 Task Token 和 generation stamp 校验，过期回报不推进当前状态。
- 终止后的任务被 Server 判定为 stale，Worker 确认任务并停止后续动作。
- 父 Run 以任何方式关闭（包括 Workflow 代码决定的 cancelled）后，仍在运行的子 Run 按[父关闭策略](/develop/child-workflows#父关闭策略)处理，默认被终止；`RequestCancel` 不会对已经收到取消请求的子 Run 重复请求。
- 已接受、尚未完成的 Update 在 Run 以 cancelled 关闭时以 `AcceptedUpdateCompletedWorkflow` 失败结束，与其他终态相同。
