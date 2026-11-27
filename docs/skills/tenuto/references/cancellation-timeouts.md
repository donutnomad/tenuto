# 取消、终止与超时

| 操作 | 触发方 | Workflow 代码 | 运行中的 Activity |
| --- | --- | --- | --- |
| 取消 | 调用端 `CancelWorkflow` | 继续运行：根 Context 被取消，代码清理后按返回值结束 | Workflow 取消它所在的 Context 时被请求取消，下一次 Heartbeat 时 `ctx` 被取消 |
| 终止 | 调用端 `TerminateWorkflow` | Run 立即关闭为 `terminated` | 之后的回报被判定为 stale |
| Activity 超时 | Server 计时 | 按 RetryPolicy 重试，最终失败时 `Get` / 桥返回错误 | 当前 Attempt 结束 |

## 取消

```go
err := c.CancelWorkflow(ctx, workflowID, runID) // c 是 sdk/client 的 Client

err = c.GetWorkflow(ctx, workflowID, runID).Get(ctx, nil)
if workflow.IsCanceledError(err) { /* Workflow 返回了取消错误，Run 为 cancelled */ }
```

HTTP：`POST /runtime/workflow/cancel`，body `{"namespace","workflow_id","run_id","reason"}`（Go Client 不带 `reason`）。`runID` 为空时按 Namespace + Workflow ID 解析 current Run；重复调用幂等（只有一条 `WorkflowCancellationRequested`），已关闭的 Run 返回错误。语义与 Temporal Go SDK 相同。

### 送达与终态

1. Server 写入 `WorkflowCancellationRequested` 并调度 Workflow Task。
2. Worker 在这条事件之后、协程推进之前取消根 Context；重放时在同一位置送达。同一个 Workflow Task 看到的事件里，取消请求之前已有结果的 Activity、timer、子 Workflow 照常返回结果；取消请求之后才到的结果不改变已按取消返回的 Future（与 Temporal 按事件顺序处理相同）。
3. Workflow 代码继续运行。已取消 Context 上的操作：`ExecuteActivity` → 请求取消 Activity；`Sleep` → 取消 timer（`TimerCancelled`），立即返回取消错误；`ExecuteChildWorkflow` → 子 Run 收到取消请求；`Await` → 条件不满足时返回取消错误；`ctx.Done()` 已关闭，`ctx.Err()` 返回 `workflow.ErrCanceled`。`WaitSignal`、Channel、`Select` 不受影响，要同时等取消就把 `ctx.Done()` 放进 Selector。
4. 返回值决定终态：取消错误（`workflow.IsCanceledError` 为 true）→ `cancelled`（`WorkflowCancelled.details` 来自该错误）；`nil` → `completed`；其他错误 → `failed`。没有取消请求时返回取消错误也是 `cancelled`。

### API（`sdk/workflow`）

| API | 说明 |
| --- | --- |
| `workflow.Context` | 接口：`Done() Channel`、`Err() error` |
| `workflow.WithCancel(ctx) (Context, CancelFunc)` | 可取消的子作用域；父取消时一起取消 |
| `workflow.NewDisconnectedContext(ctx) (Context, CancelFunc)` | 不随父取消的作用域，用于取消后的清理 |
| `workflow.ErrCanceled`、`workflow.NewCanceledError(details...)`、`workflow.IsCanceledError(err)` | 取消错误；`IsCanceledError` 也看 cause 链 |
| `ActivityOptions.WaitForCancellation`、`ChildWorkflowOptions.WaitForCancellation` | `false`（缺省）取消请求发出即返回取消错误；`true` 等操作真正结束 |

### 取消后清理（补偿）

```go
var payment map[string]any
err := workflow.ExecuteActivity(ctx, AwaitPayment, input, workflow.ActivityOptions{
    StartToCloseTimeout: time.Hour,
    HeartbeatTimeout:    10 * time.Second,
}).Get(ctx, &payment)
if workflow.IsCanceledError(err) {
    cleanupCtx, _ := workflow.NewDisconnectedContext(ctx) // 不随根 Context 取消
    var released map[string]any
    if releaseErr := workflow.ExecuteActivity(cleanupCtx, ReleaseSeat, input,
        workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(cleanupCtx, &released); releaseErr != nil {
        return releaseErr
    }
    return err // cancelled
}
```

清理不能发在已取消的 ctx 上：在它上面发起的 Activity 不会被调度，Future 直接返回取消错误。可运行示例：仓库 `docs/examples/cancellation`。

### 局部取消

```go
cctx, cancel := workflow.WithCancel(ctx)
defer cancel()
search := workflow.ExecuteActivity(cctx, Search, input, opts)
// ... 另一路先完成后
cancel() // 只取消 cctx 里的 Activity / timer / 子 Workflow
```

取消顺序确定：先按发起顺序取消本作用域的操作，再按创建顺序取消派生作用域。同一个 Workflow Task 里发起又取消的操作，命令直接撤回。

### 结果规则

| 操作与状态 | 事件 | 结果 |
| --- | --- | --- |
| Activity 未开始（排队或重试退避） | `ActivityCancelRequested` + 同事务 `ActivityCancelled` | 不再派发 |
| Activity 已开始 | `ActivityCancelRequested`；Heartbeat 得知后返回 `ctx.Err()` / 取消错误 → `ActivityCancelled`（带 details） | 返回成功 → `ActivityCompleted`（完成优先）；其他错误或超时 → 不重试，`retryState` 为 `CancelRequested`；Worker 停止交还 → `ActivityCancelled` |
| Timer | `TimerCancelled` | 不再触发 |
| 子 Workflow | `ChildWorkflowCancelRequested`，子 Run 写 `WorkflowCancellationRequested` | 子 Workflow 代码决定终态；`cancelled` 时父的 `Get` 返回取消错误 |

设计规则：

- Activity 要可取消就必须 Heartbeat（并设置 `HeartbeatTimeout`）：取消只经 `activity.RecordHeartbeat` 的响应送达。不 Heartbeat 的 Activity 只在自己返回或超时时结束。Activity 在 `ctx.Done()` 后释放事务、锁、临时文件，返回 `ctx.Err()` 或 `apperror.NewCanceled(details...)` 确认取消。
- Workflow 的取消请求本身不取消 Activity，也不阻止 Attempt 开始；只有 Workflow 取消 Activity 所在的 Context 时才发出取消请求。
- 取消中的 Activity 仍可能完成一次外部调用，用幂等键保护写入。
- 不等子 Workflow（`WaitForCancellation: false`）的父 Run 关闭后，仍在运行的子 Run 按父关闭策略处理：默认终止，`ParentClosePolicyRequestCancel` 不重复请求，`ParentClosePolicyAbandon` 继续运行（见 `child-and-continue-as-new.md`）。
- 已接受未完成的 Update 在 Run 以 `cancelled` 关闭时以 `AcceptedUpdateCompletedWorkflow` 失败结束。
- Run 以任何方式结束（完成、失败、取消、终止、超时、continue-as-new）时，仍未触发的 timer（例如 Signal 先到、被放弃的等待超时 timer）在同一事务里置为 `run_closed`：不写 `TimerCancelled`（同 Temporal），到期也不再触发。仍未结束的 Activity 同样置为 `run_closed`，不写事件，不再超时或重试；Worker 之后的心跳收到 Run 已结束。

## 终止

```go
err := c.TerminateWorkflow(ctx, workflowID, runID, "manual intervention")
// 之后 Get 返回的错误 errors.As 到 *client.TerminatedError{Reason: "manual intervention"}
```

HTTP：`POST /runtime/workflow/terminate`，body 同取消。终止立即结束 Run，不经过 Workflow Task。用于泄漏、死循环、错误版本、人工止损；已发出的外部副作用由幂等的补偿任务处理。

批量取消、终止见 `operations.md` 的 Batch。

## Activity 超时

通过 `workflow.ActivityOptions` 配置（字段是 `time.Duration`，如 `30 * time.Second`；完整说明在 `activities.md`）：

| 超时 | 计时范围 | 常见原因 |
| --- | --- | --- |
| `ScheduleToStartTimeout` | 每个 Attempt 在队列中的等待，从可派发（首次调度、退避到期或 reset）时起算 | Worker 容量不足、队列写错 |
| `StartToCloseTimeout` | 单个 Attempt | 执行卡住 |
| `ScheduleToCloseTimeout` | 整个 Call，含排队与重试 | 外部依赖持续失败 |
| `HeartbeatTimeout` | 两次 Heartbeat 的间隔 | Worker 崩溃、网络中断 |

- `ScheduleToStartTimeout` 与 `ScheduleToCloseTimeout` 到期后 Call 直接失败；`StartToCloseTimeout` 与 `HeartbeatTimeout` 按 RetryPolicy 决定下一次 Attempt。
- Activity 的 `ctx` 截止在本次 Attempt 的 deadline：`StartToCloseTimeout` 与 `ScheduleToCloseTimeout` 剩余时间中先到的一个（同 Temporal Go SDK）。Attempt 在 `ctx` 截止之后才返回时 Worker 不上报结果（成功或 `context deadline exceeded`），由 Server 判定超时，Workflow 稳定收到超时错误，`workflow.TimeoutTypeOf(err)` 返回对应种类。心跳应答明确答复 Attempt 已失效（Run 已结束、Activity 已关闭、Attempt 已过期，或 Run 不存在 `run_not_found`）时 Worker 取消 `ctx`、不上报，Activity 返回后执行槽位随即释放；心跳请求的其他错误（包括反向代理的 4xx）只算一次失败的心跳，继续执行。
- 每个调用至少设置 `StartToCloseTimeout` 或 `ScheduleToCloseTimeout`：Attempt 开始后它的存活只由超时判定。
- 超时后旧 Attempt 的回报按 Task Token 校验，过期回报不推进状态。
- 每次超时写一条 `ActivityTimedOut`，`timeoutType` 是超时种类（`ScheduleToStart`、`StartToClose`、`ScheduleToClose`、`Heartbeat`），`failure.message` 是说明，`lastHeartbeatDetails` 是超时时最后一次心跳的详情（心跳本身不写 History）。Call 因超时最终失败时，`workflow.TimeoutTypeOf(err)` 返回超时种类，不是超时错误时为空串。

## Workflow 内的业务时限

Workflow 级的等待上限用 Timer 表达：一条协程 `workflow.Sleep`，另一条等待业务事件，主协程 `workflow.Await` 两者之一。完整代码见 `time-and-randomness.md` 的「带超时的等待」。

## Workflow Task 超时

单个 Workflow Task 的执行上限由 `worker.Options.WorkflowTaskTimeout` 控制，缺省 10 秒。Worker 领取时把它报给 Server，Server 记在 `WorkflowTaskStarted.timeoutMs` 并从领取时刻计时：

- Worker 超时内没执行完时自己上报超时；Worker 领取后崩溃或失联时，Server 到期记 `WorkflowTaskTimedOut`。同一个 Workflow Task 只记一次。
- 超时后 Server 立即调度新的 Workflow Task 并结束 sticky 会话，新任务进普通队列，任何 Worker 都能领取。超时已提交、新任务还没调度时副本退出或写库失败，周期补偿与启动恢复补上调度。
- 计时持久化：Server 重启后继续计时；持有计时的副本下线后，其他副本的到期扫描在 30 秒宽限期后接管。

Workflow 函数体里的纯计算受执行预算约束（见 `go-subset.md`）；耗时计算放进 Activity。
