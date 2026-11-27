---
title: Activity 执行生命周期
description: Activity Call、Attempt、ActivityOptions、重试、超时、Heartbeat 和取消
status: Stable
---

# Activity 执行生命周期

<FeatureStatus status="Stable" />

一次 `CallX` 或 `workflow.ExecuteActivity` 形成一个 Call。生效的 Retry Policy 可以让该 Call 产生多个 Attempt。

```text
Call: payment.capture
  ├─ Attempt 1 → timeout
  ├─ Attempt 2 → application failure
  └─ Attempt 3 → completed
```

Call 使用 Scheduled Event ID 和 ActivityID（`ActivityOptions.ActivityID`，为空时取调用位置，见 [ActivityID](/sdk/activity-options#activityid)）定位；Attempt 从 1 递增，用于诊断、退避和 Heartbeat 恢复。Activity 里用 `activity.GetInfo(ctx)` 读取它们，见 [读取执行上下文](/develop/activities#读取执行上下文)。

## ActivityOptions

选项作为桥或 `ExecuteActivity` 的最后一个参数传入：

```go
paid, err := CallChargePayment(ctx, in, workflow.ActivityOptions{
    TaskQueue:              "payments",
    StartToCloseTimeout:    30 * time.Second,
    ScheduleToCloseTimeout: 5 * time.Minute,
    HeartbeatTimeout:       10 * time.Second,
    RetryPolicy: workflow.RetryPolicy{
        MaxAttempts:            5,
        InitialInterval:        time.Second,
        BackoffCoefficient:     2,
        MaxInterval:            30 * time.Second,
        NonRetryableErrorTypes: []string{"PaymentDeclined"},
    },
})
```

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `TaskQueue` | `string` | 覆盖本次调用的 Activity Task Queue |
| `ScheduleToStartTimeout` | `time.Duration` | 每个 Attempt 从可派发到 Worker 开始执行的最长等待 |
| `StartToCloseTimeout` | `time.Duration` | 单个 Attempt 的最长执行时间 |
| `ScheduleToCloseTimeout` | `time.Duration` | 整个 Call（含排队与重试）的总时限 |
| `HeartbeatTimeout` | `time.Duration` | 两次 Heartbeat 的最长间隔 |
| `RetryPolicy` | `workflow.RetryPolicy` | 本次调用的重试策略；零值使用缺省策略 |
| `WaitForCancellation` | `bool` | ctx 被取消后 Future 何时返回：`false`（缺省）取消请求发出即返回取消错误，`true` 等 Activity 结束（`ActivityCancelled`、完成或失败），见下文「取消」 |

超时字段是 `time.Duration`，零值表示不设置该超时。选项只作用于这一次调用：同一个 Activity 的另一处调用不继承这里的 `TaskQueue`、超时和重试策略。选项里没有 Build ID：Activity 由领取任务的 Worker 执行，版本由 Run 的 `policy` 与 Program 管理（见 [版本管理](/develop/versioning#activity-worker-与-build-id)）。

每个调用必须设置 `StartToCloseTimeout` 或 `ScheduleToCloseTimeout`，两者至少一个：

- 生成器检查调用点的 `workflow.ActivityOptions` 字面量，两者都缺少时报错，错误带 `file:line`：`CallX(ctx, in) 没有设置 StartToCloseTimeout 或 ScheduleToCloseTimeout；传入 workflow.ActivityOptions{StartToCloseTimeout: 30 * time.Second}`。
- 选项来自变量时生成期不可知，由 Server 在调度时检查：缺少时这次 Workflow Task 以 cause `bad_schedule_activity_attributes` 失败、按退避重试，Run 显示 Blocked，修复代码重新发布后继续。

## 投递与执行次数

Server 记录 Attempt started 之后，Worker 立即确认队列租约，然后才运行 Activity 函数。从这一刻起 Attempt 的存活只由 Start-To-Close、Schedule-To-Close 与 Heartbeat 超时判定：

- Attempt 正常运行时不会因为队列租约的可见性超时被重投；已开始的 Attempt 在队列里多出的副本被 Worker 领到时，Server 答复不执行，Worker 确认丢弃。
- Activity 结束后 Worker 在 min(Attempt 剩余时间, 60s)（至少 5s）内重试上报结果，Server 滚动重启期间结果仍能送达。
- Worker 在 Attempt 开始后崩溃、或结果没能上报，Attempt 停在 started，到期的 Start-To-Close（或 Heartbeat）超时按 Retry Policy 产生下一次 Attempt。
- Worker 优雅停止时交还因停止而结束的 Attempt：Server 不记失败，以同一个 Attempt 序号立即重新派发，不消耗重试次数。见[运行 Worker](/develop/workers#优雅退出)。
- Server 重启不会把仍在持久队列（TaskQ）里的任务再入队一次；内存队列随进程丢失，内存后端的启动恢复重新投递。

Activity 的执行语义是至少一次（at-least-once），不是只执行一次：超时重试、上报结果前崩溃都会让同一个业务操作再执行。每个 Activity 都必须幂等，外部写入使用幂等键，见 [Activity 幂等](/best-practices/activity-idempotency)。

## 超时

| Timeout | 计时范围 | 常见原因 |
| --- | --- | --- |
| Schedule-To-Start | 每个 Attempt 从可派发到 Worker Started | Worker 缺失、队列拥塞、在线 Worker 的契约都与 Program 声明不兼容 |
| Start-To-Close | 单个 Attempt 执行时间 | 下游卡住、代码超时 |
| Schedule-To-Close | 整个 Call，包括排队与重试 | 业务总时限 |
| Heartbeat | 两次 Heartbeat 的间隔 | 长任务失联、进程退出 |

Schedule-To-Start 与 Schedule-To-Close 超时后 Call 直接失败，不再重试。Start-To-Close 与 Heartbeat 超时按 Retry Policy 决定下一次 Attempt。

Worker 给 Activity 的 `ctx` 设置本次 Attempt 的截止时间：Start-To-Close 与 Schedule-To-Close 剩余时间中先到的一个（同 Temporal Go SDK）。Attempt 在 `ctx` 截止之后才返回时（成功或返回 `context deadline exceeded`）Worker 不上报结果，由 Server 判定超时并写 `ActivityTimedOut`，Workflow 稳定收到超时错误；Activity 返回后执行槽位随即释放。

每次超时在 History 里写一条 `ActivityTimedOut`，`timeoutType` 是超时种类（`ScheduleToStart`、`StartToClose`、`ScheduleToClose`、`Heartbeat`），`failure.message` 是说明（如 `activity StartToClose timeout`），`lastHeartbeatDetails` 是超时时最后一次心跳的详情。Call 因超时最终失败时，Workflow 拿到的错误是超时错误，`workflow.TimeoutTypeOf(err)` 返回它的超时种类，见 [超时错误](/sdk/activity-options#超时错误)。

Schedule-To-Start 约束每一个 Attempt，从该 Attempt 可派发的时刻起算：首个 Attempt 从调度时起算，重试的 Attempt 从退避到期时起算，Reset Activity 产生的 Attempt 从 reset 时起算。

两次 Attempt 之间的退避截止时间保存在 Activity 状态里（`nextAttemptAt`）。Server 重启或切换副本后，下一次 Attempt 仍在这个时间点派发，它的 Schedule-To-Start 也从这个时间点起算。

## RetryPolicy

| 字段 | 类型 | 缺省 | 含义 |
| --- | --- | --- | --- |
| `MaxAttempts` | `int` | `0` | 最大 Attempt 数（含首次）；`0` 表示不限次数，`1` 表示不重试 |
| `InitialInterval` | `time.Duration` | `1s` | 首次重试前的等待 |
| `BackoffCoefficient` | `float64` | `2.0` | 每次重试的间隔倍数 |
| `MaxInterval` | `time.Duration` | `100 × InitialInterval` | 重试间隔上限 |
| `NonRetryableErrors` | `[]string` | 空 | 失败 message 包含其中任一子串时停止重试 |
| `NonRetryableErrorTypes` | `[]string` | 空 | 失败 Type 与其中任一项完全相等（大小写敏感）时停止重试 |

缺省值与 Temporal 相同，由 Server 统一补齐：`ActivityOptions.RetryPolicy` 保持零值的调用使用整套缺省策略，不限次数重试；只设置部分字段的策略，其余字段取缺省值。`ActivityScheduled` 事件记录补齐后的生效策略。只想执行一次的调用写 `RetryPolicy: workflow.RetryPolicy{MaxAttempts: 1}`。

停止重试的判定顺序：

1. 失败带 `NonRetryable` 标记（`apperror.NewNonRetryable`）。
2. 失败 Type 命中 `NonRetryableErrorTypes`。
3. 失败 message 命中 `NonRetryableErrors`。
4. 已达到 `MaxAttempts`。
5. `ScheduleToCloseTimeout` 已到期，或下一次 Attempt 的开始时间会落在它之后（`retryState` 为 `Timeout`）。

没有 `ScheduleToCloseTimeout` 也没有 `MaxAttempts` 的 Call 会一直重试到成功或遇到不可重试的错误。

判定只看顶层失败。用 `fmt.Errorf("%w")` 包装应用错误后，顶层 Type 和不可重试标记属于包装层。

## Heartbeat

```go
import "github.com/donutnomad/tenuto/sdk/activity"

activity.RecordHeartbeat(ctx, map[string]any{"offset": offset})
previous := activity.GetLastHeartbeatDetails(ctx)
```

`RecordHeartbeat` 把进度提交给 Server，响应里带有取消时 Worker 取消 Activity 的 `ctx`。重试 Attempt 与 Worker 停止后重新派发的 Attempt 通过 `GetLastHeartbeatDetails` 读取最后记录的进度；首次 Attempt 返回 `nil`。

Worker 对心跳做节流，`RecordHeartbeat` 可以在循环里频繁调用：

- 两次心跳请求的最小间隔是 `HeartbeatTimeout` 的 80%，没有 `HeartbeatTimeout` 时 30s，上限 60s。
- 间隔外的一次调用立即发出（在调用方 goroutine 里同步等待，最多一个间隔）并开启间隔；间隔内的调用只保留最新的 details，间隔结束时发出。
- Activity 返回成功时完成结果取代未发出的 details；以失败、取消或 Worker 停止交还结束时，先发出未发出的最新 details，下一次 Attempt 用 `GetLastHeartbeatDetails` 读到它们。
- 设置了 `HeartbeatTimeout` 时，心跳请求连续失败超过 `HeartbeatTimeout`（Server 已按 Heartbeat 超时结束这次 Attempt）后 Worker 取消 `ctx`，结果不再上报。
- Server 以 4xx 拒绝心跳（例如 Run 不存在）时，这次 Attempt 不再有接收方：Worker 立即取消 `ctx`，结果不再上报。

Heartbeat 不写 History：Server 只更新 Activity 状态里的最后一次详情与时间（MySQL 上是一条 UPDATE），并从这个时间重新计算 Heartbeat 超时。运行中的详情在 Console 的 Pending 页签与 `activity-states` 里查看；Attempt 超时时，`ActivityTimedOut` 的 `lastHeartbeatDetails` 保存当时最后一次心跳的详情。

## 取消

Activity 的取消由 Workflow 代码发起：Activity 所在的 Context 被取消（Run 收到取消请求时的根 Context，或 `workflow.WithCancel` 的作用域）时，Workflow 产生 `RequestCancelActivity`，History 记 `ActivityCancelRequested`。Run 收到取消请求本身不取消 Activity，也不阻止新的 Attempt 开始，清理 Activity 因此能在取消之后运行。

取消的约定与 Temporal 相同：

- 尚未开始的 Activity（排队中或重试退避中）立即以 `ActivityCancelled` 结束，不再派发。
- **已开始的 Activity 要可取消就必须 Heartbeat。** 取消只经心跳请求的响应送达，没有单独的轮询通道；心跳按上面的节流间隔发出，取消的延迟不超过一个节流间隔加上 Activity 两次 `RecordHeartbeat` 的间隔。不 Heartbeat 的 Activity 收不到取消，只在自己返回或超时时结束。
- 收到取消后返回 `ctx.Err()` 或取消错误：Worker 上报取消确认，记 `ActivityCancelled`（带取消错误的 details）。返回成功：记 `ActivityCompleted`。返回其他错误或 Attempt 超时：不再重试，`retryState` 为 `CancelRequested`。Worker 停止时交还的 Attempt 以 `ActivityCancelled` 结束。
- Workflow 何时拿到结果由 `ActivityOptions.WaitForCancellation` 决定：`false`（缺省）在取消请求发出时返回取消错误，`true` 等上面的结局。
- Run 进入任一终态后，或这次 Attempt 已超时、被 Reset Activity 取代后，仍在执行的 Activity 同样在下一次 Heartbeat 时被取消 `ctx`，它的结果不再上报。

长任务在安全检查点检查 `ctx.Done()`，提交外部写入时继续使用稳定幂等键。

## 失败如何返回 Workflow

Call 最终失败时，`Get` 或桥调用返回结构化错误，保留 Activity 返回的 Type、不可重试标记、details 和 cause：

```go
paid, err := CallChargePayment(ctx, in, workflow.ActivityOptions{StartToCloseTimeout: 30 * time.Second})
if err != nil {
    if workflow.IsApplicationError(err, "PaymentDeclined") {
        // 类型化分支
    }
    return OrderOutput{}, err
}
```

Activity 函数 panic 时，Worker 捕获它并把这次 Attempt 上报为失败：Failure 的 Type 是 `PanicError`，Message 是 `activity panicked: <panic 值>`，details 带调用栈。它按 RetryPolicy 重试；把 `PanicError` 加入 `NonRetryableErrorTypes` 可以让 panic 直接成为最终失败。Worker 进程继续处理其他任务。

## Reset

Activity Reset 会提升 execution generation、清理 Sticky assignment，并以 Attempt 1 重新调度目标 Call。旧 Task 的 Stamp 失效，其迟到回报进入幂等丢弃路径。

相关内容：[Activity 重试与超时](/sdk/activity-options)、[异步 Activity 完成](/develop/async-activity-completion)、[ApplicationError](/develop/application-error)、[Activity 故障排查](/troubleshooting/activities)。
