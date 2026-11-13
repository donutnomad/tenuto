---
title: Activity 重试与超时
description: 配置 ActivityOptions、ActivityID、RetryPolicy、Attempt 与 Reset
status: Stable
---

# Activity 重试与超时

<FeatureStatus status="Stable" />

每次 `ExecuteActivity` 都可以携带独立的 ID、路由、超时和重试策略。

```go
future := workflow.ExecuteActivity(
    ctx,
    activities.SettlePayment,
    input,
    workflow.ActivityOptions{
        ActivityID:             "settle-" + orderID,
        TaskQueue:              "payments",
        ScheduleToStartTimeout: 30 * time.Second,
        StartToCloseTimeout:    5 * time.Minute,
        ScheduleToCloseTimeout: 30 * time.Minute,
        HeartbeatTimeout:       15 * time.Second,
        RetryPolicy: workflow.RetryPolicy{
            MaxAttempts:        5,
            InitialInterval:    time.Second,
            BackoffCoefficient: 2.0,
            MaxInterval:        time.Minute,
            NonRetryableErrors: []string{"InvalidAccount"},
        },
    },
)
```

## ActivityID

`ActivityID` 是这次调用在 Run 内的业务 ID，对应 Temporal 的 `ActivityOptions.ActivityID`。它写进 `ActivityScheduled` 事件，Activity 里由 `activity.GetInfo(ctx).ActivityID` 读到，同一个 Activity 的各次重试相同；Console、`__stack_trace`、`activity-states` 显示它，Reset Activity 与异步完成按它寻址。

- 为空时取调用在协程内的位置：主协程的第 n 个 Activity 是 `#an`，第 1 条 `workflow.Go` 协程里的第 1 个是 `/0#a1`（见 [稳定的 ID](/develop/determinism#稳定的-id)）。给某个调用指定 ID 不改变其他调用的缺省 ID。
- 可以是运行期计算的值，例如循环里的 `fmt.Sprintf("charge-%d", i)`。值必须确定：重放时同一调用算出的 ID 与 History 不同，Workflow Task 以 `non_deterministic_error` 失败，与换了 Activity 类型相同。
- 同一 Run 里与仍未关闭的 Activity（含已请求取消、尚未确认的）重复时，这次 Workflow Task 以 `schedule_activity_duplicate_id` 失败、按退避重试，Run 显示 Blocked。前一个 Activity 完成、失败、超时或取消之后，同一 ID 可以再用，规则同 Temporal Server。
- 不能为空串，最长 191 个字符；超出时 Workflow Task 以 `bad_schedule_activity_attributes` 失败。

```go
for i, item := range items {
    futures = append(futures, workflow.ExecuteActivity(ctx, activities.Charge, item, workflow.ActivityOptions{
        ActivityID:          fmt.Sprintf("charge-%d", i),
        StartToCloseTimeout: time.Minute,
    }))
}
```

## 四类超时

| 超时 | 覆盖范围 |
| --- | --- |
| Schedule-To-Start | 每个 Attempt 从可派发（首次调度或退避到期）到 Worker 开始执行 |
| Start-To-Close | 单次 Attempt 的执行时间 |
| Schedule-To-Close | Activity 从首次调度到最终结束的总时间 |
| Heartbeat | 两次 heartbeat 之间允许的最大间隔 |

`StartToCloseTimeout` 与 `ScheduleToCloseTimeout` 至少设置一个：Attempt 开始后 Worker 立即确认队列租约，Attempt 的存活由超时判定。生成器拒绝两者都缺少的调用点，Server 拒绝两者都缺少的调度命令（Workflow Task 以 `bad_schedule_activity_attributes` 失败）。见 [Activity 执行生命周期](/develop/activity-execution#投递与执行次数)。

## Attempt

一次 Activity Call 可以产生多个 Attempt。Call 表示 Workflow 代码中的一次逻辑调用，Attempt 表示该调用的一次实际执行尝试。

```text
Call #3
├── Attempt 1  failed: timeout
├── Attempt 2  failed: transient error
└── Attempt 3  completed
```

Trace Inspector 支持按 Call 和 Attempt 导航，查看每次执行的输入、Worker、开始时间、耗时和结果。

所有时长字段都是 `time.Duration`，零值表示未设置。

## Retry Policy

Activity 默认重试，缺省策略与 Temporal 相同，由 Server 统一补齐：

| 字段 | 缺省值 |
| --- | --- |
| `MaxAttempts` | `0`：不限次数 |
| `InitialInterval` | `1s` |
| `BackoffCoefficient` | `2.0` |
| `MaxInterval` | `100 × InitialInterval` |
| `NonRetryableErrorTypes` / `NonRetryableErrors` | 空 |

没有设置 `RetryPolicy` 的调用使用整套缺省策略；只设置了部分字段的策略，其余字段取缺省值。例如只写 `InitialInterval: 5 * time.Second`，`MaxInterval` 就是 `500s`。`ActivityScheduled` 事件记录的是补齐后的生效策略。

重试在以下任一情况停止：

- 成功；
- 失败是不可重试的：`NewNonRetryableApplicationError`，或 Type / message 命中 `NonRetryableErrorTypes` / `NonRetryableErrors`；
- 达到 `MaxAttempts`（包含首次执行，值为 `1` 表示只执行一次）；
- `ScheduleToCloseTimeout` 到期，或下一次 Attempt 的开始时间会落在它之后；
- `ScheduleToStartTimeout` 到期：没有 Worker 在期限内取走任务，换一次 Attempt 也不会改变。

没有设置 `ScheduleToCloseTimeout` 也没有设置 `MaxAttempts` 的 Activity 会一直重试到成功或遇到不可重试的错误。业务上有截止时间时设置 `ScheduleToCloseTimeout`。

退避时间按以下规则增长：

```text
interval(n) = min(initial × coefficient^(n-1), max interval)
```

## 超时错误

Activity 因超时结束时（超时后重试耗尽，或 `ScheduleToClose`、`ScheduleToStart` 到期），`Future.Get` 返回的错误是超时错误。`workflow.TimeoutTypeOf(err)` 返回超时种类，不是超时错误时返回空串：

```go
err := workflow.ExecuteActivity(ctx, activities.SettlePayment, input, opts).Get(ctx, &out)
if workflow.TimeoutTypeOf(err) == workflow.TimeoutTypeScheduleToClose {
    return compensate(ctx, input)
}
```

超时种类是 `TimeoutTypeScheduleToStart`、`TimeoutTypeStartToClose`、`TimeoutTypeScheduleToClose`、`TimeoutTypeHeartbeat`。错误的 message 是给人读的说明，例如 `activity StartToClose timeout`；History 里 `ActivityTimedOut` 事件的 `timeoutType` 字段记录同一个种类。

## Activity Reset

Console 可以把单个运行中或重试中的 Activity 重置到新的 generation，HTTP 端点 `reset-activity` 按 `activity_id` 寻址。Reset 会递增 Stamp，旧 generation 的 task、heartbeat 和完成回调会被幂等屏障拒绝，新 Attempt 从清晰状态重新调度。
