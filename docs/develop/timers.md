---
title: Timer 与 Workflow Sleep
description: Durable Timer、确定性时间、重启恢复和长时间等待
status: Stable
---

# Timer 与 Workflow Sleep

<FeatureStatus status="Stable" />

`workflow.Sleep` 创建 Durable Timer。Server 持久化 deadline，Worker 进程和 Server 进程可以在等待期间重启。

```go
if err := workflow.Sleep(ctx, 3*time.Second); err != nil {
    return err
}
```

## 时长的写法

`Sleep` 的签名是 `func Sleep(ctx Context, delay time.Duration) error`。时长用 `time` 包常量和 `time.Duration` 运算书写，生成器在生成期把它们折叠成整数纳秒：

| 时长 | 写法 |
| --- | --- |
| 500 毫秒 | `500 * time.Millisecond` |
| 90 秒 | `90 * time.Second` |
| 输入里的秒数 | `time.Duration(in.DelaySeconds) * time.Second` |
| 两个时刻的间隔 | `deadline.Sub(workflow.Now(ctx))` |

`time.Duration` 在 WLang 里是 `int64` 纳秒，整数纳秒字面量（`3000000000`）同样有效。常量表达式两侧使用整数：`1500 * time.Millisecond`。

时长也可以来自 Workflow 输入或 Activity 结果里的整数字段。时长小于等于 0 时 `Sleep` 立即返回，不创建 Timer。

## Event 生命周期

```text
ScheduleTimer Command
  → TimerScheduled
  → deadline
  → TimerFired
  → WorkflowTaskScheduled
  → Workflow 从 Sleep 后继续
```

Timer ID 由协程路径和该协程内的 Timer 序号生成，例如主协程的第一个 Timer 是 `#t1`。Server 启动恢复时读取 open Timer 状态，已到期的 Timer 立即补写 `TimerFired`。

## 并发中的 Timer

`Sleep` 只阻塞当前协程。把它放进 `workflow.Go` 协程，可以与其他协程并行等待：

```go
timedOut := false
approved := false

workflow.Go(ctx, func(ctx workflow.Context) {
    _ = workflow.Sleep(ctx, 600000000000) // 10 分钟
    timedOut = true
})
workflow.Go(ctx, func(ctx workflow.Context) {
    var decision map[string]any
    _ = workflow.WaitSignal(ctx, "approval").Get(ctx, &decision)
    approved = true
})

workflow.Await(ctx, func() bool { return approved || timedOut })
```

`Await` 返回后，另一条协程仍在等待。Run 在 Workflow 函数体返回时结束，此时仍在等待的协程和未触发的 Timer 被放弃（见 [编写 Workflow](/develop/workflows) 的「Run 何时结束」）。上例中 Signal 先到时，函数体可以直接返回，Run 随即完成，不等待 Timer 触发。Run 结束（完成、失败、取消、终止、超时、continue-as-new）时，Server 在同一个事务里把它仍未触发的 Timer 置为 `run_closed`，不写 `TimerCancelled`；到期时间到了也不再触发。

## 确定性时间

Workflow 中的当前时间使用 `workflow.Now(ctx)`：

```go
now := workflow.Now(ctx)
```

`Now` 返回最近一条已应用 Event 的时间戳，同一份 History 的每次重放得到相同的值。返回的 `time.Time` 可以调用 `Add`、`Sub`、`Before`、`After`、`Equal`、`Unix`、`UnixMilli`、`Format`、`String`，其他方法生成期报错。`time.Now()`、`time.Since`、`time.Sleep` 在 Workflow 里生成期报错。`time.Now()` 读取 Worker 的系统时钟，属于 Activity 的职责。Activity 内部可以使用标准时间 API，结果通过 Activity 输出进入 History。

基于 `workflow.Now` 的加减、比较和格式化直接写在 Workflow 里：

```go
deadline := workflow.Now(ctx).Add(2 * time.Hour)
if workflow.Now(ctx).After(deadline) {
    // 已超过期限
}
stamp := workflow.Now(ctx).Format(time.RFC3339) // 按 UTC 格式化
```

解析和构造时间（`time.Parse`、`time.Unix`、`time.Date`）在生成期报错，这类工作放进 Activity。

## 常见模式

### 有业务含义的等待

```go
for attempt := 0; attempt < 3; attempt++ {
    result, err := CallCheckStatus(ctx, CheckStatusInput{OrderID: in.OrderID}, workflow.ActivityOptions{StartToCloseTimeout: 30 * time.Second})
    if err != nil {
        return Output{}, err
    }
    if result.Ready {
        return Output{Status: "ready"}, nil
    }
    if err := workflow.Sleep(ctx, 60000000000); err != nil {
        return Output{}, err
    }
}
```

Activity 的技术性重试使用 Retry Policy；Workflow Timer 表达有业务含义的等待窗口。

### 截止时间

把剩余等待时长作为整数字段放进 Workflow 输入。Continue-As-New 时把剩余时长传入下一段 Run。

## 行为矩阵

| 场景 | 行为 |
| --- | --- |
| Worker 在等待期间退出 | Timer 继续由 Server 管理 |
| Server 在等待期间重启 | 从 Timer State 恢复 |
| deadline 已经过期 | 恢复阶段补发 TimerFired |
| `Sleep` 的 Context 被取消（Run 收到取消请求，或 `WithCancel` 的作用域被取消） | Timer 被取消，History 记 `TimerCancelled`，`Sleep` 立即返回取消错误；到期时间到了也不再触发 |
| Run 被终止 | Run 关闭，Workflow 代码停止推进 |
| Run 结束时 Timer 仍未触发 | Timer State 置为 `run_closed`，不写事件，不再触发 |
| History 重放 | 已有 TimerFired 时立即越过等待 |
| 时长小于等于 0 | 立即返回，不产生 Timer Event |
| 大量长期 Timer | 按 open Timer 状态规划数据库和恢复容量 |

相关内容：[取消、超时与终止](/develop/cancellation-timeouts)、[History 容量管理](/operations/history-scaling)。
