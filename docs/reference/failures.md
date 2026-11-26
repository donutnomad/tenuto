---
title: Failure 参考
description: Workflow、Activity、Update、Query 和 Runtime 失败的结构与处理
status: Stable
---

# Failure 参考

<FeatureStatus status="Stable" />

Tenuto 将程序失败、业务拒绝、超时、取消和确定性错误映射到不同层级。

## Failure 层级

| 层级 | 表达 | 处理 |
| --- | --- | --- |
| Activity Attempt | AttemptFailed / TimedOut | Retry Policy |
| Activity Call | ActivityFailed | Workflow 分支或补偿 |
| Workflow | WorkflowFailed | Run 终态或 Workflow Retry |
| Update | Completed failure | `*apperror.Error` |
| Query | Query 拒绝（`query_rejected`） | Go 调用端得到 `*client.QueryFailedError` |
| Runtime | Go error / HTTP error | 按状态码处理：400 改正请求，404 目标不存在，409 重新读取后重试，500 是 Server 故障、可原样重试，见 [Runtime HTTP API](/reference/http-api#失败应答) |
| Determinism | Blocked State | 代码回滚或 GetVersion |
| Workflow panic | Blocked State（`workflow_panic`） | 修复代码后重新部署，Run 从失败处继续 |

## Workflow 代码的 panic

与 Temporal Go SDK 缺省的 `BlockWorkflow` 策略相同：Workflow 代码 panic 时只让当前 Workflow Task 失败，Run 保持 Running。

- 解释器内部的 panic，以及 Workflow 代码里对应 Go 运行时 panic 的错误（`[E_RUNTIME]` 越界等、`[E_NIL_RECEIVER]`、`[E_PANIC]` 显式 `panic`）：Worker 上报 `WorkflowTaskFailed`，cause 是 `workflow_panic`，reason 是错误文本。Server 计连续失败并退避重新调度，Run 的 `blocked` 显示这个 cause。修复代码并发布后，下一次 Workflow Task 从 History 恢复并继续。
- Workflow 函数显式返回的 error 不是 panic：Run 以 `WorkflowFailed` 结束（配置了 Workflow RetryPolicy 时按它重试）。
- `_ = items[i]` 与 Go 一样求值右侧，越界时同样是 `workflow_panic`。

## 引擎产生的 Failure Type

| Type | 来源 |
| --- | --- |
| `PanicError` | Activity 函数 panic；按 Retry Policy 重试 |
| `PayloadSizeLimitExceeded` | Activity 的结果或心跳详情超过载荷上限（`WORKFLOW_PAYLOAD_SIZE_LIMIT_BYTES`），见 [容量上限](/operations/capacity-limits#载荷大小)；不可重试 |
| `ActivityResultRejected` | Server 以不可重试的 4xx 拒绝了 Activity 的结果（例如请求体超限），Worker 改报的失败；不可重试 |
| `HistoryLimitExceeded` | Run 因 Event History 超过硬上限被关闭；父 Workflow 在 `ChildWorkflowFailed` 中读到同一 Type |
| `AcceptedUpdateCompletedWorkflow` | Update 已接受、Handler 尚未完成时 Run 关闭；不可重试（`NonRetryable`），与 Temporal 同名 |
| `CanceledError` | 取消错误，与 Temporal 的 CanceledError 对应：`ctx.Err()`、被取消的 Activity / timer / 子 Workflow 返回给 Workflow 代码的错误，以及 `workflow.NewCanceledError(details...)`；不可重试。Workflow 返回它时 Run 以 Cancelled 结束、details 记进 `WorkflowCancelled`；Activity 确认取消时以它上报，details 记进 `ActivityCancelled`。用 `workflow.IsCanceledError` 或 `apperror.IsCanceled` 判断 |

## Application Error

```go
workflow.NewApplicationError(message, errorType, details...)
```

`errorType` 是稳定机器契约，`message` 是诊断文本，`details` 是有序 Payload。Update Client 返回的 `*apperror.Error` 暴露 Type、Message 和 Details。

## Retry State

Activity 和 Workflow failure 可以记录：

- `InProgress`
- `MaximumAttemptsReached`
- `NonRetryableFailure`
- `CancelRequested`：Workflow 已请求取消 Activity，之后的失败与超时不再重试
- `Timeout`：Activity 的 `ScheduleToClose` 或 `ScheduleToStart` 到期，或下一次 Attempt 会落在 `ScheduleToClose` 之后；Workflow 的 execution 时限不够下一次重试

## Timeout

Activity 的超时种类是 `ScheduleToStart`、`StartToClose`、`ScheduleToClose`、`Heartbeat`，与 Temporal 的 TimeoutType 同名。它记在三个地方：

- `ActivityTimedOut` 事件的 `timeoutType` 字段；
- 超时产生的 Failure 的 `timeoutType` 字段，`message` 是给人读的说明（如 `activity StartToClose timeout`）；这个 Failure 也出现在随后的 `ActivityAttemptFailed` 或 `ActivityFailed` 里；
- Workflow 代码拿到的错误：`workflow.TimeoutTypeOf(err)` 返回超时种类，`*apperror.Error` 的 `TimeoutType()` 返回同一个值；不是超时错误时为空串。

Workflow 级的 execution 与 run 超时记在 `WorkflowFailed` 的 `timeoutType`（`Execution`、`Run`）。诊断时同时读取 Attempt、Scheduled Event、Worker 和 deadline。

## Context 错误

Client Context 截止代表本次 RPC 或等待结束。已提交的 Signal、Update、Activity 或 Workflow 状态继续由 Server 推进。重复请求使用业务幂等键确认最终结果。

## 处理原则

错误 Type 驱动代码分支，Message 驱动日志检索，Details 驱动结构化恢复。完整工程实践见 [错误处理最佳实践](/best-practices/error-handling)。
