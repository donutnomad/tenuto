---
title: Event Type 参考
description: Event History 中的 Workflow、Task、Activity、Timer、Child 和消息事件
status: Internal
---

# Event Type 参考

<FeatureStatus status="Internal" note="Event schema 由同版本组件协同使用" />

Event 是已经发生并提交的事实。每个 Run/Branch 的 Event ID 从 1 递增，Timestamp 由 Server 写入。

## Workflow

| Event | 关键字段 | 说明 |
| --- | --- | --- |
| `WorkflowStarted` | workflowId、input、attempt、initiator、searchAttributes、requestId | Run 起点；searchAttributes 是启动时的搜索属性（Schedule 启动的 Run 带 `TemporalScheduledById` 与 `TemporalScheduledStartTime`），requestId 是启动请求的幂等键，失败重试与 Continue-As-New 派生的 Run 沿用链首的值 |
| `WorkflowCompleted` | output | 成功终态 |
| `WorkflowFailed` | error、timeoutType、retryState、newExecutionRunId | 失败终态或 Retry 链接；配置了 RetryPolicy 时 retryState 记录重试判定（InProgress / NonRetryableFailure / MaximumAttemptsReached / Timeout） |
| `WorkflowCancellationRequested` | cause | 取消请求；Worker 在这条事件之后、协程推进之前取消 Workflow 的根 Context。重复请求不写第二条 |
| `WorkflowCancelled` | details | Workflow 返回取消错误的终态；details 是该取消错误的 details |
| `WorkflowTerminated` | reason | 强制终态 |
| `WorkflowContinuedAsNew` | nextRunId、input、configVersion | Continue-As-New 终态 |

## Workflow Task

| Event | 关键字段 |
| --- | --- |
| `WorkflowTaskScheduled` | timeoutMs、attempt |
| `WorkflowTaskStarted` | scheduledEventId、workerIdentity、workerBuildId、timeoutMs（Server 计时的 start-to-close 超时）、History 指标 |
| `WorkflowTaskCompleted` | scheduledEventId、workerIdentity |
| `WorkflowTaskTimedOut` | scheduledEventId、error |
| `WorkflowTaskFailed` | cause、baseRunId、resetRunId、requestId（只在带 `request_id` 的 reset 标记上）、reason |

## Activity

| Event | 说明 |
| --- | --- |
| `ActivityScheduled` | Call 起点，包含 ActivityID（`activity.Info.ActivityID`）、Type、Queue、Timeout 和生效的 Retry Policy（Server 补齐缺省值后的策略）；不含 Build ID，任务不绑定 build |
| `ActivityStarted` | Attempt 开始，包含 Attempt、Stamp，以及实际执行的 `workerIdentity` 与 `workerBuildId`（只用于观测）。生效策略允许再次重试时，开始记录在 Activity 状态里，这条事件在 Attempt 结束时与结果一起写入；`MaxAttempts` 为 1 时开始即写入 |
| `ActivityAttemptFailed` | 可重试 Attempt 失败 |
| `ActivityTimedOut` | Attempt 超时：`timeoutType`（`ScheduleToStart`、`StartToClose`、`ScheduleToClose`、`Heartbeat`）、`retryState`、`failure`（`message` 是说明，`failure.timeoutType` 与事件的 `timeoutType` 相同）、`lastHeartbeatDetails`（超时时最后一次心跳的详情，没有心跳时省略） |
| `ActivityCompleted` | Call 成功终态 |
| `ActivityFailed` | Call 重试耗尽终态 |
| `ActivityReset` | 人工 Reset 审计与 Heartbeat 策略 |
| `ActivityCancelRequested` | Workflow 请求取消这个 Activity（`RequestCancelActivity` 命令事件）：activityId、scheduledEventId |
| `ActivityCancelled` | Activity 按取消结束的终态：取消时尚未开始（与请求同事务写入），或 Worker 确认取消；包含 cancelRequestedEventId、attempt 与 Activity 确认取消时的 details |

Heartbeat 不写 History（与 Temporal 相同）：最后一次心跳的详情与时间记在 Activity 状态里（`activity-states` 的 `lastHeartbeat`、`lastHeartbeatAt`），Heartbeat 超时从这个时间起算；Attempt 超时时详情随 `ActivityTimedOut` 写入 History。

## Timer 与 Child

Timer 使用 `TimerScheduled`、`TimerFired`、`TimerCancelled`（Workflow 取消尚未触发的 timer，命令事件）。Child 使用 `ChildWorkflowStarted`、`Completed`、`Failed`、`Cancelled`、`Terminated` 与 `ChildWorkflowCancelRequested`（Workflow 请求取消子 Workflow，命令事件，带子 Run ID），并保存 Child ID、Child Run ID 与父侧 Scheduled Event ID。`ChildWorkflowStarted` 的 `parentClosePolicy` 记录子 Workflow 的[父关闭策略](/develop/child-workflows#父关闭策略)（`terminate`、`request_cancel`、`abandon`，未指定时记为 `terminate`）。父 Run 关闭之后结束的子 Workflow 不再向父 History 追加事件。Run 结束时仍未触发的 timer 不写事件（与 Temporal 相同），它的 Timer State 置为 `run_closed`，不再触发。仍未结束（scheduled、started）的 Activity 同样不写事件，Activity State（`activity-states` 的 `status`）置为 `run_closed`，不再超时、重试或派发。

## 消息与 Marker

| Event | 说明 |
| --- | --- |
| `WorkflowSignaled` | Signal Name、input 与 requestId；同一个 Run 上同一个 requestId 只记一条 |
| `WorkflowUpdateAccepted` | Update Name、Update ID 与 Payload args |
| `WorkflowUpdateCompleted` | Success/Failure Outcome。Run 关闭时已接受、尚未完成的 Update 不写这个事件：它的结果由 Run 的终态事件确定，为 `AcceptedUpdateCompletedWorkflow` 失败 |
| `MarkerRecorded` | Side Effect、GetVersion 和确定性随机值 |
| `SearchAttributesUpserted` | 业务索引字段更新 |

Event 字段的 JSON wire 由 [Runtime HTTP API](/reference/http-api) 返回，业务代码通过 SDK 和 Console 使用其稳定语义。
