---
title: Activity 执行异常
description: 排查排队、重复 Attempt、Heartbeat、Timeout 和异步完成
status: Stable
---

# Activity 执行异常

<FeatureStatus status="Stable" />

## 长时间 Scheduled

检查 ActivityScheduled 的 Namespace、Task Queue 和 Activity Type，再检查 active Worker、Version Heartbeat 和 TaskQ physical queue。Server 日志反复出现 `activity claim refused: worker contract is incompatible` 时，在线 Worker 的契约都与 Run 当前 Program 的声明不一致：让契约兼容的 build 重新上线（例如 `pinned` Run 所需的旧 build），或者等 `ScheduleToStartTimeout` 到期。

## Run Blocked：`activity_route_unavailable`

调度时找不到能执行这个 Activity 的在线 Worker，任务不派发，History 不变，Worker 就绪或配置修正后 Run 按退避自动继续。按错误原文区分：

| 错误原文 | 原因 | 处理 |
| --- | --- | --- |
| `... is not registered in activity route catalog` / `has no active route` | 这个 Namespace 里没有任何在线 Worker 登记这个 Activity 类型 | 启动登记了它的 Worker，核对 Namespace |
| `declares a contract and no online worker on task queue <queue> reported one` | 定义或调用处指定了队列，该队列上没有提供它的 Worker | 让 Worker 拉这个队列（`@Activity(queue)` 登记的队列会自动拉取，调用处改派的队列要写进 `ActivityTaskQueues`） |
| `declares no task queue and workers serve it on unrelated task queues [...]` | 定义与调用处都没有声明队列，几个服务在互不相关的队列上提供同一类型 | 在 `@Activity(queue=...)` 或 `ActivityOptions.TaskQueue` 声明队列后重新生成并发布，或停掉其中一个服务 |

Console Workers 页的 Activity Routing 表列出每个 Activity 类型在哪些队列上有 Worker。队列的决定规则见 [Activity 的队列如何决定](/operations/namespaces-task-queues#activity-的队列如何决定)。

## Run Blocked：`bad_schedule_activity_attributes`

Workflow 调度的 Activity 既没有 `StartToCloseTimeout` 也没有 `ScheduleToCloseTimeout`，错误原文 `activity "<id>" (type <type>) sets neither StartToCloseTimeout nor ScheduleToCloseTimeout`。给该调用的 `ActivityOptions` 补上超时，重新生成并发布；Run 按退避自动继续。

## Run Blocked：`schedule_activity_duplicate_id`

调度的 Activity 的 `ActivityOptions.ActivityID` 与同一 Run 里仍未关闭的 Activity 重复（同一个 Workflow Task 里两次调度同一 ID 也算），错误原文 `activity "<id>" (type <type>): an activity with this ActivityID is still open in this run`。让并发的调用用不同的 ID（例如带上循环下标），或等前一个 Activity 结束后再用同一 ID；重新生成并发布，Run 按退避自动继续。

## 多个 Attempt

多个 Attempt 表示同一个 Call 经历重试。依次查看 `ActivityAttemptFailed`、`ActivityTimedOut`、backoff 和 NonRetryableErrors。Console Trace 的 Call 选择器用于切换同一代码行的多次调用，Attempt 选择器用于检查一次 Call 内的重试。

## Heartbeat Timeout

确认 Activity 定期调用 `RecordHeartbeat`，间隔小于 Heartbeat Timeout。检查 Worker GC、网络、下游长阻塞和进度序列化。新 Attempt 使用 Last Heartbeat Details 恢复。

## 取消延迟

先看 History 里有没有这个 Activity 的 `ActivityCancelRequested`：Workflow 的取消请求（`WorkflowCancellationRequested`）本身不取消 Activity，只有 Workflow 代码取消 Activity 所在的 Context 时才发出取消请求。在 `NewDisconnectedContext` 上发起的 Activity 不随 Workflow 取消。

有取消请求时，Activity 只通过 Heartbeat 的响应得知取消，取消在下一次 `RecordHeartbeat` 返回时生效。不 Heartbeat 的 Activity 收不到取消，运行到自己返回或超时。长任务缩短 Heartbeat 间隔、增加安全检查点，并在本地调用中传播 Context。

`WaitForCancellation: true` 的 Workflow 一直等到 `ActivityCancelled`、`ActivityCompleted` 或最终失败才继续。Activity 收到取消后返回 `ctx.Err()` 或取消错误才会记成 `ActivityCancelled`；返回其他错误记成 `ActivityFailed`（`retryState` 为 `CancelRequested`），返回成功记成 `ActivityCompleted`。

## Workflow 取消后没有结束

Run 收到取消请求后由 Workflow 代码决定终态：返回取消错误时 `cancelled`，返回 `nil` 时 `completed`。代码一直在等待不受取消影响的事件（`WaitSignal`、Channel、`Selector` 里没有 `ctx.Done()` 的 case），或清理 Activity 还没结束时 Run 保持运行。需要立即结束时用 Terminate。

## 异步完成

检查保存的 Task Token、Run Status、Scheduled Event ID、Attempt 和 Stamp。Activity Reset 会使旧 Token 进入 stale 路径。外部 Webhook 使用 provider event ID 去重。

## 重复副作用

已开始的 Attempt 不会因队列重投再执行；重复的执行来自超时后的重试，或 Worker 在上报结果前退出。网络超时可能发生在外部系统完成写入之后。Activity 至少执行一次，每个 Activity 都必须幂等：使用幂等键（操作名 + 这一次操作的身份）查询既有结果，再决定是否执行。Attempt 号码用于诊断，不能进幂等键，见 [Activity 幂等](/best-practices/activity-idempotency)。
