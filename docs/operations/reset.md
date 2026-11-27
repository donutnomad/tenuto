---
title: Workflow 与 Activity Reset
description: Reset Point、History Branch、事件重放和 generation fence
status: Preview
---

# Workflow 与 Activity Reset

<FeatureStatus status="Preview" />

Reset 用于从历史中的安全边界重新执行。Workflow Reset 创建新 Run 和 History Branch；Activity Reset 在当前 Run 中重新打开一个 Call。

## Workflow Reset

支持的定位方式：

- `first_workflow_task`
- `last_workflow_task`
- 显式 `ResetEventID`

每个 Reset 请求携带一个定位：`reset_type`（`first_workflow_task` 或 `last_workflow_task`）或 `reset_event_id`，二者选一。缺少定位、同时提供两者或 `reset_type` 取其他值时请求被拒绝，原 Run 保持原状态。

显式 Event ID 必须指向 Workflow Task 边界事件：`WorkflowTaskStarted`、`WorkflowTaskCompleted`、`WorkflowTaskFailed` 或 `WorkflowTaskTimedOut`。指向 `WorkflowTaskStarted` 时保留到该事件为止的 History，重新执行这一轮及之后的步骤；指向后三种关闭事件时映射到同一轮的 `WorkflowTaskStarted`。其他事件类型返回 `reset_event_id N is not a valid reset point: must be a workflow task boundary event (WorkflowTaskStarted/Completed/Failed/TimedOut)`；ID 不存在返回 `reset_event_id N not found`，超过最大事件 ID 返回 `reset_event_id N exceeds max event id M`。

全部校验（定位解析、重置点上未结束的子 Workflow）在终止原 Run 之前完成；校验失败时原 Run 继续运行。重置点上未结束的 Activity 与 Timer 不阻止 Reset，处理方式见[分叉点上未结束的 Activity 与 Timer](#分叉点上未结束的-activity-与-timer)。

```text
original branch: 1 ─ 2 ─ 3 ─ 4 ─ 5
                         reset ↑
new branch:      1 ─ 2 ─ 3 ─ R ─ ...
```

原 Run 进入终态（`WorkflowTerminated`，reason `reset`），新 Run 继承 Workflow ID、Workflow Type、必要输入和 Search Attributes。终止原 Run（以及 reset 历史代时仍在运行的 current Run）、创建新 Run、切换 current、写入 reset 标记、分叉点上未结束的 Activity 与 Timer 在新 Run 里的 state 与补写的关闭事件、重新应用的 Signal / Update 在同一个事务里提交：写入失败时原 Run 保持运行，没有新 Run。

## Reapply

Reset 可以重新应用锚点后的 Signal 和 Update。`ReapplyExcludeTypes` 支持排除 `signal` 或 `update`。执行前应逐项检查外部消息是否仍具业务效力。重新应用的 Update 与带 request ID 的 Signal 在新 Run 上保留去重：同一个 Update ID 发到新 Run 时返回已接受的记录，不会再接受一次。

### request_id 去重

Reset 请求可以带 `request_id`（Go Client 的 `ResetWorkflowExecutionOptions.RequestID`，最长 128 字节）。同一个 Run 最近一次 Reset 来自同一个 `request_id` 时，请求不再 fork，直接返回那次 Reset 出的 Run ID；不带 `request_id` 时每次请求都 Reset 一次。`request_id` 记在新 Run 的 reset 标记上，去重只看那次 Reset 出的 Run 自己写下的标记，不看它 fork 来的祖先前缀（对应 Temporal 只比较 current Run 的 `CreateRequestId`）。多个副本同时处理同一个请求时，提交在原 Run 的行锁下比较它的最新 Reset，后到的一方重新查找并返回先提交的结果。批量 Reset 为每个 Run 生成确定性的 `request_id`，见 [Batch Operation](/operations/batch-operations#执行与恢复)。

## Activity Reset

Activity Reset 以 Run ID 和 Activity ID 定位 Run 里一个尚未关闭的 Activity（`reset-activity` 的 `activity_id`）：

- Attempt 重新从 1 开始，Schedule-To-Start 从 reset 时刻起算。
- Schedule-To-Close 的起点不变，仍是这次调用最初的调度时间（`ActivityScheduled` 的时间），所以 Reset 不会延长整个调用的期限，期限快到时应改用 Workflow Reset。Temporal 的 `ResetActivity` 同样只把 Attempt 置 1、不改首次调度时间。
- Stamp 与 Execution Generation 提升。
- 旧 in-flight Task 和异步完成 Token 失效。
- Sticky assignment 被清理。
- 可选择重置 Heartbeat details。

## 分叉点上未结束的 Activity 与 Timer

保留下来的 History 里可以有已调度、尚未结束的 Activity，或已启动、尚未触发的 Timer。Reset 按保留的 History 判断它们在分叉点上的状态，在新 Run 里逐个处理：

| 分叉点上的状态 | 新 Run 里的处理 | 计时起点 |
| --- | --- | --- |
| Activity 已调度、未开始 | 保留，以新 Run 的 state 重新派发，Attempt 从 1 开始 | `ScheduledTime` 改成 reset 时刻，Schedule-To-Start 与 Schedule-To-Close 都从 reset 时刻重新起算 |
| Activity 已开始 | 在 reset 标记之后写入 `ActivityFailed`：failure 类型 `ResetWorkflowError`、`nonRetryable`，`retryState` 为 `NonRetryableFailure`，`lastHeartbeatDetails` 是 reset 时原 Run 里这个 Activity 最后一次心跳的详情（可能晚于分叉点）；Workflow 代码在这次调用上收到这个失败 | 不再计时 |
| Activity 已请求取消、开始只是 transient | 在 reset 标记之后写入 `ActivityCancelled`，不再派发 | 不再计时 |
| Timer 已启动、未触发 | 保留，deadline 不变（`TimerScheduled` 的时刻 + 时长）；reset 时已过期的立即触发 | 原起点不变 |

「已开始」指保留的 History 里有这个 Activity 当前 attempt 的 `ActivityStarted`，之后没有同一 Activity 的 `ActivityAttemptFailed` 或 `ActivityReset`。有重试策略的 Activity 开始执行时 `ActivityStarted` 只记在 state 上（transient），等这次 attempt 结束才和结果一起写入 History；只执行一次（`MaxAttempts: 1`）的 Activity 开始时立即写。所以分叉点上正在执行的有重试 Activity、以及在重试退避中的 Activity，都按「未开始」重新派发；按「已开始」失败的只有 `MaxAttempts: 1` 且已开始的 Activity。

补写的关闭事件写在 reset 标记（`WorkflowTaskFailed`，cause `reset_workflow`）之后、重新应用的 Signal / Update 之前，新 Run 的首个 Workflow Task 处理它们。原 Run 上仍在执行的 attempt 在原 Run 终止后失效：它的 task token 指向原 Run，迟到的完成与失败上报被拒绝（原 Run 已有终态事件，不再追加），不进入任何 History。

重置点上未结束的子 Workflow 仍然阻止 Reset（错误 `cannot reset: pending child workflows exist at reset point`）：保留的 History 里有 `ChildWorkflowStarted`、没有它的关闭事件（`ChildWorkflowCompleted` / `Failed` / `Cancelled` / `Terminated`）。分叉点之后才关闭的子 Workflow 也算未结束。

以上判定和新 Run 的 state 都按保留的 History 求出，包括 reset 继承的前缀：Activity 的类型、队列、输入、超时、重试策略与 Trace 取自 `ActivityScheduled`，Timer 的 deadline 取自 `TimerScheduled`。对 reset 出来的 Run 再 reset 到它的分叉点之前，结果与直接 reset 原 Run 相同。

以上处理与 Temporal 一致（源码：`service/history/ndc/workflow_resetter.go` 的 `prepareResetWorkflow` 与 `failInflightActivity`，核对版本 `temporalio/temporal@67959df`），区别有两处：

- 心跳详情：Temporal 从按 History 重建的状态里取，心跳不写 History，通常是空的；Tenuto 取原 Run 在 reset 时的最新心跳。
- 已请求取消、开始只是 transient 的 Activity：Temporal 带着取消请求重新派发，Worker 开始后经心跳得知取消；Tenuto 按「未开始的 Activity 被取消时同事务结束」的规则直接写 `ActivityCancelled`。

正常执行时 Schedule-To-Close 从首次调度（`ActivityScheduled` 的时间）起算，重试不改变起点，与 Temporal 的 `FirstScheduledTime` 相同（`service/history/workflow/timer_sequence.go` 的 `getActivityScheduleToCloseTimeout`）；Workflow Reset 重新派发的 Activity 从 reset 时刻起算。

## 操作 Runbook

1. 保存目标 Run 的 History、Program Version 和 Worker Build ID。
2. 确认故障原因已经通过代码、配置或下游修复。
3. 选择 Workflow Task 边界或要重置的 Activity（Activity ID）。
4. 填写 Reason 和稳定 Request ID。
5. 执行 Reset 并记录新 Run ID。
6. 观察首个 Workflow Task、Activity 路由和 Sticky fallback。
7. 验证业务副作用的幂等记录。

## 风险矩阵

| 风险 | 控制 |
| --- | --- |
| 重放旧 Activity 副作用 | 业务幂等键、外部状态核对 |
| 重放过期 Signal/Update | Reapply exclude 和消息有效期 |
| 选择半个 Workflow Task | Server 边界校验 |
| 旧 Task 迟到 | generation/stamp fence；Workflow Reset 后原 Run 的终态拒绝追加事件 |
| 新代码仍与 History 失配 | Reset 前 Replay 测试 |

相关内容：[Console 操作](/console/operations)、[安全发布](/best-practices/safe-deployments)。
