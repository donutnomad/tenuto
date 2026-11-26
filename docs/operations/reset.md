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

全部校验（定位解析、待处理 Activity/Timer、未关闭的子 Workflow）在终止原 Run 之前完成；校验失败时原 Run 继续运行。

```text
original branch: 1 ─ 2 ─ 3 ─ 4 ─ 5
                         reset ↑
new branch:      1 ─ 2 ─ 3 ─ R ─ ...
```

原 Run 进入终态（`WorkflowTerminated`，reason `reset`），新 Run 继承 Workflow ID、Workflow Type、必要输入和 Search Attributes。终止原 Run（以及 reset 历史代时仍在运行的 current Run）、创建新 Run、切换 current、写入 reset 标记与重新应用的 Signal / Update 在同一个事务里提交：写入失败时原 Run 保持运行，没有新 Run。

## Reapply

Reset 可以重新应用锚点后的 Signal 和 Update。`ReapplyExcludeTypes` 支持排除 `signal` 或 `update`。执行前应逐项检查外部消息是否仍具业务效力。

## Activity Reset

Activity Reset 以 Run ID 和 Scheduled Event ID 定位 Call：

- Attempt 重新从 1 开始，Schedule-To-Start 从 reset 时刻起算。
- Schedule-To-Close 的起点不变，仍是这次调用最初的调度时间（`ActivityScheduled` 的时间），所以 Reset 不会延长整个调用的期限，期限快到时应改用 Workflow Reset。Temporal 的 `ResetActivity` 同样只把 Attempt 置 1、不改首次调度时间。
- Stamp 与 Execution Generation 提升。
- 旧 in-flight Task 和异步完成 Token 失效。
- Sticky assignment 被清理。
- 可选择重置 Heartbeat details。

## 分叉点上未结束的 Activity 与 Timer

Tenuto 的 Workflow Reset 拒绝这样的重置点：保留下来的 History 里有已调度、尚未结束的 Activity 或已启动、尚未触发的 Timer（错误见上文的校验）。需要从这里重跑时，选更早的边界，或等它们结束后选更晚的边界。

Temporal 允许这样的重置点。它按保留下来的 History 重建新 Run 的状态，再逐个处理未结束的项（源码：`service/history/ndc/workflow_resetter.go` 的 `prepareResetWorkflow` 与 `failInflightActivity`，核对版本 `temporalio/temporal@67959df`）：

| 分叉点上的状态 | Temporal 的处理 | 计时起点 |
| --- | --- | --- |
| Activity 已调度、未开始 | 保留，在新 Run 里重新派发 | `ScheduledTime` 与 `FirstScheduledTime` 都改成 reset 时刻（新 Run 的 StartTime），Schedule-To-Start 与 Schedule-To-Close 都从 reset 时刻重新起算 |
| Activity 已开始 | 直接写入 `ActivityTaskFailed`，failure 类型 `ResetWorkflowFailure`（带上最后一次 Heartbeat details），重试状态为不可重试；Workflow 代码在这次调用上收到失败 | 不再计时 |
| Timer 已启动、未触发 | 保留，按原 deadline 触发：deadline = `TimerStarted` 事件时间 + `StartToFireTimeout`，reset 时已过期的立即触发 | 原起点不变 |

Temporal 正常执行时，Schedule-To-Close 从首次调度（`ActivityTaskScheduled` 事件时间，即 `FirstScheduledTime`）起算，重试不改变起点（`service/history/workflow/timer_sequence.go` 的 `getActivityScheduleToCloseTimeout`）；Tenuto 相同，起点是 `ActivityScheduled` 的时间。

## 操作 Runbook

1. 保存目标 Run 的 History、Program Version 和 Worker Build ID。
2. 确认故障原因已经通过代码、配置或下游修复。
3. 选择 Workflow Task 边界或 Activity Scheduled Event。
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
| 旧 Task 迟到 | generation/stamp fence |
| 新代码仍与 History 失配 | Reset 前 Replay 测试 |

相关内容：[Console 操作](/console/operations)、[安全发布](/best-practices/safe-deployments)。
