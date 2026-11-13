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
- Stamp 与 Execution Generation 提升。
- 旧 in-flight Task 和异步完成 Token 失效。
- Sticky assignment 被清理。
- 可选择重置 Heartbeat details。

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
