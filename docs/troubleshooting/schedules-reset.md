---
title: Schedule、History 与 Reset 异常
description: 排查错过触发、重复触发、History 过大和 Reset 边界
status: Preview
---

# Schedule、History 与 Reset 异常

<FeatureStatus status="Preview" />

## Schedule 错过触发

检查 paused、next_run_at、时区、Cron、Schedule Driver leader 和 Server 日志。落后超过 `catchup_window`（缺省 365 天）的到期点不触发，计入 `missed_catchup_window`；待补的到期点超过 1000 个时较早的计入 `buffer_dropped`。

## Schedule 重复触发

检查 `tenuto_leader_lock` schema、Node ID 唯一性和 `UpdateScheduleNextRunAtCAS`。业务 Workflow ID 和 Activity 幂等键继续提供第二层保护。

## History 过大

查看 History length、size 和 Continue-As-New reasons。将循环状态压缩为 cursor，在业务安全点启动下一 segment。Run Chain 页面用于浏览全部 segment。

## Reset 被拒绝

显式 Reset Event ID 需要对应 Workflow Task 边界事件：`WorkflowTaskStarted`、`WorkflowTaskCompleted`、`WorkflowTaskFailed` 或 `WorkflowTaskTimedOut`。在 Event History 中选择其中一种事件的 ID。

## Reset 后仍 Blocked

1. 对新 Run 的 branch 执行 Replay。
2. 核对 reset point 之前的 Program 兼容性。
3. 核对 reapply 的 Signal 和 Update。
4. 核对 Worker Build ID 和 capability。
5. 核对旧 Activity 副作用的幂等记录。

## Activity Reset 后旧结果出现

检查 Event 的 Attempt 和 Stamp。有效 completion 应对应新 generation；旧 Task 回报会进入 stale 幂等路径。
