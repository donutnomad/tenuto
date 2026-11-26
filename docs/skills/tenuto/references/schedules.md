# Schedule

Schedule 按 Cron 周期启动 Workflow。它保存 Workflow Type、输入、Cron、时区、Overlap Policy 与下一次触发时间；每次触发等价于一次 `start`。

Workflow 内部的周期逻辑（一个 Run 里循环等待）用 `workflow.Sleep` + Continue-As-New，见 `time-and-randomness.md`。两者的选择：每个周期是一次独立的业务执行、要独立的 History 与失败边界时用 Schedule；周期之间要在内存里延续状态时用循环。

## 创建

```go
handle, err := c.ScheduleClient().Create(ctx, client.ScheduleOptions{ // c 是 sdk/client 的 Client，Namespace 取自它
    WorkflowType:   "daily-report",                       // @Workflow(type=...)
    Description:    "每日 02:30 生成报表",
    CronExpression: "30 2 * * *",                         // 5 段：分 时 日 月 周
    TimeZone:       "Asia/Shanghai",                      // IANA 时区，空值为 UTC
    Args:           []any{map[string]any{"scope": "all"}}, // 至多一个，JSON object
    OverlapPolicy:  client.ScheduleOverlapPolicySkip,     // "SKIP"
    Paused:         false,
})
d, err := handle.Describe(ctx)
// handle.GetID()（Server 生成）、d.NextRunAt、d.LastRunAt、d.LastRunID、d.RunCount
```

- Cron 是 5 段表达式，粒度到分钟；无法解析或永远不会匹配（`0 0 31 2 *`）时 Create/Update 返回 `invalid cron_expression`（HTTP 400）。按 `TimeZone` 的墙上时间匹配：夏令时前拨日不存在的时刻当天跳过；回拨日重复的时刻只在第一次出现时触发一次（同 Vixie cron；Temporal 两次都触发）。日与星期两段都被限定（都不以 `*` 开头）时满足任意一段即触发：`0 0 13 * 5` 是每月 13 日和每个周五。
- 触发顺序：catch-up 检查 → Overlap 检查 → 以到期点的 Workflow ID 启动 Run（该 ID 的 Run 已在运行时不再启动）→ CAS 推进 `next_run_at`。启动失败时 `next_run_at` 不动，调度器下一轮重试；`Trigger` 失败时 `next_run_at` 保持原值。
- `OverlapPolicy` 只接受大写值，空值保存为 `SKIP`；其他值（`skip`、`BUFFER_ONE`、`CANCEL_OTHER`、`TERMINATE_OTHER` 等）Create/Update 返回 `invalid overlap_policy`（HTTP 400）。语义同 Temporal：

| 值 | 行为 |
| --- | --- |
| `SKIP`（缺省） | 上一次触发的执行链仍在运行时跳过本次 |
| `ALLOW_ALL` | 每个到期 tick 都启动 Run，与仍在运行的 Run 并行 |

- 执行链：上一次触发的 Run（`LastRunID`）所在 Workflow ID 的 current Run，覆盖 Continue-As-New、失败重试、Reset 接续的那一代。读取失败时跳过本次；`LastRunID` 已被清理时视为已结束。
- 每次触发的 Run 有自己的 Workflow ID：`schedule-<Schedule ID>-<触发时刻>`，触发时刻是 UTC、秒级 RFC 3339（到期触发取 cron 到期点，`Trigger` 取调用时刻），Workflow Type 是 `WorkflowType`。触发从不接替仍在运行的 Run；同一到期点重复启动落在同一个 Workflow ID 上，那个 Run 仍在运行时不会再启动第二个。
- 每次触发的 Run 带 Server 写入的搜索属性（同 Temporal）：`TemporalScheduledById` = Schedule ID，`TemporalScheduledStartTime` = 名义触发时间（同上，UTC 秒级 RFC 3339）。它们在 `WorkflowStarted` 的启动属性里，Continue-As-New 与重试的 Run 继承；调用端写入返回 `client.ErrInvalidArgument`。

## 找到一个 Schedule 的全部 Run

```go
runs, err := c.ListWorkflow(ctx, client.ListWorkflowOptions{
    SearchAttributes: map[string]string{"TemporalScheduledById": handle.GetID()},
    NewestFirst:      true,
})
// 同一个过滤器用于批量操作：StartBatchOperationOptions{Filter: client.ListWorkflowOptions{SearchAttributes: ...}}
```

HTTP：`list` / `start-batch` 的 `filter.search_attributes` 写 `{"TemporalScheduledById":"<Schedule ID>"}`。Console 的 Schedules 页面 Run Count 链接到 `/<namespace>/runs?schedule_id=<Schedule ID>`。

## 管理

| 方法 | 作用 |
| --- | --- |
| `c.ScheduleClient().GetHandle(ctx, id)` | 已有 Schedule 的句柄（不访问 Server） |
| `handle.Describe(ctx)` | 读取 |
| `handle.Update(ctx, client.ScheduleUpdateOptions{...})` | 修改；字段是指针，只改非 nil 的：`Description`、`CronExpression`、`TimeZone`、`Args`、`OverlapPolicy` |
| `handle.Pause(ctx)` | 暂停：清空下一次触发时间，已启动的 Run 继续 |
| `handle.Unpause(ctx)` | 恢复：从当前时间重新计算下一次触发 |
| `handle.Trigger(ctx) (client.WorkflowRun, error)` | 立即触发一次；遵守 Overlap Policy，`SKIP` 下执行链仍在运行时返回 `client.ErrConflict`（`previous scheduled run is still running`，HTTP 409） |
| `handle.Delete(ctx)` | 删除；已有 Run 保留 |
| `c.ScheduleClient().List(ctx, client.ScheduleListOptions{WorkflowType, Paused, Offset, Limit})` | 列表；`Paused` 是 `*bool` |

```go
cron := "0 * * * *"
err := handle.Update(ctx, client.ScheduleUpdateOptions{CronExpression: &cron})
```

## HTTP

`POST /runtime/workflow/` 下：

| 路径 | 请求体 |
| --- | --- |
| `create-schedule` | `{namespace, workflow_type, description, cron_expression, timezone, input, overlap_policy, paused}` |
| `get-schedule`、`delete-schedule`、`pause-schedule`、`resume-schedule`、`trigger-schedule` | `{namespace, schedule_id}` |
| `update-schedule` | `{namespace, schedule_id, update:{description, cron_expression, timezone, input, overlap_policy}}` |
| `list-schedules` | `{filter:{namespace, workflow_type, paused, offset, limit}}` |

```bash
curl -sS -X POST http://127.0.0.1:7400/runtime/workflow/create-schedule \
  -H 'Content-Type: application/json' \
  -d '{"namespace":"orders","workflow_type":"daily-report","cron_expression":"30 2 * * *","timezone":"Asia/Shanghai","overlap_policy":"SKIP"}'
```

## 行为边界

- Catch-up 窗口是一小时：下一次触发时间落后当前时间超过一小时（例如 Server 长时间停机）时，该次触发被跳过并推进到未来的时间点。
- 被调度的 Workflow 自己处理重复执行：用业务周期（如日期）作为 Activity 的幂等键。
- Schedule 触发的 Run 在 Workflow 里拿到的时间用 `workflow.Now(ctx)`；需要「本次计划时间」时由 Activity 读取 Run 的搜索属性 `TemporalScheduledStartTime`（`DescribeWorkflowExecution`），或把它放进业务逻辑可推导的输入。

## 排查

| 症状 | 检查 |
| --- | --- |
| 到点没有启动 | Schedule 是否 `paused`；`next_run_at`；`SKIP` 下 `last_run_id` 的执行链（含 Continue-As-New 之后的 Run）是否仍在运行；Workflow 定义是否已登记 |
| 重启后没有补跑 | 是否超出一小时 Catch-up 窗口 |
| 触发时间偏移 | `timezone` 与 Cron 的组合；夏令时前拨日跳过不存在的时刻，回拨日重复的时刻只触发一次 |
| `Trigger` 返回 409（`client.ErrConflict`） | `SKIP` 下执行链仍在运行（`previous scheduled run is still running`），或同一秒内重复触发 |
| `Trigger` 返回其他错误 | Schedule 状态、定义是否存在；`Trigger` 遇到连接中断 / 502 / 504 不自动重试（重发会再触发一次），先按 `TemporalScheduledById` 列 Run 确认 |
| 创建/更新返回 `invalid overlap_policy` | 取值只能是 `SKIP` 或 `ALLOW_ALL`（大写） |
