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
    CatchupWindow:  time.Hour,                            // 0 取 365 天，最小 10 秒
})
d, err := handle.Describe(ctx)
// handle.GetID()（Server 生成）、d.NextRunAt、d.LastRunAt、d.LastRunID、d.RunCount、d.CatchupWindow、d.MissedCatchupWindow、d.BufferDropped、d.StartFailed
```

- Cron 是 5 段表达式，粒度到分钟；无法解析或永远不会匹配（`0 0 31 2 *`）时 Create/Update 返回 `invalid cron_expression`（HTTP 400）。按 `TimeZone` 的墙上时间匹配：夏令时前拨日不存在的时刻当天跳过；回拨日重复的时刻只在第一次出现时触发一次（同 Vixie cron；Temporal 两次都触发）。日与星期两段都被限定（都不以 `*` 开头）时满足任意一段即触发：`0 0 13 * 5` 是每月 13 日和每个周五。
- 触发顺序：catch-up 检查（逐个到期点）→ Overlap 检查 → 以到期点的 Workflow ID 启动 Run（该 ID 的 Run 已在运行时不再启动）→ CAS 把 `next_run_at` 推进到这个到期点之后的下一个 cron 时刻。启动失败：配置或请求错误（Workflow 类型没有部署、输入不合法）直接丢弃这个到期点；其他错误按退避（1s 起翻倍，最长 60s）重试，期限从到期点起算、取 min(`catchup_window`, 1 小时)（同 Temporal）：超过 1 小时丢弃，`catchup_window` 更短时超过它计入 `missed_catchup_window`。丢弃前按到期点的 Workflow ID 确认启动是否其实已提交，已提交时按启动成功记录。丢弃计入 `start_failed`，`next_run_at` 推进到下一个到期点，所以持续失败时 `next_run_at` 最多落后 1 小时，修好后只补出这 1 小时内的到期点。`Trigger` 不改变 `next_run_at`（同 Temporal TriggerImmediately），积压的到期点照常补触发。
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
| `handle.Update(ctx, client.ScheduleUpdateOptions{...})` | 修改；字段是指针，只改非 nil 的：`Description`、`CronExpression`、`TimeZone`、`Args`、`OverlapPolicy`、`CatchupWindow` |
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
| `create-schedule` | `{namespace, workflow_type, description, cron_expression, timezone, input, overlap_policy, paused, catchup_window}`（`catchup_window` 是纳秒） |
| `get-schedule`、`delete-schedule`、`pause-schedule`、`resume-schedule`、`trigger-schedule` | `{namespace, schedule_id}` |
| `update-schedule` | `{namespace, schedule_id, update:{description, cron_expression, timezone, input, overlap_policy, catchup_window}}` |
| `list-schedules` | `{filter:{namespace, workflow_type, paused, offset, limit}}` |

```bash
curl -sS -X POST http://127.0.0.1:7400/runtime/workflow/create-schedule \
  -H 'Content-Type: application/json' \
  -d '{"namespace":"orders","workflow_type":"daily-report","cron_expression":"30 2 * * *","timezone":"Asia/Shanghai","overlap_policy":"SKIP"}'
```

## 行为边界

- Catch-up：错过的到期点逐个判定（同 Temporal）：落后当前时间超过 `catchup_window`（缺省 365 天，最小 10 秒）的到期点不触发，计入 `missed_catchup_window`；窗口内的每个到期点按 Overlap Policy 处理，`ALLOW_ALL` 逐个补启动，`SKIP` 在补启动的 Run 仍在运行时跳过后面的到期点。窗口内待补的到期点超过 1000 个（Temporal 的 buffer 上限）时，同 Temporal 保留最早的 1000 个，buffer 满之后到达的到期点（到当前时刻为止）计入 `buffer_dropped`；保留的处理完后从当前时刻之后的到期点继续，buffer 没满时 `buffer_dropped` 不增长。超出窗口的那一段只计数，不逐个处理，长期停机后第一轮扫描不会变慢。补触发分批进行，每轮扫描一个 Schedule 最多处理 100 个到期点，每个 Run 的启动是独立的事务。
- 被调度的 Workflow 自己处理重复执行：用业务周期（如日期）作为 Activity 的幂等键。
- Schedule 触发的 Run 在 Workflow 里拿到的时间用 `workflow.Now(ctx)`；需要「本次计划时间」时由 Activity 读取 Run 的搜索属性 `TemporalScheduledStartTime`（`DescribeWorkflowExecution`），或把它放进业务逻辑可推导的输入。

## 排查

| 症状 | 检查 |
| --- | --- |
| 到点没有启动 | Schedule 是否 `paused`；`next_run_at`；`SKIP` 下 `last_run_id` 的执行链（含 Continue-As-New 之后的 Run）是否仍在运行；Workflow 定义是否已登记 |
| 重启后没有补跑 | `MissedCatchupWindow` / `BufferDropped` 是否增长；到期点是否落后超过 `CatchupWindow` |
| `StartFailed` 增长、到期点没有 Run | Server 日志 `schedule driver: start workflow failed` 的 `err`：`config_error=true` 时登记缺少的 Workflow 定义或修正输入；其他错误检查数据库。丢弃的到期点不会补跑 |
| 触发时间偏移 | `timezone` 与 Cron 的组合；夏令时前拨日跳过不存在的时刻，回拨日重复的时刻只触发一次 |
| `Trigger` 返回 409（`client.ErrConflict`） | `SKIP` 下执行链仍在运行（`previous scheduled run is still running`），或同一秒内重复触发 |
| `Trigger` 返回其他错误 | Schedule 状态、定义是否存在；`Trigger` 遇到连接中断 / 502 / 504 不自动重试（重发会再触发一次），先按 `TemporalScheduledById` 列 Run 确认 |
| 创建/更新返回 `invalid overlap_policy` | 取值只能是 `SKIP` 或 `ALLOW_ALL`（大写） |
