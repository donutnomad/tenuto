---
title: Schedule 与定时启动
description: 用 cron 表达式周期启动 Workflow：创建、暂停、手动触发与错过触发的处理
status: Preview
---

# Schedule 与定时启动

<FeatureStatus status="Preview" />

Schedule 按 cron 表达式周期启动 Workflow Run，适合对账、数据同步和定期清理。Schedule 保存在 Server，调度器重启后从持久化状态恢复。

## 创建 Schedule

```go
import "github.com/donutnomad/tenuto/sdk/client"

c, err := client.Dial(client.Options{HostPort: serverURL, Namespace: "orders"})
if err != nil {
    return err
}
defer c.Close()

handle, err := c.ScheduleClient().Create(ctx, client.ScheduleOptions{
    WorkflowType:   "daily-reconcile",
    Description:    "每日对账",
    CronExpression: "30 2 * * *",
    TimeZone:       "Asia/Shanghai",
    Args:           []any{map[string]any{"scope": "all"}},
    OverlapPolicy:  client.ScheduleOverlapPolicySkip,
})
scheduleID := handle.GetID() // 由 Server 生成
```

| 字段 | 必填 | 含义 |
| --- | --- | --- |
| `WorkflowType` | 是 | 要启动的 Workflow Type，即 `@Workflow` 的 `type`（HTTP 字段 `workflow_type`） |
| `CronExpression` | 是 | 5 段 cron 表达式 |
| `TimeZone` | 否 | IANA 时区名，例如 `Asia/Shanghai`；空值为 UTC |
| `Args` | 否 | 至多一个，编码成 JSON 对象作为每次启动的 Workflow 输入 |
| `OverlapPolicy` | 否 | `ScheduleOverlapPolicySkip`（`SKIP`，缺省）或 `ScheduleOverlapPolicyAllowAll`（`ALLOW_ALL`）；其他值创建失败 |
| `Paused` | 否 | 创建后处于暂停状态 |
| `CatchupWindow` | 否 | 到期点之后仍会补触发的最长延迟（HTTP 字段 `catchup_window`，纳秒）；0 取 365 天，小于 10 秒创建失败。见[触发流程与错过触发](#触发流程与错过触发) |
| `Description` | 否 | 说明文字 |

Schedule 与它启动的 Run 在 Client 的 Namespace 里。

## cron 表达式

表达式固定 5 段，以空白分隔：

```text
分钟(0-59) 小时(0-23) 日(1-31) 月(1-12) 星期(0-6，0 为周日)
```

每段支持 `*`、`*/N`、单个数值 `N`、范围 `N-M` 和逗号分隔的组合。最小粒度是分钟。日与星期两段都以 `*` 开头之外的写法限定时，满足其中任意一段的日期即触发（`0 0 13 * 5` 是每月 13 日和每个周五）；只限定其中一段时按该段匹配。段数不是 5、数值越界，或永远不会匹配（如 `0 0 31 2 *`、`0 0 31 4,6,9,11 *`，所列月份里没有所列的日）时，创建与更新返回 `invalid cron_expression`（HTTP 400）。

表达式按 `TimeZone` 的墙上时间匹配。夏令时切换日：

- 前拨（例如美东 3 月 02:00 跳到 03:00）：当天不存在的时刻不触发，`30 2 * * *` 那一天跳过，次日照常。
- 回拨（例如美东 11 月 02:00 回到 01:00，01:00–01:59 出现两次）：重复的时刻只在第一次出现时触发一次，`30 1 * * *` 当天在夏令时的 01:30 触发，标准时间的 01:30 不再触发。与 Vixie cron 相同；Temporal Schedule 在两次出现时都触发，这一点不同。
- 半小时的切换（如 `Australia/Lord_Howe` 回拨 30 分钟）按同样的规则处理。

| 表达式 | 含义 |
| --- | --- |
| `*/5 * * * *` | 每 5 分钟 |
| `0 * * * *` | 每小时整点 |
| `30 2 * * *` | 每天 02:30 |
| `0 9 * * 1-5` | 周一到周五 09:00 |

## Overlap Policy

Overlap Policy 决定上一次触发的 Run 仍在运行时这次触发怎么处理，语义与 Temporal Schedule 的同名策略一致。

| 取值 | 行为 |
| --- | --- |
| `SKIP`（缺省） | 上一次触发的 Run 仍在运行时跳过本次触发 |
| `ALLOW_ALL` | 每次触发都启动新 Run，与仍在运行的 Run 并行 |

- 取值区分大小写。创建或更新时给出其他值（包括小写的 `skip`、Temporal 的 `BUFFER_ONE`、`CANCEL_OTHER`、`TERMINATE_OTHER`）返回 `invalid overlap_policy`（HTTP 400），Schedule 保持原值。
- 「仍在运行」按上一次触发的 Run 所在 Workflow ID 的 current Run 判定：那个 Run 经 [Continue-As-New](/develop/continue-as-new)、失败重试或 Reset 接续时，看的是接续的那一代。整条链结束之后下一个到期点才启动新 Run。
- 读取上一次 Run 的状态失败时跳过本次触发；上一次的 Run 已被清理（不存在）时视为已结束。
- 一次触发从不接替仍在运行的 Run：每次触发都有自己的 Workflow ID（见下一节）。

执行时间可能超过周期的任务使用 `SKIP`，保证同一 Schedule 同一时刻只有一条执行链。

## Schedule 的 Run 如何命名

每次触发启动的 Run 有自己的 Workflow ID：`schedule-<Schedule ID>-<触发时刻>`，触发时刻是 UTC、精确到秒的 RFC 3339 时间，例如 `schedule-6f1c...-2026-11-10T02:30:00Z`。到期触发取 cron 到期点，`Trigger` 取调用时刻。Workflow Type 是创建时给出的 `WorkflowType`。

- Signal、Query、Update、Cancel 按这个 Workflow ID 寻址到这次触发（及它 Continue-As-New 的后续 Run）。最近一次触发的 Run ID 在 Schedule 的 `LastRunID`。
- 一个 Schedule 的全部 Run 的 Workflow ID 都以 `schedule-<Schedule ID>-` 开头；Run 列表按 Workflow ID 过滤时给出完整的 Workflow ID（某一次触发）。按 Schedule 列出全部 Run 用下一节的搜索属性。
- 同一个到期点重复启动（例如上一次启动已提交、应答丢失后的重试）落在同一个 Workflow ID 上：那个 Run 仍在运行时不会再启动第二个。
- 两个 Schedule 指向同一个 Workflow Type 时 Workflow ID 互不相同；手动 `ExecuteWorkflow` 同一个类型也不受 Schedule 的 Run 影响。

## 找到一个 Schedule 的全部 Run

Schedule 启动的 Run 带两个由 Server 写入的搜索属性，键与 Temporal 相同：

| 键 | 值 |
| --- | --- |
| `TemporalScheduledById` | Schedule ID |
| `TemporalScheduledStartTime` | 名义触发时间：到期触发是 cron 到期点，`Trigger` 是调用时刻；UTC、精确到秒的 RFC 3339 文本，与 Workflow ID 里的时间相同 |

两个键写在 `WorkflowStarted` 的启动搜索属性里，Continue-As-New 与重试产生的 Run 照常继承，所以按 `TemporalScheduledById` 过滤得到这个 Schedule 启动的全部执行链。调用端写入这两个键返回 `ErrInvalidArgument`。

```go
runs, err := c.ListWorkflow(ctx, client.ListWorkflowOptions{
    SearchAttributes: map[string]string{"TemporalScheduledById": scheduleID},
    NewestFirst:      true,
})

// 批量操作用同一个过滤条件，例如删除 Schedule 后终止它仍在运行的 Run
op, err := c.StartBatchOperation(ctx, client.StartBatchOperationOptions{
    Type:   client.BatchOperationTypeTerminate,
    Filter: client.ListWorkflowOptions{SearchAttributes: map[string]string{"TemporalScheduledById": scheduleID}},
    Reason: "schedule retired",
})
```

Console 的 Schedules 页面上，Run Count 链接到按这个 Schedule 过滤的 Runs 列表（`/<namespace>/runs?schedule_id=<Schedule ID>`）。

## 管理 Schedule

```go
handle := c.ScheduleClient().GetHandle(ctx, scheduleID)
description, err := handle.Describe(ctx)

cron := "0 3 * * *"
err = handle.Update(ctx, client.ScheduleUpdateOptions{CronExpression: &cron})

err = handle.Pause(ctx)
err = handle.Unpause(ctx)

run, err := handle.Trigger(ctx) // 返回这次启动的 WorkflowRun

list, err := c.ScheduleClient().List(ctx, client.ScheduleListOptions{})

err = handle.Delete(ctx)
```

| 操作 | 行为 |
| --- | --- |
| `Update` | 只修改非 nil 字段；cron 或时区变化且未暂停时重算下一次执行时间 |
| `Pause` | 停止触发并清空下一次执行时间；已启动的 Run 继续执行 |
| `Unpause` | 从当前时间起重算下一次执行时间 |
| `Trigger` | 立即启动一次并返回它的 `WorkflowRun`，同样遵守 Overlap Policy：`SKIP` 下上一次触发的 Run 仍在运行时返回 `ErrConflict`（`previous scheduled run is still running`，HTTP 409）。与 Temporal 的 TriggerImmediately 相同，不改变下一次执行时间，积压的到期点照常补触发；同一秒内重复触发得到同一个 Run |
| `Delete` | 删除 Schedule |

`Describe` 返回的 `ScheduleDescription` 包含 `NextRunAt`、`LastRunAt`、`LastRunID`、`RunCount` 与跳过计数 `MissedCatchupWindow`、`BufferDropped`、`StartFailed`，用于观察执行情况。

## 触发流程与错过触发

调度器处理到期的 Schedule：

1. 到期点落后当前时间超过 `CatchupWindow`：不触发，计入 `MissedCatchupWindow`，处理下一个到期点。
2. `SKIP` 策略下检查上一次触发的执行链是否仍在运行，仍在运行就跳过这个到期点，推进下一次执行时间。
3. 以这个到期点的 Workflow ID（`schedule-<Schedule ID>-<到期点>`）启动 Workflow。这个 Workflow ID 的 Run 已在运行时（上一次启动其实已提交，或另一个副本刚启动了它）不再启动第二个，直接推进下一次执行时间。启动失败的处理见下文。
4. 用 CAS 把下一次执行时间推进到这个到期点之后的下一个 cron 时刻（错过的后续到期点接着处理）；多个 Server 副本同时触发同一个到期点时只有一个推进成功，由它记录 `LastRunAt`、`LastRunID`、`RunCount`。

先启动再推进：两步之间进程退出时，下一次执行时间仍是这个到期点，下一轮以同一个 Workflow ID 重新启动，得到已启动的那个 Run，不会重复执行。

启动失败分两类处理（同 Temporal scheduler）：Workflow 类型没有部署、输入不合法、载荷超限这类配置或请求错误不重试，这个到期点直接丢弃；存储或网络暂时不可用等其他错误按退避（1 秒起翻倍，最长 60 秒）重试，重试期限从到期点起算，取 min(`catchup_window`, 1 小时)（同 Temporal 启动 Workflow 的本地 Activity：`ScheduleToCloseTimeout` 取 catchup 截止时间，上限 1 小时）。到期点之后 1 小时仍没有启动成功的丢弃并计入 `start_failed`；`catchup_window` 小于 1 小时时，重试先超过 catchup window，这个到期点计入 `missed_catchup_window`。丢弃之前 Server 按到期点的 Workflow ID 确认一次：启动其实已经提交（提交之后入队失败或应答丢失）时按启动成功记录，不计入 `start_failed`。丢弃或跳过之后下一次执行时间推进到它之后的到期点，后面的到期点照常处理。因此启动持续失败时下一次执行时间最多落后当前时间 1 小时，修好之后只补出这 1 小时内的到期点（按 Overlap Policy）。补触发时已落后 1 小时以上的到期点只尝试一次。重试状态保存在 leader 的内存里，leader 切换后从新的一次尝试开始退避。`Describe` 返回的 `StartFailed` 是累计丢弃的到期点数，Server 每次丢弃记 ERROR `schedule driver: start workflow failed, dropping this fire time`（`config_error` 区分两类），每次重试前记 WARN `schedule driver: start workflow failed, will retry`。

Server 停机或数据库不可用期间错过的到期点在恢复后补触发。错过的到期点逐个判定（同 Temporal）：落后当前时间超过 `catchup_window`（缺省 365 天，最小 10 秒）的到期点不触发，计入 `missed_catchup_window`；窗口内的每个到期点按 Overlap Policy 处理，`ALLOW_ALL` 逐个补启动，`SKIP` 在补启动的 Run 仍在运行时跳过后面的到期点。窗口内待补的到期点超过 1000 个（Temporal 的 buffer 上限）时，同 Temporal 保留最早的 1000 个，buffer 满之后到达的到期点（到当前时刻为止）计入 `buffer_dropped`；保留的处理完后从当前时刻之后的到期点继续，buffer 没满时 `buffer_dropped` 不增长。超出窗口的那一段只计数，不逐个处理，长期停机后第一轮扫描不会变慢。补触发分批进行，每轮扫描一个 Schedule 最多处理 100 个到期点，每个 Run 的启动是独立的事务。 不希望停机后补跑的 Schedule 把 `CatchupWindow` 设小（例如 `time.Minute`）；`Describe` 返回的 `MissedCatchupWindow` 与 `BufferDropped` 是累计跳过的到期点数。需要补齐每个周期的业务在 Workflow 输入或业务数据库里记录处理进度，由 Workflow 自己计算要处理的区间。

## 周期任务设计

- Activity 写入使用幂等键，允许触发与网络重试。
- Workflow 内的当前时间取 `workflow.Now(ctx)`，业务日期由 Activity 根据处理进度计算。
- 长期运行的周期 Workflow 用 [Continue-As-New](/develop/continue-as-new) 控制 History 大小。
- 用 Schedule 表达「周期启动」，用 Workflow 内的 `workflow.Sleep` 表达「一次 Run 内的等待」。

运维侧内容见 [Schedule 运维](/operations/schedules)。
