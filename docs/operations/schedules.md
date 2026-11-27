---
title: Schedule 运维
description: Schedule CRUD、Overlap Policy、错过触发和 Leader 诊断
status: Preview
---

# Schedule 运维

<FeatureStatus status="Preview" />

Schedule 保存 Cron、时区、Workflow Type、输入、Overlap Policy 和下一次触发时间。每次触发的 Run 有自己的 Workflow ID `schedule-<Schedule ID>-<触发时刻 UTC RFC 3339>`，一个 Schedule 的 Run 都以 `schedule-<Schedule ID>-` 开头，最近一次触发的 Run ID 在 `last_run_id`。MySQL 部署使用 leader election，多副本同时处理同一个到期点时，按到期点的 Workflow ID 只启动一个 Run，`next_run_at` 的 CAS 决定由哪个副本记录它。

## 支持的策略

| Policy | 行为 |
| --- | --- |
| `SKIP`（缺省） | 上一次触发的执行链仍在运行时跳过本次 |
| `ALLOW_ALL` | 每个到期 tick 都创建 Run，与仍在运行的 Run 并行 |

Policy 只接受以上大写值，空值保存为 `SKIP`；其他值在 Create/Update 时返回 400 `invalid overlap_policy`。执行链指上一次触发的 Run 及它经 Continue-As-New、重试、Reset 接续的 Run，`SKIP` 按这个 Workflow ID 的 current Run 判定。

## 生命周期操作

- Create：校验 Cron 和 IANA timezone，计算 `next_run_at`。
- Update：修改描述、Cron、时区、输入、Overlap Policy 和 catch-up window。
- Pause：清空 `next_run_at`，已启动 Run 继续执行。
- Resume：从当前时间重新计算下一次触发。
- Trigger：人工立即触发，Workflow ID 取触发时刻（同一秒内重复触发得到同一个 Run）；同 Temporal 的 TriggerImmediately，不改变 `next_run_at`，积压的到期点照常补触发；遵守 Overlap Policy，`SKIP` 下执行链仍在运行时返回 409。
- Delete：删除后续触发配置，已有 Run 继续保留。

## Catch-up

每个 Schedule 有自己的 `catchup_window`（纳秒，缺省 365 天，最小 10 秒，Create / Update 时给出更小的值返回 400）。错过的到期点逐个判定（同 Temporal）：落后当前时间超过 `catchup_window`（缺省 365 天，最小 10 秒）的到期点不触发，计入 `missed_catchup_window`；窗口内的每个到期点按 Overlap Policy 处理，`ALLOW_ALL` 逐个补启动，`SKIP` 在补启动的 Run 仍在运行时跳过后面的到期点。窗口内待补的到期点超过 1000 个（Temporal 的 buffer 上限）时，同 Temporal 保留最早的 1000 个，buffer 满之后到达的到期点（到当前时刻为止）计入 `buffer_dropped`；保留的处理完后从当前时刻之后的到期点继续，buffer 没满时 `buffer_dropped` 不增长。超出窗口的那一段只计数，不逐个处理，长期停机后第一轮扫描不会变慢。补触发分批进行，每轮扫描一个 Schedule 最多处理 100 个到期点，每个 Run 的启动是独立的事务。 跳过时 Server 记 WARN `schedule driver: skipped fire times outside the catch-up window or over the buffer limit`（带本次的 `missed_catchup_window` 与 `buffer_dropped`）。停机恢复后的突发量由窗口控制：分钟级的 `ALLOW_ALL` Schedule 停机一天后恢复会补启约 1000 个 Run。

## 启动失败

启动失败分两类处理（同 Temporal scheduler）：Workflow 类型没有部署、输入不合法、载荷超限这类配置或请求错误不重试，这个到期点直接丢弃；存储或网络暂时不可用等其他错误按退避（1 秒起翻倍，最长 60 秒）重试，重试期限从到期点起算，取 min(`catchup_window`, 1 小时)（同 Temporal 启动 Workflow 的本地 Activity：`ScheduleToCloseTimeout` 取 catchup 截止时间，上限 1 小时）。到期点之后 1 小时仍没有启动成功的丢弃并计入 `start_failed`；`catchup_window` 小于 1 小时时，重试先超过 catchup window，这个到期点计入 `missed_catchup_window`。丢弃之前 Server 按到期点的 Workflow ID 确认一次：启动其实已经提交（提交之后入队失败或应答丢失）时按启动成功记录，不计入 `start_failed`。丢弃或跳过之后下一次执行时间推进到它之后的到期点，后面的到期点照常处理。因此启动持续失败时下一次执行时间最多落后当前时间 1 小时，修好之后只补出这 1 小时内的到期点（按 Overlap Policy）。补触发时已落后 1 小时以上的到期点只尝试一次。重试状态保存在 leader 的内存里，leader 切换后从新的一次尝试开始退避。

Server 每次丢弃记 ERROR `schedule driver: start workflow failed, dropping this fire time`（带 `fire_time`、`attempts`、`config_error` 与 `err`），每次重试前记 WARN `schedule driver: start workflow failed, will retry`（带 `attempt` 与 `next_attempt`）。`start_failed` 持续增长时按日志里的 `err` 处理：配置错误（`config_error=true`）登记缺少的 Program 或修正 Schedule 的输入，其他错误检查数据库与 Server 状态。

## Leader 与 CAS

MySQL 模式使用 `tenuto_leader_lock` 选举 Schedule Driver leader，lease TTL 为 15 秒，Heartbeat 为 5 秒。leader 副本正常停机时先停下驱动再释放锁，其他副本在 5 秒内接任；崩溃或卡顿的 leader 要等租约过期。每秒扫描 due Schedule：先以到期点的 Workflow ID 启动 Run（已在运行则不再启动），再 CAS 推进 `next_run_at`，CAS 成功的节点记录这次触发。

## 排障矩阵

| 症状 | 检查 |
| --- | --- |
| next_run_at 长期在过去 | leader 表、节点 Heartbeat、Server 日志 |
| 同一 tick 出现多个 Run | CAS schema、Node ID 唯一性、数据库事务 |
| SKIP 持续跳过 | `last_run_id` 所在 Workflow ID 的 current Run（可能是 Continue-As-New 之后的 Run）是否仍在运行 |
| 重启后缺少补跑 | `missed_catchup_window` / `buffer_dropped` 是否增长；到期点是否落后超过 `catchup_window` |
| 时区时间偏移 | IANA timezone、DST 切换点、Cron 表达式 |
| Trigger 返回 409 `previous scheduled run is still running` | `SKIP` 下上一次触发的执行链仍在运行 |
| `start_failed` 增长、Schedule 不启动 Run | Server 日志 `start workflow failed` 的 `err` 与 `config_error`；Workflow Type 是否已登记、输入是否合法 |
| Trigger 返回其他错误 | Schedule 状态、Workflow Definition |

人工变更应记录 Schedule ID、操作者、原因、旧值、新值和关联工单。
