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
- Update：修改描述、Cron、时区、输入和 Overlap Policy。
- Pause：清空 `next_run_at`，已启动 Run 继续执行。
- Resume：从当前时间重新计算下一次触发。
- Trigger：人工立即触发，并使用 CAS 推进下一次时间；遵守 Overlap Policy，`SKIP` 下执行链仍在运行时返回 409。
- Delete：删除后续触发配置，已有 Run 继续保留。

## Catch-up

当前 catch-up window 为一小时。`next_run_at` 落后当前时间超过一小时时，driver 推进到未来时间并跳过该次触发。该策略保护服务恢复时的突发流量。

## Leader 与 CAS

MySQL 模式使用 `tenuto_leader_lock` 选举 Schedule Driver leader，lease TTL 为 15 秒，Heartbeat 为 5 秒。leader 副本正常停机时先停下驱动再释放锁，其他副本在 5 秒内接任；崩溃或卡顿的 leader 要等租约过期。每秒扫描 due Schedule：先以到期点的 Workflow ID 启动 Run（已在运行则不再启动），再 CAS 推进 `next_run_at`，CAS 成功的节点记录这次触发。

## 排障矩阵

| 症状 | 检查 |
| --- | --- |
| next_run_at 长期在过去 | leader 表、节点 Heartbeat、Server 日志 |
| 同一 tick 出现多个 Run | CAS schema、Node ID 唯一性、数据库事务 |
| SKIP 持续跳过 | `last_run_id` 所在 Workflow ID 的 current Run（可能是 Continue-As-New 之后的 Run）是否仍在运行 |
| 重启后缺少补跑 | 是否超过一小时 catch-up window |
| 时区时间偏移 | IANA timezone、DST 切换点、Cron 表达式 |
| Trigger 返回 409 `previous scheduled run is still running` | `SKIP` 下上一次触发的执行链仍在运行 |
| Trigger 返回其他错误 | Schedule 状态、Workflow Definition |

人工变更应记录 Schedule ID、操作者、原因、旧值、新值和关联工单。
