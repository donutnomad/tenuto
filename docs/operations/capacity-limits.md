---
title: 容量规划与限制
description: History、Worker、数据库、TaskQ、Schedule 和 Visibility 容量模型
status: Stable
---

# 容量规划与限制

<FeatureStatus status="Stable" />

Tenuto 容量由事件写入率、open mutable state、Task Queue 吞吐、Worker 并发和 Sticky Cache 内存共同决定。

## 核心维度

| 维度 | 主要驱动 | 保护措施 |
| --- | --- | --- |
| History events/s | Workflow Task、Activity Attempt、Signal、Update | Continue-As-New、限流 |
| Open Activity | 下游延迟、异步完成 | Timeout、Heartbeat、Worker 容量 |
| Open Timer | 长等待数量 | Timer State 索引、恢复预算 |
| TaskQ backlog | 到达率与 Worker 吞吐 | 队列拆分、扩容、背压 |
| Sticky memory | 活跃 Run 与 session 大小 | MaxEntries、MaxEstimatedBytes、IdleTTL |
| Visibility scan | Run 总量与 Filter 宽度 | Namespace、状态和时间窗口 |

## History

Workflow 使用 `workflow.GetInfo(ctx)` 读取 activation 开始时的 History 长度、大小和 Continue-As-New 建议。

```go
info := workflow.GetInfo(ctx)
if info.GetContinueAsNewSuggested() {
    return workflow.ContinueAsNew(ctx, "orders.LongRunning", compactState)
}
```

长期循环应把跨段状态压缩为业务 cursor、必要聚合值和幂等窗口。

## 载荷大小

单个载荷 JSON 编码后不超过 `WORKFLOW_PAYLOAD_SIZE_LIMIT_BYTES`（默认 2 MiB，与 Temporal `limit.blobSize.error` 的缺省值相同，最大 3 MiB）。每个 Runtime HTTP 请求体不超过 4 MiB，超过时应答 400。Task Token 不含载荷，一次 Activity 调用只发送 token、Worker 身份和这次上报的载荷，所以一个不超过上限的载荷总能放进请求体。

| 载荷 | 检查位置 | 超限时 |
| --- | --- | --- |
| Workflow 输入（Start、SignalWithStart、UpdateWithStart） | Server 受理请求 | 400，不创建 Run |
| Signal 输入、Update 参数 | Server 受理请求 | 400，History 不变 |
| Activity 输入、子 Workflow 输入、Continue-As-New 输入、Marker 数据、Workflow 结果 | Server 提交 Workflow Task 的命令 | 与 `bad_schedule_activity_attributes` 同类的命令错误：Workflow Task 以 `payload_size_limit_exceeded` 失败，命令不落库，Run 保持 Running、显示 Blocked 并退避重试；缩小载荷后发布新代码，Run 继续 |
| Activity 结果、心跳详情 | Worker 发送之前（上限随 `begin` 的应答下发），Server 再检查一次 | Attempt 以不可重试的 `PayloadSizeLimitExceeded` 失败结束，结果不发送；Workflow 代码从 `Get` 拿到这个 Type 的应用错误 |
| 异步完成的结果 | Server 受理 `complete` | 记为不可重试的 `PayloadSizeLimitExceeded` 失败 |

Worker 收到 Server 不可重试的 4xx（400、404、409、413、422）时不立即退回投递，避免队列无退避地重投：

- `begin` 被拒绝：按连续拒绝次数退避（1s 起翻倍，最长 1 分钟）后退回租约。
- Activity 结果被拒绝：改报一次不可重试的 `ActivityResultRejected` 失败，Attempt 不必等到 StartToClose 超时再重跑。
- Workflow Task 的上报被拒绝：既不确认也不退回，投递留到队列的可见性超时（TaskQ 默认 60s）；已开始的 Workflow Task 由 Server 的 Workflow Task 超时重新调度。

大数据放在外部存储，载荷里传引用。一个 Workflow Task 里同时调度多个大输入的 Activity 时，全部命令也要放进一个 4 MiB 的请求体；超过时 Workflow Task 按上一条超时重试，Run 不推进；分批调度（先 `Get` 前一批的结果再调度下一批）或缩小输入。

## 数据库连接池

`WORKFLOW_DB_MAX_OPEN_CONNS` 是每个副本 Workflow 与 TaskQ 两个连接池各自的上限，所有副本合计最多 `副本数 × 2 × WORKFLOW_DB_MAX_OPEN_CONNS` 条，这个数加上 migrate 与运维工具的余量要小于 MySQL 的 `max_connections`（见 [数据库连接数](/deploy/production#数据库连接数)）。`MAX_IDLE_CONNS` 覆盖稳态并发，connection lifetime 与数据库或代理的连接回收策略配合。

## 保留期回收

开启 `WORKFLOW_RETENTION` 后，GC leader 每 `WORKFLOW_GC_SCAN_INTERVAL` 开始一轮扫描，一批批（`WORKFLOW_GC_BATCH`）处理到期的 History tree，直到没有到期任务或本轮用满一个扫描间隔。单个 tree 的删除是十几条按索引的语句，一个 leader 的回收吞吐远高于常见的 Run 创建速度（每个 Child Workflow 是一棵独立的 tree），实测数字见 [生产就绪与验证结果](/operations/production-readiness#压测)。`tenuto.retention.gc.backlog` 与 `tenuto.retention.gc.backlog.oldest_age` 持续增长说明回收跟不上，leader 同时记 WARN `retention gc: deletion backlog is growing`，见 [可观测性](/operations/observability#指标)。

## 负载测试

已测得的吞吐、延迟与每个 Workflow 的语句数见 [生产就绪与验证结果](/operations/production-readiness#验证结果)。至少覆盖峰值 Start、Signal、Update、Activity completion、Timer fire 和 History pagination。压测同时观测下游系统，确保重试风暴和 Schedule 集中触发处于容量预算内。

## 降载顺序

1. 业务入口限流和批量请求收敛。
2. 扩展对应 Task Queue 的 Worker。
3. 暂停低优先级 Schedule。
4. 缩小 Visibility 查询窗口。
5. 对长 History Run 执行受控 Continue-As-New。

相关内容：[History 容量管理](/operations/history-scaling)、[Worker 性能调优](/operations/worker-performance)。
