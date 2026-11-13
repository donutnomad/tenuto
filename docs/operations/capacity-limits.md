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
