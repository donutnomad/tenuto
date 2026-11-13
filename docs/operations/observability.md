---
title: 可观测性
description: Tenuto 指标、日志、追踪与告警设计
status: Stable
---

# 可观测性

<FeatureStatus status="Stable" />

Tenuto 的可观测性围绕 Run、Workflow Task、Activity Attempt 和 Task Queue 四个层次建设。

## 启用 OpenTelemetry

Tenuto 通过 OTLP/gRPC 导出 traces、metrics 和 logs。默认关闭，启用示例：

```bash
export TENUTO_OTEL_ENABLED=true
export TENUTO_OTEL_TRACE_SAMPLE_RATIO=1
export TENUTO_OTEL_METRIC_EXPORT_INTERVAL=60s
export TENUTO_OTEL_LOG_LEVEL=INFO
export TENUTO_OTEL_SHUTDOWN_TIMEOUT=5s
export OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4317
export OTEL_SERVICE_NAME=tenuto-server
```

HTTP Runtime、TaskQ gRPC client 和 Worker runtime HTTP client 使用 W3C TraceContext 与 Baggage。Workflow Task、Activity 和 Timer 的 carrier 写入 scheduled history/state，并在恢复、重试和进程重启后继续传播。

关键 spans 包括 `StartWorkflow`、`RunWorkflowTask`、`CompleteWorkflowTask`、`RunActivity`、`CompleteActivity`、`FailActivity`、`FireTimer`、`DispatchWorkflowTask`、`DispatchActivityTask` 和 `persistence.transaction <operation>`。SDK 的 Update 与 Update-With-Start 产生语义 client spans。

## 日志字段

所有 Server、Worker 和客户端日志建议包含：

```text
namespace
workflow_id
run_id
workflow_type
task_queue
workflow_task_id
activity_id
scheduled_event_id
attempt
task_token_hash
```

Task Token 本身包含签名材料，日志使用哈希或截断值，避免泄露完整 Token。

## 指标

核心指标包括：

- Run 按状态、Workflow Type、Namespace 的数量
- Workflow Task 调度延迟、执行耗时和失败率
- Activity Schedule-To-Start、Start-To-Close、重试次数和最终失败率
- Task Queue backlog、lease 过期、投递失败和 Worker poll 数
- Replay 耗时、History 长度、Sticky Cache 命中率和逐出次数
- Update Accepted 到 Completed 的延迟

当前内置 OTel metrics：

| 指标 | 维度 |
| --- | --- |
| `tenuto.workflow.task.executions` / `tenuto.workflow.task.duration` | namespace、workflow type、outcome |
| `tenuto.activity.executions` / `tenuto.activity.duration` | namespace、activity type、task queue、outcome |
| `tenuto.task.dispatches` / `tenuto.task.dispatch.duration` | namespace、task type、task queue、outcome |
| `tenuto.persistence.transactions` / `tenuto.persistence.transaction.duration` | operation、outcome |
| `tenuto.persistence.pool.connections`（gauge）/ `tenuto.persistence.pool.wait.count` / `tenuto.persistence.pool.wait.duration` | operation（`workflow` 或 `taskq` 连接池）、pool state（`in_use` / `idle`） |
| `tenuto.retention.gc.reclaimed` | gc object（`tree`、`orphan_event_run`、`orphan_tree_row`） |
| `tenuto.retention.gc.failures` | outcome（`deferred`、`parked`） |
| `tenuto.retention.gc.backlog`（gauge）/ `tenuto.retention.gc.backlog.oldest_age`（gauge，秒） | 无；只由持有 GC 租约的副本在每轮扫描后上报 |

Workflow ID、Run ID 和 Scheduled Event ID 记录在 spans 与 logs 中。metrics 维度保持低基数。

## 告警建议

| 告警 | 说明 |
| --- | --- |
| backlog 持续增长 | Worker 容量或队列路由不足 |
| Schedule-To-Start 超阈值 | 队列拥塞、Worker 不在线或 Namespace 配置错误 |
| Replay 耗时增长 | History 过大、Program 复杂度增加或 Cache 命中率下降 |
| stale task 增长 | Worker 版本、网络或重复投递异常 |
| Run 长时间无进展 | Workflow 等待 Signal、Activity 或 Timer |
| `tenuto.retention.gc.backlog.oldest_age` 超过两个 `WORKFLOW_GC_SCAN_INTERVAL` | 保留期回收跟不上到期速度，或删除持续失败（看 `tenuto.retention.gc.failures` 与 leader 的 WARN `retention gc: deletion backlog is growing`） |
| `tenuto.persistence.pool.wait.count` 持续增长 | 连接池已是瓶颈；调整前按 [数据库连接数](/deploy/production#数据库连接数) 核对 MySQL 的 `max_connections` |
| Runtime API 503 增多 | 数据库或任务队列暂时不可用（连接数打满、锁等待、分区换主），Server 日志 WARN `runtime http request failed: dependency temporarily unavailable` 带原始错误 |
| WARN `task queue ack failed` 持续出现 | 任务队列的分区反复换主或 owner 不可达；确认失败不影响结局，被确认的任务在可见性超时后重投并被丢弃，持续出现时查 matching 的分区租约 |

## Trace 与 History

Console Trace 用 Program 和 History 重放生成调试视图。Trace 适合定位步骤、变量、并发 lane、Activity call 和 Child Workflow 栈。History 适合确认事件顺序、Attempt、失败详情和 Run Chain。两者结合可以区分业务阻塞、队列阻塞和 Worker 故障。
