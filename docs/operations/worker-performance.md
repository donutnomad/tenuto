---
title: Worker 部署与性能调优
description: Worker 并发、poll、扩缩容、Sticky Cache 和优雅退出
status: Stable
---

# Worker 部署与性能调优

<FeatureStatus status="Stable" />

Worker 容量由任务到达率、单任务耗时、并发槽、Task Queue 路由和 Sticky 命中率共同决定。

## 快速检查

- Workflow Worker 与 Activity Worker 分开部署和扩缩容。
- Identity 使用实例唯一值，Build ID 使用部署版本。
- 每个 Task Queue 都有 active Worker 和充足并发槽。
- SIGTERM 期间停止 poll，并为 in-flight Activity 预留 drain 时间。
- Workflow Worker 配置 Execution Cache，并持续导出 Snapshot。

## 并发度

`WorkflowConcurrency` 控制并行 activation 数量；`ActivityConcurrency` 对每个 Activity Task Queue 生效。

初始容量可以按 Little's Law 估算：

```text
required concurrency ≈ peak arrival rate × p95 execution time × safety factor
```

Workflow Task 主要消耗 CPU 和内存，Activity Task 主要受下游延迟和连接池约束。调整并发后同时观察数据库、HTTP、MQ 和 HSM 等依赖的容量。

## Task Queue 拆分

按资源特征和故障域拆分队列：

| 队列 | 工作负载 | 调优方向 |
| --- | --- | --- |
| `payments` | 高价值短调用 | 限流、低延迟、严格幂等 |
| `reports` | CPU 或 I/O 长任务 | 独立并发、Heartbeat |
| `callbacks` | 外部回调 | 高并发、短超时 |

## Sticky Cache

命中率在预热后持续低于 80% 时，检查 Identity 稳定性、Build ID、IdleTTL、容量逐出和 sticky schedule-to-start deadline。缓存满且 active entry 全部 pinned 时，新 admission 会被拒绝并走 cold restore。

## 扩缩容

- 扩容后等待 Worker 注册、Heartbeat 和 readiness 完成。
- 缩容前停止新 poll，保留完成 in-flight task 的时间。
- Sticky owner 下线后，Server 会在 assignment 到期时回退普通队列。
- 大规模滚动发布会降低短期命中率，容量告警应区分发布窗口和稳态。

## 关键指标

| 指标 | 含义 |
| --- | --- |
| schedule-to-start | 队列等待与 Worker 容量 |
| task execution latency | 用户代码与下游耗时 |
| poll error / empty poll | TaskQ 和 Worker 连通性 |
| cache hit / miss | 增量执行效率 |
| eviction by reason | 容量、TTL、版本和 generation 变化 |
| activity retry / timeout | 下游可靠性和 Timeout 配置 |

相关内容：[运行 Worker](/develop/workers)、[Sticky Execution Cache](/operations/sticky-cache)、[Worker 路由排查](/troubleshooting/worker-routing)。
