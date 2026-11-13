---
title: Sticky Execution Cache
description: 增量激活、缓存配置、逐出和监控
status: Stable
---

# Sticky Execution Cache

<FeatureStatus status="Stable" />

Sticky Execution Cache 在 Workflow Worker 内保存已恢复的 execution session。缓存命中后，Worker 消费 Event History 增量并继续执行，减少重复重放成本。

## 配置 Cache

```go
workflowWorker, err := worker.New(c, worker.Options{
    Role:     worker.RoleWorkflow,
    Identity: "orders-worker-01",
    BuildID:  "orders@2026.07.21",
    ExecutionCache: &worker.ExecutionCacheOptions{
        MaxEntries:        1_000,
        MaxEstimatedBytes: 512 << 20,
        IdleTTL:           5 * time.Minute,
    },
})
if err != nil {
    return err
}
```

`ExecutionCacheOptions` 的零值字段取缺省值：`MaxEntries` 1000、`MaxEstimatedBytes` 256 MiB、`IdleTTL` 5 分钟。Sticky Execution 依赖稳定的 `Identity`。

配置 Cache 后，Worker 自动声明 incremental activation 和 sticky execution capability。

## Cache Key

Cache Entry 由以下字段共同定位：

```text
Namespace
RunID
WorkflowType
ArtifactFingerprint
ExecutorABIVersion
```

Execution Generation、Sticky Generation、Worker Build ID 和 capability 元数据用于防止旧 session 被错误复用。

## 容量规划

```text
cache budget = worker memory limit
             - process baseline
             - workflow/activity concurrency headroom
             - Go runtime and transport headroom
```

初始建议把 `MaxEstimatedBytes` 设为 Worker 内存限制的 20%，保留至少 30% RSS 余量。`MaxEntries` 作为独立上限，避免大量小 session 占满管理结构。

## 逐出原因

| 原因 | 含义 |
| --- | --- |
| `capacity` | 容量达到上限，按 LRU 逐出空闲 Entry |
| `ttl` | 超过 IdleTTL |
| `invalidated` | Run 状态或 artifact 发生失效 |
| `generation_mismatch` | 新旧 generation fence 不一致 |
| `shutdown` | Worker 正常退出 |

活跃 lease 会保护 Entry，容量压力期间优先逐出空闲 session。

## 监控

定期导出 `ExecutionCache.Snapshot()`：

- Entries 与 EstimatedBytes。
- Hits、Misses 与命中率。
- Admissions 与 Rejected。
- 按原因统计的 Evictions。

常用告警：预热后命中率低于 80%、持续 rejected admission、capacity eviction 超过 admission 的 10%、sticky fallback 超过 Workflow Task 的 1%。

## 回退路径

Worker 停止、缓存 miss、Entry 逐出、sticky schedule-to-start 超时或 Workflow Task 超时后，任务回到普通队列并执行 cold restore。Event History 与 mutable state始终是权威数据来源。

sticky 投递的代数或目标 Worker 与 Run 当前的 sticky 会话不一致时（会话在投递交到 Worker 之后被清除或换了 Worker），这份投递按普通投递处理：Workflow Task 仍 open 时照常开始，已关闭时由 Server 确认丢弃。
