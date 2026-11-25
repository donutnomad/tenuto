---
title: History 容量管理
description: Event History 限制、分页、Continue-As-New 与投影
status: Stable
---

# History 容量管理

<FeatureStatus status="Stable" />

Event History 大小直接影响 cold replay、网络传输、存储和调试体验。Tenuto 同时使用硬限制、Continue-As-New 建议、分页读取和 execution-state projection 控制成本。

## 默认限制

| 预算 | Warning | Hard | 终态预留 |
| --- | ---: | ---: | ---: |
| Events | 10,240 | 51,200 | 32 |
| Canonical JSON | 10 MiB | 50 MiB | 64 KiB |

业务事件使用 `hard - reserved` 范围。终态和 Workflow Task close 事件可以使用预留空间，保证 Run 能够形成明确终态。

每个 Event 整条以 canonical JSON 存入 `tenuto_events.event_json`（`LONGBLOB`），`event_size_bytes` 是这段 JSON 的字节数，也就是上表 Canonical JSON 预算的计量单位。事件表其余的列（`tree_id`、`branch_id`、`node_id`、`run_id`、`type`、`dedupe_key`、`timestamp`）用于定位、排序、去重和回收；排查单个事件的内容用 History API 或 Console，运维 SQL 只按这些列过滤。

## Workflow 内主动切换

```go
info := workflow.GetInfo(ctx)
if info.GetContinueAsNewSuggested() {
    return workflow.ContinueAsNew(ctx, "orders.LongRunning", compactState)
}
```

永久循环、定时聚合、批量消费和高频 Signal Workflow 应在 warning 边界执行 Continue-As-New。

## History 分页

`/runtime/workflow/history-page` 使用 HMAC Token 固定：

- Run ID
- branch
- snapshot 上界
- 排序方向
- 下一页位置

运行中追加事件不会改变当前分页快照。Console 首次打开 Run Detail 时按需加载事件页。

## Execution State Projection

Run List 与 Detail 读取 `tenuto_execution_states`，避免为每一行扫描完整 Event History。Projection 保存状态、等待点、失败、blocked、Search Attributes、Event 数量、字节数和最新 Event ID。

## 运维检查

```sql
SELECT COUNT(*) AS pending
FROM tenuto_runs AS r
LEFT JOIN tenuto_execution_states AS s ON s.run_id = r.id
WHERE s.run_id IS NULL;
```

Projection 行与 Run 行在同一个事务里写入，这个计数应为 0。

## Hard Limit 处理

1. 记录当前 Event 数、字节数和触发 reason。
2. 检查最近 Workflow Task 的 Continue-As-New 建议。
3. 调整 Workflow 的切换阈值和携带状态。
4. 使用新 Run 验证 History 从紧凑起点重新增长。
