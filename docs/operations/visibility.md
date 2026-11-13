---
title: Visibility 运维
description: Search Attributes、Run 筛选、Blocked State 和告警入口
status: Preview
---

# Visibility 运维

<FeatureStatus status="Preview" note="Search Attribute 查询仍由 Runtime 过滤" />

Visibility 用于快速找到需要关注的 Run，Event History 用于解释其完整执行过程。

## 推荐字段

| 字段 | 用途 | 示例 |
| --- | --- | --- |
| 业务主键 | 精确检索 | `order_id=order-1042` |
| 区域或租户 | 故障域筛选 | `region=ap-southeast-1` |
| 业务阶段 | 运营队列 | `approval=pending` |
| 风险等级 | 告警分级 | `tier=critical` |

字段值当前为字符串。敏感字段使用脱敏 ID，字段数量和基数纳入容量评估。

## 排障流程

1. 按 Namespace、Workflow Type、状态和 Search Attributes 缩小 Run 集合。
2. 查看 Current Waiting 与 Blocked Reason。
3. 进入 Run Detail 检查 Attempt、Worker Build ID 和事件时间线。
4. 对代码问题使用 Trace 或离线 Replay。
5. 对批量问题创建带精确 filter 的 Batch Operation。

## 告警建议

- Running Run 的年龄超过业务 SLO。
- Blocked Run 数量增长。
- 同一 Activity Type 的 timeout 或 retry 激增。
- History warning threshold 命中率上升。
- 某 Namespace active Worker 降为零。

## 查询成本

当前 Search Attribute filter 在 Runtime 层执行。大数据量环境应优先使用 Namespace、状态、Workflow Type 和时间窗口收敛集合，并控制 Console 查询页大小。

相关内容：[Visibility 概念](/concepts/visibility)、[Run 与 Event History](/console/runs)、[Batch Operation](/operations/batch-operations)。
