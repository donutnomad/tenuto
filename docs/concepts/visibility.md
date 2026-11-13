---
title: Visibility 与 Search Attributes
description: Run 检索、业务索引、等待点和 Blocked State
status: Preview
---

# Visibility 与 Search Attributes

<FeatureStatus status="Preview" note="Search Attribute 过滤当前由 Runtime 执行" />

Visibility 为运维检索提供 Run 摘要。Event History 继续承担审计与重放，Visibility 面向列表、筛选和告警。

## Run 摘要

Run List 可以展示并筛选：

- Namespace、Workflow ID、Workflow Type、Run ID 和状态。
- History 长度、失败原因、创建与关闭时间。
- 当前等待的 Workflow Task、Activity、Timer、Child Workflow、Signal 或 Update。
- Search Attributes 业务字段。
- Blocked State 与确定性错误。

## Search Attributes

```go
err := c.UpsertSearchAttributes(ctx, workflowID, runID, map[string]string{
    "order_id": "order-1042",
    "region":   "ap-southeast-1",
})

runs, err := c.ListWorkflow(ctx, client.ListWorkflowOptions{
    SearchAttributes: map[string]string{"region": "ap-southeast-1"},
})
```

`c` 是 [`sdk/client`](/sdk/client) 的 `Client`，Namespace 取自它；`runID` 为空时写入当前 Run。给出的键覆盖原值，其余键保留。搜索属性由调用端写入；Workflow 代码内写入搜索属性的入口只有 `workflow.GetVersion`，它自动维护 `TemporalChangeVersion`。

以下键由 Server 维护，调用端写入它们返回 `ErrInvalidArgument`：

| 键 | 写入方 | 值 |
| --- | --- | --- |
| `TemporalChangeVersion` | `workflow.GetVersion` | JSON 数组编码的 `changeID-version` 列表，按成员匹配 |
| `TemporalScheduledById` | Schedule 启动 Run 时 | Schedule ID；按它过滤列出一个 Schedule 的全部 Run（见 [Schedule](/develop/scheduling#找到一个-schedule-的全部-run)） |
| `TemporalScheduledStartTime` | Schedule 启动 Run 时 | 名义触发时间，UTC RFC 3339 秒级文本 |

属性更新会写入 Run 状态和 `SearchAttributesUpserted` Event。`WorkflowStarted` Event 记录启动时的属性，派生 Run 按 Temporal 语义继承：

| 派生方式 | 新 Run 的搜索属性 |
| --- | --- |
| Continue-As-New | 上一段 Run 的当前属性，包括 GetVersion 写入的 `TemporalChangeVersion` |
| 重试 | 失败 Run 启动时的属性，运行期写入的键不继承 |
| Reset 到某个事件 | 启动时的属性加上该事件及之前的全部写入 |
| 不指定事件的 Reset | 基准 Run 启动时的属性 |

升级前启动的 Run 的 `WorkflowStarted` 没有这份记录，重试和 Reset 继承它的当前属性。其中 `TemporalChangeVersion` 在重试时不继承，在 Reset 时只保留重置点之前的决策。

字段命名应保持业务稳定性。高基数字段适合精确定位，状态和区域适合列表筛选。敏感数据应使用脱敏标识。

## Blocked State

确定性失配属于可通过代码修复或回滚恢复的执行错误。Server 将 Run 标记为 blocked 并保存原因，运维人员可以定位 Build ID、Program version 和最后成功的 Workflow Task。

诊断顺序：

1. 查看失败 Workflow Task 和 Worker Build ID。
2. 对该 Run 的 History 执行新旧代码 Replay。
3. 回滚或使用 `GetVersion` 恢复兼容分支。
4. 观察 Run 重新领取 Workflow Task 后的状态。

相关内容：[Run 与 Event History](/console/runs)、[确定性约束](/develop/determinism)、[Visibility 运维](/operations/visibility)。
