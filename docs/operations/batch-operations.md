---
title: Batch Operation
description: 批量 Cancel、Terminate、Reset 的选择语义、异步执行、进度、取消与恢复
status: Preview
---

# Batch Operation

<FeatureStatus status="Preview" />

Batch Operation 对 Run List Filter 选中的全部 Run 逐个执行 `cancel`、`terminate` 或 `reset`。Batch 是持久化的后台任务：`start-batch` 只校验请求、保存一条 `pending` 记录并返回（HTTP 202），Server 的后台驱动分页处理选中的 Run，随时可以用 `get-batch` / `list-batches` 查看进度，用 `cancel-batch` 取消。

记录保存在数据库（MySQL 部署的 `tenuto_batch_operations` 表；内存后端在进程内），Server 重启或多副本部署下任一副本都能读到同一条记录。

## 选择哪些 Run

- 目标是 Filter 匹配的全部 Run，Filter 里的 `page_token`、`limit` 与 `order_desc` 不参与；提供 `run_ids` 时目标就是这些 Run，已结束的也在内。
- **只处理 Batch 创建之前创建的 Run**（精确到创建时刻所在的那一毫秒）。Reset 产生的新 Run、Batch 运行期间新启动的 Run 都不在目标之内，Batch 一定会结束。
- 选择在翻页时按条件实时求值：驱动按 Run 的 `(created_at, id)` 顺序用游标翻页，每页读取当时仍匹配 Filter 的 Run。处理到之前就离开 Filter 的 Run（例如在别处被终止、不再是 running）不再处理。
- `total_count` 在处理第一页时取当时的匹配数；Batch 结束时改为实际处理的 Run 数。

## 执行前

1. 在 Run List 使用同一 Filter 预览目标集合。
2. 固定 Namespace、Workflow Type、状态和业务时间窗口。
3. 为 Reset 选择 Reset Type（`first_workflow_task` 或 `last_workflow_task`）；缺少 Reset Type 的 Batch Reset 在创建时被拒绝（400），不产生记录。
4. 填写可审计 Reason。
5. 对少量 Run 先执行 canary 操作。

## 状态与进度

```text
pending ──► running ──► completed
   │           ├──────► failed
   └───────────┴──────► cancelled
```

| 状态 | 含义 |
| --- | --- |
| `pending` | 已受理，后台驱动尚未开始 |
| `running` | 驱动正在分页处理 |
| `completed` | 全部选中的 Run 已处理，且至少一个成功或没有失败 |
| `failed` | 全部选中的 Run 已处理，有失败且没有一个成功 |
| `cancelled` | 用户取消了 Batch；已处理的计数保留，其余 Run 不再处理 |

每个 Run 计入三个计数之一：

| 计数 | cancel | terminate | reset |
| --- | --- | --- | --- |
| `succeeded_count` | 已请求取消（Run 何时结束取决于 Workflow 对取消的处理） | 已终止 | 已 Reset，产生了新 Run |
| `skipped_count` | Run 已结束 | Run 已结束 | Run 在 Batch 创建之后已被 Reset 过 |
| `failed_count` | 动作返回错误 | 动作返回错误 | 动作返回错误 |

Batch 的状态只描述 Batch 本身：批量取消在全部取消请求发出后就是 `completed`，此时 Run 可能仍在运行。`errors` 保存最多 20 条逐 Run 错误样本（`{run_id, message}`），更多的失败只计数。时间字段是 `created_at`、`started_at`（驱动开始处理）、`finished_at`（结束或取消）与 `updated_at`。

## 执行与恢复

- 驱动每秒检查一次 `pending` 与 `running` 的 Batch，按创建顺序逐个处理，每页 100 个 Run。每页的动作全部执行后，计数与游标一起保存。
- 多副本部署（同一个 MySQL）下驱动经数据库选主，只有 leader 副本处理 Batch（lock 名 `workflow:batch-driver`，与 Schedule、保留期 GC、reconciler 同一张 leader 表）。
- Server 停止、崩溃或换 leader 后，下一任驱动从最后保存的游标继续。保存之前已经执行的那一页会重做一次，重做对每种动作都不会重复生效：
  - terminate：上次已终止的 Run 已结束，这次计入 `skipped_count`；
  - cancel：已请求过取消的 Run 再次请求不写新事件，仍计入 `succeeded_count`；已结束的计入 `skipped_count`；
  - reset：Run 的最新 Reset 产生的新 Run 晚于 Batch 创建时，视为已 Reset，计入 `skipped_count`。

  因此恢复后的计数准确，但上一任驱动在崩溃前完成的那部分动作可能计入 `skipped_count` 而不是 `succeeded_count`。
- 租约切换的瞬间两个副本都可能认为自己是 leader。记录带版本号，保存基于过期读取时被拒绝：落后的驱动丢弃自己那一页的结果并停下，计数不重复。两个驱动同时处理同一页时，terminate 与 cancel 的第二次执行按上面的规则不重复生效；reset 带确定性的 `request_id`（由 Batch ID 与 Run ID 组成，对应 Temporal batcher 的 `deterministicRequestID`），Server 按它去重，第二次执行返回第一次 reset 出的 Run，同一个 Run 不会被 reset 两次。

## 取消 Batch

`cancel-batch` 把 `pending` 或 `running` 的 Batch 标为 `cancelled`，驱动在下一页之前停下。取消时正在处理的那一页仍会执行完并计入计数。已执行的动作不回滚。重复取消返回同一条 `cancelled` 记录；取消已经 `completed` 或 `failed` 的 Batch 返回 409。

## 操作选择

| 类型 | 语义 | 典型场景 |
| --- | --- | --- |
| Cancel | 取消请求交给 Workflow 代码，由它清理并决定终态 | 业务撤销、补偿 |
| Terminate | 立即终态 | 故障隔离、失控执行 |
| Reset | 从 Workflow Task 边界创建新 Run | 代码修复后重放 |

## 验证

- `succeeded_count + failed_count + skipped_count` 与目标集合相符。
- 失败 Run 逐条进入人工队列；样本之外的失败用同一 Filter 在 Run List 中查找。
- Cancel 的 Run 在 History 里有 `WorkflowCancellationRequested`，Workflow 代码取消的 Activity 有 `ActivityCancelRequested`。
- Reset 产生新 Run ID 和正确 Run Chain。
- 告警、审计和业务系统同步更新。

Batch ID、Filter、Reason、操作者和最终结果应长期保留在操作审计中。
