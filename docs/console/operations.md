---
title: Run 控制、Reset 与 Batch
description: Console 中的取消、终止、Reset、Schedule 和批量操作
status: Preview
---

# Run 控制、Reset 与 Batch

<FeatureStatus status="Preview" />

Console 中的写操作会改变持久化执行状态。操作前确认 Namespace、Workflow ID、Run ID、业务影响和恢复路径。

## Cancel 与 Terminate

Cancel 把取消请求交给 Workflow 代码：根 Context 被取消，Workflow 取消自己的 Activity、Timer 与子 Workflow，做完清理后按返回值结束（返回取消错误时为 `cancelled`），适合业务撤销。Terminate 立即关闭 Run，适合故障隔离。两种操作都需要填写 Reason 并核对目标 Run。

## Workflow Reset

Reset 面板只列出 Workflow Task 边界。选择边界后预览：

- 原 Run 与新 Run 的关系。
- Reset Event ID、Build ID 和时间。
- Signal、Update reapply 选项。
- Search Attributes 和输入继承。

执行后进入新 Run，确认第一个 Workflow Task 成功并观察 Run Chain。

## Activity Reset

在 Activity Call 上选择 Reset，查看全部 Attempts、最后 Heartbeat（运行中取自 Activity 状态，超时的 Attempt 取自 `ActivityTimedOut`，被 Workflow Reset 打断的 Attempt 取自新 Run 的 `ActivityFailed`）和外部幂等键。Reset 后 Attempt 从 1 开始，旧 Token 和旧 Worker 回报由 Stamp 隔离。

## Schedule

Schedule 页面支持 Create、Pause、Resume、Trigger 和 Delete。每次操作的结果显示在列表上方：失败时显示 Server 返回的错误，Trigger 成功时显示新 Run ID 并链接到 Run Detail。Delete 先弹出确认框。列表按 Workflow Type（精确匹配）和 Active / Paused 状态在 Server 侧筛选，Overlap Policy 列显示每个 Schedule 的 `SKIP` 或 `ALLOW_ALL`。Run Count 链接到 Runs 列表，只列这个 Schedule 启动的 Run（按搜索属性 `TemporalScheduledById` 过滤，含 Continue-As-New 与重试产生的 Run）。

## Batch

New Batch 对话框选择操作类型（Cancel、Terminate、Reset），并按 Workflow ID 与状态筛选目标 Run。未选状态时只匹配运行中的 Run。

- Reset 需要选择锚点：Last Decision Point（最近一次 Workflow Task 完成处）或 First Decision Point（从头重跑）。
- 对话框实时显示当前匹配的 Run 数；匹配数为 0 时不能提交。
- 不填任何筛选条件时，操作覆盖 Namespace 内全部运行中的 Run。对话框显示警告，勾选写明操作类型和 Namespace 的确认项后才能提交。

提交后 Server 立即受理，Batch 在后台执行：页面上方提示已受理，新 Batch 以 `pending` 出现在列表里并自动展开。列表有 `pending` 或 `running` 的 Batch 时每 3 秒刷新一次，全部结束后停止刷新。

列表每行显示类型、状态、进度条（已处理数 / 总数，绿色成功、红色失败、灰色跳过）、Succeeded、Failed、Skipped 计数、创建与结束时间。展开一行显示一句话的结果摘要、Filter、Reason、Reset Type 与最多 20 条逐 Run 错误样本。批量 Cancel 的成功数是「已请求取消」的 Run 数，Run 何时结束取决于 Workflow。状态徽章覆盖 `pending`、`running`、`completed`、`failed` 与 `cancelled`。

`pending` 与 `running` 的 Batch 行上有 Cancel batch 按钮：Server 在下一页之前停下，已处理的 Run 保留结果，Batch 变为 `cancelled`。各状态与计数的语义见 [Batch Operation](/operations/batch-operations)。

## 审计

记录操作者、操作类型、目标、Reason、Request ID、开始时间、完成时间和结果。高风险操作关联工单或事故编号。
