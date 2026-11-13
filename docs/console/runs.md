---
title: Run 与 Event History
description: 使用 Console 定位 Workflow Execution 和历史事件
status: Stable
---

# Run 与 Event History

<FeatureStatus status="Stable" />

Run 列表是生产诊断的第一入口。建议先按 Namespace、Workflow Type、状态和 Search Attributes 缩小范围，再进入 Run Detail。

## 启动 Run

Start Run 页面分开填写两项：Workflow Type 从已登记的定义里选择；Workflow ID 标识这次执行，留空时由 Server 生成。同一个 Workflow Type 用不同的 Workflow ID 可以同时运行多个 Run。

## 状态与等待点

从 Schedules 页面进入时（`?schedule_id=`），列表只含这个 Schedule 启动的 Run，筛选栏显示 Schedule 标签，点击它去掉这个筛选。列表标题是符合筛选的 Run 数（`1 Run`、`38 Runs`）。改筛选、改排序（点 Start 列头切换先新后旧）或改每页条数时回到第一页。列表同时展示 Run Status、开始时间（Start）、结束时间（End，Run 未结束时为 —）和当前等待点。`running` Run 可能正在等待 Activity、Timer、Signal、Update、Child Workflow 或 Workflow Task。

Blocked 标记表示运行中的 Run 的 Workflow Task 连续失败（如 `non_deterministic_error`、`activity_route_unavailable`、`schedule_activity_duplicate_id`），status 仍为 `running`。详情页的 Workflow Blocked 卡片给出 cause、reason、连续失败次数和持续时间。Run 结束（完成、失败、取消、终止、Continue-As-New）后不再是 Blocked，Blocked 筛选也不再返回它。

## 失败与待处理的 Activity

失败的 Run 在摘要下方显示 Failure 卡片：失败 message、type、cause 链与 details；Workflow 级或 Activity 超时导致的失败同时标出 timeout 类型（如 `Run`、`StartToClose`）。

Pending 页签列出未结束的 Activity，内容取自 `activity-states`：当前尝试序号、Worker、开始时间、最近一次心跳（心跳不写 History，只在这里）、最近一次失败；在重试退避中的 Activity 显示下一次尝试的时间。最大尝试次数大于 1 的 Activity 执行中没有 `ActivityStarted` 事件，这些信息同样来自 `activity-states`。

## Event History

Event History 使用稳定快照分页，每页默认 200 条。分页 Token 绑定 Run、branch 和快照上界，运行中追加新事件不会打乱当前浏览窗口。

Timeline 的 Steps 显示每一步的耗时：Activity 从开始到结束，Child Workflow 从 `ChildWorkflowStarted` 到它的完成、失败、取消或终止事件。时间轴刻度按刻度间隔取精度，1s 到 1m 之间带一位小数（如 `1.2s`、`1.6s`），相邻刻度不会显示相同的值。

Run Detail 先加载最新的一页。History 超过一页时，Timeline 的 Steps 只覆盖已加载的事件并注明加载范围；未结束的 Activity 即使 `ActivityScheduled` 在更早的页里，也按 `activity-states` 显示在 Steps 与 Pending 中。

事件详情里的载荷（Update 的 args 与 outcome、取消与失败的 details）按 `json/plain-v1` 解码后显示为 JSON；Export JSON 导出原始事件。

常用排查顺序：

1. 查找最新的 Workflow Task 边界。
2. 检查当前 open Activity、Timer 或 Child。
3. 查看 Activity Attempt 的 started、failed、timed out 与 retry；运行中的心跳在 Pending 页签，超时 Attempt 的最后一次心跳在 `ActivityTimedOut` 里。
4. 对照 Worker Identity 和 Build ID。
5. 打开 Trace 重放到对应源码行。

## Run Chain

关系面板展示：

- Parent / Child Workflow。
- Continue-As-New 的上一段与下一段。
- Reset 原 Run、Reset Run 与锚点。

Chain 节点按执行时间排列，并显示方向标签，帮助判断事件先后关系。

## 批量操作

Runs 页面支持勾选多个 Run 发起 Cancel、Terminate 或 Reset。勾选只作用于当前页和当前筛选，换页、改筛选或改排序后自动清空。勾选的 Run 无论状态都在目标之内，失败或已终止的 Run 也可以批量 Reset。提交后 Server 受理并在后台执行，列表上方提示已受理并链接到 Batch 页面查看进度、计数与逐 Run 错误。

Run Detail 中的 Workflow ID 链接打开按该 Workflow ID 筛选的 Runs 页面。
