---
title: Continue-As-New
description: 使用新的 Run Segment 控制长期 Workflow 的 History 大小
status: Stable
---

# Continue-As-New

<FeatureStatus status="Stable" />

Continue-As-New 关闭当前 Run，并使用新的输入创建同一业务链上的下一代 Run。它适合永续循环、长期订阅和高事件量 Workflow。

## 基本用法

```go
type LoopState struct {
    Cursor int64 `json:"cursor"`
}

// @Workflow(type="processor")
func Processor(ctx workflow.Context, state LoopState) error {
    cursor := state.Cursor
    for i := 0; i < 100; i++ {
        out, err := CallProcessBatch(ctx, ProcessBatchInput{Cursor: cursor}, workflow.ActivityOptions{StartToCloseTimeout: 30 * time.Second})
        if err != nil {
            return err
        }
        cursor = out.NextCursor
    }
    return workflow.ContinueAsNew(ctx, "processor", LoopState{Cursor: cursor})
}
```

签名：`func ContinueAsNew(ctx Context, workflowType string, input any) error`。

- `workflowType` 是下一代 Run 使用的 Workflow Type，通常与当前 Workflow 相同。
- `input` 成为下一代 Run 的输入。
- 调用后当前协程立即结束，它后面的语句不再执行。当前 Run 状态变为 `continued_as_new`，新 Run 从 `WorkflowStarted` 开始。

## 依据 History 建议切换

```go
info := workflow.GetInfo(ctx)
if info.GetContinueAsNewSuggested() {
    return workflow.ContinueAsNew(ctx, "processor", state)
}
```

| 方法 | 含义 |
| --- | --- |
| `GetCurrentHistoryLength()` | 当前 Workflow Task 开始时的 History Event 数 |
| `GetCurrentHistorySize()` | 当前 Workflow Task 开始时的 History 字节数 |
| `GetContinueAsNewSuggested()` | Server 是否建议切换 |
| `GetContinueAsNewReasons()` | 建议原因列表 |

原因取值：

| 常量 | 值 |
| --- | --- |
| `workflow.ContinueAsNewReasonHistoryEventCount` | `history_event_count` |
| `workflow.ContinueAsNewReasonHistorySizeBytes` | `history_size_bytes` |

建议按 Server 配置的 History 上限给出：Workflow Task 开始时 History 达到告警阈值（事件数或字节数）就带上对应原因，调低上限的部署在碰到硬上限之前收到建议。

## 切换前的收尾

- 在调用 `ContinueAsNew` 之前等待本 Run 启动的协程、Activity 和 Child Workflow 完成，把需要延续的结果写进下一代输入。仍在运行的 Child Workflow 在切换时按各自的[父关闭策略](/develop/child-workflows#父关闭策略)处理，默认被终止。
- 已接受、Handler 尚未完成的 Update 在切换时以 `AcceptedUpdateCompletedWorkflow` 失败结束，见 [Update](/develop/updates#行为矩阵)。
- 新 Run 的 History 从 `WorkflowStarted` 开始。持续接收 Signal 的 Workflow 在切换点前处理完已到达的消息，并把去重所需的状态放进输入。
- Update Handler 和 Query Handler 在新 Run 里重新注册。

## Run Segment

每次 Continue-As-New 产生的 Segment 都有独立 Run ID 和 Event History。Workflow ID 把这些 Segment 关联为线性 Run Chain。按 Workflow ID 发送的 Signal、Query 和 Update 解析到 current Run，即最新的 Segment。

Console 和 Trace 提供上一段、当前段、下一段导航。

## 搜索属性的继承

新 Run 继承上一段 Run 的当前搜索属性，包括 `GetVersion` 写入的 `TemporalChangeVersion`。

## 状态传递

下一段输入只包含继续执行业务所需的紧凑状态，例如 cursor、聚合值和待处理标识。大批历史明细保存在业务数据库，通过 Activity 按需读取。
