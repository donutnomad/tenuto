---
title: Signal-With-Start
description: 原子解析 current Run 并投递启动 Signal
status: Preview
---

# Signal-With-Start

<FeatureStatus status="Preview" />

Signal-With-Start 在同一个 Workflow ID 临界区内解析 current Run：存在进行中的 Run 时复用它；没有 Run 或上一个 Run 已结束时按 Workflow Type 启动新 Run，再写入 Signal。

```go
run, err := c.SignalWithStartWorkflow(
    ctx,
    "approval-"+orderID,  // Workflow ID：留空时 SDK 生成，等同于每次都启动新 Run
    "approval-requested", // Signal Name
    map[string]any{"message_id": requestID, "requested_by": userID},
    client.StartWorkflowOptions{},
    "approval-flow",      // Workflow Type：没有进行中的 Run 时启动哪个定义
    ApprovalInput{OrderID: orderID},
)
// run.GetRunID() 是接收 Signal 的 Run：新启动的或复用的
```

`c` 是 [`sdk/client`](/sdk/client) 的 `Client`，Namespace 取自它。`StartWorkflowOptions.ID` 留空或与 `workflowID` 相同；`WorkflowIDConflictPolicy` 留空。

## 原子范围

```text
per Namespace + Workflow ID lock
  → resolve current Run
  → 没有进行中的 Run：一个事务写入 Run、current 指针、WorkflowStarted、WorkflowSignaled
  → 有进行中的 Run：append WorkflowSignaled
```

该范围防止并发请求创建多个 current Run。启动和 Signal 一起提交：Signal 写入失败时 Run 也不会留下。新 Run 使用该 Workflow Type 当前 active 的 Program，与 `StartWorkflow`、Update-With-Start 一致。

## 重试窗口

请求已提交而客户端在收到响应前失联时，重试会复用已启动的 current Run 并追加第二条 Signal。Payload 应包含业务 Message ID，Workflow 依据该 ID 去重。

## 行为矩阵

| 场景 | 结果 |
| --- | --- |
| current Run 缺失 | 创建 Run 并写 Signal |
| current Run 正在运行 | 复用 Run 并写 Signal |
| 多个请求并发启动同一 ID | keyed lock 串行，只产生一个 current Run |
| current Run 已结束 | 启动新 Run 并写 Signal，新 Run 成为 current |
| current Run 正在运行且类型与请求的 Workflow Type 不同 | 返回类型不一致错误 |
| Workflow ID 为空 | SDK 生成 Workflow ID（HTTP 请求留空时由服务端生成），启动新 Run 并写 Signal |
| Signal Name 或 Workflow Type 为空 | 请求校验失败 |
| Signal 写入失败 | 启动一并回滚，调用方使用相同业务 Message ID 重试 |

需要同步结果时使用 [Update-With-Start](/sdk/update-with-start)。
