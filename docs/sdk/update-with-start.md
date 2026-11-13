---
title: Update-With-Start
description: 原子启动或复用 Workflow 并等待强类型 Update 结果
status: Stable
---

# Update-With-Start

<FeatureStatus status="Stable" />

Update-With-Start 在一次原子操作中完成“启动或复用 Workflow”与“发送 Update”。它适合 HTTP POST 需要同步返回初始化结果，同时 Workflow 在后台继续运行的业务。

## Workflow Handler

```go
const CreateUpdateName = "create-record"

type CreateResult struct {
    RecordID string `json:"record_id"`
    Status   string `json:"status"`
}

func ApprovalWorkflow(ctx workflow.Context, input CreateInput) error {
    var result CreateResult
    createDone := false

    if err := workflow.SetUpdateHandler(ctx, CreateUpdateName,
        func(ctx workflow.Context) (CreateResult, error) {
            _ = workflow.Await(ctx, func() bool { return createDone })
            return result, nil
        },
    ); err != nil {
        return err
    }

    if err := workflow.ExecuteActivity(
        ctx,
        activities.CreateRecord,
        input,
    ).Get(ctx, &result); err != nil {
        return err
    }

    createDone = true
    return nil
}
```

## 发起 Update-With-Start

```go
c, err := client.Dial(client.Options{HostPort: "tenuto-server:7400", Namespace: "approval"})
if err != nil {
    return CreateResult{}, err
}
defer c.Close()

start := c.NewWithStartWorkflowOperation(
    client.StartWorkflowOptions{
        ID:                       "create-" + input.RecordID,
        WorkflowIDConflictPolicy: client.WorkflowIDConflictPolicyUseExisting,
    },
    "approval-workflow", // @Workflow(id=...) 声明的 Workflow Type
    input,
)

handle, err := c.UpdateWithStartWorkflow(ctx, client.UpdateWithStartWorkflowOptions{
    StartWorkflowOperation: start,
    UpdateOptions: client.UpdateWorkflowOptions{
        UpdateName:   CreateUpdateName,
        WaitForStage: client.WorkflowUpdateStageCompleted,
    },
})
if err != nil {
    return CreateResult{}, err
}

var result CreateResult
if err := handle.Get(ctx, &result); err != nil {
    return CreateResult{}, err
}
return result, nil
```

`StartWorkflowOptions.ID` 必填，`WorkflowIDConflictPolicy` 必须是 `WorkflowIDConflictPolicyUseExisting`。启动规格只能执行一次；`start.Get(ctx)` 返回接受 Update 的 Run（新启动的或复用的）。`UpdateOptions` 的 `WorkflowID` 与 `RunID` 留空，目标来自启动规格。

## WaitForStage

| 值 | 初次调用返回条件 | Handle.Get 行为 |
| --- | --- | --- |
| `WorkflowUpdateStageAccepted` | Update 已持久化并获得 Update ID | 继续等待 Completed |
| `WorkflowUpdateStageCompleted` | Handler 已返回结果或错误 | 读取已完成结果 |

同一个 Handle 可以并发、多次调用 `Get`。每次调用使用自己的 Context 取消边界。

## 失败与超时

`WaitForStage=Completed` 是一次有 Context 边界的等待。客户端 Context 到期时，HTTP/RPC 调用返回 Context 错误，Update 本身仍保存在 Server 中，后续可以使用同一个 Update ID 重新查询。

以下情况会让 Completed 等待无法得到成功结果：

| 场景 | 结果 |
| --- | --- |
| Update Name 没有对应 Handler（主协程第一次阻塞或返回时仍未注册） | Accepted 后写入 Completed failure，Client 返回 `*apperror.Error`，`Type()` 为 `UnknownUpdate`，message 为 `no update handler registered for "<name>"` |
| Handler 内 Await 条件长期不满足 | Update 保持 Accepted，直到外部事件推进或 Context 到期 |
| Handler 返回业务错误 | Completed failure，Client 返回 `*apperror.Error` |
| Run 被取消或终止 | Update 等待以终态错误结束 |
| 该 Workflow ID 的 current Run 已经结束 | 启动新 Run 并把 Update 交给它，新 Run 成为 current |
| 用已经记录过的 Update ID 重试，而当时的 Run 已经结束 | 返回当时记录的 Update 结果，不启动新 Run |

客户端应设置合理的 Context timeout，并保存 `WorkflowID`、`RunID`、`UpdateID`。HTTP 层可以把“等待超时”映射为 202/504，同时通过后台查询确认 Update 最终状态。

## Start 与 Update 的边界

`UpdateWithStartWorkflow` 只负责启动或复用 Run 并提交 Update。存在进行中的 current Run 时复用它；没有 Run 或 current Run 已经结束时启动新 Run，启动与 Update 的接受在同一个事务里提交。新 Run 使用该 Workflow Type 当前 active 的 Program，与 `StartWorkflow`、Signal-With-Start 一致。Handler 仍然必须由 Workflow 代码注册；Workflow 结束后无法补执行一个尚未完成的 Handler。需要延迟处理时，让 Workflow 保持运行并使用 `Await`、Timer 或 Signal 等待条件。

## 版本发布

Update Name、参数顺序、参数类型和结果类型属于运行时接口。兼容发布保留旧 Handler，并在新 Handler 中增加版本分支；旧 Run 完成后再清理旧契约。

## 强类型错误

Workflow 返回：

```go
return workflow.NewApplicationError(
    "approval rejected",
    "ApprovalRejected",
    DecisionDetail{Rule: "risk-policy"},
)
```

Client 解码：

```go
var updateErr *apperror.Error
if errors.As(err, &updateErr) {
    var detail DecisionDetail
    _ = updateErr.Details(&detail)
    log.Printf("type=%s rule=%s", updateErr.Type(), detail.Rule)
}
```
