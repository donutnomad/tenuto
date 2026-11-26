---
title: Child Workflow
description: 启动子 Workflow、等待结果、并发组合与失败处理
status: Stable
---

# Child Workflow

<FeatureStatus status="Stable" />

Child Workflow 把独立生命周期、独立 History 或可复用的业务过程拆成新的 Workflow Execution。父 Run 启动子 Run 并等待它的终态。

## 启动并等待

```go
var child map[string]any
err := workflow.ExecuteChildWorkflow(ctx, "child-wf", input).Get(ctx, &child)
if err != nil {
    return err
}
```

签名：`func ExecuteChildWorkflow(ctx Context, workflowType string, input any, opts ...ChildWorkflowOptions) ChildWorkflowFuture`。

- `workflowType` 是子 Workflow 的 Workflow Type，即子 Workflow 的 `@Workflow(type=...)`，需要已在同一 Namespace 登记。
- `input` 是结构体或 `map[string]any`，作为子 Workflow 的输入。
- `opts` 可选：`ParentClosePolicy`（见下文「父关闭策略」）与 `WaitForCancellation`（见下文「取消子 Workflow」）。
- 调用时立即产生启动命令，返回的 Future 用 `Get` 等待；`Get` 的目标是 `*map[string]any`，内容是子 Run 的 Output。

Server 在父 Run 写入 `ChildWorkflowStarted`，创建子 Run 并调度它的 Workflow Task。子 Run 的 Workflow Type 是 `workflowType`，Workflow ID 是 `<父 Run ID>/<Child ID>`（例如 `01a1…/#c1`）。因此一个父 Run 可以启动多个同类型的子 Workflow，多个父 Run 可以同时启动同类型的子 Workflow，该类型自己的顶层 Run 也不受影响。子 Run 进入终态后，父 Run 收到对应事件并继续执行。

## 失败处理

| 子 Run 终态 | 父 Run 的 `Get` |
| --- | --- |
| `completed` | 返回 `nil`，`out` 写入子 Run 的 Output |
| `failed` | 返回结构化错误，保留子 Workflow 返回的 Type、details 和 cause |
| `cancelled` | 返回取消错误，`workflow.IsCanceledError(err)` 为 true |

子 Workflow 返回的应用错误可以在父 Workflow 里按类型分支：

```go
var out map[string]any
err := workflow.ExecuteChildWorkflow(ctx, "child-wf", input).Get(ctx, &out)
if err != nil {
    if workflow.IsApplicationError(err, "ChildDeclined") {
        return workflow.NewApplicationErrorWithCause("child declined, parent aborted", "ParentAborted", err)
    }
    return err
}
```

## 取消子 Workflow

发起子 Workflow 的 Context 被取消（父 Run 收到取消请求时的根 Context，或 `workflow.WithCancel` 的作用域）时，父 Workflow 产生 `RequestCancelChildWorkflow`：父 History 记 `ChildWorkflowCancelRequested`，子 Workflow 的当前 Run 收到取消请求（`WorkflowCancellationRequested`），由子 Workflow 的代码决定它的终态。父 Run 提交请求之后、向子 Run 发出之前进程退出时，子 Run 的恢复补发请求。

`ChildWorkflowOptions.WaitForCancellation` 决定父 Workflow 何时拿到结果，与 Temporal 相同：

| `WaitForCancellation` | 父 Run 的 `Get` |
| --- | --- |
| `false`（默认） | 取消请求发出即返回取消错误，子 Workflow 继续处理取消 |
| `true` | 等子 Workflow 结束：以 cancelled 结束时返回取消错误，完成或失败时返回它的结果 |

```go
var out map[string]any
err := workflow.ExecuteChildWorkflow(ctx, "refund-workflow", input, workflow.ChildWorkflowOptions{
    WaitForCancellation: true,
}).Get(ctx, &out)
```

不等子 Workflow 的父 Run 关闭之后，仍在运行的子 Workflow 按父关闭策略处理：`RequestCancel` 不对已经收到取消请求的子 Run 重复请求，`Terminate` 终止它。

## 父关闭策略

父 Run 关闭时，仍在运行的子 Workflow 按启动时选择的父关闭策略处理（与 Temporal 的 Parent Close Policy 相同）：

| 策略 | 常量 | 父 Run 关闭时 |
| --- | --- | --- |
| Terminate（默认） | `workflow.ParentClosePolicyTerminate` | 终止子 Workflow，终止原因为 `by parent close policy` |
| RequestCancel | `workflow.ParentClosePolicyRequestCancel` | 向子 Workflow 请求取消，子 Run 自己决定如何结束 |
| Abandon | `workflow.ParentClosePolicyAbandon` | 子 Workflow 继续运行 |

```go
audit := workflow.ExecuteChildWorkflow(ctx, "audit-workflow", input, workflow.ChildWorkflowOptions{
    ParentClosePolicy: workflow.ParentClosePolicyAbandon,
})
```

- 父 Run 以任何方式关闭都适用：完成、失败（含超时与失败后重试）、Workflow 代码返回取消错误的 cancelled、终止、continue-as-new、被 reset 取代。子 Workflow 属于启动它的那个 Run，不随 continue-as-new 或重试转给下一代 Run。
- 策略作用在子 Workflow 的当前 Run 上：子 Workflow 自己 continue-as-new、重试或被 reset 之后，处理的是接替的 Run。
- 已经结束的子 Workflow 不受影响。被终止的子 Workflow 按它自己启动的子 Workflow 的策略继续向下处理。
- Abandon 的子 Workflow 在父 Run 关闭之后结束时，父 Run 的 History 不再追加事件。
- 策略记录在父 Run 的 `ChildWorkflowStarted` 事件（`parentClosePolicy`）与子关联状态里。父 Run 关闭之后、处理子 Workflow 之前 Server 崩溃时，启动恢复或全量补偿（`WORKFLOW_RECONCILE_FULL_SCAN_INTERVAL`，默认 10m）从仍在运行的子 Run 出发补做。
- 父 Run 关闭时还没建好的子 Run 不再建立。

## 并发启动多个 Child

连续调用 `ExecuteChildWorkflow` 会在同一个 Workflow Task 里启动多个子 Run，之后依次 `Get`：

```go
payment := workflow.ExecuteChildWorkflow(ctx, "payment-workflow", paymentInput)
inventory := workflow.ExecuteChildWorkflow(ctx, "inventory-workflow", inventoryInput)

var paymentResult map[string]any
if err := payment.Get(ctx, &paymentResult); err != nil {
    return err
}
var inventoryResult map[string]any
if err := inventory.Get(ctx, &inventoryResult); err != nil {
    return err
}
```

等待最先结束的一个，用 Selector 的 `AddChildFuture`：

```go
sel := workflow.NewSelector(ctx)
sel.AddChildFuture(ctx, payment, func(f workflow.ChildWorkflowFuture) {
    _ = f.Get(ctx, &paymentResult)
})
sel.AddChildFuture(ctx, inventory, func(f workflow.ChildWorkflowFuture) {
    _ = f.Get(ctx, &inventoryResult)
})
sel.Select(ctx)
```

一次 `Select` 只处理一个 case，处理两个子 Run 需要调用两次。

## 确定性

Child 的 ID 由协程路径和该协程内的 Child 序号生成（主协程的第一个 Child 是 `#c1`）。新增、删除或重排 `ExecuteChildWorkflow` 调用属于需要 [版本保护](/develop/versioning) 的修改。

## 限制

- 子 Workflow 的输入输出通过 JSON object 传递。
- 子 Workflow 选项只有父关闭策略；子 Run 的 Workflow ID 与超时由引擎决定。
- 同一 Namespace 和 Workflow ID 同时只保留一个活跃 Run 的规则见 [编写 Workflow](/develop/workflows) 的「启动 Run」；子 Run 的 Workflow ID 由父 Run ID 和 Child ID 组成，与顶层 Run 和其他子 Run 互不冲突。
- 按 Workflow ID 查询、Signal 或取消子 Run 时使用 `<父 Run ID>/<Child ID>`，或者直接使用子 Run ID。

## Trace Step Into

在 Trace 调试台中，执行到 `ExecuteChildWorkflow` 调用行时可以 Step Into：

1. 当前栈顶切换到 Child Workflow。
2. 父 Workflow 栈保留在下一层。
3. 代码区域聚焦 Child 源码和当前行。
4. 变量区域显示 Child 当前栈帧变量。
5. Step Out 返回父 Workflow 调用点之后。

## 边界建议

适合拆为 Child Workflow 的场景：

- 独立业务生命周期，例如支付、审批、履约。
- 需要单独查看、取消或重试的子过程。
- 多个父流程共享的持久化编排。
- 希望隔离 Event History 增长的流程片段。
