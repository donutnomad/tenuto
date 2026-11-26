# Child Workflow 与 Continue-As-New

## Child Workflow

```go
// func ExecuteChildWorkflow(ctx Context, workflowType string, input any, opts ...ChildWorkflowOptions) ChildWorkflowFuture
var child map[string]any
err := workflow.ExecuteChildWorkflow(ctx, "child-wf", input).Get(ctx, &child)
if err != nil {
    return err
}
```

- `workflowType` 是子 Workflow 的 Workflow Type（它的 `@Workflow(type=...)`），需要已在同一 Namespace 登记：子 Workflow 的 `WorkflowDefinition()` 放进某个 Worker 的 `worker.Options.Workflows`。
- `input` 是 struct 或 `map[string]any`，编码成 JSON object。
- 调用时立即产生启动命令；`Get` 的目标是 `*map[string]any`，内容是子 Run 的 Output。
- 子 Run 的 Workflow ID 是 `<父 Run ID>/<Child ID>`，例如 `01a1…/#c1`（`server/biz/domain/execution/child_start.go`）。同一个父 Run 可以启动多个同类型的子 Workflow，它们与该类型的顶层 Run 互不冲突。
- 按 Workflow ID 对子 Run 做 Signal、Query、取消时用 `<父 Run ID>/<Child ID>`，或直接用子 Run ID。

### 结果与失败

| 子 Run 终态 | 父 Run 的 `Get` |
| --- | --- |
| `completed` | 返回 `nil`，`out` 写入子 Run 的 Output |
| `failed` | 返回结构化错误，保留子 Workflow 返回的 Type、details、cause |
| `cancelled` | 返回取消错误，`workflow.IsCanceledError(err)` 为 true |

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

`out` 是 `map[string]any`，元素类型是 `any`：在 Workflow 里与字面量直接比较恒为 `false`。需要按子 Run 的输出字段分支时，把 map 交给一个 Activity 转成 struct。

### 父关闭策略

父 Run 关闭时仍在运行的子 Workflow 按启动时的 `workflow.ChildWorkflowOptions{ParentClosePolicy: ...}` 处理（Temporal Parent Close Policy 语义）：

| 常量 | 父 Run 关闭时 |
| --- | --- |
| `workflow.ParentClosePolicyTerminate`（默认，不传 options 即此值） | 终止子 Workflow，原因 `by parent close policy` |
| `workflow.ParentClosePolicyRequestCancel` | 向子 Workflow 请求取消 |
| `workflow.ParentClosePolicyAbandon` | 子 Workflow 继续运行，结束时不再写父 History |

```go
audit := workflow.ExecuteChildWorkflow(ctx, "audit-workflow", input, workflow.ChildWorkflowOptions{
    ParentClosePolicy: workflow.ParentClosePolicyAbandon,
})
```

- 任何关闭方式都适用：完成、失败（含超时、失败后重试）、Workflow 返回取消错误的 cancelled、终止、continue-as-new、被 reset 取代。子 Workflow 不随 continue-as-new 转给下一代 Run：要在下一代继续等待的子 Workflow，在切换前 `Get` 它，或用 Abandon 并在下一代按 Workflow ID 交互。
- 作用在子 Workflow 的当前 Run（子 Workflow 自己 continue-as-new / 重试 / reset 后的接替 Run）；已结束的子 Workflow 不受影响；被终止的子 Workflow 按它自己子关联的策略继续向下处理。
- 策略写在父 Run 的 `ChildWorkflowStarted.parentClosePolicy` 与子关联状态；崩溃后由仍在运行的子 Run 在启动恢复或全量补偿（`WORKFLOW_RECONCILE_FULL_SCAN_INTERVAL`，默认 10m）中补做。

### 并发

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

连续调用在同一个 Workflow Task 里启动多个子 Run。等待最先结束的一个用 `Selector.AddChildFuture`（见 `concurrency.md`）。

### 能力范围

- `ChildWorkflowOptions` 只有 `ParentClosePolicy`：子 Run 的 Workflow ID 与超时由引擎决定。
- 父 Run 关闭（含主函数提前返回）后子 Run 按父关闭策略处理；默认被终止，需要继续运行时用 Abandon。
- Child ID 由协程路径与该协程内的 Child 序号生成（主协程第一个是 `#c1`）；新增、删除、重排 `ExecuteChildWorkflow` 调用需要版本保护。

### 何时拆成 Child

- 独立的业务生命周期（支付、审批、履约），需要单独查看、取消、重试。
- 多个父流程共用的持久化编排。
- 隔离 Event History 的增长。

只是复用一段步骤、不需要独立 Run 时，把步骤写成同包普通函数由 Workflow 调用（函数体随 Workflow 一起翻译），或做成 Activity。

## Continue-As-New

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
- 调用后当前协程立即结束，后面的语句不再执行。当前 Run 状态变为 `continued_as_new`，新 Run 从 `WorkflowStarted` 开始，有新的 Run ID 与独立的 History；Workflow ID 不变。
- 按 Workflow ID 发送的 Signal、Query、Update 解析到 current Run，即最新一段。上一段的终态事件、关闭状态与下一段 Run 同事务提交，按 Workflow ID 的请求不会落到已关闭的上一段；发给已结束 Run 的 Signal、Update、取消返回 `ErrInvalidArgument`。

### 按 History 建议切换

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

原因常量：`workflow.ContinueAsNewReasonHistoryEventCount`（`history_event_count`）、`workflow.ContinueAsNewReasonHistorySizeBytes`（`history_size_bytes`）。建议按 Server 配置的 History 告警阈值给出。

History 缺省限制：

| 预算 | Warning（开始建议切换） | Hard（Run 被关闭） |
| --- | ---: | ---: |
| Events | 10,240 | 51,200 |
| Canonical JSON | 10 MiB | 50 MiB |

超过 Hard 上限的 Run 以 Failure Type `HistoryLimitExceeded` 关闭。永续循环、定时聚合、高频 Signal 的 Workflow 在循环里检查 `GetContinueAsNewSuggested()`，或用固定的迭代上限。

### 切换前的收尾

- 先等待本 Run 启动的协程、Activity 与子 Workflow 完成（`wg.Wait`、`Get`），把需要延续的结果写进下一代输入。仍在运行的子 Workflow 在切换时按父关闭策略处理（默认终止）。
- 已接受、Handler 尚未完成的 Update 在切换时以 `AcceptedUpdateCompletedWorkflow` 失败结束。
- 持续接收 Signal 的 Workflow 在切换点前处理完已到达的消息，把去重所需的状态放进输入。
- Update Handler 与 Query Handler 在新 Run 的函数开头重新注册（同一个函数，自然满足）。
- 下一代输入只带继续执行所需的紧凑状态（cursor、聚合值、待处理标识）；明细放在业务数据库，由 Activity 按需读取。

### 继承

新 Run 继承上一段的当前搜索属性，包括 `GetVersion` 写入的 `TemporalChangeVersion`。新 Run 的 History 从头开始，旧 History 里的 `GetVersion` Marker 不带过来：新一段首次经过各个 change ID 时记录 `maxSupported`。
