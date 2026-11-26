# Signal、Query、Update

| 需求 | 使用 | History |
| --- | --- | --- |
| 通知 Workflow 一个外部事实，调用方不等结果 | Signal | 写入 `WorkflowSignaled` |
| 读取 Workflow 当前状态，不改变它 | Query | 不写入 |
| 修改状态并同步取得结果或业务错误 | Update | `WorkflowUpdateAccepted` + `WorkflowUpdateCompleted` |
| Run 不存在时先启动再投递 | Signal-With-Start、Update-With-Start | 同上 |

注册顺序：在 Workflow 函数开头、第一次阻塞之前注册全部 Query 与 Update Handler，然后才开始调度 Activity 或等待。调用端 API 的完整签名见 `client-api.md`。

## Signal

### 接收

```go
// func WaitSignal(ctx Context, name string) SignalFuture
var decision map[string]any
if err := workflow.WaitSignal(ctx, "approval-result").Get(ctx, &decision); err != nil {
    return err
}
```

- `Get` 的目标是 `*map[string]any`，内容是调用端发送的 JSON object。
- Signal 可以先于等待点到达：它保存在 History 里，Workflow 走到 `WaitSignal` 时立即取得。
- 同名 Signal 是队列（同 Temporal Signal Channel）：`SignalFuture` 第一次被消费（`Get` 返回或 Selector 回调执行）时取走下一条未消费的同名 Signal，之后 `Get` 返回同一条；`WaitSignal` 调用本身与没被选中的 Future 都不占 Signal。多个同名 Future 按消费先后分配。Selector 只在 Future 此刻能取走一条 Signal 时选中它：两个同名 Future 只到一条 Signal 时只选中一个，另一个的 `Select` 等下一条。Selector 循环里每轮内联 `WaitSignal` 不会漏 Signal。Query handler 里让还没取到 Signal 的 `SignalFuture` 取 Signal（`Get` 或 `Select`）被拒：`query handler must not consume a Signal`。
- `map[string]any` 的元素是 `any`，在 Workflow 里直接与字面量比较恒为 `false`；需要按 Signal 内容分支时把 map 交给 Activity 处理并返回 struct。

循环接收（长期接收的 Workflow 用 Continue-As-New 控制 History 长度）：

```go
for i := 0; i < in.ExpectedApprovals; i++ {
    var approval map[string]any
    if err := workflow.WaitSignal(ctx, "approval").Get(ctx, &approval); err != nil {
        return Output{}, err
    }
}
```

等待最先到达的一个：

```go
approve := workflow.WaitSignal(ctx, "approve")
reject := workflow.WaitSignal(ctx, "reject")

outcome := ""
sel := workflow.NewSelector(ctx)
sel.AddSignalFuture(ctx, approve, func(f workflow.SignalFuture) {
    var out map[string]any
    _ = f.Get(ctx, &out)
    outcome = "approved"
})
sel.AddSignalFuture(ctx, reject, func(f workflow.SignalFuture) {
    var out map[string]any
    _ = f.Get(ctx, &out)
    outcome = "rejected"
})
sel.Select(ctx)
```

在协程里接收，主协程用 `Await` 等它修改的状态：

```go
stopped := false
workflow.Go(ctx, func(ctx workflow.Context) {
    var stop map[string]any
    _ = workflow.WaitSignal(ctx, "stop").Get(ctx, &stop)
    stopped = true
})
```

### 发送

```go
err := c.SignalWorkflow(ctx, workflowID, runID, "approval-result",
    map[string]any{"message_id": messageID, "approved": true})

run, err := c.SignalWithStartWorkflow(ctx, workflowID, "approval-requested",
    map[string]any{"message_id": requestID},    // Signal 参数
    client.StartWorkflowOptions{},
    "approval-flow",                            // Workflow Type
    map[string]any{"order_id": orderID})        // 启动输入
```

`c` 是 `sdk/client` 的 Client（`client.Dial`），Namespace 取自它。

- `runID` 为空时按 Namespace + Workflow ID 解析 current Run。Signal 参数是 JSON object，可以为 nil。
- 交付是业务层 at-least-once：调用超时可能发生在事件已提交之后，重试形成第二条 Signal。payload 带稳定的 `message_id`，由处理它的 Activity 去重。
- 目标 Run 已终态时返回 `client.ErrInvalidArgument`；Workflow 不再等待该名称时 Signal 留在 History 无人消费。
- `SignalWithStartWorkflow`：该 Workflow ID 存在运行中的 Run 时向它写 Signal；没有 Run 或 current Run 已结束时按 Workflow Type 启动新 Run，启动与 Signal 在一个事务里提交。`workflowID` 留空时 SDK 生成，等同于每次都启动新 Run。SDK 以同一个 request ID 重发时，current Run（含已结束）或它的链首（continue-as-new、失败重试）记过这条 Signal 就返回第一次的 Run 与事件 ID，不启动第二个 Run（同 Temporal）。
- 重试 Run 在 Workflow 重试的退避期内收到 Signal：写入 History，不提前开始，退避到期一并处理；SignalWithStart、取消请求与 Update 让它立即开始（同 Temporal 默认行为）。

## Query

### 注册

```go
// func SetQueryHandler(ctx Context, name string, handler func(ctx Context, input map[string]any) map[string]any) error
err := workflow.SetQueryHandler(ctx, "progress",
    func(ctx workflow.Context, _ map[string]any) map[string]any {
        return map[string]any{
            "stage":     stage,
            "processed": processed,
            "total":     total,
        }
    },
)
if err != nil {
    return err
}
```

- Handler 签名固定，写成内联闭包；名称非空；同名重复注册时后一次覆盖前一次。
- Handler 只读 Workflow 内存变量与闭包捕获；它产生的命令被丢弃。
- 业务拒绝：返回 `map[string]any{"__query_error": "progress is private"}`，调用端得到 `*client.QueryFailedError`（`Message` 是拒绝原因）。
- 内置 Query `__stack_trace` 不需要注册，由 Server 直接回答，返回 Run 的逻辑进度：`steps`（每个 Activity 的 `activity_id`、`activity_type`、状态）、`waiting`（当前等待点的类型、`activity_id`、`child_id`、`activity_type`、`timer_id`）和 `summary`；粒度为 Activity，不区分协程。

### 调用

```go
value, err := c.QueryWorkflow(ctx, workflowID, runID, "progress")
var rejected *client.QueryFailedError
if errors.As(err, &rejected) {
    return fmt.Errorf("query rejected: %s", rejected.Message)
}
if err != nil {
    return err
}
var progress map[string]any
err = value.Get(&progress)
```

| 场景 | 结果 |
| --- | --- |
| 名称未注册 | `*client.QueryFailedError`，`Message` 含 `unknown queryType` |
| 首个 Workflow Task 尚未完成 | `*client.QueryFailedError`，`workflow has not completed its first workflow task yet; query handlers are not registered`；稍后重试 |
| 重放遇到确定性失配 | `*client.QueryFailedError`，`Message` 含重放失败原因 |
| Run 已终态 | 重放终态 History 后执行 Handler |

Query 读到的是最后一个已完成 Workflow Task 之后的状态；其后到达的 Signal、Activity 结果在下一个 Workflow Task 完成后才体现。

## Update

### Handler

```go
type ChangeLimitResult struct {
    Previous int64 `json:"previous"`
    Current  int64 `json:"current"`
}

// func SetUpdateHandler(ctx Context, updateName string, handler any) error
err := workflow.SetUpdateHandler(ctx, "change-limit",
    func(ctx workflow.Context, next int64) (ChangeLimitResult, error) {
        if next <= 0 {
            return ChangeLimitResult{}, workflow.NewApplicationError(
                "limit must be positive", "InvalidLimit", next,
            )
        }
        previous := limit
        limit = next
        return ChangeLimitResult{Previous: previous, Current: limit}, nil
    },
)
if err != nil {
    return err
}
```

- 签名：`func(workflow.Context, ...Args) error` 或 `func(workflow.Context, ...Args) (Result, error)`。内联函数，业务参数个数固定（不支持 `...T`）。
- 参数与 `Result` 用 struct、标量、切片或 map，嵌套深度不限。
- Handler 属于 Workflow 代码，遵守全部确定性规则；可以用 `Await`、桥、Activity Future、Timer、Channel、Selector、`workflow.Go`。
- Handler 在 `update:<UpdateID>` 协程里执行，多个 Update 按 Accepted 事件顺序执行。
- `SetUpdateHandler` 是处理 Update 的唯一方式。

等待主流程推进后再返回（Update-With-Start 的典型用法）：

```go
// @Workflow(type="approval-workflow")
func ApprovalWorkflow(ctx workflow.Context, input CreateInput) error {
    var result CreateResult
    createDone := false

    if err := workflow.SetUpdateHandler(ctx, "create-record",
        func(ctx workflow.Context) (CreateResult, error) {
            _ = workflow.Await(ctx, func() bool { return createDone })
            return result, nil
        },
    ); err != nil {
        return err
    }

    created, err := CallCreateRecord(ctx, CreateRecordInput{RecordID: input.RecordID}, workflow.ActivityOptions{StartToCloseTimeout: 30 * time.Second})
    if err != nil {
        return err
    }
    result = CreateResult{RecordID: created.RecordID, Status: created.Status}
    createDone = true

    // 主函数返回会放弃执行中的 Handler：后续还有 Update 要处理时在这里保持运行
    var done map[string]any
    return workflow.WaitSignal(ctx, "close").Get(ctx, &done)
}
```

### 生命周期

```text
请求 → Validator（可选，Server 侧）→ WorkflowUpdateAccepted → Handler 执行
                                                   ├─ 返回结果 → WorkflowUpdateCompleted (success)
                                                   └─ 返回 error → WorkflowUpdateCompleted (failure)
```

| 场景 | History | 调用结果 |
| --- | --- | --- |
| Handler 成功 | Accepted + Completed success | `handle.Get` 解码强类型结果 |
| Handler 返回错误 | Accepted + Completed failure | `*apperror.Error` |
| 没有同名 Handler（主协程第一次阻塞或返回时仍未注册） | Accepted + Completed failure | `*apperror.Error`，`Type() == "UnknownUpdate"`，message `no update handler registered for "<name>"` |
| 参数无法解码 | Accepted + Completed failure | Type `DataConversionError` |
| Handler 的 `Await` 尚未满足 | Accepted | Handle 保持可等待 |
| 同一 Update ID 重试或并发发送 | 复用已有记录 | 相同阶段或结果 |
| 调用端 Context 先截止 | 服务端继续推进 | 用相同 Update ID 再次调用取结果 |
| Accepted 后、Handler 完成前 Run 关闭（主函数返回、失败、超时、取消、终止、continue-as-new） | Accepted + Run 终态事件，不写 Completed | `*apperror.Error`，`Type() == "AcceptedUpdateCompletedWorkflow"`，`NonRetryable()`；等待中的调用在关闭时立即返回 |
| Run 已终态后发送 | 无新 Accepted | Update ID 已记录过时返回当时的阶段或结果；新的 Update ID 返回错误 |
| Validator 拒绝 | 无事件 | `errors.Is(err, client.ErrUpdateRejected)` |

Validator 是 Server 进程内按 Update Name 注册的无状态输入校验器（`(*engine.Server).RegisterUpdateValidator`），`sdk/workflow` 没有对应 API。依赖 Workflow 状态的校验写在 Handler 里，返回 `workflow.NewApplicationError` 表达拒绝。

### 调用

```go
handle, err := c.UpdateWorkflow(ctx, client.UpdateWorkflowOptions{
    WorkflowID:   workflowID,
    UpdateID:     requestID,                      // 稳定的请求 ID：重试幂等
    UpdateName:   "change-limit",
    Args:         []any{int64(5000)},             // 按 Handler 业务参数顺序
    WaitForStage: client.WorkflowUpdateStageCompleted,
})
if err != nil {
    return err
}
var result ChangeLimitResult
if err := handle.Get(ctx, &result); err != nil {
    return err
}
```

Update-With-Start 见 `client-api.md`。

### 跨副本等待

- 等 `Completed` 的请求在 Server 上最多等 20s，到期应答 `Accepted`，`handle.Get` 以同一 Update ID 重发（幂等，不重复执行 Handler）。
- 受理请求的副本与完成 Update 的副本可以不同。等待方先读 Run 的 `next_event_id` 再读 History，未完成时等 Run 变化：本副本的提交立即唤醒；有等待者期间副本每 100ms 用一条批量查询读取被等待 Run 的 `next_event_id`，变大就唤醒重读。其他副本完成的 Update 最多晚 100ms 返回。`run.Get` 的 `wait-closed` 用同一机制。
- 代价：没有等待者时不读库；有等待者时每副本每秒 10 条批量读取（每 500 个 Run 一条），与等待者数量无关；Run 每变化一次，它的每个 Update 等待者重读一次 execution state 与 History。
- 等待者只在受理请求的副本内存里：副本重启时等待中的 `handle.Get` / `run.Get` 返回连接错误，对同一个句柄再调用一次。

## 持久化契约

Signal 名、Query 名、Update 名、Update 参数顺序与类型、结果类型、Application Error Type 都是调用端与 Workflow 之间的契约。发布新版本时保留仍有 Run 在用的 Handler 与名称，这些 Run 完成后再移除。

## Update 排查

1. 在 Run History 搜索 Update ID，看最高阶段。
2. 没有 Accepted：Validator 拒绝、目标 Run 不对或请求校验失败。
3. 只有 Accepted：Handler 已开始执行，停在 `Await`、Activity 或 Timer；用 Query `__stack_trace` 看 Run 当前等待的 Activity 或 Timer（它不区分 Handler 与主协程），或 Run 已终态。
4. Accepted + Completed failure 且 Type 为 `UnknownUpdate`：名称不一致，或注册晚于主函数第一次阻塞。
