# 应用错误

Activity 失败、Workflow 终态失败、Update 失败、Child Workflow 失败共用一种结构：`protocol.Failure{Message, Type, NonRetryable, Details, Cause}`。Go 侧的表示是 `*apperror.Error`（包 `github.com/donutnomad/tenuto/sdk/apperror`）。`Type` 是稳定的机器契约，驱动分支与重试；`Message` 用于诊断；`Details` 是有序的结构化载荷。

## 构造

| 位置 | API |
| --- | --- |
| Activity、普通 Go | `apperror.New(message, errorType, details...)`、`apperror.NewWithCause(message, errorType, cause, details...)`、`apperror.NewNonRetryable(message, errorType, cause, details...)`、`apperror.NewWithOptions(message, errorType, apperror.Options{NonRetryable, Cause, Details})` |
| Workflow 函数体 | `workflow.NewApplicationError(message, errorType, details...)`、`workflow.NewApplicationErrorWithCause(message, errorType, cause, details...)`、`workflow.NewNonRetryableApplicationError(message, errorType, cause, details...)` |

```go
// Activity：永久失败 + 结构化上下文
return ChargeOutput{}, apperror.NewNonRetryable(
    "card declined", "PaymentDeclined", nil,
    map[string]any{"reason": "insufficient_funds", "order_id": in.OrderID})

// Activity：可重试的临时失败
return ChargeOutput{}, apperror.New("payment provider unavailable", "PaymentUnavailable")
```

- 没有 detail 时省略 `details` 参数；传 `nil` 会写入一个 null detail。
- Activity 返回普通 `error` 也合法，失败只带 message，`Type` 为空。
- Workflow 函数体里用 `workflow.*` 构造应用错误；普通错误用 `fmt.Errorf`（`%w` 包装，文本与 Go 相同）。`errors` 包在生成期报错。

## 在 Workflow 里按类型分支

来源：`docs/examples/saga-workflow/workflow.go`。

```go
type PaymentFailureDetail struct {
    OrderID string `json:"order_id"`
    Reason  string `json:"reason"`
}

paid, err := CallChargePayment(ctx, in, workflow.ActivityOptions{StartToCloseTimeout: 30 * time.Second})
if err != nil {
    if workflow.IsApplicationError(err, "PaymentDeclined") {
        var detail PaymentFailureDetail
        detailErr := workflow.ApplicationErrorDetail(err, 0, &detail)
        reason := "unknown"
        if detailErr == nil && detail.Reason != "" {
            reason = detail.Reason
        }
        return OrderOutput{}, workflow.NewApplicationError(
            "payment declined by provider, reason="+reason, "PaymentDeclined")
    } else {
        // 其余错误：结果不确定，保留原错误作为 cause
        return OrderOutput{}, workflow.NewApplicationErrorWithCause(
            "charge payment outcome unknown", "SagaManualReconciliation", err)
    }
}
```

| API | 行为 |
| --- | --- |
| `workflow.IsApplicationError(err, errorType) bool` | Type 完全相等（大小写敏感）；普通 error 恒为 `false` |
| `workflow.ApplicationErrorDetailCount(err) int` | details 个数；非应用错误为 0 |
| `workflow.ApplicationErrorDetail(err, index, out) error` | 把第 `index` 个 detail 解码到 `out`（非 nil 指针）；失败时 `out` 保持原值 |

`IsApplicationError` 的源码形态：

```go
if workflow.IsApplicationError(err, "X") { ... } else { ... }   // 支持
if !workflow.IsApplicationError(err, "X") { ... }               // 支持
declined := workflow.IsApplicationError(err, "X")               // 支持：先赋值
if declined && retries > 0 { ... }
```

`else if workflow.IsApplicationError(...)`、`if init; workflow.IsApplicationError(...)`、与其他条件用 `&&` / `||` 直接组合、放在 `for` / `switch` 条件位置，生成期报错。先赋值给变量再用变量。

`ApplicationErrorDetail` 的目标：

- 在 Workflow 文件里声明 struct DTO 接收结构化 detail。
- 标量 detail 用 `*string` 等具体指针；扁平 map 用 `*map[string]string`。
- `*map[string]any` 的元素是 `any`，`detail["k"] == "literal"` 恒为 `false`。

Child Workflow 失败时父 Workflow 的 `Get` 返回同样的结构化错误，`IsApplicationError` 与 `ApplicationErrorDetail` 同样可用。

## Workflow 失败

Workflow 函数返回非 nil `error` 时 Run 进入 `failed`，错误编码成终态 Failure。Workflow 代码 panic（越界、nil 解引用、非 comma-ok 类型断言 `v.(T)` 失败、`panic(...)`，`_ = items[i]`、`_ = p.Field`、超出执行预算的 `[E_BUDGET]` 同样算；载荷解码等其余 `[E_TYPE]` 不算）不让 Run 失败：与 Temporal 缺省的 BlockWorkflow 相同，Workflow Task 以 `workflow_panic` 失败，Run 保持 `running` 并显示 Blocked，按退避重试，修复代码发布后从失败处继续。调用端经 `WorkflowRun.Get` 读取失败：

```go
err := c.GetWorkflow(ctx, workflowID, runID).Get(ctx, &out) // c 是 sdk/client 的 Client
var execErr *client.WorkflowExecutionError                  // WorkflowID、RunID（链上最后一个 Run）、WorkflowType
var appErr *apperror.Error
switch {
case err == nil:
case workflow.IsCanceledError(err): // Workflow 返回了取消错误（Run 为 cancelled）
case errors.As(err, &appErr):
    typeName := appErr.Type() // "" = 普通 error
    var detail PaymentFailureDetail
    _ = appErr.Details(&detail)
    nonRetryable := appErr.NonRetryable()
    cause := appErr.Unwrap()
    timeout := appErr.TimeoutType() // client.TimeoutTypeWorkflowExecution / TimeoutTypeWorkflowRun：执行或 Run 超时
}
```

Run 终止时原因是 `*client.TerminatedError{Reason}`。`Get` 跟随 continue-as-new 与 Workflow 重试，只在链上最后一个 Run 结束时返回。HTTP `POST /runtime/workflow/get-run` 的响应里同一份失败在 `terminal_failure`。

## 调用端读取 Update 失败

```go
handle, err := c.UpdateWorkflow(ctx, opts)
if err != nil {
    // 请求本身失败：校验、传输、Validator 拒绝（errors.Is(err, client.ErrUpdateRejected)）
    return err
}
if err := handle.Get(ctx, &result); err != nil {
    var updateErr *apperror.Error
    if errors.As(err, &updateErr) {
        switch updateErr.Type() {
        case "UnknownUpdate":        // 没有同名 Handler
        case "InvalidLimit":         // Handler 返回的业务错误
        }
        var detail LimitDetail
        _ = updateErr.Details(&detail)
    }
    return err
}
```

`*apperror.Error` 的方法：`Error()` / `Message()`、`Type()`、`NonRetryable()`、`HasDetails()`、`Details(targets ...any) error`、`Unwrap()`。

## 引擎产生的 Failure Type

| Type | 来源 |
| --- | --- |
| `PanicError` | Activity 函数 panic |
| `PayloadSizeLimitExceeded` | Activity 结果或心跳详情超过载荷上限（默认 2 MiB，`WORKFLOW_PAYLOAD_SIZE_LIMIT_BYTES`）；不可重试 |
| `ActivityResultRejected` | Server 以不可重试的 4xx 拒绝 Activity 结果，Worker 改报的失败；不可重试 |
| `UnknownUpdate` | Update 名没有注册 Handler |
| `DataConversionError` | Update 参数无法解码成 Handler 的参数类型 |
| `HandlerExecutionError` | Update Handler 执行出错（非业务返回的错误） |
| `HistoryLimitExceeded` | Run 因 Event History 超过硬上限被关闭：任何写入（命令、Signal、Update、Activity / Timer 结果、`WorkflowTaskScheduled`）会超过上限时，Server 用终态预留写 `WorkflowFailed`，message 写明指标、预计值与上限（Temporal 写 `WorkflowExecutionTerminated`） |
| `AcceptedUpdateCompletedWorkflow` | Update 已接受、Handler 尚未完成时 Run 关闭（不可重试） |

## 设计规则

- 调用端与 Workflow 按 `Type` 分支，不解析 `Message`。
- 业务明确拒绝（下游确认没有副作用）与结果不确定（超时、响应丢失）分开处理：前者可以自动补偿，后者保留 cause 交给人工核对。示例见 `docs/examples/saga-workflow/`。
- 永久失败在 Activity 侧用 `apperror.NewNonRetryable` 标记，或在调用侧列入 `NonRetryableErrorTypes`。
- Update Name、Application Error Type、details 结构属于持久化契约，改动按 `versioning.md` 处理。
