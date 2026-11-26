---
title: ApplicationError 实现与兼容说明
description: 共享应用错误类型、Failure 结构化载荷、三条出口、类型化重试与 workflow 内类型分支的用法、持久化与发布约束
status: Stable
---

# ApplicationError 实现与兼容说明

本文说明 `ApplicationError` 在本项目里的最终形态与使用方式。

## 1. 两种构造入口，一份表示

| 位置 | 构造方式 | 用途 |
| --- | --- | --- |
| Activity / 普通 Go | `apperror.New` / `apperror.NewWithCause` / `apperror.NewNonRetryable` / `apperror.NewWithOptions` | 原生 Go 值，Activity 直接 `return` |
| Workflow body | `workflow.NewApplicationError` / `workflow.NewApplicationErrorWithCause` / `workflow.NewNonRetryableApplicationError` | 解释器宿主实现，返回同一个共享错误 |

两者最终都编码成 `protocol.Failure`：

```go
type Failure struct {
	Message      string
	Type         string
	NonRetryable bool
	Details      Payloads
	Cause        *Failure
}
```

Update outcome、Workflow 终态、Activity 失败、子工作流失败共用这同一个类型，没有第二种失败表示。失败由 `Failure != nil` 表达，`nil` 表示成功；普通 Go error 仍合法，由 `apperror.Encode` 统一编成 `Failure`（`Type` 可为空）。

```go
// Activity：永久失败 + 结构化上下文
return ChargeOutput{}, apperror.NewNonRetryable(
	"card declined", "PaymentDeclined", nil,
	map[string]any{"reason": "insufficient_funds", "card_id": in.CardID})

// Activity：可重试临时失败（没有 detail 就不要传 nil，否则会写入一个 null detail）
return ChargeOutput{}, apperror.New("payment provider unavailable", "PaymentUnavailable")
```

## 2. 三条出口与落库位置

| 出口 | 事件 | 落库 |
| --- | --- | --- |
| Workflow body 返回错误 | `WorkflowFailedEvent.Failure` | `tenuto_events.event_json`：整个事件的 JSON，`Failure` 是其中的字段 |
| Activity 失败（含中间 attempt） | `ActivityAttemptFailedEvent.Failure` / `ActivityFailedEvent.Failure` | 同上；`tenuto_activity_states.retry_last_failure` 仍只存 message |
| Update handler 失败 | `UpdateOutcome.Failure` | 同上，`WorkflowUpdateCompleted` 事件的 `Outcome` 字段 |
| 子工作流失败 | `ChildWorkflowFailedEvent.Failure`（父历史） | 同上 |

每个事件整条存进 `tenuto_events.event_json`（`history.CanonicalEventJSON` 编码，`history.DecodeEventJSON` 解码），读出的事件与写入的事件逐字段相同；事件表其余的列只用于定位、排序、去重和回收。事件消息里的 `Error` 字段始终镜像 `Failure.Message`。`ChildWorkflowFailed` 与 Activity/Workflow 失败同形，父 run 的 `future.Get` 拿到的错误走同一条 `apperror.FromFailure` 还原路径，因此 `workflow.IsApplicationError` 与 `ApplicationErrorDetail` 在子失败上同样可用。

### 2.1 载荷校验落在唯一的写历史边界

`run.NormalizeEventFailures` 是失败载荷校验与 canonical 化的唯一实现，登记了 `ActivityAttemptFailed`、`ActivityFailed`、`WorkflowFailed`、`ChildWorkflowFailed`、`WorkflowUpdateCompleted.Outcome` 五个槽。调用点：

| 边界 | 行为 |
| --- | --- |
| gorm `eventToRow` | 写入前 Normalize；超限返回错误，不落库 |
| memory `EventStore.Append` / `AppendIdempotent` / WFT close 与事务缓冲 | 同一实现；超限在事务提交前返回错误，状态与事件都不变 |
| `NewWorkflowFailedFailureEvent` | 构造终态事件时 Normalize；retry 路径与直接失败路径共用，超限在写状态之前返回错误 |

`NewWorkflowFailedEvent(errMsg, timeoutType)` 是 message-only Failure 的构造便捷入口：它编出 `Failure{Message}`（等同 `Encode(errors.New(errMsg))`），没有 details，标准化恒成功。

## 3. workflow body 内读取

```go
paid, err := CallChargePayment(ctx, in, workflow.ActivityOptions{StartToCloseTimeout: 30 * time.Second})
if err != nil {
	if workflow.IsApplicationError(err, "PaymentDeclined") {
		// 明确拒绝：下游确认未扣款，可以自动补偿
		_, _ = CallReleaseInventory(ctx, ReleaseInventoryInput{...}, workflow.ActivityOptions{StartToCloseTimeout: 30 * time.Second})
		return OrderOutput{}, workflow.NewApplicationError(
			"payment declined by provider, reserved inventory released", "PaymentDeclined")
	}
	// 其余错误：结果不确定，带 cause 保留原始类型，交给人工核对
	return OrderOutput{}, workflow.NewApplicationErrorWithCause(
		"charge payment outcome unknown, manual reconciliation required",
		"SagaManualReconciliation", err)
}
```

- `IsApplicationError(err, type)`：Type 完全相等（大小写敏感）。普通 error 与旧数据（无 type）恒为 `false`。可以带 `!` 取反，也可以放进 `if/else`。
- `ApplicationErrorDetailCount(err)`：details 个数。
- `ApplicationErrorDetail(err, index, out)`：把第 index 个 detail 按 payload 解码到 `out`（非 nil 指针）。无 details、index 越界返回 `apperror.ErrNoDetails`；`out` 为 nil 或非指针返回 `apperror.ErrDetailTarget`；payload 与目标类型不匹配返回解码错误。

实现说明：解码先写到独立临时目标，成功后才写回 `out`；返回非 nil 错误时 `out` 保持调用前的值（有回归测试锁定）。

  目标类型可以直接写业务 DTO：

  ```go
  type PaymentFailureDetail struct {
      OrderID string `json:"order_id"`
      Reason  string `json:"reason"`
  }

  var detail PaymentFailureDetail
  if err := workflow.ApplicationErrorDetail(err, 0, &detail); err != nil {
      // 明确错误：ErrNoDetails / ErrDetailTarget / 解码失败
  }
  reason := detail.Reason
  ```

  workflow 函数体内声明的局部 DTO 由生成器（go2wlang）自动收集 descriptor，不需要把 DTO 塞进 Activity 的 Input/Output。标量 detail 用 `*string` 之类的具体指针；扁平 map 用 `*map[string]string`。

  `map[string]any` 目标的元素类型是 `any`，语言语义下 `detail["k"] == "literal"` 恒为 false（`any` 与 `string` 类型名不同）。混合类型 detail 要么声明具体 DTO，要么用 `to.String`/`to.Int64` 等访问器取值后再比较。

生成的 wlang 守卫只接受有限表达式形态，`workflow.IsApplicationError(...)` 在 `if` 条件位置由 sdkgen 自动下沉为临时变量（求值位置与次数不变）。以下形态给出 `file:line` 明确错误，不静默改语义：`if init; workflow.IsApplicationError(...)`、`else if workflow.IsApplicationError(...)`、与其它条件用 `&&`/`||` 组合。遇到这些形态时先给判定赋值变量，再在条件里使用变量。

## 4. 重试判定

任一命中即停止重试，顺序固定：

1. 顶层 `Failure.NonRetryable == true`；
2. 顶层 `Failure.Type` 与 `NonRetryableErrorTypes` 中某项完全相等；
3. 顶层 `Failure.Message` 包含 `NonRetryableErrors` 中某项（既有子串语义）；
4. 否则按 `MaxAttempts` 与截止时间。

判定只看顶层失败：`fmt.Errorf("%w")` 包装后 Type 与 NonRetryable 取顶层（`wrapError` / `false`），内层 flag 不提升，cause 保留内层失败。message 仍是包装后的完整文本，因此显式配置的 legacy `NonRetryableErrors` 子串**可能命中** wrapper message；`NonRetryableErrorTypes` 只匹配顶层 type，命中的是 `wrapError` 而不是内层 type。这一点与 Temporal 的「只读顶层 failure_info」一致。

重试是否发生只看各自显式配置的策略：Activity 与 Workflow 都按自己的 `RetryPolicy` 判定，**未配置策略时不重试**（Activity 的 `RetryState` 为 `RetryPolicyNotSet`，Workflow 的 `RetryState` 为空）。`MaxAttempts=0` 表示不限次数，但仍受截止时间约束。

配置了 `RetryPolicy` 的 Workflow 每次失败都在 `WorkflowFailed.retryState` 记录判定结果：重试时为 `InProgress`（同时带 `newExecutionRunId`），不再重试时为 `NonRetryableFailure`、`MaximumAttemptsReached` 或 `Timeout`（下一次退避会越过执行截止时间）。

重试 Run 的首个 Workflow Task 在退避到期后调度。退避期内按 Workflow ID 发来的 Signal（含 Signal-With-Start 落到重试 Run 上）照常写入重试 Run 的 History，不提前开始重试，到期后的第一个 Workflow Task 一并处理；取消请求与 Update 立即调度 Workflow Task，重试 Run 提前开始，退避到期时不再多调度一次。这与 Temporal 相同。

与 Temporal 的差异：Temporal 的 Activity 在未显式配置时使用服务端默认重试策略，Workflow 需要显式设置；本项目两者都要求显式配置，不套用 Temporal 的默认值。

```json
{
  "retry_policy": {
    "max_attempts": 3,
    "initial_interval": "1s",
    "backoff_coefficient": 2,
    "non_retryable_errors": ["card declined"],
    "non_retryable_error_types": ["PaymentDeclined"]
  }
}
```

## 5. 客户端读取

```go
// UpdateWorkflow 成功只代表请求被接受，它返回 handle；handler 执行失败在 Get 里出现。
handle, err := c.UpdateWorkflow(ctx, opts) // c 是 sdk/client 的 Client
if err != nil {
	return err // 请求本身失败（校验、传输、拒绝），不是 handler 的应用错误
}
if err := handle.Get(ctx, &result); err != nil {
	var updateErr *apperror.Error
	if errors.As(err, &updateErr) {
		typeName := updateErr.Type()
		var detail ChargeFailure
		_ = updateErr.Details(&detail)
	}
	return err
}

err = c.GetWorkflow(ctx, workflowID, runID).Get(ctx, &out)
var appErr *apperror.Error
if errors.As(err, &appErr) { // err 是 *client.WorkflowExecutionError，Unwrap 得到 Workflow 的应用错误
	typeName := appErr.Type()            // "" = untyped（普通 error）
	var detail ChargeFailure
	_ = appErr.Details(&detail)
	nonRetryable := appErr.NonRetryable()
	cause := appErr.Unwrap()
}
```

客户端直接返回 `*apperror.Error`（没有单独的错误类型别名），`Details` 按 `json/plain-v1` 解码。

- Workflow 的失败载荷取自 `WorkflowFailed.Failure`，事件的 `Error` 字段只是它的 message 投影。
- `Get` 跟随 continue-as-new 与 Workflow 重试，返回链上最后一个 Run 的失败；执行超时与 Run 超时的 `TimeoutType()` 是 `client.TimeoutTypeWorkflowExecution` / `TimeoutTypeWorkflowRun`。见 [Go Client](/sdk/client#get-跟随执行链)。

## 6. 单一格式与单一模型

本项目不保留历史格式或旧入口的兼容支线：

| 项 | 唯一形态 |
| --- | --- |
| 失败表示 | `protocol.Failure`；`Failure != nil` 失败，`nil` 成功；普通 error 由 `apperror.Encode` 编成 Failure（Type 可为空） |
| Update outcome | `UpdateOutcome{Success *Payload, Failure *Failure}`，`Validate` 要求恰好一个；completed 事件只有 `Outcome` |
| Update 存储 | 与其它事件相同，整个事件的 JSON 存在 `tenuto_events.event_json`；`Payload.Data` 是 JSON 里的 base64 字符串，字节原样往返 |
| Activity 失败上报 | `HandleActivityFailed(..., failure protocol.Failure)`；`CompleteActivity/ByID/CompleteActivityTask(..., failure *protocol.Failure)` |
| Update 拒绝 | 哨兵 `protocol.ErrUpdateRejected` 一个值；HTTP 400 + `protocol.Failure` JSON，client 还原成 `*apperror.Error` |
| 子工作流失败 | `ChildWorkflowFailedEvent.Failure`；`Error` 只是 message 投影 |

发布顺序按单一格式执行：server 与 worker 使用同一份协议版本，不提供跨版本降级语义。

## 7. 已知限制

1. Application Error 不支持 `NextRetryDelay`、`Category` 和 `EncodedAttributes`：重试间隔只由 RetryPolicy 决定，错误不携带分类字段，错误属性不做单独编码。
2. `WorkflowTaskFailed` 仍只有 `Cause`/`Reason` 两个字符串（诊断缺口，与业务失败通道分开）。
3. `ApplicationErrorDetail` 的目标类型需要 descriptor：函数体局部 DTO 由 go2wlang 收集；跨包类型仍需在生成器可见范围内（与 activity 契约类型同机制）。
