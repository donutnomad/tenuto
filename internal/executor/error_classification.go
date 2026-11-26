package executor

import (
	"errors"
	"fmt"
	"strings"

	"github.com/donutnomad/tenuto/protocol/history"
	wlerrors "github.com/donutnomad/wlang/errors"
)

// WorkflowPanicError 是 Workflow 执行中的 panic：解释器自身的 panic，或 Workflow 代码里对应 Go panic 的运行时错误
// （越界、nil 解引用、显式 panic）。
type WorkflowPanicError struct {
	Value any
}

func (e WorkflowPanicError) Error() string {
	return fmt.Sprintf("workflow executor panic: %v", e.Value)
}

// workflowPanicCodes 是 wlang 里对应 Go 运行时 panic 的错误码：Go 写成的 Workflow 在这些地方 panic。
// E_SYMBOL 在运行时只来自经 nil 指针取字段（`variable path "p.N" not found`）或翻译缺陷：Go 源码已通过类型检查，
// 不会有真正缺失的符号；两者都能靠修复后重新发布恢复。
var workflowPanicCodes = map[string]bool{
	wlerrors.CodeRuntime:     true,
	wlerrors.CodePanic:       true,
	wlerrors.CodeNilReceiver: true,
	wlerrors.CodeSymbol:      true,
	// E_BUDGET：一个 Workflow Task 执行的语句超过上限（按批计数），或单个值超过大小上限。对标 Temporal Go SDK
	// 的 deadlock detector：Workflow 代码在一个 Workflow Task 里运行过久按 panic 处理，Workflow Task 失败并重试，
	// Run 不失败；修复代码后从失败处继续。
	wlerrors.CodeBudget: true,
}

// failedTypeAssertionMessage 是 wlang 非 comma-ok 类型断言（Go 的 `v.(T)`）失败时 E_TYPE 的消息前缀，Go 在这里 panic。
// E_TYPE 的其他来源不归为 panic：类型系统不一致（运算、赋值、参数的类型不符）是通过类型检查的 Go 代码不会产生的
// 翻译缺陷，载荷解码失败（registry 的 JSON codec）是数据转换错误，在 Go 里都不对应 panic。comma-ok 形式
// （type.assert.ok）与 type switch（type.is）不报错。
const failedTypeAssertionMessage = "type.assert cannot pass "

// WorkflowTaskBlockedCause 返回让 Workflow Task 失败、Run 保持 Running 的错误的 cause，其余错误返回空串：
//   - 非确定性错误：non_deterministic_error。
//   - panic（WorkflowPanicError、E_RUNTIME、E_PANIC、E_NIL_RECEIVER、E_SYMBOL、E_BUDGET，以及非 comma-ok 类型断言失败的
//     E_TYPE）：workflow_panic，对标 Temporal Go SDK
//     缺省的 BlockWorkflow 策略。修复代码后新的 Workflow Task 从失败处继续。
//
// 用户代码显式返回的 error 不经过这里：解释器把它变成 CmdFailWorkflow，Workflow 照常失败。
func WorkflowTaskBlockedCause(err error) string {
	if err == nil {
		return ""
	}
	var nondeterministic NonDeterministicError
	if errors.As(err, &nondeterministic) {
		return history.WorkflowTaskFailedCauseNonDeterministic
	}
	var panicked WorkflowPanicError
	if errors.As(err, &panicked) {
		return history.WorkflowTaskFailedCauseWorkflowPanic
	}
	var langErr *wlerrors.LangError
	if errors.As(err, &langErr) && (workflowPanicCodes[langErr.Code] ||
		langErr.Code == wlerrors.CodeType && strings.HasPrefix(langErr.Message, failedTypeAssertionMessage)) {
		return history.WorkflowTaskFailedCauseWorkflowPanic
	}
	return ""
}
