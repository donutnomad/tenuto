package executor

import (
	"errors"
	"fmt"

	"github.com/donutnomad/tenuto/protocol/history"
	wlerrors "github.com/donutnomad/wlang/errors"
)

// WorkflowPanicError 是 Workflow 执行中的 panic：解释器自身的 panic，或 Workflow 代码里对应 Go panic 的运行时错误
// （越界、nil 解引用、显式 panic）。
type WorkflowPanicError struct {
	Value any
}

func (e WorkflowPanicError) Error() string { return fmt.Sprintf("workflow executor panic: %v", e.Value) }

// workflowPanicCodes 是 wlang 里对应 Go 运行时 panic 的错误码：Go 写成的 Workflow 在这些地方 panic。
var workflowPanicCodes = map[string]bool{
	wlerrors.CodeRuntime:     true,
	wlerrors.CodePanic:       true,
	wlerrors.CodeNilReceiver: true,
}

// WorkflowTaskBlockedCause 返回让 Workflow Task 失败、Run 保持 Running 的错误的 cause，其余错误返回空串：
//   - 非确定性错误：non_deterministic_error。
//   - panic（WorkflowPanicError、E_RUNTIME、E_PANIC、E_NIL_RECEIVER）：workflow_panic，对标 Temporal Go SDK
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
	if errors.As(err, &langErr) && workflowPanicCodes[langErr.Code] {
		return history.WorkflowTaskFailedCauseWorkflowPanic
	}
	return ""
}
