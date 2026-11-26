package executor

import (
	"errors"
	"fmt"
	"testing"

	"github.com/donutnomad/tenuto/protocol/history"
	wlerrors "github.com/donutnomad/wlang/errors"
)

// 非确定性错误与 panic（解释器 panic、对应 Go 运行时 panic 的 wlang 错误码）让 WFT 失败、Run 保持 Running；
// 其余执行错误让 Workflow 失败。
func TestWorkflowTaskBlockedCause(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{name: "nil", want: ""},
		{name: "unknown", err: errors.New("configuration failure"), want: ""},
		{name: "nondeterministic", err: NewNonDeterministicError("command mismatch"), want: history.WorkflowTaskFailedCauseNonDeterministic},
		{name: "wrapped nondeterministic", err: fmt.Errorf("workflow replay: %w", NewNonDeterministicError("command mismatch")), want: history.WorkflowTaskFailedCauseNonDeterministic},
		{name: "interpreter panic", err: WorkflowPanicError{Value: "nil map"}, want: history.WorkflowTaskFailedCauseWorkflowPanic},
		{name: "index out of range", err: fmt.Errorf("eval: %w", wlerrors.Newf(wlerrors.CodeRuntime, "arr.get index 6 out of range [0,3)")), want: history.WorkflowTaskFailedCauseWorkflowPanic},
		{name: "explicit panic", err: wlerrors.New(wlerrors.CodePanic, "boom"), want: history.WorkflowTaskFailedCauseWorkflowPanic},
		{name: "nil receiver", err: wlerrors.New(wlerrors.CodeNilReceiver, "nil"), want: history.WorkflowTaskFailedCauseWorkflowPanic},
		{name: "nil pointer field", err: wlerrors.New(wlerrors.CodeSymbol, `variable path "p.N" not found and no default`), want: history.WorkflowTaskFailedCauseWorkflowPanic},
		{name: "budget", err: wlerrors.New(wlerrors.CodeBudget, "MaxStringBytes exceeded"), want: ""},
		{name: "failed type assertion", err: wlerrors.Newf(wlerrors.CodeType, "type.assert cannot pass %s as %s", "boolean", "string"), want: history.WorkflowTaskFailedCauseWorkflowPanic},
		{name: "payload decode type error", err: wlerrors.New(wlerrors.CodeType, "expected int64, got string"), want: ""},
		{name: "operator type mismatch", err: wlerrors.New(wlerrors.CodeType, `operator "+" requires matching types, got int64 and string`), want: ""},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := WorkflowTaskBlockedCause(test.err); got != test.want {
				t.Fatalf("WorkflowTaskBlockedCause() = %q, want %q", got, test.want)
			}
		})
	}
}
