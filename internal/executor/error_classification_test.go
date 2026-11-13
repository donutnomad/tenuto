package executor

import (
	"errors"
	"fmt"
	"testing"
)

func TestClassifyWorkflowError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want WorkflowErrorDisposition
	}{
		{name: "nil", want: WorkflowErrorFail},
		{name: "unknown", err: errors.New("configuration failure"), want: WorkflowErrorFail},
		{name: "nondeterministic", err: NewNonDeterministicError("command mismatch"), want: WorkflowErrorBlock},
		{name: "wrapped nondeterministic", err: fmt.Errorf("workflow replay: %w", NewNonDeterministicError("command mismatch")), want: WorkflowErrorBlock},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := ClassifyWorkflowError(test.err); got != test.want {
				t.Fatalf("ClassifyWorkflowError() = %v, want %v", got, test.want)
			}
		})
	}
}
