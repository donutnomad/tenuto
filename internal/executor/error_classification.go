package executor

import "errors"

type WorkflowErrorDisposition uint8

const (
	WorkflowErrorFail WorkflowErrorDisposition = iota
	WorkflowErrorBlock
)

// ClassifyWorkflowError blocks only on nondeterministic replay failures that
// code rollback or repair can resolve. Unknown errors fail visibly.
func ClassifyWorkflowError(err error) WorkflowErrorDisposition {
	var nondeterministic NonDeterministicError
	if errors.As(err, &nondeterministic) {
		return WorkflowErrorBlock
	}
	return WorkflowErrorFail
}
