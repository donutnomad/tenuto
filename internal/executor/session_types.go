package executor

import (
	"context"
	"errors"
	"fmt"

	"github.com/donutnomad/tenuto/protocol/history"
)

// ErrExecutionSessionInvalid 标记 session 状态、activation 增量或 history 形状的校验失败。
// 这类错误与 workflow 代码无关：Worker 丢弃 session，本次 Workflow Task 记为失败并退避重试，
// 下一次从 Event History cold restore。
var ErrExecutionSessionInvalid = errors.New("execution session is invalid")

var (
	ErrExecutionSessionClosed          = fmt.Errorf("%w: closed", ErrExecutionSessionInvalid)
	ErrExecutionSessionAlreadyRestored = fmt.Errorf("%w: already restored", ErrExecutionSessionInvalid)
	ErrExecutionSessionNotRestored     = fmt.Errorf("%w: not restored", ErrExecutionSessionInvalid)
	ErrExecutionSessionPendingCommit   = fmt.Errorf("%w: an activation is pending commit", ErrExecutionSessionInvalid)
	ErrExecutionSessionNotPending      = fmt.Errorf("%w: no activation is pending commit", ErrExecutionSessionInvalid)
	ErrWorkflowTaskCompletionRejected  = errors.New("workflow task completion was rejected")
)

type HistoryIterator interface {
	Next(context.Context) ([]history.Event, error)
}

type RestoreResult struct {
	AppliedThroughEventID int64
	EstimatedSize         int64
}

type ExecutionSession interface {
	Restore(context.Context, HistoryIterator) (RestoreResult, error)
	Activate(context.Context, history.WorkflowActivation) (history.ActivationResult, error)
	Commit(history.WorkflowTaskCompletionAck) error
	AppliedThroughEventID() int64
	EstimatedSize() int64
	Close() error
}
