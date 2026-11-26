package execution

import (
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
)

type ChildCloseRecoveryAction struct {
	EventType history.EventType
	Output    map[string]any
	Reason    string
	// Failure 是子工作流终态的结构化失败，冷恢复与正常路径写同一份载荷。
	Failure *protocol.Failure
}

// ChildCloseRecoveryActionFor derives the parent notification from the child
// Run's terminal status and its terminal History event.
func (r *Runtime) ChildCloseRecoveryActionFor(current *Run, status history.Status) (ChildCloseRecoveryAction, bool, error) {
	if current.ParentRunID == "" || current.ParentScheduledEventID == 0 {
		return ChildCloseRecoveryAction{}, false, nil
	}
	switch status {
	case history.StatusCompleted:
		output := current.Output
		if event, _, ok, err := r.StatusEventFromHistory(current.ID); err != nil {
			return ChildCloseRecoveryAction{}, false, err
		} else if ok && event.WorkflowCompleted != nil {
			output = event.WorkflowCompleted.Output
		}
		return ChildCloseRecoveryAction{EventType: history.EventChildWorkflowCompleted, Output: output}, true, nil
	case history.StatusCancelled:
		return ChildCloseRecoveryAction{EventType: history.EventChildWorkflowCancelled}, true, nil
	case history.StatusTerminated:
		reason := ""
		if event, _, ok, err := r.StatusEventFromHistory(current.ID); err != nil {
			return ChildCloseRecoveryAction{}, false, err
		} else if ok && event.WorkflowTerminated != nil {
			reason = event.WorkflowTerminated.Reason
		}
		return ChildCloseRecoveryAction{EventType: history.EventChildWorkflowTerminated, Reason: reason}, true, nil
	case history.StatusFailed:
		reason := ""
		var failure *protocol.Failure
		if event, _, ok, err := r.StatusEventFromHistory(current.ID); err != nil {
			return ChildCloseRecoveryAction{}, false, err
		} else if ok && event.WorkflowFailed != nil {
			// 失败后由重试 run 接续：子工作流尚未结束，由重试 run 的终态传播。
			if event.WorkflowFailed.NewExecutionRunID != "" {
				return ChildCloseRecoveryAction{}, false, nil
			}
			reason = event.WorkflowFailed.Error
			failure = event.WorkflowFailed.Failure
		}
		return ChildCloseRecoveryAction{EventType: history.EventChildWorkflowFailed, Reason: reason, Failure: failure}, true, nil
	default:
		return ChildCloseRecoveryAction{}, false, nil
	}
}
