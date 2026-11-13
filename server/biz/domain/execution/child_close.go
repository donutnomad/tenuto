package execution

import (
	"fmt"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
)

type ChildClose struct {
	Child          *Run
	ParentTreeID   string
	ParentBranchID string
	Type           history.EventType
	Output         map[string]any
	// Reason 只用于终止（Terminated）事件；失败终态的文本来自 Failure.Message。
	Reason string
	// Failure 是失败终态的唯一载荷。EventChildWorkflowFailed 必须携带它；
	// 取消/完成不需要。
	Failure *protocol.Failure
	Now     time.Time
}

// childCloseEvent 是子终态事件与父侧 status 的纯函数映射。失败终态必须携带结构化 Failure；
// 缺失是协议错误，不以任何字符串回退补造。
func childCloseEvent(request ChildClose) (history.Event, ChildWorkflowStatus, error) {
	child := request.Child
	switch request.Type {
	case history.EventChildWorkflowCompleted:
		return history.NewChildWorkflowCompletedEvent(child.ParentChildID, child.ID, child.ParentScheduledEventID, request.Output),
			ChildWorkflowStatusEnums.Completed, nil
	case history.EventChildWorkflowCancelled:
		return history.NewChildWorkflowCancelledEvent(child.ParentChildID, child.ID, child.ParentScheduledEventID),
			ChildWorkflowStatusEnums.Cancelled, nil
	case history.EventChildWorkflowTerminated:
		return history.NewChildWorkflowTerminatedEvent(child.ParentChildID, child.ID, child.ParentScheduledEventID, request.Reason),
			ChildWorkflowStatusEnums.Terminated, nil
	default:
		if request.Failure == nil {
			return history.Event{}, "", fmt.Errorf("child close for %s: failure payload is required", child.ID)
		}
		return history.NewChildWorkflowFailedEvent(child.ParentChildID, child.ID, child.ParentScheduledEventID, *request.Failure),
			ChildWorkflowStatusEnums.Failed, nil
	}
}

// PropagateChildClose commits a child terminal fact to the parent's History
// and current state. Recovery may call it when the initiated state is absent.
// A parent that already closed keeps its History: only the child link state is
// updated, and propagated is false because the parent has nothing to process.
func PropagateChildClose(tx ExecutionTx, request ChildClose) (propagated bool, err error) {
	child := request.Child
	parent, err := tx.GetWorkflowExecutionStateForUpdate(child.ParentRunID)
	if err != nil {
		return false, err
	}
	state, found, err := tx.GetChildWorkflowStateForUpdate(child.ParentRunID, child.ParentScheduledEventID)
	if err != nil || found && state.IsTerminal() {
		return false, err
	}
	event, status, err := childCloseEvent(request)
	if err != nil {
		return false, err
	}
	propagated = !parent.IsTerminal()
	if propagated {
		if _, err := tx.Append(child.ParentRunID, request.ParentTreeID, request.ParentBranchID, event); err != nil {
			return false, err
		}
	}
	if !found {
		// ChildWorkflowState.WorkflowID 记子 Workflow 的类型；子 Run 的 WorkflowID 是它自己的实例 ID。
		state = &ChildWorkflowState{
			RunID: child.ParentRunID, ScheduledEventID: child.ParentScheduledEventID,
			ChildID: child.ParentChildID, ChildRunID: child.ID,
			WorkflowID: child.WorkflowType, WorkflowConfigVersion: child.WorkflowConfigVersion,
			CreatedAt: request.Now,
		}
	}
	state.setStatus(status)
	state.setChildRunID(child.ID)
	switch request.Type {
	case history.EventChildWorkflowCompleted:
		state.setOutput(request.Output)
	case history.EventChildWorkflowTerminated:
		state.setReason(request.Reason)
	case history.EventChildWorkflowFailed:
		state.setError(request.Failure.Message)
	}
	state.setUpdatedAt(request.Now)
	if err := tx.UpsertChildWorkflowState(state); err != nil {
		return false, err
	}
	return propagated, nil
}
