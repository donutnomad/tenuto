package execution

import "time"

type ChildStart struct {
	ParentRunID      string
	ChildID          string
	ChildRunID       string
	ScheduledEventID int64
	Namespace        string
	WorkflowID       string
	ConfigVersion    string
	Input            map[string]any
}

// ChildWorkflowID is the workflow ID of the child started by one parent Run
// under one child ID. Every child owns its ID, so children of one workflow
// type never compete for the current-run pointer of that type; restarting the
// same child start resolves to the same ID.
func ChildWorkflowID(parentRunID, childID string) string {
	return parentRunID + "/" + childID
}

// NewChildRun starts the child's first attempt on its own History branch.
// request.WorkflowID names the child's workflow type.
func NewChildRun(request ChildStart, now time.Time) *Run {
	return NewRun(NewRunOptions{
		ID: request.ChildRunID, Namespace: request.Namespace,
		WorkflowID:            ChildWorkflowID(request.ParentRunID, request.ChildID),
		WorkflowType:          request.WorkflowID,
		WorkflowConfigVersion: request.ConfigVersion, ParentRunID: request.ParentRunID,
		ParentChildID: request.ChildID, ParentScheduledEventID: request.ScheduledEventID,
		Input: request.Input, Attempt: 1, CreatedAt: now,
	})
}

// MarkStarted records that the child Run exists. A terminal child state stays
// authoritative during recovery races; the result reports whether the state
// changed.
func (s *ChildWorkflowState) MarkStarted(childRunID string, now time.Time) bool {
	if s.IsTerminal() {
		return false
	}
	s.setStatus(ChildWorkflowStatusEnums.Started)
	s.setChildRunID(childRunID)
	s.setUpdatedAt(now)
	return true
}

// MarkChildStarted advances the parent's child state inside the transaction
// that creates the child Run, so the child Run and the parent's Started state
// commit together.
func MarkChildStarted(tx ExecutionTx, request ChildStart, now time.Time) error {
	state, found, err := tx.GetChildWorkflowStateForUpdate(request.ParentRunID, request.ScheduledEventID)
	if err != nil {
		return err
	}
	if !found {
		state = &ChildWorkflowState{
			RunID: request.ParentRunID, ScheduledEventID: request.ScheduledEventID,
			ChildID: request.ChildID, WorkflowID: request.WorkflowID,
			WorkflowConfigVersion: request.ConfigVersion, Input: request.Input,
			CreatedAt: now,
		}
	}
	if !state.MarkStarted(request.ChildRunID, now) {
		return nil
	}
	return tx.UpsertChildWorkflowState(state)
}
