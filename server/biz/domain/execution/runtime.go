package execution

import (
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
)

// Runtime answers status questions from the execution state and finds the
// lifecycle event that History records for a Run.
type StatusStore interface {
	GetEvents(runID string) ([]history.Event, error)
	GetWorkflowExecutionState(runID string) (*WorkflowExecutionState, error)
}

type Runtime struct {
	store StatusStore
}

func NewRuntime(store StatusStore) *Runtime {
	return &Runtime{store: store}
}

// CurrentStatus returns the status projected into the execution state.
func (r *Runtime) CurrentStatus(runID string) (history.Status, error) {
	state, err := r.store.GetWorkflowExecutionState(runID)
	if err != nil {
		return "", err
	}
	return state.Status, nil
}

// CheckCancellation returns whether a cancellation request was already
// recorded for a Running Run.
func (r *Runtime) CheckCancellation(runID string) (bool, error) {
	state, err := r.store.GetWorkflowExecutionState(runID)
	if err != nil {
		return false, err
	}
	if state.Status != history.StatusRunning {
		return false, protocol.InvalidRequestf("workflow %s is %s", runID, state.Status)
	}
	return state.CancelRequested, nil
}

func (r *Runtime) CheckTermination(runID string) error {
	status, err := r.CurrentStatus(runID)
	if err != nil {
		return err
	}
	return CheckTerminationStatus(runID, status)
}

func (r *Runtime) StatusEventFromHistory(runID string) (history.Event, history.Status, bool, error) {
	events, err := r.store.GetEvents(runID)
	if err != nil {
		return history.Event{}, "", false, err
	}
	var latest history.Event
	var latestStatus history.Status
	var found bool
	for _, event := range events {
		status, lifecycle := statusFromLifecycleEvent(event.EventType())
		if !lifecycle {
			continue
		}
		if history.IsTerminalStatus(status) {
			return event, status, true, nil
		}
		latest, latestStatus, found = event, status, true
	}
	if found {
		return latest, latestStatus, true, nil
	}
	return history.Event{}, "", false, nil
}

func statusFromLifecycleEvent(eventType history.EventType) (history.Status, bool) {
	switch eventType {
	case history.EventWorkflowStarted:
		return history.StatusRunning, true
	case history.EventWorkflowCompleted:
		return history.StatusCompleted, true
	case history.EventWorkflowFailed:
		return history.StatusFailed, true
	case history.EventWorkflowContinuedAsNew:
		return history.StatusContinuedAsNew, true
	case history.EventWorkflowCancelled:
		return history.StatusCancelled, true
	case history.EventWorkflowTerminated:
		return history.StatusTerminated, true
	default:
		return "", false
	}
}
