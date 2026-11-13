package execution

import (
	"fmt"

	"github.com/donutnomad/tenuto/protocol/history"
)

type WorkflowQueryResult struct {
	Run              *Run                           `json:"run"`
	QueryName        string                         `json:"query_name,omitempty"`
	HistoryLength    int                            `json:"history_length"`
	SearchAttributes map[string]string              `json:"search_attributes,omitempty"`
	CurrentWaiting   WaitingState                   `json:"current_waiting,omitempty"`
	FailureReason    string                         `json:"failure_reason,omitempty"`
	Signals          []history.WorkflowSignalRecord `json:"signals,omitempty"`
	Updates          []history.WorkflowUpdateRecord `json:"updates,omitempty"`

	QueryResult   map[string]any `json:"query_result,omitempty"`
	QueryRejected bool           `json:"query_rejected,omitempty"`
	QueryError    string         `json:"query_error,omitempty"`
}

// ProjectWorkflowQuery 组装查询响应：当前状态取自 execution state，signal 与 update 记录取自 History。
func ProjectWorkflowQuery(current *Run, queryName string, state *WorkflowExecutionState, events []history.Event) WorkflowQueryResult {
	result := WorkflowQueryResult{
		Run:              current,
		QueryName:        queryName,
		HistoryLength:    len(events),
		SearchAttributes: cloneStringMap(state.SearchAttributes),
		CurrentWaiting:   state.CurrentWaiting,
		FailureReason:    state.FailureReason,
	}
	for _, event := range events {
		if event.WorkflowSignaled != nil {
			result.Signals = append(result.Signals, history.WorkflowSignalRecord{
				EventID: event.ID,
				Name:    event.WorkflowSignaled.Name,
				Input:   event.WorkflowSignaled.Input,
			})
		}
	}
	result.Updates = UpdateRecordsFromEvents(events)
	return result
}

func SearchAttributesFromHistory(base map[string]string, events []history.Event) map[string]string {
	attrs := cloneStringMap(base)
	for _, event := range events {
		if event.SearchAttributesUpserted == nil {
			continue
		}
		if attrs == nil {
			attrs = make(map[string]string)
		}
		for key, value := range event.SearchAttributesUpserted.Attributes {
			if key != "" {
				attrs[key] = fmt.Sprint(value)
			}
		}
	}
	return attrs
}

// StackTraceProgress describes the logical position of a Run: activity
// progress comes from History, the current wait point and terminal status from
// the execution state.
func StackTraceProgress(state *WorkflowExecutionState, events []history.Event) map[string]any {
	closedByScheduledID := make(map[int64]string)
	for _, event := range events {
		if event.ActivityCompleted != nil && event.ActivityCompleted.ScheduledEventID > 0 {
			closedByScheduledID[event.ActivityCompleted.ScheduledEventID] = "completed"
		}
		if event.ActivityFailed != nil && event.ActivityFailed.ScheduledEventID > 0 {
			closedByScheduledID[event.ActivityFailed.ScheduledEventID] = "failed"
		}
		if event.ActivityCancelled != nil && event.ActivityCancelled.ScheduledEventID > 0 {
			closedByScheduledID[event.ActivityCancelled.ScheduledEventID] = "cancelled"
		}
	}
	steps := make([]map[string]any, 0)
	for _, event := range events {
		if event.ActivityScheduled == nil {
			continue
		}
		status := "running"
		if closed, ok := closedByScheduledID[event.ID]; ok {
			status = closed
		}
		steps = append(steps, map[string]any{
			"activity_id":   event.ActivityScheduled.ActivityID,
			"activity_type": event.ActivityScheduled.ActivityType,
			"status":        status,
		})
	}
	w := state.CurrentWaiting
	waiting := map[string]any{}
	if w.Type != "" {
		waiting["type"] = string(w.Type)
		if w.ActivityID != "" {
			waiting["activity_id"] = w.ActivityID
		}
		if w.ChildID != "" {
			waiting["child_id"] = w.ChildID
		}
		if w.ActivityType != "" {
			waiting["activity_type"] = w.ActivityType
		}
		if w.TimerID != "" {
			waiting["timer_id"] = w.TimerID
		}
	}
	return map[string]any{
		"waiting": waiting, "steps": steps,
		"summary": stackTraceSummary(state.Status, w),
		"note":    "logical workflow position from event history (activity-level)",
	}
}

func stackTraceSummary(status history.Status, waiting WaitingState) string {
	switch status {
	case history.StatusCompleted:
		return "workflow completed"
	case history.StatusFailed:
		return "workflow failed"
	case history.StatusCancelled:
		return "workflow cancelled"
	case history.StatusTerminated:
		return "workflow terminated"
	}
	switch waiting.Type {
	case WaitingActivity:
		return fmt.Sprintf("waiting on activity %s (%s)", waiting.ActivityID, waiting.ActivityType)
	case WaitingTimer:
		return fmt.Sprintf("waiting on timer %s", waiting.TimerID)
	case WaitingChildWorkflow:
		return fmt.Sprintf("waiting on child workflow %s", waiting.ChildID)
	case WaitingWorkflowTask:
		return "waiting for next workflow task to be processed"
	default:
		return "running; no pending activity (may be waiting on a signal/timer or between steps)"
	}
}
