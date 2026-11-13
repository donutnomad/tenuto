package executor

import "github.com/donutnomad/tenuto/protocol/history"

// SplitBatches groups each completed Workflow Task with the commands it made.
// External events after a completion belong to the following decision batch.
// The final batch represents the current live task, even when it is empty.
func SplitBatches(events []history.Event) [][]history.Event {
	var batches [][]history.Event
	current := []history.Event{}
	for _, event := range events {
		if event.WorkflowTaskCompleted != nil {
			current = append(current, event)
			batches = append(batches, current)
			current = []history.Event{}
			continue
		}
		if len(batches) > 0 && IsCommandEvent(event) {
			last := len(batches) - 1
			batches[last] = append(batches[last], event)
			continue
		}
		current = append(current, event)
	}
	return append(batches, current)
}

// IsCommandEvent identifies History facts produced by a Workflow decision.
func IsCommandEvent(event history.Event) bool {
	return event.ActivityScheduled != nil ||
		event.ActivityCancelRequested != nil ||
		event.TimerScheduled != nil ||
		event.TimerCancelled != nil ||
		event.ChildWorkflowStarted != nil ||
		event.ChildWorkflowCancelRequested != nil ||
		event.MarkerRecorded != nil ||
		event.WorkflowUpdateCompleted != nil ||
		history.IsChangeVersionUpsert(event)
}
