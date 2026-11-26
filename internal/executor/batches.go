package executor

import "github.com/donutnomad/tenuto/protocol/history"

// SplitBatches 把 History 切成决策批：每个已完成的 Workflow Task 一批，包含它的 Worker 做决策时看到的事件、
// WorkflowTaskCompleted 与它产出的命令事件。最后一批是当前 live 任务看到的事件，可能为空。
//
// Workflow Task 进行期间落库的事实排在它的 WorkflowTaskCompleted 之前，ID 大于
// WorkflowTaskCompleted.SeenThroughEventID。Worker 决策时没有看到它们，它们归入下一批，与下一个 Workflow Task
// 实际看到的事件一致。
func SplitBatches(events []history.Event) [][]history.Event {
	var batches [][]history.Event
	current := []history.Event{}
	for _, event := range events {
		if completed := event.WorkflowTaskCompleted; completed != nil {
			seen, carried := splitUnseen(current, completed.SeenThroughEventID)
			batches = append(batches, append(seen, event))
			current = carried
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

// splitUnseen 把一批事件分成 Worker 已看到的（ID 不大于 seenThrough）与未看到的两段；seenThrough 为 0 时全部已看到。
func splitUnseen(events []history.Event, seenThrough int64) (seen, carried []history.Event) {
	if seenThrough <= 0 {
		return events, []history.Event{}
	}
	carried = []history.Event{}
	seen = events[:0:0]
	for _, event := range events {
		if event.ID > seenThrough {
			carried = append(carried, event)
		} else {
			seen = append(seen, event)
		}
	}
	return seen, carried
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
