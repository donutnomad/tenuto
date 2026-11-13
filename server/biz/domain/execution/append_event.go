package execution

import (
	"time"

	"github.com/donutnomad/tenuto/protocol/history"
)

// StampEvent 给没有时间的事件写入 now，已带时间的事件原样返回。存储在追加事件时用自己的时钟调用它，
// History 事件的时间因此都来自追加它的存储所用的时钟。
func StampEvent(event history.Event, now time.Time) history.Event {
	if event.Timestamp.IsZero() {
		event.Timestamp = now
	}
	return event
}

// AppendEvent routes Run lifecycle facts to the store operation that owns
// their idempotency rule. Scheduled and closed Workflow Tasks have dedicated
// first-wins writes; the remaining lifecycle facts use event dedupe keys.
func AppendEvent(store EventStore, runID, treeID, branchID string, event history.Event) (int64, bool, error) {
	switch {
	case event.WorkflowTaskScheduled != nil:
		return store.AppendWorkflowTaskScheduled(runID, treeID, branchID, event)
	case history.IsWorkflowTaskCloseEventType(event.EventType()):
		return store.AppendWorkflowTaskClosed(runID, treeID, branchID, event)
	case event.ActivityAttemptFailed != nil:
		event.ActivityAttemptFailed.Attempt = history.NormalizeActivityAttempt(event.ActivityAttemptFailed.Attempt)
	case event.ActivityTimedOut != nil:
		event.ActivityTimedOut.Attempt = history.NormalizeActivityAttempt(event.ActivityTimedOut.Attempt)
	}
	return store.AppendIdempotent(runID, treeID, branchID, event)
}
