package executor

import (
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol/history"
)

func TestSplitColdRestoreHistoryKeepsCommittedCommandTailTogether(t *testing.T) {
	events := []history.Event{
		executionSessionEvent(1, history.NewWorkflowStartedEvent(nil, "", "", "", "", 0)),
		executionSessionEvent(2, history.NewWorkflowTaskStartedEvent(1, "worker", "build", 0)),
		executionSessionEvent(3, history.NewWorkflowTaskCompletedEvent(1, "worker")),
		executionSessionEvent(4, history.NewActivityScheduledEvent(history.ActivityScheduledEvent{ActivityID: "#a1"})),
		executionSessionEvent(5, history.NewActivityCompletedEvent(history.ActivityCompletedEvent{ScheduledEventID: 4})),
		executionSessionEvent(6, history.NewWorkflowTaskScheduledEvent(0)),
		executionSessionEvent(7, history.NewWorkflowTaskStartedEvent(6, "worker", "build", 0)),
	}
	restore, activation := SplitColdRestoreHistory(events)
	if len(restore) != 4 || restore[len(restore)-1].ID != 4 {
		t.Fatalf("restore history = %+v", restore)
	}
	if len(activation) != 3 || activation[0].ID != 5 || activation[len(activation)-1].ID != 7 {
		t.Fatalf("activation history = %+v", activation)
	}
}

func TestSplitColdRestoreHistoryLeavesInitialRunForFirstActivation(t *testing.T) {
	events := []history.Event{
		executionSessionEvent(1, history.NewWorkflowStartedEvent(map[string]any{"id": "one"}, "", "", "", "", 0)),
		executionSessionEvent(2, history.NewWorkflowTaskScheduledEvent(0)),
		executionSessionEvent(3, history.NewWorkflowTaskStartedEvent(2, "worker", "build", 0)),
	}
	restore, activation := SplitColdRestoreHistory(events)
	if len(restore) != 0 || len(activation) != len(events) {
		t.Fatalf("restore=%d activation=%d", len(restore), len(activation))
	}
}

func executionSessionEvent(id int64, event history.Event) history.Event {
	event.ID = id
	event.Timestamp = time.Unix(id, 0).UTC()
	return event
}
