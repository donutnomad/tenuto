package executor

import (
	"slices"
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
	restore, activation, live := SplitColdRestoreHistory(events)
	if len(live) != len(activation) {
		t.Fatalf("live = %+v, want activation", live)
	}
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
	restore, activation, live := SplitColdRestoreHistory(events)
	if len(restore) != 0 || len(activation) != len(events) || len(live) != len(events) {
		t.Fatalf("restore=%d activation=%d", len(restore), len(activation))
	}
}

// 最后一个 Workflow Task 进行期间到达的 Signal 排在 WorkflowTaskCompleted 之前：activation 从它开始以保持 ID 连续，
// live 去掉已提交的 WorkflowTaskCompleted 与命令事件，与 SplitBatches 的最后一批一致。
func TestSplitColdRestoreHistoryStartsActivationAtUnseenFacts(t *testing.T) {
	completed := history.NewWorkflowTaskCompletedEvent(1, "worker")
	completed.WorkflowTaskCompleted.SeenThroughEventID = 2
	events := []history.Event{
		executionSessionEvent(1, history.NewWorkflowStartedEvent(nil, "", "", "", "", 0)),
		executionSessionEvent(2, history.NewWorkflowTaskStartedEvent(1, "worker", "build", 0)),
		executionSessionEvent(3, history.NewWorkflowSignaledEvent("tick", nil)),
		executionSessionEvent(4, completed),
		executionSessionEvent(5, history.NewActivityScheduledEvent(history.ActivityScheduledEvent{ActivityID: "#a1"})),
		executionSessionEvent(6, history.NewWorkflowTaskScheduledEvent(0)),
		executionSessionEvent(7, history.NewWorkflowTaskStartedEvent(6, "worker", "build", 0)),
	}
	restore, activation, live := SplitColdRestoreHistory(events)
	if got := eventIDs(restore); !slices.Equal(got, []int64{1, 2, 3, 4, 5}) {
		t.Fatalf("restore = %v", got)
	}
	if got := eventIDs(activation); !slices.Equal(got, []int64{3, 4, 5, 6, 7}) {
		t.Fatalf("activation = %v", got)
	}
	batches := SplitBatches(events)
	if got, want := eventIDs(live), eventIDs(batches[len(batches)-1]); !slices.Equal(got, want) || !slices.Equal(got, []int64{3, 6, 7}) {
		t.Fatalf("live = %v, last batch = %v", got, want)
	}
}

func eventIDs(events []history.Event) []int64 {
	ids := make([]int64, len(events))
	for i, event := range events {
		ids[i] = event.ID
	}
	return ids
}

func executionSessionEvent(id int64, event history.Event) history.Event {
	event.ID = id
	event.Timestamp = time.Unix(id, 0).UTC()
	return event
}
