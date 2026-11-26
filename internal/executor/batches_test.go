package executor

import (
	"slices"
	"testing"

	"github.com/donutnomad/tenuto/protocol/history"
)

func TestSplitBatchesKeepsInterleavedCommandsWithDecision(t *testing.T) {
	events := []history.Event{
		{ID: 1, WorkflowStarted: &history.WorkflowStartedEvent{}},
		{ID: 2, WorkflowTaskCompleted: &history.WorkflowTaskCompletedEvent{}},
		{ID: 3, ActivityCompleted: &history.ActivityCompletedEvent{}},
		{ID: 4, ActivityScheduled: &history.ActivityScheduledEvent{}},
	}
	batches := SplitBatches(events)
	if len(batches) != 2 || len(batches[0]) != 3 || len(batches[1]) != 1 ||
		batches[0][2].ID != 4 || batches[1][0].ID != 3 {
		t.Fatalf("decision batches = %+v", batches)
	}
}

func TestIsCommandEventSeparatesDecisionAndExternalSearchAttributes(t *testing.T) {
	decision := history.NewSearchAttributesUpsertedEvent(map[string]any{history.ChangeVersionSearchAttribute: `["change-a-1"]`})
	if !IsCommandEvent(decision) {
		t.Fatal("GetVersion search attribute upsert must belong to the workflow task's command events")
	}
	external := history.NewSearchAttributesUpsertedEvent(map[string]any{"order": "one"})
	if IsCommandEvent(external) {
		t.Fatal("externally upserted search attributes must stay in the following decision batch")
	}
}

// Workflow Task 进行期间到达的事实（ID 大于 SeenThroughEventID）排在 WorkflowTaskCompleted 之前，归入下一批。
func TestSplitBatchesMovesUnseenFactsToNextBatch(t *testing.T) {
	events := []history.Event{
		{ID: 1, WorkflowStarted: &history.WorkflowStartedEvent{}},
		{ID: 2, WorkflowTaskScheduled: &history.WorkflowTaskScheduledEvent{}},
		{ID: 3, WorkflowTaskStarted: &history.WorkflowTaskStartedEvent{ScheduledEventID: 2}},
		{ID: 4, WorkflowSignaled: &history.WorkflowSignaledEvent{Name: "a"}},
		{ID: 5, ActivityCompleted: &history.ActivityCompletedEvent{}},
		{ID: 6, WorkflowTaskCompleted: &history.WorkflowTaskCompletedEvent{ScheduledEventID: 2, SeenThroughEventID: 3}},
		{ID: 7, TimerScheduled: &history.TimerScheduledEvent{TimerID: "t"}},
		{ID: 8, WorkflowSignaled: &history.WorkflowSignaledEvent{Name: "b"}},
		{ID: 9, WorkflowTaskScheduled: &history.WorkflowTaskScheduledEvent{}},
		{ID: 10, WorkflowTaskStarted: &history.WorkflowTaskStartedEvent{ScheduledEventID: 9}},
	}
	batches := SplitBatches(events)
	want := [][]int64{{1, 2, 3, 6, 7}, {4, 5, 8, 9, 10}}
	if len(batches) != len(want) {
		t.Fatalf("batches = %+v", batches)
	}
	for i := range want {
		if got := eventIDs(batches[i]); !slices.Equal(got, want[i]) {
			t.Fatalf("batch %d = %v, want %v", i, got, want[i])
		}
	}
}
