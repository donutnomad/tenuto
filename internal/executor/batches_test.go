package executor

import (
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
