package execution_test

import (
	"testing"

	"github.com/donutnomad/tenuto/protocol"

	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
)

func TestAppendEventUsesWorkflowTaskIdempotencyAndNormalizesAttempt(t *testing.T) {
	const runID = "run-1"
	store := newStoreWithRun(t, runID)
	scheduled := history.NewWorkflowTaskScheduledEvent(1000)
	id, inserted, err := execution.AppendEvent(store, runID, runID, runID, scheduled)
	if err != nil || !inserted || id == 0 {
		t.Fatalf("scheduled id=%d inserted=%v err=%v", id, inserted, err)
	}
	duplicateID, duplicateInserted, err := execution.AppendEvent(store, runID, runID, runID, scheduled)
	if err != nil || duplicateInserted || duplicateID != id {
		t.Fatalf("duplicate scheduled id=%d inserted=%v err=%v", duplicateID, duplicateInserted, err)
	}
	closed := history.NewWorkflowTaskCompletedEvent(id, "worker")
	if _, inserted, err := execution.AppendEvent(store, runID, runID, runID, closed); err != nil || !inserted {
		t.Fatalf("close inserted=%v err=%v", inserted, err)
	}
	if _, inserted, err := execution.AppendEvent(store, runID, runID, runID, closed); err != nil || inserted {
		t.Fatalf("duplicate close inserted=%v err=%v", inserted, err)
	}
	failed := history.NewActivityAttemptFailedEvent(history.ActivityAttemptFailedEvent{
		ScheduledEventID: 4, Attempt: 0,
		Failure: &protocol.Failure{Message: "boom"},
	})
	if _, _, err := execution.AppendEvent(store, runID, runID, runID, failed); err != nil {
		t.Fatal(err)
	}
	events, err := store.GetEvents(runID)
	if err != nil || len(events) != 3 || events[2].ActivityAttemptFailed == nil || events[2].ActivityAttemptFailed.Attempt != 1 {
		t.Fatalf("history=%+v err=%v", events, err)
	}
}
