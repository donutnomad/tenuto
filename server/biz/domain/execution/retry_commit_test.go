package execution_test

import (
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

func TestPrepareStartedRunRecordsStartSearchAttributes(t *testing.T) {
	empty := execution.NewRun(execution.NewRunOptions{ID: "run-empty", WorkflowID: "payment", WorkflowConfigVersion: "v1"})
	_, _, started, err := execution.PrepareStartedRun(empty, "Payment", 0)
	if err != nil {
		t.Fatal(err)
	}
	if attrs := started.WorkflowStarted.SearchAttributes; len(attrs) != 0 {
		t.Fatalf("empty start attrs = %#v, want empty", attrs)
	}

	old := execution.NewRun(execution.NewRunOptions{ID: "run-1", WorkflowID: "payment", WorkflowConfigVersion: "v1",
		SearchAttributes: map[string]string{"env": "prod", history.ChangeVersionSearchAttribute: `["foo-1"]`}})
	continued := execution.NewContinuedRun(old, "run-2", "", "payment", "v1", nil, time.Now())
	_, _, started, err = execution.PrepareStartedRun(continued, "Payment", 0)
	if err != nil {
		t.Fatal(err)
	}
	continued.SearchAttributes["env"] = "mutated"
	attrs := started.WorkflowStarted.SearchAttributes
	if attrs["env"] != "prod" || attrs[history.ChangeVersionSearchAttribute] != `["foo-1"]` {
		t.Fatalf("continue-as-new start attrs = %v, want old run's current attrs", attrs)
	}
}

func TestNewRetryRunUsesStartSearchAttributes(t *testing.T) {
	old := execution.NewRun(execution.NewRunOptions{ID: "run-1", WorkflowID: "payment", WorkflowConfigVersion: "v1",
		SearchAttributes: map[string]string{"env": "prod", "biz": "x", history.ChangeVersionSearchAttribute: `["foo-1"]`}})
	now := time.Now()
	if next := execution.NewRetryRun(old, "run-2", now, map[string]string{}); len(next.SearchAttributes) != 0 {
		t.Fatalf("retry with empty start attrs = %v", next.SearchAttributes)
	}
	if next := execution.NewRetryRun(old, "run-3", now, map[string]string{"env": "prod"}); len(next.SearchAttributes) != 1 || next.SearchAttributes["env"] != "prod" {
		t.Fatalf("retry with start attrs = %v", next.SearchAttributes)
	}
	if old.SearchAttributes[history.ChangeVersionSearchAttribute] == "" {
		t.Fatalf("old run attrs mutated: %v", old.SearchAttributes)
	}
}

func TestCommitRetryRunMovesCurrentPointerWithHistoryAndTimer(t *testing.T) {
	store := memory.NewExecutionManager()
	old := execution.NewRun(execution.NewRunOptions{ID: "run-1", WorkflowID: "payment", WorkflowConfigVersion: "v1", Input: map[string]any{"order": "one"}})
	oldTree, oldBranch, oldStarted, err := execution.PrepareStartedRun(old, "Payment", 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.RunInTx(func(tx execution.ExecutionTx) error {
		return execution.CommitStartedRun(tx, old, "", oldTree, oldBranch, oldStarted)
	}); err != nil {
		t.Fatal(err)
	}

	now := time.Date(2026, time.October, 27, 10, 0, 0, 0, time.UTC)
	next := execution.NewRetryRun(old, "run-2", now, nil)
	_, _, started, err := execution.PrepareStartedRun(next, "Payment", 0)
	if err != nil {
		t.Fatal(err)
	}
	timer := &execution.TimerState{RunID: next.ID, ScheduledEventID: 123, Kind: execution.TimerKindEnums.RetryBackoff, Status: execution.TimerStatusEnums.Scheduled, Deadline: now.Add(time.Second)}
	var committed bool
	if err := store.RunInTx(func(tx execution.ExecutionTx) error {
		var commitErr error
		committed, commitErr = execution.CommitRetryRun(tx, execution.RetryRunCommit{
			OldRunID: old.ID, OldTreeID: oldTree, OldBranchID: oldBranch,
			NewRun: next, StartedEvent: started, TimerState: timer,
			Failure: protocol.Failure{Message: "transient"}, RetryState: "InProgress",
		})
		return commitErr
	}); err != nil || !committed {
		t.Fatalf("committed=%v err=%v", committed, err)
	}
	current, err := store.GetCurrentRunID(old.Namespace, old.WorkflowID)
	if err != nil || current != next.ID {
		t.Fatalf("current=%q err=%v", current, err)
	}
	storedOld, err := store.Get(old.ID)
	if err != nil || storedOld.Status != history.StatusFailed {
		t.Fatalf("old run=%+v err=%v", storedOld, err)
	}
	events, err := store.GetEvents(old.ID)
	if err != nil || len(events) != 2 || events[1].WorkflowFailed == nil || events[1].WorkflowFailed.NewExecutionRunID != next.ID {
		t.Fatalf("old history=%+v err=%v", events, err)
	}
	events, err = store.GetEvents(next.ID)
	if err != nil || len(events) != 1 || events[0].WorkflowStarted == nil {
		t.Fatalf("retry history=%+v err=%v", events, err)
	}
	storedTimer, found, err := store.GetTimerState(next.ID, timer.ScheduledEventID)
	if err != nil || !found || storedTimer.Kind != execution.TimerKindEnums.RetryBackoff {
		t.Fatalf("timer=%+v found=%v err=%v", storedTimer, found, err)
	}
	if next.WorkflowConfigVersion != old.WorkflowConfigVersion || next.Attempt != 2 {
		t.Fatalf("retry identity=%+v", next)
	}
}
