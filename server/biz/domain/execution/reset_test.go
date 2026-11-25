package execution_test

import (
	"errors"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

func TestCommitResetRunMovesCurrentAndRecordsBaseLink(t *testing.T) {
	store := memory.NewExecutionManager()
	base := execution.NewRun(execution.NewRunOptions{
		ID: "base", WorkflowID: "flow", WorkflowType: "flow", WorkflowConfigVersion: "config-v1",
	})
	tree, branch, started, err := execution.PrepareStartedRun(base, "Flow", 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.RunInTx(func(tx execution.ExecutionTx) error {
		return execution.CommitStartedRun(tx, base, "", tree, branch, started)
	}); err != nil {
		t.Fatal(err)
	}

	next := execution.NewResetRun(base, "reset", base.ID, nil, time.Now())
	marker := history.NewWorkflowTaskFailedEvent(history.WorkflowTaskFailedEvent{
		Cause: "reset_workflow", BaseRunID: base.ID, ResetRunID: next.ID,
	})
	request := execution.ResetRunCommit{
		NewRun: next, ExpectedCurrent: base.ID, BaseRunID: base.ID,
		TreeID: next.ID, BranchID: next.ID, FailedEvent: marker,
	}
	if err := store.RunInTx(func(tx execution.ExecutionTx) error {
		return execution.CommitResetRun(tx, request)
	}); err != nil {
		t.Fatal(err)
	}
	current, err := store.GetCurrentRunID(base.Namespace, base.WorkflowID)
	if err != nil || current != next.ID {
		t.Fatalf("current=%q err=%v", current, err)
	}
	storedBase, err := store.Get(base.ID)
	if err != nil || storedBase.ResetRunID != next.ID {
		t.Fatalf("base=%+v err=%v", storedBase, err)
	}
	events, err := store.GetEvents(next.ID)
	if err != nil || len(events) != 1 || events[0].WorkflowTaskFailed == nil {
		t.Fatalf("reset history=%+v err=%v", events, err)
	}
	if next.WorkflowConfigVersion != base.WorkflowConfigVersion {
		t.Fatalf("reset version=%+v", next)
	}

	conflict := execution.NewResetRun(base, "conflict", base.ID, nil, time.Now())
	request.NewRun = conflict
	if err := store.RunInTx(func(tx execution.ExecutionTx) error {
		return execution.CommitResetRun(tx, request)
	}); !errors.Is(err, execution.ErrCurrentRunConflict) {
		t.Fatalf("conflict error=%v", err)
	}
	if _, err := store.Get(conflict.ID); err == nil {
		t.Fatal("conflicting reset Run persisted")
	}
}

func TestResetReapplyEventsSelectsExternalInputs(t *testing.T) {
	events := []history.Event{
		{ID: 1, WorkflowSignaled: &history.WorkflowSignaledEvent{}},
		{ID: 2, WorkflowSignaled: &history.WorkflowSignaledEvent{}},
		{ID: 3, WorkflowUpdateAccepted: &history.WorkflowUpdateAcceptedEvent{}},
		{ID: 4, WorkflowTaskScheduled: &history.WorkflowTaskScheduledEvent{}},
	}
	selected := execution.ResetReapplyEvents(events, 1, []string{string(execution.ResetReapplyExcludeTypeEnums.Signal)})
	if len(selected) != 1 || selected[0].ID != 3 {
		t.Fatalf("selected=%+v", selected)
	}
}

func TestResetPendingBoundary(t *testing.T) {
	for _, tc := range []struct {
		name    string
		events  []history.Event
		cut     int64
		blocked bool
	}{
		{"parallel activity", []history.Event{{ID: 1, ActivityScheduled: &history.ActivityScheduledEvent{}}, {ID: 2, ActivityScheduled: &history.ActivityScheduledEvent{}}, {ID: 3, ActivityCompleted: &history.ActivityCompletedEvent{ScheduledEventID: 1}}, {ID: 5, ActivityCompleted: &history.ActivityCompletedEvent{ScheduledEventID: 2}}}, 4, true},
		{"activities closed", []history.Event{{ID: 1, ActivityScheduled: &history.ActivityScheduledEvent{}}, {ID: 2, ActivityCompleted: &history.ActivityCompletedEvent{ScheduledEventID: 1}}}, 3, false},
		{"timer pending", []history.Event{{ID: 1, TimerScheduled: &history.TimerScheduledEvent{}}, {ID: 3, TimerFired: &history.TimerFiredEvent{ScheduledEventID: 1}}}, 2, true},
		{"timer fired", []history.Event{{ID: 1, TimerScheduled: &history.TimerScheduledEvent{}}, {ID: 2, TimerFired: &history.TimerFiredEvent{ScheduledEventID: 1}}}, 3, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := execution.CheckResetPendingEvents(tc.events, tc.cut)
			if (err != nil) != tc.blocked {
				t.Fatalf("err=%v blocked=%v", err, tc.blocked)
			}
		})
	}
}

func TestNewResetRunKeepsAttemptAndRestartsDeadlines(t *testing.T) {
	deadline := time.Now().Add(time.Hour)
	runDeadline := time.Now().Add(time.Minute)
	base := execution.NewRun(execution.NewRunOptions{
		ID: "base", Namespace: "tenant-a", WorkflowID: "flow", WorkflowType: "Flow",
		WorkflowConfigVersion: "v1", FirstRunID: "first", DeadlineAt: &deadline,
		Attempt: 3, Initiator: execution.InitiatorEnums.Retry,
		ParentRunID: "parent", ParentChildID: "child-1", ParentScheduledEventID: 7,
	})
	base.RunDeadlineAt = &runDeadline
	base.TargetWorkerIdentity = "worker-a"
	now := time.Now()
	token := []byte("fork-token")

	next := execution.NewResetRun(base, "reset", base.ID, token, now)

	if next.ID != "reset" || next.FirstRunID != "first" || next.ContinuedRunID != base.ID ||
		next.Namespace != "tenant-a" || next.WorkflowType != "Flow" || string(next.BranchToken) != "fork-token" ||
		next.Status != history.StatusRunning || !next.CreatedAt.Equal(now) || next.TargetWorkerIdentity != "worker-a" {
		t.Fatalf("reset identity=%+v", next)
	}
	if next.ParentRunID != "parent" || next.ParentChildID != "child-1" || next.ParentScheduledEventID != 7 {
		t.Fatalf("reset parent link=%+v", next)
	}
	if next.Attempt != 3 || next.Initiator != execution.InitiatorEnums.Retry {
		t.Fatalf("attempt=%d initiator=%q, want 3 Retry", next.Attempt, next.Initiator)
	}
	if next.DeadlineAt != nil || next.RunDeadlineAt != nil {
		t.Fatalf("deadlines=%v/%v, want fresh execution and run timeouts", next.DeadlineAt, next.RunDeadlineAt)
	}
}

// Happy Path: reset 点之后的外部输入与 reset 标记在同一事务写入新 Run；父 Run 仍打开的子关联改指新 Run。
// Edge Case: 已终态的子关联保持原样。
func TestCommitResetRunReappliesInputsAndRepointsParentLink(t *testing.T) {
	for _, tc := range []struct {
		name        string
		linkStatus  execution.ChildWorkflowStatus
		wantChildID string
	}{
		{"open child link follows the reset run", execution.ChildWorkflowStatusEnums.Started, "reset"},
		{"closed child link keeps the run that closed it", execution.ChildWorkflowStatusEnums.Completed, "base"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := memory.NewExecutionManager()
			parent := execution.NewRun(execution.NewRunOptions{ID: "parent", WorkflowID: "parent-flow", WorkflowType: "parent-flow", WorkflowConfigVersion: "v1"})
			base := execution.NewRun(execution.NewRunOptions{
				ID: "base", WorkflowID: "child-flow", WorkflowType: "child-flow", WorkflowConfigVersion: "v1",
				ParentRunID: parent.ID, ParentChildID: "child-1", ParentScheduledEventID: 5,
			})
			for _, run := range []*execution.Run{parent, base} {
				tree, branch, started, err := execution.PrepareStartedRun(run, "Flow", 0)
				if err != nil {
					t.Fatal(err)
				}
				if err := store.RunInTx(func(tx execution.ExecutionTx) error {
					return execution.CommitStartedRun(tx, run, "", tree, branch, started)
				}); err != nil {
					t.Fatal(err)
				}
			}
			if err := store.UpsertChildWorkflowState(&execution.ChildWorkflowState{
				RunID: parent.ID, ScheduledEventID: 5, ChildID: "child-1", ChildRunID: base.ID,
				WorkflowType: "child-flow", Status: tc.linkStatus,
			}); err != nil {
				t.Fatal(err)
			}

			now := time.Now()
			next := execution.NewResetRun(base, "reset", base.ID, nil, now)
			if err := store.RunInTx(func(tx execution.ExecutionTx) error {
				return execution.CommitResetRun(tx, execution.ResetRunCommit{
					NewRun: next, ExpectedCurrent: base.ID, BaseRunID: base.ID,
					TreeID: next.ID, BranchID: next.ID,
					FailedEvent: history.NewWorkflowTaskFailedEvent(history.WorkflowTaskFailedEvent{
						Cause: execution.WorkflowTaskFailedCauseResetWorkflow, BaseRunID: base.ID, ResetRunID: next.ID,
					}),
					ReapplyEvents: []history.Event{history.NewWorkflowSignaledEvent("approve", map[string]any{"by": "ops"})},
					Now:           now,
				})
			}); err != nil {
				t.Fatal(err)
			}

			events, err := store.GetEvents(next.ID)
			if err != nil || len(events) != 2 || events[0].WorkflowTaskFailed == nil || events[1].WorkflowSignaled == nil {
				t.Fatalf("reset history = %+v, err = %v; want the reset marker followed by the signal", events, err)
			}
			link, found, err := store.GetChildWorkflowState(parent.ID, 5)
			if err != nil || !found || link.ChildRunID != tc.wantChildID || link.Status != tc.linkStatus {
				t.Fatalf("parent child link = %+v, found = %v, err = %v; want child run %q", link, found, err, tc.wantChildID)
			}
		})
	}
}
