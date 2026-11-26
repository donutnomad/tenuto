package execution_test

import (
	"errors"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol"
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

// resetPendingEvents 给事件按顺序编号（从 1 开始）。
func resetPendingEvents(events ...history.Event) []history.Event {
	for i := range events {
		events[i].ID = int64(i + 1)
	}
	return events
}

func resetBaseActivity(scheduledEventID int64, activityID string) *execution.ActivityState {
	scheduled := time.Unix(100, 0)
	return &execution.ActivityState{
		RunID: "base", ScheduledEventID: scheduledEventID, ActivityID: activityID, ActivityType: "Charge", TaskQueue: "payments",
		TargetWorkerIdentity: "worker-a", Attempt: 3, Stamp: 7, StartedEventID: execution.TransientEventID,
		Status: execution.ActivityStatusEnums.Started, ScheduledTime: scheduled, NextAttemptAt: &scheduled,
		ScheduleToStartTimeoutMs: 1000, StartToCloseTimeoutMs: 2000, ScheduleToCloseTimeoutMs: 3000, HeartbeatTimeoutMs: 400,
		RetryPolicy: &history.RetryPolicy{MaxAttempts: 5}, Input: map[string]any{"amount": 3},
		LastHeartbeat: map[string]any{"offset": 42}, TraceContext: map[string]string{"traceparent": "t"},
	}
}

// PlanResetPendingWork 按保留的 History 处理分叉点上未结束的 Activity 与 Timer。
func TestPlanResetPendingWork(t *testing.T) {
	now := time.Unix(500, 0)
	deadline := time.Unix(450, 0)
	started := func(scheduledEventID int64, attempt int) history.Event {
		return history.NewActivityStartedEvent(history.ActivityStartedEvent{ScheduledEventID: scheduledEventID, Attempt: attempt, WorkerIdentity: "worker-a"})
	}
	attemptFailed := func(scheduledEventID int64, attempt int) history.Event {
		return history.NewActivityAttemptFailedEvent(history.ActivityAttemptFailedEvent{ScheduledEventID: scheduledEventID, Attempt: attempt})
	}
	scheduled := history.NewActivityScheduledEvent(history.ActivityScheduledEvent{ActivityID: "a"})

	t.Run("Happy Path: an unstarted activity is redispatched from attempt 1 at the reset time", func(t *testing.T) {
		events := resetPendingEvents(scheduled)
		work, err := execution.PlanResetPendingWork(events, 1, []*execution.ActivityState{resetBaseActivity(1, "a")}, nil, "next", "fix", now)
		if err != nil {
			t.Fatal(err)
		}
		if len(work.Events) != 0 || len(work.Activities) != 1 {
			t.Fatalf("work = %+v, want one redispatched activity", work)
		}
		got := work.Activities[0]
		if got.RunID != "next" || got.ScheduledEventID != 1 || got.Attempt != 1 || got.Stamp != 1 ||
			got.Status != execution.ActivityStatusEnums.Scheduled || got.StartedEventID != execution.EmptyEventID ||
			!got.ScheduledTime.Equal(now) || got.NextAttemptAt != nil || got.LastHeartbeat != nil {
			t.Fatalf("new state = %+v, want a fresh first attempt scheduled at the reset time", got)
		}
		if got.ActivityType != "Charge" || got.TaskQueue != "payments" || got.TargetWorkerIdentity != "worker-a" ||
			got.ScheduleToCloseTimeoutMs != 3000 || got.HeartbeatTimeoutMs != 400 || got.RetryPolicy.MaxAttempts != 5 || got.Input["amount"] != 3 {
			t.Fatalf("new state = %+v, want the base state's static fields", got)
		}
		if deadline, ok := execution.ActivityTimeoutDeadline(got, protocol.TimeoutTypeScheduleToClose); !ok || !deadline.Equal(now.Add(3*time.Second)) {
			t.Fatalf("schedule-to-close deadline = %v, want 3s after the reset", deadline)
		}
	})

	t.Run("Edge Case: an activity waiting for its next attempt counts as unstarted", func(t *testing.T) {
		events := resetPendingEvents(scheduled, started(1, 1), attemptFailed(1, 1))
		work, err := execution.PlanResetPendingWork(events, 3, []*execution.ActivityState{resetBaseActivity(1, "a")}, nil, "next", "fix", now)
		if err != nil || len(work.Events) != 0 || len(work.Activities) != 1 || work.Activities[0].Attempt != 1 {
			t.Fatalf("work = %+v, err = %v; want a redispatch from attempt 1", work, err)
		}
	})

	t.Run("Edge Case: an activity reset after its start counts as unstarted", func(t *testing.T) {
		events := resetPendingEvents(scheduled, started(1, 1), history.Event{ActivityReset: &history.ActivityResetEvent{ScheduledEventID: 1}})
		work, err := execution.PlanResetPendingWork(events, 3, []*execution.ActivityState{resetBaseActivity(1, "a")}, nil, "next", "fix", now)
		if err != nil || len(work.Events) != 0 || len(work.Activities) != 1 {
			t.Fatalf("work = %+v, err = %v; want a redispatch", work, err)
		}
	})

	t.Run("Happy Path: a started activity fails with a non-retryable reset failure", func(t *testing.T) {
		events := resetPendingEvents(scheduled, attemptFailed(1, 1), started(1, 2))
		work, err := execution.PlanResetPendingWork(events, 3, []*execution.ActivityState{resetBaseActivity(1, "a")}, nil, "next", "bad deploy", now)
		if err != nil || len(work.Activities) != 0 || len(work.Events) != 1 || work.Events[0].ActivityFailed == nil {
			t.Fatalf("work = %+v, err = %v; want one ActivityFailed", work, err)
		}
		failed := work.Events[0].ActivityFailed
		if failed.ScheduledEventID != 1 || failed.StartedEventID != 3 || failed.Attempt != 2 || failed.Identity != "worker-a" ||
			failed.RetryState != "NonRetryableFailure" || failed.Error != "bad deploy" || failed.LastHeartbeatDetails["offset"] != 42 {
			t.Fatalf("failed = %+v, want the started attempt interrupted by reset", failed)
		}
		if failed.Failure == nil || failed.Failure.Type != protocol.ResetWorkflowFailureType || !failed.Failure.NonRetryable || failed.Failure.Message != "bad deploy" {
			t.Fatalf("failure = %+v, want a non-retryable ResetWorkflowError", failed.Failure)
		}
	})

	t.Run("Edge Case: a cancel-requested activity whose start was transient closes as cancelled", func(t *testing.T) {
		events := resetPendingEvents(scheduled, started(1, 1), attemptFailed(1, 1),
			history.Event{ActivityCancelRequested: &history.ActivityCancelRequestedEvent{ScheduledEventID: 1}})
		work, err := execution.PlanResetPendingWork(events, 4, []*execution.ActivityState{resetBaseActivity(1, "a")}, nil, "next", "fix", now)
		if err != nil || len(work.Activities) != 0 || len(work.Events) != 1 || work.Events[0].ActivityCancelled == nil {
			t.Fatalf("work = %+v, err = %v; want one ActivityCancelled", work, err)
		}
		if cancelled := work.Events[0].ActivityCancelled; cancelled.ScheduledEventID != 1 || cancelled.CancelRequestedEventID != 4 || cancelled.Attempt != 2 {
			t.Fatalf("cancelled = %+v", cancelled)
		}
	})

	t.Run("Edge Case: closed activities and events after the cutoff are ignored", func(t *testing.T) {
		events := resetPendingEvents(scheduled, history.Event{ActivityCompleted: &history.ActivityCompletedEvent{ScheduledEventID: 1}},
			history.NewActivityScheduledEvent(history.ActivityScheduledEvent{ActivityID: "b"}))
		work, err := execution.PlanResetPendingWork(events, 2, nil, nil, "next", "fix", now)
		if err != nil || len(work.Events) != 0 || len(work.Activities) != 0 || len(work.Timers) != 0 {
			t.Fatalf("work = %+v, err = %v; want nothing pending", work, err)
		}
	})

	t.Run("Happy Path: a pending timer keeps its deadline; a fired one is dropped", func(t *testing.T) {
		events := resetPendingEvents(history.NewTimerScheduledEvent("t1", nil, 1000), history.NewTimerScheduledEvent("t2", nil, 1000),
			history.Event{TimerFired: &history.TimerFiredEvent{ScheduledEventID: 2}})
		base := []*execution.TimerState{
			{RunID: "base", ScheduledEventID: 1, TimerID: "t1", Kind: execution.TimerKindEnums.UserTimer, Deadline: deadline, Status: execution.TimerStatusEnums.RunClosed},
			{RunID: "base", ScheduledEventID: 2, TimerID: "t2", Kind: execution.TimerKindEnums.UserTimer, Deadline: deadline, Status: execution.TimerStatusEnums.Fired},
		}
		work, err := execution.PlanResetPendingWork(events, 3, nil, base, "next", "fix", now)
		if err != nil || len(work.Timers) != 1 {
			t.Fatalf("work = %+v, err = %v; want one timer", work, err)
		}
		if got := work.Timers[0]; got.RunID != "next" || got.ScheduledEventID != 1 || got.TimerID != "t1" || !got.Deadline.Equal(deadline) || got.Status != execution.TimerStatusEnums.Scheduled {
			t.Fatalf("timer = %+v, want t1 scheduled at its original deadline", got)
		}
	})

	t.Run("Error Case: a pending activity without base state", func(t *testing.T) {
		if _, err := execution.PlanResetPendingWork(resetPendingEvents(scheduled), 1, nil, nil, "next", "fix", now); err == nil {
			t.Fatal("expected an error for a missing activity state")
		}
	})
}

// Happy Path: 补写的关闭事件写在 reset 标记之后、重新应用的输入之前，保留的 Activity 与 Timer state 建在新 Run 上，
// 全部在同一个事务里提交。
func TestCommitResetRunWritesPendingWork(t *testing.T) {
	store := memory.NewExecutionManager()
	base := execution.NewRun(execution.NewRunOptions{ID: "base", WorkflowID: "flow", WorkflowType: "flow", WorkflowConfigVersion: "v1"})
	tree, branch, startedEvent, err := execution.PrepareStartedRun(base, "Flow", 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.RunInTx(func(tx execution.ExecutionTx) error {
		return execution.CommitStartedRun(tx, base, "", tree, branch, startedEvent)
	}); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	next := execution.NewResetRun(base, "reset", base.ID, nil, now)
	activity := resetBaseActivity(10, "a")
	activity.RunID, activity.Status = next.ID, execution.ActivityStatusEnums.Scheduled
	pending := execution.ResetPendingWork{
		Events:     []history.Event{history.NewActivityFailedEvent(history.ActivityFailedEvent{ScheduledEventID: 11, Failure: &protocol.Failure{Type: protocol.ResetWorkflowFailureType}})},
		Activities: []*execution.ActivityState{activity},
		Timers:     []*execution.TimerState{{RunID: next.ID, ScheduledEventID: 12, TimerID: "t", Kind: execution.TimerKindEnums.UserTimer, Deadline: now, Status: execution.TimerStatusEnums.Scheduled}},
	}
	if err := store.RunInTx(func(tx execution.ExecutionTx) error {
		return execution.CommitResetRun(tx, execution.ResetRunCommit{
			NewRun: next, ExpectedCurrent: base.ID, BaseRunID: base.ID, TreeID: next.ID, BranchID: next.ID,
			FailedEvent:   history.NewWorkflowTaskFailedEvent(history.WorkflowTaskFailedEvent{Cause: execution.WorkflowTaskFailedCauseResetWorkflow}),
			Pending:       pending,
			ReapplyEvents: []history.Event{history.NewWorkflowSignaledEvent("approve", nil)},
			Now:           now,
		})
	}); err != nil {
		t.Fatal(err)
	}
	events, err := store.GetEvents(next.ID)
	if err != nil || len(events) != 3 || events[0].WorkflowTaskFailed == nil || events[1].ActivityFailed == nil || events[2].WorkflowSignaled == nil {
		t.Fatalf("reset history = %+v, err = %v; want marker, activity failure, signal", events, err)
	}
	if state, found, err := store.GetActivityState(next.ID, 10); err != nil || !found || !state.IsScheduled() {
		t.Fatalf("activity state = %+v, found = %v, err = %v", state, found, err)
	}
	if state, found, err := store.GetTimerState(next.ID, 12); err != nil || !found || !state.IsScheduled() {
		t.Fatalf("timer state = %+v, found = %v, err = %v", state, found, err)
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
