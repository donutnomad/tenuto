package executiontest

import (
	"errors"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
)

// HistoryBranchContract 断言 History 的 branch 语义：ForkBranch 产出可解析的 branch token，
// 事件幂等（dedupe key 与 Workflow Task close）的范围是 Run 自己的 branch，不含 reset 时 fork 来的前缀；
// open 的 Workflow Task 只有一个。
func HistoryBranchContract(t *testing.T, newManager func(t *testing.T) execution.ExecutionManager) {
	timer := func(id string) history.Event {
		return history.Event{TimerScheduled: &history.TimerScheduledEvent{TimerID: id}}
	}
	appendIdempotent := func(t *testing.T, manager execution.ExecutionManager, run *execution.Run, event history.Event) (int64, bool) {
		t.Helper()
		branch, err := execution.DeserializeBranchToken(run.BranchToken)
		if err != nil {
			t.Fatal(err)
		}
		id, inserted, err := manager.AppendIdempotent(run.ID, branch.TreeID, branch.BranchID, event)
		if err != nil {
			t.Fatal(err)
		}
		return id, inserted
	}

	t.Run("ForkedRunDedupesOnItsOwnBranch", func(t *testing.T) {
		manager := newManager(t)
		base := execution.NewRun(execution.NewRunOptions{ID: "run-base", WorkflowID: "wf-fork", WorkflowConfigVersion: "v1", CreatedAt: contractNow})
		createRun(t, manager, base)
		prefixID, _ := appendIdempotent(t, manager, base, timer("t-prefix"))
		appendIdempotent(t, manager, base, timer("t-after-fork"))

		forked, err := manager.ForkBranch(execution.ForkBranchRequest{SourceRunID: base.ID, ForkNodeID: prefixID, NewRunID: "run-reset", NewBranchID: "run-reset"})
		if err != nil {
			t.Fatal(err)
		}
		branch, err := execution.DeserializeBranchToken(forked.BranchToken)
		if err != nil {
			t.Fatalf("fork returned an unparsable branch token %q: %v", forked.BranchToken, err)
		}
		baseBranch, _ := execution.DeserializeBranchToken(base.BranchToken)
		if branch.TreeID != baseBranch.TreeID || branch.BranchID != "run-reset" || execution.GetBeginNodeID(branch) != prefixID+1 {
			t.Fatalf("forked branch = %+v, want tree %s, branch run-reset beginning at %d", branch, baseBranch.TreeID, prefixID+1)
		}
		reset := execution.NewRun(execution.NewRunOptions{ID: "run-reset", WorkflowID: "wf-fork", WorkflowConfigVersion: "v1", BranchToken: forked.BranchToken, CreatedAt: contractNow})
		createRun(t, manager, reset)

		// 前缀里已有的 dedupe key 在新 branch 上不命中；新 branch 上的重复写入命中。
		id, inserted := appendIdempotent(t, manager, reset, timer("t-prefix"))
		if !inserted || id != prefixID+1 {
			t.Fatalf("append of a prefix dedupe key on the forked branch = (%d, %v), want (%d, true)", id, inserted, prefixID+1)
		}
		if again, inserted := appendIdempotent(t, manager, reset, timer("t-prefix")); inserted || again != id {
			t.Fatalf("repeated append on the forked branch = (%d, %v), want (%d, false)", again, inserted, id)
		}
		events, err := manager.GetEvents(reset.ID)
		if err != nil {
			t.Fatal(err)
		}
		if len(events) != 2 || events[0].ID != prefixID || events[1].ID != id {
			t.Fatalf("forked run history = %+v, want the prefix event and the new event", events)
		}
	})

	// reset 带着分叉点上未结束的 Activity / Timer 提交：补写的关闭事件、新 Run 的 state 与新 Run 一起写入；
	// 提交失败时新 Run 与它的 state 都不存在。
	t.Run("ResetCommitsPendingWorkWithTheForkedRun", func(t *testing.T) {
		manager := newManager(t)
		base := execution.NewRun(execution.NewRunOptions{ID: "run-pending-base", WorkflowID: "wf-pending", WorkflowConfigVersion: "v1", CreatedAt: contractNow})
		createRun(t, manager, base)
		redispatch, _ := appendIdempotent(t, manager, base, history.NewActivityScheduledEvent(history.ActivityScheduledEvent{ActivityID: "redispatch", ActivityType: "T"}))
		interrupted, _ := appendIdempotent(t, manager, base, history.NewActivityScheduledEvent(history.ActivityScheduledEvent{ActivityID: "interrupted", ActivityType: "T"}))
		appendIdempotent(t, manager, base, history.NewActivityStartedEvent(history.ActivityStartedEvent{ScheduledEventID: interrupted, Attempt: 1, Stamp: 1}))
		pendingTimer := history.NewTimerScheduledEvent("t-pending", nil, int64(time.Hour/time.Millisecond))
		pendingTimer.Timestamp = contractNow
		timerID, _ := appendIdempotent(t, manager, base, pendingTimer)
		cutoff, _ := appendIdempotent(t, manager, base, history.NewWorkflowTaskScheduledEvent(0))
		for id, activityID := range map[int64]string{redispatch: "redispatch", interrupted: "interrupted"} {
			if err := manager.UpsertActivityState(&execution.ActivityState{RunID: base.ID, ScheduledEventID: id, ActivityID: activityID, ActivityType: "T",
				Attempt: 1, Stamp: 1, Status: execution.ActivityStatusEnums.Started, ScheduledTime: contractNow,
				LastHeartbeat: map[string]any{"offset": "9"}, CreatedAt: contractNow, UpdatedAt: contractNow}); err != nil {
				t.Fatal(err)
			}
		}
		events, err := manager.GetEvents(base.ID)
		if err != nil {
			t.Fatal(err)
		}
		activities, err := manager.ListActivityStates(base.ID)
		if err != nil {
			t.Fatal(err)
		}

		commit := func(newRunID, expectedCurrent string) error {
			forked, err := manager.ForkBranch(execution.ForkBranchRequest{SourceRunID: base.ID, ForkNodeID: cutoff, NewRunID: newRunID, NewBranchID: newRunID})
			if err != nil {
				t.Fatal(err)
			}
			branch, err := execution.DeserializeBranchToken(forked.BranchToken)
			if err != nil {
				t.Fatal(err)
			}
			resetAt := contractNow.Add(time.Minute)
			pending := execution.PlanResetPendingWork(execution.ResetPendingInput{
				Events: events, Cutoff: cutoff, BaseActivities: activities, NewRunID: newRunID, Reason: "fix", Now: resetAt,
			})
			next := execution.NewResetRun(base, newRunID, base.ID, forked.BranchToken, resetAt)
			return manager.RunInTx(func(tx execution.ExecutionTx) error {
				return execution.CommitResetRun(tx, execution.ResetRunCommit{
					NewRun: next, ExpectedCurrent: expectedCurrent, BaseRunID: base.ID, TreeID: branch.TreeID, BranchID: branch.BranchID,
					FailedEvent: history.NewWorkflowTaskFailedEvent(history.WorkflowTaskFailedEvent{ScheduledEventID: cutoff, Cause: execution.WorkflowTaskFailedCauseResetWorkflow}),
					Pending:     pending, Now: resetAt,
				})
			})
		}

		// 期望的 current 不是实际的 current：CAS 失败，整个事务回滚。
		createRun(t, manager, execution.NewRun(execution.NewRunOptions{ID: "run-pending-stale", WorkflowID: "wf-pending-other", WorkflowConfigVersion: "v1", CreatedAt: contractNow}))
		if err := commit("run-pending-conflict", "run-pending-stale"); !errors.Is(err, execution.ErrCurrentRunConflict) {
			t.Fatalf("reset with a stale current: err=%v, want ErrCurrentRunConflict", err)
		}
		if states, err := manager.ListActivityStates("run-pending-conflict"); err != nil || len(states) != 0 {
			t.Fatalf("a failed reset left activity states %+v (err %v)", states, err)
		}
		if states, err := manager.ListTimerStates("run-pending-conflict"); err != nil || len(states) != 0 {
			t.Fatalf("a failed reset left timer states %+v (err %v)", states, err)
		}

		current, err := manager.GetCurrentRunID(base.Namespace, base.WorkflowID)
		if err != nil && !errors.Is(err, execution.ErrNoCurrentRun) {
			t.Fatal(err)
		}
		if err := commit("run-pending-reset", current); err != nil {
			t.Fatal(err)
		}
		resetEvents, err := manager.GetEvents("run-pending-reset")
		if err != nil {
			t.Fatal(err)
		}
		last := resetEvents[len(resetEvents)-1]
		if last.ActivityFailed == nil || last.ActivityFailed.ScheduledEventID != interrupted || last.ActivityFailed.LastHeartbeatDetails["offset"] != "9" ||
			last.ActivityFailed.Failure == nil || last.ActivityFailed.Failure.Type != protocol.ResetWorkflowFailureType {
			t.Fatalf("reset history ends with %+v, want the interrupted activity's ResetWorkflowError", last)
		}
		states, err := manager.ListActivityStates("run-pending-reset")
		if err != nil || len(states) != 1 || states[0].ScheduledEventID != redispatch || !states[0].IsScheduled() || !states[0].ScheduledTime.Equal(contractNow.Add(time.Minute)) {
			t.Fatalf("reset run activity states = %+v (err %v), want the unstarted activity scheduled at the reset", states, err)
		}
		timer, found, err := manager.GetTimerState("run-pending-reset", timerID)
		if err != nil || !found || !timer.IsScheduled() || !timer.Deadline.Equal(contractNow.Add(time.Hour)) {
			t.Fatalf("reset run timer = %+v found=%v err=%v, want TimerScheduled time + delay", timer, found, err)
		}
	})

	t.Run("ForkOfMissingRun", func(t *testing.T) {
		manager := newManager(t)
		_, err := manager.ForkBranch(execution.ForkBranchRequest{SourceRunID: "run-missing", ForkNodeID: 1, NewRunID: "run-new", NewBranchID: "run-new"})
		if !errors.Is(err, execution.ErrRunNotFound) {
			t.Fatalf("fork of a missing run: err=%v, want execution.ErrRunNotFound", err)
		}
	})

	t.Run("SingleOpenWorkflowTask", func(t *testing.T) {
		manager := newManager(t)
		const runID = "run-wft"
		createContractRun(t, manager, runID)
		scheduled := history.Event{WorkflowTaskScheduled: &history.WorkflowTaskScheduledEvent{}}
		first, inserted, err := manager.AppendWorkflowTaskScheduled(runID, runID, runID, scheduled)
		if err != nil || !inserted {
			t.Fatalf("first schedule = (%d, %v, %v)", first, inserted, err)
		}
		if again, inserted, err := manager.AppendWorkflowTaskScheduled(runID, runID, runID, scheduled); err != nil || inserted || again != first {
			t.Fatalf("schedule while a task is open = (%d, %v, %v), want (%d, false, nil)", again, inserted, err, first)
		}
		closed := history.NewWorkflowTaskFailedEvent(history.WorkflowTaskFailedEvent{ScheduledEventID: first, Cause: "test"})
		if _, inserted, err := manager.AppendWorkflowTaskClosed(runID, runID, runID, closed); err != nil || !inserted {
			t.Fatalf("close = (%v, %v)", inserted, err)
		}
		if _, inserted, err := manager.AppendWorkflowTaskClosed(runID, runID, runID, closed); err != nil || inserted {
			t.Fatalf("second close of the same task = (%v, %v), want not inserted", inserted, err)
		}
		next, inserted, err := manager.AppendWorkflowTaskScheduled(runID, runID, runID, scheduled)
		if err != nil || !inserted || next <= first {
			t.Fatalf("schedule after close = (%d, %v, %v), want a new task", next, inserted, err)
		}
		state, err := manager.GetWorkflowExecutionState(runID)
		if err != nil || state.OpenWorkflowTaskEventID != next {
			t.Fatalf("open workflow task = %+v err=%v, want %d", state, err, next)
		}
	})
}
