package executiontest

import (
	"errors"
	"testing"

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
