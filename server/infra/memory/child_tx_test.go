package memory

import (
	"errors"
	"testing"

	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/biz/domain/execution/executiontest"
)

// Happy Path: RunInTx 内 append ChildWorkflowStarted + upsert ChildWorkflowState 全成功，提交后两者都可见。
func TestExecutionManagerChildCommits(t *testing.T) {
	mgr := NewExecutionManager()
	_ = mgr.RunInTx(func(tx execution.ExecutionTx) error {
		return tx.CreateRun(&execution.Run{ID: "parent-1", BranchToken: executiontest.BranchToken("parent-1"), Status: history.StatusRunning})
	})
	err := mgr.RunInTx(func(tx execution.ExecutionTx) error {
		if _, _, e := tx.AppendIdempotent("parent-1", "parent-1", "parent-1",
			history.NewChildWorkflowStartedEvent("child-flow", "child_1", "child-run-1", "v1", map[string]any{"order_id": "o-1"})); e != nil {
			return e
		}
		return tx.UpsertChildWorkflowState(&execution.ChildWorkflowState{
			RunID: "parent-1", ScheduledEventID: 1, ChildID: "child_1", WorkflowID: "child-flow",
			Status: execution.ChildWorkflowStatusEnums.Initiated,
		})
	})
	if err != nil {
		t.Fatal(err)
	}
	if events, _ := mgr.GetEvents("parent-1"); len(events) != 1 {
		t.Fatalf("events = %d, want 1", len(events))
	}
	if _, ok, _ := mgr.GetChildWorkflowState("parent-1", 1); !ok {
		t.Fatal("child state not committed")
	}
}

// Error Case: fn 返回 error → ChildWorkflowStarted 与 ChildWorkflowState 都回滚（互不残留）。
func TestExecutionManagerChildRollsBack(t *testing.T) {
	mgr := NewExecutionManager()
	_ = mgr.RunInTx(func(tx execution.ExecutionTx) error {
		return tx.CreateRun(&execution.Run{ID: "parent-1", BranchToken: executiontest.BranchToken("parent-1"), Status: history.StatusRunning})
	})
	wantErr := errors.New("boom")
	err := mgr.RunInTx(func(tx execution.ExecutionTx) error {
		_, _, _ = tx.AppendIdempotent("parent-1", "parent-1", "parent-1",
			history.NewChildWorkflowStartedEvent("child-flow", "child_1", "child-run-1", "v1", nil))
		_ = tx.UpsertChildWorkflowState(&execution.ChildWorkflowState{RunID: "parent-1", ScheduledEventID: 1, ChildID: "child_1", WorkflowID: "child-flow", Status: execution.ChildWorkflowStatusEnums.Initiated})
		return wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("err = %v, want boom", err)
	}
	if events, _ := mgr.GetEvents("parent-1"); len(events) != 0 {
		t.Fatalf("event leaked on rollback: %d", len(events))
	}
	if _, ok, _ := mgr.GetChildWorkflowState("parent-1", 1); ok {
		t.Fatal("child state leaked on rollback")
	}
}

// Edge Case: GetChildWorkflowStateForUpdate 在同事务内读到本事务未提交的 upsert（先看 pending）。
func TestChildStateForUpdateSeesPending(t *testing.T) {
	mgr := NewExecutionManager()
	_ = mgr.RunInTx(func(tx execution.ExecutionTx) error {
		return tx.CreateRun(&execution.Run{ID: "parent-1", BranchToken: executiontest.BranchToken("parent-1"), Status: history.StatusRunning})
	})
	err := mgr.RunInTx(func(tx execution.ExecutionTx) error {
		if e := tx.UpsertChildWorkflowState(&execution.ChildWorkflowState{RunID: "parent-1", ScheduledEventID: 1, ChildID: "child_1", WorkflowID: "child-flow", Status: execution.ChildWorkflowStatusEnums.Completed}); e != nil {
			return e
		}
		st, ok, e := tx.GetChildWorkflowStateForUpdate("parent-1", 1)
		if e != nil {
			return e
		}
		if !ok || st.Status != execution.ChildWorkflowStatusEnums.Completed {
			t.Fatalf("for-update did not see pending upsert: ok=%v st=%+v", ok, st)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
