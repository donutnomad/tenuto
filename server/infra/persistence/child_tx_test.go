package persistence

import (
	"errors"
	"testing"

	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
)

// Happy Path: gorm RunInTx 提交后 ChildWorkflowStarted + ChildWorkflowState 都落库。
func TestStoreRunInTxChildCommits(t *testing.T) {
	store := newTestStore(t)
	createTestRun(t, store, "parent-1")
	err := store.Executions.RunInTx(func(tx execution.ExecutionTx) error {
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
	if events, _ := store.Executions.GetEvents("parent-1"); len(events) == 0 {
		t.Fatal("event not committed")
	}
	if _, ok, _ := store.Executions.GetChildWorkflowState("parent-1", 1); !ok {
		t.Fatal("child state not committed")
	}
}

// Error Case: fn 返回 error → DB 事务回滚，ChildWorkflowStarted 与 ChildWorkflowState 都不落库。
func TestStoreRunInTxChildRollsBack(t *testing.T) {
	store := newTestStore(t)
	createTestRun(t, store, "parent-1")
	wantErr := errors.New("boom")
	err := store.Executions.RunInTx(func(tx execution.ExecutionTx) error {
		_, _, _ = tx.AppendIdempotent("parent-1", "parent-1", "parent-1", history.NewChildWorkflowStartedEvent("child-flow", "child_1", "child-run-1", "v1", nil))
		_ = tx.UpsertChildWorkflowState(&execution.ChildWorkflowState{RunID: "parent-1", ScheduledEventID: 1, ChildID: "child_1", WorkflowID: "child-flow", Status: execution.ChildWorkflowStatusEnums.Initiated})
		return wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("err = %v", err)
	}
	if events, _ := store.Executions.GetEvents("parent-1"); len(events) != 0 {
		t.Fatalf("event leaked on rollback: %d", len(events))
	}
	if _, ok, _ := store.Executions.GetChildWorkflowState("parent-1", 1); ok {
		t.Fatal("child state leaked on rollback")
	}
}
