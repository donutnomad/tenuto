package execution_test

import (
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

func TestPropagateChildCloseRecoversMissingStateOnce(t *testing.T) {
	store := memory.NewExecutionManager()
	parent := execution.NewRun(execution.NewRunOptions{ID: "parent", WorkflowID: "flow", WorkflowType: "flow", WorkflowConfigVersion: "v1"})
	tree, branch, started, err := execution.PrepareStartedRun(parent, "Flow", 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.RunInTx(func(tx execution.ExecutionTx) error {
		return execution.CommitStartedRun(tx, parent, "", tree, branch, started)
	}); err != nil {
		t.Fatal(err)
	}
	child := &execution.Run{
		ID: "child-run", ParentRunID: parent.ID, ParentChildID: "child-1",
		ParentScheduledEventID: 2, WorkflowID: "subflow", WorkflowConfigVersion: "v2",
	}
	request := execution.ChildClose{Child: child, ParentTreeID: tree, ParentBranchID: branch,
		Type: history.EventChildWorkflowCompleted, Output: map[string]any{"ok": true}, Now: time.Now()}
	for attempt := range 2 {
		var propagated bool
		if err := store.RunInTx(func(tx execution.ExecutionTx) error {
			var closeErr error
			propagated, closeErr = execution.PropagateChildClose(tx, request)
			return closeErr
		}); err != nil || propagated != (attempt == 0) {
			t.Fatalf("attempt=%d propagated=%v err=%v", attempt, propagated, err)
		}
	}
	state, found, err := store.GetChildWorkflowState(parent.ID, child.ParentScheduledEventID)
	if err != nil || !found || state.Status != execution.ChildWorkflowStatusEnums.Completed || state.Output["ok"] != true {
		t.Fatalf("state=%+v found=%v err=%v", state, found, err)
	}
	events, err := store.GetEvents(parent.ID)
	if err != nil || len(events) != 2 || events[1].ChildWorkflowCompleted == nil {
		t.Fatalf("history=%+v err=%v", events, err)
	}
}

// Edge Case: 父 Run 已结束时，子 Run 的终态只更新子关联状态，父 History 不再增长。
func TestPropagateChildCloseLeavesTerminalParentHistoryUntouched(t *testing.T) {
	store := memory.NewExecutionManager()
	parent := execution.NewRun(execution.NewRunOptions{ID: "parent", WorkflowID: "flow", WorkflowType: "flow", WorkflowConfigVersion: "v1"})
	tree, branch, started, err := execution.PrepareStartedRun(parent, "Flow", 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.RunInTx(func(tx execution.ExecutionTx) error {
		if err := execution.CommitStartedRun(tx, parent, "", tree, branch, started); err != nil {
			return err
		}
		_, err := execution.Terminate(tx, parent, tree, branch, "operator")
		return err
	}); err != nil {
		t.Fatal(err)
	}
	child := &execution.Run{
		ID: "child-run", ParentRunID: parent.ID, ParentChildID: "child-1",
		ParentScheduledEventID: 2, WorkflowID: "subflow-1", WorkflowType: "subflow", WorkflowConfigVersion: "v2",
	}
	var propagated bool
	if err := store.RunInTx(func(tx execution.ExecutionTx) error {
		var closeErr error
		propagated, closeErr = execution.PropagateChildClose(tx, execution.ChildClose{
			Child: child, ParentTreeID: tree, ParentBranchID: branch,
			Type: history.EventChildWorkflowCompleted, Output: map[string]any{"ok": true}, Now: time.Now(),
		})
		return closeErr
	}); err != nil {
		t.Fatal(err)
	}
	if propagated {
		t.Fatal("a terminal parent has nothing to process; propagated must be false")
	}
	events, err := store.GetEvents(parent.ID)
	if err != nil || len(events) != 2 || events[1].WorkflowTerminated == nil {
		t.Fatalf("parent history = %+v, err = %v; want WorkflowStarted and WorkflowTerminated only", events, err)
	}
	state, found, err := store.GetChildWorkflowState(parent.ID, child.ParentScheduledEventID)
	if err != nil || !found || state.Status != execution.ChildWorkflowStatusEnums.Completed || state.WorkflowType != "subflow" {
		t.Fatalf("child state = %+v, found = %v, err = %v", state, found, err)
	}
}
