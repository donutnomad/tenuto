package execution_test

import (
	"strings"
	"testing"

	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

func TestRunInteractionsCommitHistoryAndState(t *testing.T) {
	store := memory.NewExecutionManager()
	current := execution.NewRun(execution.NewRunOptions{
		ID: "run-1", WorkflowID: "flow", WorkflowType: "flow", WorkflowConfigVersion: "v1",
	})
	tree, branch, started, err := execution.PrepareStartedRun(current, "Flow", 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.RunInTx(func(tx execution.ExecutionTx) error {
		return execution.CommitStartedRun(tx, current, "", tree, branch, started)
	}); err != nil {
		t.Fatal(err)
	}

	var acceptedID, duplicateID int64
	if err := store.RunInTx(func(tx execution.ExecutionTx) error {
		if err := execution.UpsertSearchAttributes(tx, current, tree, branch, map[string]string{"order": "one", "": "ignored"}); err != nil {
			return err
		}
		if _, err := execution.RecordSignal(tx, current, tree, branch, "ready", "", map[string]any{"ok": true}); err != nil {
			return err
		}
		acceptedID, err = execution.AcceptUpdate(tx, current, tree, branch, "rename", "update-1", nil)
		if err != nil {
			return err
		}
		duplicateID, err = execution.AcceptUpdate(tx, current, tree, branch, "rename", "update-1", nil)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if acceptedID == 0 || duplicateID != acceptedID {
		t.Fatalf("accepted=%d duplicate=%d", acceptedID, duplicateID)
	}
	stored, err := store.Get(current.ID)
	if err != nil || stored.SearchAttributes["order"] != "one" || stored.SearchAttributes[""] != "" {
		t.Fatalf("search attributes=%+v err=%v", stored, err)
	}
	events, err := store.GetEvents(current.ID)
	if err != nil || len(events) != 4 || events[1].SearchAttributesUpserted == nil || events[2].WorkflowSignaled == nil || events[3].WorkflowUpdateAccepted == nil {
		t.Fatalf("history=%+v err=%v", events, err)
	}
	state, err := store.GetWorkflowExecutionState(current.ID)
	if err != nil || state.CancelRequested {
		t.Fatalf("unexpected cancellation request: state=%+v err=%v", state, err)
	}
	if err := execution.RequestCancellation(store, current, tree, branch, "stop"); err != nil {
		t.Fatal(err)
	}
	state, err = store.GetWorkflowExecutionState(current.ID)
	if err != nil || !state.CancelRequested {
		t.Fatalf("cancellation state=%+v err=%v", state, err)
	}
	current.Status = history.StatusCompleted
	if err := execution.EnsureActive(current); err == nil {
		t.Fatal("accepted terminal Run")
	}
}

func TestUpsertSearchAttributesRejectsReservedChangeVersionKey(t *testing.T) {
	store := memory.NewExecutionManager()
	current := execution.NewRun(execution.NewRunOptions{ID: "run-reserved", WorkflowID: "flow", WorkflowType: "flow", WorkflowConfigVersion: "v1"})
	tree, branch, started, err := execution.PrepareStartedRun(current, "Flow", 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.RunInTx(func(tx execution.ExecutionTx) error {
		return execution.CommitStartedRun(tx, current, "", tree, branch, started)
	}); err != nil {
		t.Fatal(err)
	}
	err = store.RunInTx(func(tx execution.ExecutionTx) error {
		return execution.UpsertSearchAttributes(tx, current, tree, branch, map[string]string{history.ChangeVersionSearchAttribute: `["x-1"]`})
	})
	if err == nil || !strings.Contains(err.Error(), "reserved key") {
		t.Fatalf("expected reserved key error, got: %v", err)
	}
}
