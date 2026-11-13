package persistence

import (
	"testing"

	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
)

func commitGormChangeVersion(t *testing.T, store *Store, value string) execution.WorkflowTaskCompletionResult {
	t.Helper()
	input := map[string]any{history.ChangeVersionSearchAttribute: value}
	run, err := store.Executions.Get("run-1")
	if err != nil {
		t.Fatal(err)
	}
	commit := execution.WorkflowTaskCommit{
		Run: run, TreeID: "run-1", BranchID: "run-1",
		CloseEvent:    history.NewWorkflowTaskCompletedEvent(2, "worker-1"),
		CommandEvents: []history.Event{history.NewSearchAttributesUpsertedEvent(input), history.NewWorkflowCompletedEvent(nil)},
		Commands:      []history.Command{{Type: history.CmdUpsertSearchAttributes, Input: input}, {Type: history.CmdCompleteWorkflow}},
	}
	var result execution.WorkflowTaskCompletionResult
	if err := store.Executions.RunInTx(func(tx execution.ExecutionTx) error {
		var commitErr error
		result, commitErr = execution.CommitWorkflowTask(tx, commit)
		return commitErr
	}); err != nil {
		t.Fatal(err)
	}
	return result
}

func TestGormCommitSyncsSearchAttributesAndKeepsTerminalProjection(t *testing.T) {
	store := newTestStore(t)
	createRunWF(t, store, "run-1", "wf-1", history.StatusRunning)

	if result := commitGormChangeVersion(t, store, "winner"); !result.ClosedInserted {
		t.Fatalf("winner result = %+v", result)
	}
	if result := commitGormChangeVersion(t, store, "loser"); result.ClosedInserted {
		t.Fatalf("loser result = %+v", result)
	}

	current, err := store.Executions.Get("run-1")
	if err != nil {
		t.Fatal(err)
	}
	if current.Status != history.StatusCompleted || current.SearchAttributes[history.ChangeVersionSearchAttribute] != "winner" {
		t.Fatalf("run = status %s attrs %v", current.Status, current.SearchAttributes)
	}
	state, err := store.Executions.GetWorkflowExecutionState("run-1")
	if err != nil {
		t.Fatal(err)
	}
	if state.Status != history.StatusCompleted || state.SearchAttributes[history.ChangeVersionSearchAttribute] != "winner" {
		t.Fatalf("state = status %s attrs %v", state.Status, state.SearchAttributes)
	}
}
