package execution

import (
	"testing"

	"github.com/donutnomad/tenuto/protocol/history"
)

type statusTestStore struct {
	events []history.Event
}

func (s *statusTestStore) GetEvents(string) ([]history.Event, error) { return s.events, nil }

func (s *statusTestStore) GetWorkflowExecutionState(runID string) (*WorkflowExecutionState, error) {
	events := make([]history.Event, len(s.events))
	for i, event := range s.events {
		event.ID = int64(i + 1)
		events[i] = event
	}
	return BuildWorkflowExecutionState(&Run{ID: runID, Status: history.StatusRunning}, events)
}

// Happy Path：CurrentStatus 读 execution state 投影的状态；ReconcileRunStatus 把 History 里终态事件的状态与输出写到 Run 上。
func TestRuntimeReconcilesTerminalHistory(t *testing.T) {
	store := &statusTestStore{events: []history.Event{
		history.NewWorkflowStartedEvent(nil, "run-1", "", "workflow", "Workflow", 0),
		history.NewWorkflowCompletedEvent(map[string]any{"result": "done"}),
	}}
	run := &Run{ID: "run-1", Status: history.StatusRunning}
	runtime := NewRuntime(store)
	if status, err := runtime.CurrentStatus(run.ID); err != nil || status != history.StatusCompleted {
		t.Fatalf("current status = %s, err=%v", status, err)
	}
	event, status, found, err := runtime.StatusEventFromHistory(run.ID)
	if err != nil || !found || status != history.StatusCompleted {
		t.Fatalf("status event: status=%s found=%t err=%v", status, found, err)
	}
	if err := ReconcileRunStatus(&workflowTaskTestTx{}, run, event, status); err != nil {
		t.Fatal(err)
	}
	if run.Status != history.StatusCompleted || run.Output["result"] != "done" {
		t.Fatalf("reconciled run: status=%s output=%v", run.Status, run.Output)
	}
	if patch := run.ExportPatch(); !patch.Status.IsPresent() || !patch.Output.IsPresent() {
		t.Fatal("reconciled fields are not tracked for the patch update")
	}
}
