package execution_test

import (
	"errors"
	"testing"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

func TestCommitStartedRunKeepsCurrentAndHistoryAtomic(t *testing.T) {
	store := memory.NewExecutionManager()
	start := func(id string) error {
		candidate := execution.NewRun(execution.NewRunOptions{
			ID:                    id,
			WorkflowID:            "payment",
			WorkflowConfigVersion: "version-1",
		})
		treeID, branchID, started, err := execution.PrepareStartedRun(candidate, "Payment", 0)
		if err != nil {
			return err
		}
		return store.RunInTx(func(tx execution.ExecutionTx) error {
			return execution.CommitStartedRun(tx, candidate, "", treeID, branchID, started)
		})
	}
	if err := start("run-1"); err != nil {
		t.Fatal(err)
	}
	if err := start("run-2"); !errors.Is(err, execution.ErrCurrentRunConflict) {
		t.Fatalf("second start error = %v, want current run conflict", err)
	}
	current, err := store.GetCurrentRunID(protocol.DefaultNamespace, "payment")
	if err != nil || current != "run-1" {
		t.Fatalf("current run = %q, err = %v", current, err)
	}
	if _, err := store.Get("run-2"); err == nil {
		t.Fatalf("conflicting run persisted: %v", err)
	}
	conflictingEvents, err := store.GetEvents("run-2")
	if err != nil || len(conflictingEvents) != 0 {
		t.Fatalf("conflicting history = %+v, err = %v", conflictingEvents, err)
	}
	events, err := store.GetEvents("run-1")
	if err != nil || len(events) != 1 || events[0].WorkflowStarted == nil {
		t.Fatalf("started history = %+v, err = %v", events, err)
	}
}

func TestNewWorkflowVersionRunPinsProgramAndWorkflowType(t *testing.T) {
	started, err := execution.NewWorkflowVersionRun(execution.WorkflowStartOptions{
		Namespace: " tenant ", WorkflowID: "run-family", ConfigWorkflowID: "payment",
		ProgramVersion: "program-v2", Initiator: "workflow",
	})
	if err != nil {
		t.Fatal(err)
	}
	if started.Namespace != "tenant" || started.WorkflowID != "run-family" || started.WorkflowType != "payment" {
		t.Fatalf("run identity = namespace %q, workflow %q, type %q", started.Namespace, started.WorkflowID, started.WorkflowType)
	}
	if started.WorkflowConfigVersion != "program-v2" {
		t.Fatalf("pinned program version = %q", started.WorkflowConfigVersion)
	}

	started, err = execution.NewWorkflowVersionRun(execution.WorkflowStartOptions{
		WorkflowID: "payment", ConfigWorkflowID: "payment", ProgramVersion: "program-v2",
	})
	if err != nil {
		t.Fatal(err)
	}
	if started.WorkflowType != "payment" {
		t.Fatalf("workflow type for matching workflow ID = %q, want payment", started.WorkflowType)
	}
}

func TestNewWorkflowVersionRunValidatesIdentityAndProgram(t *testing.T) {
	tests := []struct {
		name string
		opts execution.WorkflowStartOptions
	}{
		{name: "missing workflow ID", opts: execution.WorkflowStartOptions{ConfigWorkflowID: "payment", ProgramVersion: "v1"}},
		{name: "missing config workflow ID", opts: execution.WorkflowStartOptions{WorkflowID: "payment", ProgramVersion: "v1"}},
		{name: "missing program version", opts: execution.WorkflowStartOptions{WorkflowID: "payment", ConfigWorkflowID: "payment"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := execution.NewWorkflowVersionRun(test.opts); err == nil {
				t.Fatal("expected invalid start options error")
			}
		})
	}
}
