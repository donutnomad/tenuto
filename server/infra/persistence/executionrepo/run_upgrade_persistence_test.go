package executionrepo

import (
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
)

// TestRunUpgradeFieldsRoundTrip 固定 AutoUpgrade/Pinned 审计字段与 target 快照的持久化。
func TestRunUpgradeFieldsRoundTrip(t *testing.T) {
	repository := newTestStore(t)
	want := &execution.Run{
		ID: "run-upgrade", Namespace: "tenant", WorkflowID: "wf", WorkflowType: "wf",
		WorkflowConfigVersion:         "vB",
		OriginalWorkflowConfigVersion: "vA",
		Policy:                        "auto_upgrade",
		PolicySource:                  "run_override",
		TargetWorkerIdentity:          "worker-a",
		UpgradeError:                  "auto upgrade to vX failed",
		Status:                        history.StatusRunning,
		CreatedAt:                     time.Now(),
	}
	if err := repository.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(rootBranch(t, want)) }); err != nil {
		t.Fatal(err)
	}
	got, err := repository.Get(want.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.OriginalWorkflowConfigVersion != want.OriginalWorkflowConfigVersion ||
		got.Policy != want.Policy || got.PolicySource != want.PolicySource ||
		got.TargetWorkerIdentity != want.TargetWorkerIdentity || got.UpgradeError != want.UpgradeError {
		t.Fatalf("upgrade fields round-trip = %+v", got)
	}
}

// Integration: SQL history retains the version used by an upgrade attempt.
func TestWorkflowTaskStartedProgramVersionRoundTrip(t *testing.T) {
	repository := newTestStore(t)
	current := &execution.Run{ID: "started-version", Namespace: "tenant", WorkflowID: "wf", WorkflowConfigVersion: "old", Status: history.StatusRunning, CreatedAt: time.Now()}
	if err := repository.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(rootBranch(t, current)) }); err != nil {
		t.Fatal(err)
	}
	event := history.NewWorkflowTaskStartedEvent(7, "worker", "build", 0)
	event.WorkflowTaskStarted.ProgramVersion = "candidate"
	if _, err := repository.Append(current.ID, current.ID, current.ID, event); err != nil {
		t.Fatal(err)
	}
	events, err := repository.GetEvents(current.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].WorkflowTaskStarted == nil || events[0].WorkflowTaskStarted.ProgramVersion != "candidate" {
		t.Fatalf("history lost selected program: %+v", events)
	}
}
