package execution_test

import (
	"testing"
	"time"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

func TestMarkChildStartedPreservesTerminalState(t *testing.T) {
	store := memory.NewExecutionManager()
	request := execution.ChildStart{
		ParentRunID: "parent", ChildID: "child-1", ChildRunID: "child-run",
		ScheduledEventID: 3, Namespace: "tenant-a", WorkflowID: "subflow",
		ConfigVersion: "v1", Input: map[string]any{"id": "one"},
	}
	child := execution.NewChildRun(request, time.Now())
	if child.ParentRunID != request.ParentRunID || child.WorkflowConfigVersion != "v1" || child.Namespace != "tenant-a" {
		t.Fatalf("child=%+v", child)
	}
	markStarted := func() error {
		return store.RunInTx(func(tx execution.ExecutionTx) error { return execution.MarkChildStarted(tx, request, time.Now()) })
	}
	if err := store.RunInTx(func(tx execution.ExecutionTx) error {
		return tx.CreateRun(execution.NewRun(execution.NewRunOptions{ID: request.ParentRunID, WorkflowID: "parent-flow", WorkflowConfigVersion: "v1"}))
	}); err != nil {
		t.Fatal(err)
	}
	if err := markStarted(); err != nil {
		t.Fatal(err)
	}
	state, found, err := store.GetChildWorkflowState(request.ParentRunID, request.ScheduledEventID)
	if err != nil || !found || state.Status != execution.ChildWorkflowStatusEnums.Started || state.ChildRunID != request.ChildRunID {
		t.Fatalf("state=%+v found=%v err=%v", state, found, err)
	}
	if err := store.RunInTx(func(tx execution.ExecutionTx) error {
		_, err := execution.PropagateChildClose(tx, execution.ChildClose{
			Child: child, ParentTreeID: request.ParentRunID, ParentBranchID: request.ParentRunID,
			Type: history.EventChildWorkflowCompleted, Output: map[string]any{"ok": true}, Now: time.Now(),
		})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := markStarted(); err != nil {
		t.Fatal(err)
	}
	state, _, err = store.GetChildWorkflowState(request.ParentRunID, request.ScheduledEventID)
	if err != nil || state.Status != execution.ChildWorkflowStatusEnums.Completed {
		t.Fatalf("terminal state=%+v err=%v", state, err)
	}
}

func TestNewContinuedRunPreservesDefinitionAndDeadline(t *testing.T) {
	deadline := time.Now().Add(time.Hour)
	old := execution.NewRun(execution.NewRunOptions{
		ID: "old", WorkflowID: "flow", WorkflowConfigVersion: "v1",
		DeadlineAt: &deadline,
		Attempt:    3,
	})
	next := execution.NewContinuedRun(old, "next", "tenant-a", "flow-v2", "v2", map[string]any{"id": "two"}, time.Now())
	if next.Attempt != 1 || next.WorkflowConfigVersion != "v2" || next.FirstRunID != "old" || next.ContinuedRunID != "old" || next.DeadlineAt != &deadline {
		t.Fatalf("continued run=%+v", next)
	}
}

func TestNewChildRunStartsAtFirstAttempt(t *testing.T) {
	child := execution.NewChildRun(execution.ChildStart{
		ParentRunID: "parent", ChildID: "child-1", ChildRunID: "child-run",
		ScheduledEventID: 3, Namespace: " tenant-a ", WorkflowID: "subflow", ConfigVersion: "v1",
	}, time.Now())
	if child.ID != "child-run" || child.FirstRunID != "child-run" || child.Namespace != "tenant-a" ||
		child.ParentRunID != "parent" || child.ParentChildID != "child-1" || child.ParentScheduledEventID != 3 ||
		child.WorkflowConfigVersion != "v1" || child.Status != history.StatusRunning || len(child.BranchToken) == 0 {
		t.Fatalf("child=%+v", child)
	}
	if child.Attempt != 1 {
		t.Fatalf("child attempt=%d, want 1", child.Attempt)
	}
	// MaxAttempts=2 允许且仅允许一次重试，首次退避等于 InitialInterval。
	policy := &program.RetryPolicy{InitialInterval: "2s", BackoffCoefficient: 2, MaxAttempts: 2}
	failure := protocol.Failure{Message: "transient"}
	now := time.Now()
	retry, delay, _ := execution.DecideWorkflowRetry(child, policy, failure, now)
	if !retry || delay != 2*time.Second {
		t.Fatalf("first failure retry=%v delay=%v, want retry after 2s", retry, delay)
	}
	second := execution.NewRetryRun(child, "child-run-2", now, nil)
	if retry, _, state := execution.DecideWorkflowRetry(second, policy, failure, now); retry || state != "MaximumAttemptsReached" {
		t.Fatalf("second failure retry=%v state=%q", retry, state)
	}
}
