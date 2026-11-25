package execution_test

import (
	"errors"
	"testing"

	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

func TestMarkTerminalSetsStatusAndOutput(t *testing.T) {
	run := execution.NewRun(execution.NewRunOptions{ID: "run-1", WorkflowID: "flow", WorkflowType: "flow"})
	if err := run.MarkTerminal(history.StatusRunning, nil); err == nil {
		t.Fatal("accepted nonterminal status")
	}
	output := map[string]any{"result": "ok"}
	if err := run.MarkTerminal(history.StatusCompleted, output); err != nil {
		t.Fatal(err)
	}
	if run.Status != history.StatusCompleted || run.Output["result"] != "ok" {
		t.Fatalf("run=%+v", run)
	}
	failed := execution.NewRun(execution.NewRunOptions{ID: "run-2", WorkflowID: "flow", WorkflowType: "flow"})
	if err := failed.MarkTerminal(history.StatusFailed, output); err != nil || failed.Output != nil {
		t.Fatalf("failed run output=%v err=%v, want no output", failed.Output, err)
	}
}

func TestTerminateCommitsEventAndStatus(t *testing.T) {
	store := memory.NewExecutionManager()
	candidate := execution.NewRun(execution.NewRunOptions{ID: "run-1", WorkflowID: "flow", WorkflowType: "flow", WorkflowConfigVersion: "v1"})
	tree, branch, started, err := execution.PrepareStartedRun(candidate, "Flow", 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.RunInTx(func(tx execution.ExecutionTx) error {
		return execution.CommitStartedRun(tx, candidate, "", tree, branch, started)
	}); err != nil {
		t.Fatal(err)
	}
	var inserted bool
	if err := store.RunInTx(func(tx execution.ExecutionTx) error {
		var terminateErr error
		inserted, terminateErr = execution.Terminate(tx, candidate, tree, branch, "manual")
		return terminateErr
	}); err != nil || !inserted {
		t.Fatalf("inserted=%v err=%v", inserted, err)
	}
	stored, err := store.Get(candidate.ID)
	if err != nil || stored.Status != history.StatusTerminated {
		t.Fatalf("run=%+v err=%v", stored, err)
	}
	events, err := store.GetEvents(candidate.ID)
	if err != nil || len(events) != 2 || events[1].WorkflowTerminated == nil {
		t.Fatalf("history=%+v err=%v", events, err)
	}
}

func TestTerminateKeepsStatusWhenRunAlreadyClosed(t *testing.T) {
	for _, test := range []struct {
		name     string
		terminal history.Event
		status   history.Status
	}{
		{name: "completed", terminal: history.NewWorkflowCompletedEvent(map[string]any{"ok": true}), status: history.StatusCompleted},
		{name: "cancelled", terminal: history.NewWorkflowCancelledEvent(nil), status: history.StatusCancelled},
		{name: "continued as new", terminal: history.NewWorkflowContinuedAsNewEvent("Flow", "run-2", "v1", nil), status: history.StatusContinuedAsNew},
		{name: "terminated", terminal: history.NewWorkflowTerminatedEvent("first"), status: history.StatusTerminated},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := memory.NewExecutionManager()
			candidate := execution.NewRun(execution.NewRunOptions{ID: "run-1", WorkflowID: "flow", WorkflowType: "flow", WorkflowConfigVersion: "v1"})
			tree, branch, started, err := execution.PrepareStartedRun(candidate, "Flow", 0)
			if err != nil {
				t.Fatal(err)
			}
			if err := store.RunInTx(func(tx execution.ExecutionTx) error {
				return execution.CommitStartedRun(tx, candidate, "", tree, branch, started)
			}); err != nil {
				t.Fatal(err)
			}
			// 终态事件已落 History，状态投影仍停在 running。
			if _, inserted, err := store.AppendIdempotent(candidate.ID, tree, branch, test.terminal); err != nil || !inserted {
				t.Fatalf("inserted=%v err=%v", inserted, err)
			}

			var inserted bool
			if err := store.RunInTx(func(tx execution.ExecutionTx) error {
				var terminateErr error
				inserted, terminateErr = execution.Terminate(tx, candidate, tree, branch, "manual")
				return terminateErr
			}); err != nil || inserted {
				t.Fatalf("inserted=%v err=%v", inserted, err)
			}
			stored, err := store.Get(candidate.ID)
			if err != nil || stored.Status != history.StatusRunning {
				t.Fatalf("terminate overwrote projection: run=%+v err=%v", stored, err)
			}
			status, err := execution.NewRuntime(store).CurrentStatus(stored.ID)
			if err != nil || status != test.status {
				t.Fatalf("reconciled status=%q err=%v, want %q", status, err, test.status)
			}
		})
	}
}

func TestTerminationRejectsEveryTerminalStatus(t *testing.T) {
	for _, status := range []history.Status{
		history.StatusCompleted, history.StatusFailed, history.StatusCancelled, history.StatusTerminated, history.StatusContinuedAsNew,
	} {
		if !history.IsTerminalStatus(status) {
			t.Fatalf("%s is not terminal", status)
		}
		if err := execution.CheckTerminationStatus("run-1", status); err == nil {
			t.Fatalf("CheckTerminationStatus accepted %s", status)
		}
		if err := execution.EnsureTerminable(&execution.Run{ID: "run-1", Status: status}); err == nil {
			t.Fatalf("EnsureTerminable accepted %s", status)
		}
	}
	if err := execution.CheckTerminationStatus("run-1", history.StatusRunning); err != nil {
		t.Fatal(err)
	}
}

type failUpdateRunTx struct{ execution.ExecutionTx }

var errRunWrite = errors.New("run write failed")

func (failUpdateRunTx) UpdateRun(*execution.Run) error { return errRunWrite }

func newStartedRun(t *testing.T, store *memory.ExecutionManager, runID string) *execution.Run {
	t.Helper()
	run := execution.NewRun(execution.NewRunOptions{ID: runID, WorkflowID: "flow-" + runID, WorkflowType: "flow", WorkflowConfigVersion: "v1"})
	tree, branch, started, err := execution.PrepareStartedRun(run, "Flow", 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.RunInTx(func(tx execution.ExecutionTx) error {
		return execution.CommitStartedRun(tx, run, "", tree, branch, started)
	}); err != nil {
		t.Fatal(err)
	}
	return run
}

// WorkflowFailed 不带重试时与 Run 的 Failed 状态同事务提交；带重试时 Run 保持 Running，等重试 Run 建好时关闭。
func TestAppendWorkflowFailedClosesRunUnlessRetried(t *testing.T) {
	store := memory.NewExecutionManager()
	final := newStartedRun(t, store, "run-final")
	retried := newStartedRun(t, store, "run-retried")
	if err := store.RunInTx(func(tx execution.ExecutionTx) error {
		if _, err := execution.AppendWorkflowFailed(tx, final, final.ID, final.ID, history.NewWorkflowFailedEvent("boom", "")); err != nil {
			return err
		}
		failed := history.NewWorkflowFailedEvent("boom", "")
		failed.WorkflowFailed.NewExecutionRunID = "run-retry"
		_, err := execution.AppendWorkflowFailed(tx, retried, retried.ID, retried.ID, failed)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	for runID, want := range map[string]history.Status{final.ID: history.StatusFailed, retried.ID: history.StatusRunning} {
		stored, err := store.Get(runID)
		if err != nil || stored.Status != want {
			t.Fatalf("run %s status=%v err=%v, want %s", runID, stored.Status, err, want)
		}
	}
}

// 写 Run 失败时 WorkflowFailed 与 Run 状态一起回滚。
func TestAppendWorkflowFailedRollsBackWithRunWrite(t *testing.T) {
	store := memory.NewExecutionManager()
	run := newStartedRun(t, store, "run-1")
	err := store.RunInTx(func(tx execution.ExecutionTx) error {
		_, err := execution.AppendWorkflowFailed(failUpdateRunTx{tx}, run, run.ID, run.ID, history.NewWorkflowFailedEvent("boom", ""))
		return err
	})
	if !errors.Is(err, errRunWrite) {
		t.Fatalf("err = %v, want the run write failure", err)
	}
	events, err := store.GetEvents(run.ID)
	if err != nil || len(events) != 1 {
		t.Fatalf("history after rollback = %+v, err = %v; want only WorkflowStarted", events, err)
	}
	if stored, err := store.Get(run.ID); err != nil || stored.Status != history.StatusRunning {
		t.Fatalf("run after rollback = %+v, err = %v", stored, err)
	}
}

// History 超限时 WFT 完成、WorkflowFailed 与 Run 的 Failed 状态同事务提交，写 Run 失败时一起回滚。
func TestCloseWorkflowTaskAtHistoryLimitClosesRun(t *testing.T) {
	for _, test := range []struct {
		name       string
		failRun    bool
		wantStatus history.Status
		wantEvents int
	}{
		{name: "committed", wantStatus: history.StatusFailed, wantEvents: 5},
		{name: "rolled back", failRun: true, wantStatus: history.StatusRunning, wantEvents: 3},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := memory.NewExecutionManager()
			run := newStartedRun(t, store, "run-1")
			for _, event := range []history.Event{history.NewWorkflowTaskScheduledEvent(0), history.NewWorkflowTaskStartedEvent(2, "worker-1", "", 0)} {
				if _, err := store.Append(run.ID, run.ID, run.ID, event); err != nil {
					t.Fatal(err)
				}
			}
			err := store.RunInTx(func(tx execution.ExecutionTx) error {
				if test.failRun {
					tx = failUpdateRunTx{tx}
				}
				_, err := execution.CloseWorkflowTaskAtHistoryLimit(tx, run, run.ID, run.ID, history.NewWorkflowTaskCompletedEvent(2, "worker-1"), "history limit", "HistoryLimitExceeded")
				return err
			})
			if test.failRun != errors.Is(err, errRunWrite) || !test.failRun && err != nil {
				t.Fatalf("err = %v", err)
			}
			events, err := store.GetEvents(run.ID)
			if err != nil || len(events) != test.wantEvents {
				t.Fatalf("history = %+v, err = %v; want %d events", events, err, test.wantEvents)
			}
			if stored, err := store.Get(run.ID); err != nil || stored.Status != test.wantStatus {
				t.Fatalf("run status = %v, err = %v; want %s", stored.Status, err, test.wantStatus)
			}
		})
	}
}
