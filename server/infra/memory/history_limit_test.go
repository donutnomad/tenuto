package memory

import (
	"errors"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/biz/domain/execution/executiontest"
)

func TestExecutionManagerHistoryLimitReservesTerminalCloseBudget(t *testing.T) {
	limits := history.HistoryLimits{
		WarningEvents: 2, WarningBytes: 1 << 20,
		HardEvents: 5, HardBytes: 2 << 20,
		ReservedCloseEvents: 2, ReservedCloseBytes: 1024,
	}
	mgr, err := NewExecutionManagerWithHistoryLimits(limits)
	if err != nil {
		t.Fatal(err)
	}
	run := &execution.Run{ID: "limit-run", BranchToken: executiontest.BranchToken("limit-run"), WorkflowID: "flow", Status: history.StatusRunning, CreatedAt: time.Now()}
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(run) }); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if _, err := mgr.Append(run.ID, run.ID, run.ID, history.NewWorkflowStartedEvent(nil, "", "", "", "", 0)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := mgr.Append(run.ID, run.ID, run.ID, history.NewWorkflowStartedEvent(nil, "", "", "", "", 0)); err == nil {
		t.Fatal("business event consumed reserved close budget")
	} else {
		var limitErr *history.HistoryLimitExceededError
		if !errors.As(err, &limitErr) {
			t.Fatalf("error = %v", err)
		}
	}
	if _, _, err := mgr.AppendIdempotent(run.ID, run.ID, run.ID, history.NewWorkflowFailedEvent("history limit exceeded", "HistoryLimitExceeded")); err != nil {
		t.Fatalf("terminal event could not consume close reserve: %v", err)
	}
}

// TestExecutionManagerHistoryLimitAppliesToWorkflowTaskPaths 锁死 AppendWorkflowTaskScheduled /
// AppendWorkflowTaskClosed 两条非事务路径同样受 history 限额约束。修复前它们只算 event size、
// 不做 budget 检查（而同文件事务内路径与 gormstore.appendProjectedEvent 都检查），导致用 memory
// 后端验限额会得到假通过。
func TestExecutionManagerHistoryLimitAppliesToWorkflowTaskPaths(t *testing.T) {
	newManager := func(t *testing.T, runID string) (*ExecutionManager, *execution.Run) {
		t.Helper()
		limits := history.HistoryLimits{
			WarningEvents: 2, WarningBytes: 1 << 20,
			HardEvents: 4, HardBytes: 2 << 20,
			ReservedCloseEvents: 2, ReservedCloseBytes: 1024,
		}
		mgr, err := NewExecutionManagerWithHistoryLimits(limits)
		if err != nil {
			t.Fatal(err)
		}
		run := &execution.Run{ID: runID, BranchToken: executiontest.BranchToken(runID), WorkflowID: "flow", Status: history.StatusRunning, CreatedAt: time.Now()}
		if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(run) }); err != nil {
			t.Fatal(err)
		}
		// 灌满非 close 事件可用额度（HardEvents-ReservedCloseEvents = 2）。
		for range 2 {
			if _, err := mgr.Append(run.ID, run.ID, run.ID, history.NewWorkflowStartedEvent(nil, "", "", "", "", 0)); err != nil {
				t.Fatal(err)
			}
		}
		return mgr, run
	}

	t.Run("scheduled", func(t *testing.T) {
		mgr, run := newManager(t, "wft-scheduled-limit-run")
		// WorkflowTaskScheduled 不是 close 事件，不得动用预留额度 → 必须被拒。
		_, inserted, err := mgr.AppendWorkflowTaskScheduled(run.ID, run.ID, run.ID, history.NewWorkflowTaskScheduledEvent(0))
		if err == nil {
			t.Fatal("AppendWorkflowTaskScheduled ignored the history budget")
		}
		var limitErr *history.HistoryLimitExceededError
		if !errors.As(err, &limitErr) {
			t.Fatalf("error = %v, want *engine.HistoryLimitExceededError", err)
		}
		if inserted {
			t.Fatal("rejected workflow task schedule must not be inserted")
		}
		history, err := mgr.GetEvents(run.ID)
		if err != nil {
			t.Fatal(err)
		}
		if len(history) != 2 {
			t.Fatalf("rejected event leaked into history: %+v", history)
		}
	})

	t.Run("closed consumes reserved budget", func(t *testing.T) {
		mgr, run := newManager(t, "wft-closed-limit-run")
		// WorkflowTaskCompleted 是 close 事件（UsesReservedHistoryCloseBudget=true），
		// 可动用预留额度 → 在 HardEvents 内应当放行，与 gormstore 一致。
		if _, inserted, err := mgr.AppendWorkflowTaskClosed(run.ID, run.ID, run.ID,
			history.NewWorkflowTaskCompletedEvent(1, "worker")); err != nil {
			t.Fatalf("close event could not consume reserved budget: %v", err)
		} else if !inserted {
			t.Fatal("expected close event to be inserted")
		}
	})

	t.Run("closed beyond hard limit", func(t *testing.T) {
		mgr, run := newManager(t, "wft-closed-hard-limit-run")
		// 再灌两条 close 事件占满 HardEvents=4，第三条应被硬限额拒绝。
		for i := range 2 {
			if _, _, err := mgr.AppendWorkflowTaskClosed(run.ID, run.ID, run.ID,
				history.NewWorkflowTaskCompletedEvent(int64(i+1), "worker")); err != nil {
				t.Fatalf("close event %d rejected too early: %v", i, err)
			}
		}
		_, _, err := mgr.AppendWorkflowTaskClosed(run.ID, run.ID, run.ID,
			history.NewWorkflowTaskCompletedEvent(3, "worker"))
		if err == nil {
			t.Fatal("AppendWorkflowTaskClosed ignored the hard history limit")
		}
		var limitErr *history.HistoryLimitExceededError
		if !errors.As(err, &limitErr) {
			t.Fatalf("error = %v, want *engine.HistoryLimitExceededError", err)
		}
	})
}

func TestExecutionManagerHistoryLimitRollsBackWholeWorkflowTaskBatch(t *testing.T) {
	limits := history.HistoryLimits{
		WarningEvents: 2, WarningBytes: 1 << 20,
		HardEvents: 5, HardBytes: 2 << 20,
		ReservedCloseEvents: 2, ReservedCloseBytes: 1024,
	}
	mgr, err := NewExecutionManagerWithHistoryLimits(limits)
	if err != nil {
		t.Fatal(err)
	}
	run := &execution.Run{ID: "batch-limit-run", BranchToken: executiontest.BranchToken("batch-limit-run"), WorkflowID: "flow", Status: history.StatusRunning, CreatedAt: time.Now()}
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(run) }); err != nil {
		t.Fatal(err)
	}
	scheduledID, err := mgr.Append(run.ID, run.ID, run.ID, history.NewWorkflowTaskScheduledEvent(0))
	if err != nil {
		t.Fatal(err)
	}
	_, err = mgr.AppendWorkflowTaskCompletedWithEvents(run.ID, run.ID, run.ID,
		history.NewWorkflowTaskCompletedEvent(scheduledID, "worker"),
		[]history.Event{
			history.NewActivityScheduledEvent(history.ActivityScheduledEvent{ActivityID: "first"}),
			history.NewActivityScheduledEvent(history.ActivityScheduledEvent{ActivityID: "second"}),
		},
	)
	if err == nil {
		t.Fatal("oversized workflow task batch succeeded")
	}
	history, err := mgr.GetEvents(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 1 || history[0].ID != scheduledID {
		t.Fatalf("partial workflow task batch committed: %+v", history)
	}
}
