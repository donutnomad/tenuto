package memory

import (
	"errors"
	"testing"

	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/biz/domain/execution/executiontest"
)

func memSetCurrent(t *testing.T, mgr *ExecutionManager, namespace, workflowID, runID string) {
	t.Helper()
	err := mgr.RunInTx(func(tx execution.ExecutionTx) error {
		ok, e := tx.SetCurrentRunCAS(namespace, workflowID, "", runID)
		if e != nil {
			return e
		}
		if !ok {
			t.Fatalf("expected first SetCurrentRunCAS to succeed for %s", runID)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// Happy Path: 存一个 is_current run，GetCurrentRunID 返回它。
func TestMemoryGetCurrentRunIDRoundTrip(t *testing.T) {
	mgr := NewExecutionManager()
	_ = mgr.RunInTx(func(tx execution.ExecutionTx) error {
		return tx.CreateRun(&execution.Run{ID: "run-1", BranchToken: executiontest.BranchToken("run-1"), WorkflowID: "wf-1", Status: history.StatusRunning})
	})
	memSetCurrent(t, mgr, "", "wf-1", "run-1")

	got, err := mgr.GetCurrentRunID("", "wf-1")
	if err != nil {
		t.Fatal(err)
	}
	if got != "run-1" {
		t.Fatalf("GetCurrentRunID = %q, want run-1", got)
	}
	run, _ := mgr.Get("run-1")
	if !run.IsCurrent {
		t.Fatal("persisted run.IsCurrent = false, want true")
	}
}

// Error Case: 从未设过 current → ErrNoCurrentRun。
func TestMemoryGetCurrentRunIDNoCurrent(t *testing.T) {
	mgr := NewExecutionManager()
	_ = mgr.RunInTx(func(tx execution.ExecutionTx) error {
		return tx.CreateRun(&execution.Run{ID: "run-1", BranchToken: executiontest.BranchToken("run-1"), WorkflowID: "wf-1", Status: history.StatusRunning})
	})
	_, err := mgr.GetCurrentRunID("", "wf-1")
	if !errors.Is(err, execution.ErrNoCurrentRun) {
		t.Fatalf("err = %v, want ErrNoCurrentRun", err)
	}
}

// Happy Path: 终态 run 也能作为 current 被查到。
func TestMemoryGetCurrentRunIDTerminalIsCurrent(t *testing.T) {
	mgr := NewExecutionManager()
	_ = mgr.RunInTx(func(tx execution.ExecutionTx) error {
		return tx.CreateRun(&execution.Run{ID: "run-1", BranchToken: executiontest.BranchToken("run-1"), WorkflowID: "wf-1", Status: history.StatusRunning})
	})
	memSetCurrent(t, mgr, "", "wf-1", "run-1")

	executiontest.UpdateRun(t, mgr, "run-1", func(run *execution.Run) error {
		return run.MarkTerminal(history.StatusCompleted, nil)
	})

	got, err := mgr.GetCurrentRunID("", "wf-1")
	if err != nil {
		t.Fatalf("err = %v, terminal run should still be current", err)
	}
	if got != "run-1" {
		t.Fatalf("GetCurrentRunID = %q, want run-1", got)
	}
}

// Happy Path: 正确 expectedOld 切换成功（旧 false 新 true，只有一个 current）。
func TestMemorySetCurrentRunCASSwitch(t *testing.T) {
	mgr := NewExecutionManager()
	_ = mgr.RunInTx(func(tx execution.ExecutionTx) error {
		return tx.CreateRun(&execution.Run{ID: "run-1", BranchToken: executiontest.BranchToken("run-1"), WorkflowID: "wf-1", Status: history.StatusRunning})
	})
	_ = mgr.RunInTx(func(tx execution.ExecutionTx) error {
		return tx.CreateRun(&execution.Run{ID: "run-2", BranchToken: executiontest.BranchToken("run-2"), WorkflowID: "wf-1", Status: history.StatusRunning})
	})
	memSetCurrent(t, mgr, "", "wf-1", "run-1")

	err := mgr.RunInTx(func(tx execution.ExecutionTx) error {
		ok, e := tx.SetCurrentRunCAS("", "wf-1", "run-1", "run-2")
		if e != nil {
			return e
		}
		if !ok {
			t.Fatal("CAS with correct expectedOld should succeed")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	got, _ := mgr.GetCurrentRunID("", "wf-1")
	if got != "run-2" {
		t.Fatalf("after switch = %q, want run-2", got)
	}
	r1, _ := mgr.Get("run-1")
	r2, _ := mgr.Get("run-2")
	if r1.IsCurrent {
		t.Fatal("old run-1 should no longer be current")
	}
	if !r2.IsCurrent {
		t.Fatal("new run-2 should be current")
	}
	memAssertSingleCurrent(t, mgr, "wf-1")
}

// Error Case: 错误 expectedOld（被切走）→ CAS 失败，不改 current。
func TestMemorySetCurrentRunCASMismatch(t *testing.T) {
	mgr := NewExecutionManager()
	_ = mgr.RunInTx(func(tx execution.ExecutionTx) error {
		return tx.CreateRun(&execution.Run{ID: "run-1", BranchToken: executiontest.BranchToken("run-1"), WorkflowID: "wf-1", Status: history.StatusRunning})
	})
	_ = mgr.RunInTx(func(tx execution.ExecutionTx) error {
		return tx.CreateRun(&execution.Run{ID: "run-3", BranchToken: executiontest.BranchToken("run-3"), WorkflowID: "wf-1", Status: history.StatusRunning})
	})
	memSetCurrent(t, mgr, "", "wf-1", "run-1")

	err := mgr.RunInTx(func(tx execution.ExecutionTx) error {
		ok, e := tx.SetCurrentRunCAS("", "wf-1", "run-99", "run-3")
		if e != nil {
			return e
		}
		if ok {
			t.Fatal("CAS with wrong expectedOld should fail")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	got, _ := mgr.GetCurrentRunID("", "wf-1")
	if got != "run-1" {
		t.Fatalf("current changed on failed CAS: got %q, want run-1", got)
	}
	r3, _ := mgr.Get("run-3")
	if r3.IsCurrent {
		t.Fatal("run-3 should NOT be current after failed CAS")
	}
	memAssertSingleCurrent(t, mgr, "wf-1")
}

func memAssertSingleCurrent(t *testing.T, mgr *ExecutionManager, workflowID string) {
	t.Helper()
	count := 0
	for _, r := range mgr.RunStore.snapshots() {
		if r.isCurrent && r.workflowID == workflowID {
			count++
		}
	}
	if count > 1 {
		t.Fatalf("workflow %s has %d current runs, want at most 1", workflowID, count)
	}
}
