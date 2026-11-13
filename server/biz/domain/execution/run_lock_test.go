package execution_test

import (
	"errors"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

// 这些用例固定写路径依据锁下读到的 Run 做判定：调用方在事务之前准备了输入，之后另一个事务改了 Run，
// 提交的事务按改过的 Run 判定。

func autoUpgradeRun(t *testing.T, store *memory.ExecutionManager, runID string) *execution.Run {
	t.Helper()
	run := execution.NewRun(execution.NewRunOptions{ID: runID, WorkflowID: "flow-" + runID, WorkflowType: "flow", WorkflowConfigVersion: "v1", CreatedAt: time.Now()})
	if err := run.ApplyStartPolicy(execution.RunPolicyAutoUpgrade, ""); err != nil {
		t.Fatal(err)
	}
	treeID, branchID, started, err := execution.PrepareStartedRun(run, "Flow", 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.RunInTx(func(tx execution.ExecutionTx) error {
		return execution.CommitStartedRun(tx, run, "", treeID, branchID, started)
	}); err != nil {
		t.Fatal(err)
	}
	return run
}

// pinRun 以另一个事务的身份把 Run 的策略改成 pinned。
func pinRun(t *testing.T, store *memory.ExecutionManager, runID string) {
	t.Helper()
	if err := store.RunInTx(func(tx execution.ExecutionTx) error {
		run, err := tx.GetRunForUpdate(runID)
		if err != nil {
			return err
		}
		return execution.OverrideRunPolicy(tx, run, execution.RunPolicyPinned, runID, runID, nil)
	}); err != nil {
		t.Fatal(err)
	}
}

func chooseProgram(t *testing.T, store *memory.ExecutionManager, runID string, candidate execution.UpgradeCandidate) execution.ProgramVersionChoice {
	t.Helper()
	var choice execution.ProgramVersionChoice
	if err := store.RunInTx(func(tx execution.ExecutionTx) error {
		var err error
		choice, err = execution.ChooseWorkflowTaskProgram(tx, runID, 0, candidate)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	return choice
}

func TestChooseWorkflowTaskProgramDecidesOnTheLockedRun(t *testing.T) {
	t.Run("Happy Path: an auto_upgrade run adopts the candidate", func(t *testing.T) {
		store := memory.NewExecutionManager()
		autoUpgradeRun(t, store, "run-adopt")
		if got := chooseProgram(t, store, "run-adopt", execution.UpgradeCandidate{Version: "v2"}); got != (execution.ProgramVersionChoice{Version: "v2"}) {
			t.Fatalf("choice = %+v, want v2", got)
		}
	})
	t.Run("Edge Case: a policy pinned after the candidate was read keeps the current version", func(t *testing.T) {
		store := memory.NewExecutionManager()
		autoUpgradeRun(t, store, "run-pinned")
		pinRun(t, store, "run-pinned")
		if got := chooseProgram(t, store, "run-pinned", execution.UpgradeCandidate{Version: "v2"}); got != (execution.ProgramVersionChoice{Version: "v1"}) {
			t.Fatalf("choice = %+v, want the current v1", got)
		}
	})
	t.Run("Error Case: a target conflict pauses the run in the same transaction", func(t *testing.T) {
		store := memory.NewExecutionManager()
		autoUpgradeRun(t, store, "run-conflict")
		got := chooseProgram(t, store, "run-conflict", execution.UpgradeCandidate{Version: "v2", TargetConflict: errors.New("target cannot run inventory.reserve")})
		if got.Version != "v1" || got.ConfigError == "" {
			t.Fatalf("choice = %+v, want a pause on v1", got)
		}
		run, err := store.Get("run-conflict")
		if err != nil || run.ConfigError != got.ConfigError {
			t.Fatalf("stored config error = %q err=%v, want %q", run.ConfigError, err, got.ConfigError)
		}
	})
	t.Run("Error Case: a run paused by another transaction stays paused", func(t *testing.T) {
		store := memory.NewExecutionManager()
		autoUpgradeRun(t, store, "run-paused")
		if err := store.RunInTx(func(tx execution.ExecutionTx) error {
			run, err := tx.GetRunForUpdate("run-paused")
			if err != nil {
				return err
			}
			return execution.PauseRun(tx, run, "contract mismatch")
		}); err != nil {
			t.Fatal(err)
		}
		if got := chooseProgram(t, store, "run-paused", execution.UpgradeCandidate{Version: "v2"}); got.ConfigError != "contract mismatch" {
			t.Fatalf("choice = %+v, want the existing pause", got)
		}
	})
}

// Integration：set-policy 拒绝已结束的 Run，按锁下读到的 execution state 判定。
func TestOverrideRunPolicyRejectsClosedRun(t *testing.T) {
	store := memory.NewExecutionManager()
	autoUpgradeRun(t, store, "run-closed")
	if err := store.RunInTx(func(tx execution.ExecutionTx) error {
		run, err := tx.GetRunForUpdate("run-closed")
		if err != nil {
			return err
		}
		_, err = execution.Terminate(tx, run, "run-closed", "run-closed", "operator")
		return err
	}); err != nil {
		t.Fatal(err)
	}
	err := store.RunInTx(func(tx execution.ExecutionTx) error {
		run, err := tx.GetRunForUpdate("run-closed")
		if err != nil {
			return err
		}
		return execution.OverrideRunPolicy(tx, run, execution.RunPolicyPinned, "run-closed", "run-closed", nil)
	})
	if !errors.Is(err, protocol.ErrInvalidRequest) {
		t.Fatalf("set policy on a terminated run: err = %v, want an invalid request", err)
	}
}

// Integration：重试 Run 在事务之前按旧 Run 构造；提交之前另一个事务把旧 Run 改成 pinned，重试 Run 采用 pinned。
func TestCommitRetryRunTakesPolicyOfLockedOldRun(t *testing.T) {
	store := memory.NewExecutionManager()
	old := autoUpgradeRun(t, store, "run-retry-old")
	next := execution.NewRetryRun(old, "run-retry-next", time.Now(), nil)
	_, _, started, err := execution.PrepareStartedRun(next, "Flow", 0)
	if err != nil {
		t.Fatal(err)
	}
	pinRun(t, store, old.ID)
	if err := store.RunInTx(func(tx execution.ExecutionTx) error {
		_, err := execution.CommitRetryRun(tx, execution.RetryRunCommit{
			OldRunID: old.ID, OldTreeID: old.ID, OldBranchID: old.ID,
			NewRun: next, StartedEvent: started,
			TimerState: execution.NewRetryBackoffTimer(next.ID, time.Second, time.Now(), nil),
			Failure:    protocol.Failure{Message: "transient"}, RetryState: "InProgress",
		})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	assertPinned(t, store, next.ID)
}

// Integration：接续 Run 在事务之前按旧 Run 构造；提交之前另一个事务把旧 Run 改成 pinned，接续 Run 采用 pinned。
func TestCommitContinuedRunTakesPolicyOfLockedOldRun(t *testing.T) {
	store := memory.NewExecutionManager()
	old := autoUpgradeRun(t, store, "run-can-old")
	next := execution.NewContinuedRun(old, "run-can-next", "", "flow", "v1", nil, time.Now())
	treeID, branchID, started, err := execution.PrepareStartedRun(next, "Flow", 0)
	if err != nil {
		t.Fatal(err)
	}
	pinRun(t, store, old.ID)
	if err := store.RunInTx(func(tx execution.ExecutionTx) error {
		return execution.CommitContinuedRun(tx, old.ID, next, treeID, branchID, started)
	}); err != nil {
		t.Fatal(err)
	}
	assertPinned(t, store, next.ID)
	if closed, err := store.Get(old.ID); err != nil || closed.Status != history.StatusContinuedAsNew {
		t.Fatalf("old run = %+v err=%v, want ContinuedAsNew", closed, err)
	}
}

func assertPinned(t *testing.T, store *memory.ExecutionManager, runID string) {
	t.Helper()
	run, err := store.Get(runID)
	if err != nil {
		t.Fatal(err)
	}
	if run.Policy != execution.RunPolicyPinned || run.PolicySource != execution.PolicySourceRunOverride {
		t.Fatalf("run %s policy = %q/%q, want the pinned override committed before it was created", runID, run.Policy, run.PolicySource)
	}
}
