package executiontest

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
)

// TransactionContract 断言 RunInTx 的可见性与错误语义：事务内读 execution state 能看到本事务已做的写入，
// 事务内任何一步失败都让 RunInTx 返回错误并且不留下部分写入。
func TransactionContract(t *testing.T, newManager func(t *testing.T) execution.ExecutionManager) {
	started := func(runID string) history.Event {
		return history.NewWorkflowStartedEvent(map[string]any{"order": "o-1"}, runID, "", "wf-"+runID, "flow", 0)
	}

	t.Run("StateSeesEventsAppendedInTheSameTx", func(t *testing.T) {
		manager := newManager(t)
		const runID = "run-visible"
		createContractRun(t, manager, runID)
		inTx(t, manager, func(tx execution.ExecutionTx) error {
			before, err := tx.GetWorkflowExecutionStateForUpdate(runID)
			if err != nil {
				return err
			}
			first, err := tx.Append(runID, runID, runID, started(runID))
			if err != nil {
				return err
			}
			second, err := tx.Append(runID, runID, runID, history.NewWorkflowCancellationRequestedEvent("operator"))
			if err != nil {
				return err
			}
			after, err := tx.GetWorkflowExecutionStateForUpdate(runID)
			if err != nil {
				return err
			}
			if after.LastEventID != second || after.NextEventID != second+1 || after.HistoryEventCount != before.HistoryEventCount+2 {
				t.Errorf("state read in the tx misses its own appends (ids %d, %d): last=%d next=%d count=%d", first, second, after.LastEventID, after.NextEventID, after.HistoryEventCount)
			}
			if !after.HasWorkflowStarted || !after.CancelRequested {
				t.Errorf("state read in the tx misses the projection of its own appends: started=%v cancel=%v", after.HasWorkflowStarted, after.CancelRequested)
			}
			return nil
		})
	})

	t.Run("StateOfRunCreatedInTheSameTx", func(t *testing.T) {
		manager := newManager(t)
		const runID = "run-created-in-tx"
		inTx(t, manager, func(tx execution.ExecutionTx) error {
			if err := tx.CreateRun(execution.NewRun(execution.NewRunOptions{ID: runID, WorkflowID: "wf-" + runID, WorkflowConfigVersion: "v1", CreatedAt: contractNow})); err != nil {
				return err
			}
			id, err := tx.Append(runID, runID, runID, started(runID))
			if err != nil {
				return err
			}
			state, err := tx.GetWorkflowExecutionStateForUpdate(runID)
			if err != nil {
				t.Errorf("state of a run created in the tx: %v", err)
				return nil
			}
			if state.LastEventID != id || !state.HasWorkflowStarted {
				t.Errorf("state of a run created in the tx: last=%d started=%v, want last=%d", state.LastEventID, state.HasWorkflowStarted, id)
			}
			return nil
		})
	})

	// 每个用例在一次事务里先追加一个事件，再做一步会失败的写入：写入返回错误，RunInTx 返回它，事件不落库。
	failures := []struct {
		name    string
		write   func(tx execution.ExecutionTx) error
		wantErr error
	}{
		{"CreateExistingRun", func(tx execution.ExecutionTx) error {
			return tx.CreateRun(execution.NewRun(execution.NewRunOptions{ID: "run-existing", WorkflowID: "wf-other", WorkflowConfigVersion: "v1", CreatedAt: contractNow}))
		}, execution.ErrRunAlreadyExists},
		{"CreateRunWithoutBranchToken", func(tx execution.ExecutionTx) error {
			run := execution.NewRun(execution.NewRunOptions{ID: "run-no-branch", WorkflowID: "wf-no-branch", WorkflowConfigVersion: "v1", CreatedAt: contractNow})
			run.BranchToken = nil
			return tx.CreateRun(run)
		}, execution.ErrRunBranchTokenMissing},
		{"UpdateMissingRun", func(tx execution.ExecutionTx) error {
			missing := execution.NewRun(execution.NewRunOptions{ID: "run-missing", WorkflowID: "wf-missing", WorkflowConfigVersion: "v1", CreatedAt: contractNow})
			return tx.UpdateRun(missing)
		}, execution.ErrRunNotFound},
		{"LinkResetOfMissingRun", func(tx execution.ExecutionTx) error {
			return tx.SetResetRunID("run-missing", "run-existing")
		}, execution.ErrRunNotFound},
		{"AppendToMissingRun", func(tx execution.ExecutionTx) error {
			_, err := tx.Append("run-missing", "run-missing", "run-missing", started("run-missing"))
			return err
		}, execution.ErrRunNotFound},
	}
	for _, tc := range failures {
		t.Run("FailedWriteRollsBack/"+tc.name, func(t *testing.T) {
			manager := newManager(t)
			const runID = "run-existing"
			createContractRun(t, manager, runID)
			err := manager.RunInTx(func(tx execution.ExecutionTx) error {
				if _, err := tx.Append(runID, runID, runID, started(runID)); err != nil {
					t.Fatal(err)
				}
				return tc.write(tx)
			})
			if err == nil {
				t.Fatal("the failing write returned no error")
			}
			if tc.wantErr != nil && !errors.Is(err, tc.wantErr) {
				t.Errorf("err = %v, want %v", err, tc.wantErr)
			}
			events, err := manager.GetEvents(runID)
			if err != nil {
				t.Fatal(err)
			}
			if len(events) != 0 {
				t.Errorf("the failed transaction left %d events behind", len(events))
			}
		})
	}

	t.Run("LinkResetOfMissingRunOutsideTx", func(t *testing.T) {
		manager := newManager(t)
		if err := manager.SetResetRunID("run-missing", "run-new"); !errors.Is(err, execution.ErrRunNotFound) {
			t.Errorf("err = %v, want execution.ErrRunNotFound", err)
		}
		createContractRun(t, manager, "run-base")
		// 重复写入同一个值仍然成功：Run 存在。
		for range 2 {
			if err := manager.SetResetRunID("run-base", "run-new"); err != nil {
				t.Errorf("link reset on an existing run: %v", err)
			}
		}
		if run, err := manager.Get("run-base"); err != nil || run.ResetRunID != "run-new" {
			t.Errorf("reset link not stored: %+v err=%v", run, err)
		}
	})

	// 终态事件之后 History 不再接受事件：重复的终态写入按去重返回已记录的事件，其余追加返回 ErrRunClosed。
	t.Run("ClosedRunRejectsAppends", func(t *testing.T) {
		manager := newManager(t)
		const runID = "run-closed"
		createContractRun(t, manager, runID)
		if _, err := manager.Append(runID, runID, runID, started(runID)); err != nil {
			t.Fatal(err)
		}
		completedID, inserted, err := manager.AppendIdempotent(runID, runID, runID, history.NewWorkflowCompletedEvent(nil))
		if err != nil || !inserted {
			t.Fatalf("append terminal event: inserted=%v err=%v", inserted, err)
		}

		if id, inserted, err := manager.AppendIdempotent(runID, runID, runID, history.NewWorkflowCompletedEvent(nil)); err != nil || inserted || id != completedID {
			t.Errorf("duplicate terminal append = id:%d inserted:%v err:%v; want the recorded event %d", id, inserted, err, completedID)
		}
		if _, err := manager.Append(runID, runID, runID, history.NewWorkflowSignaledEvent("late", nil)); !errors.Is(err, execution.ErrRunClosed) {
			t.Errorf("signal after terminal: err = %v, want ErrRunClosed", err)
		}
		if _, _, err := manager.AppendIdempotent(runID, runID, runID, history.NewTimerScheduledEvent("late", nil, 0)); !errors.Is(err, execution.ErrRunClosed) {
			t.Errorf("deduped event after terminal: err = %v, want ErrRunClosed", err)
		}
		if _, _, err := manager.AppendWorkflowTaskScheduled(runID, runID, runID, history.NewWorkflowTaskScheduledEvent(1000)); !errors.Is(err, execution.ErrRunClosed) {
			t.Errorf("workflow task after terminal: err = %v, want ErrRunClosed", err)
		}

		// 同一事务里先写终态再写别的事件：后一次追加被拒，整个事务回滚。
		const sameTxRunID = "run-closed-same-tx"
		createContractRun(t, manager, sameTxRunID)
		err = manager.RunInTx(func(tx execution.ExecutionTx) error {
			if _, err := tx.Append(sameTxRunID, sameTxRunID, sameTxRunID, history.NewWorkflowCompletedEvent(nil)); err != nil {
				return err
			}
			_, err := tx.Append(sameTxRunID, sameTxRunID, sameTxRunID, history.NewWorkflowSignaledEvent("late", nil))
			return err
		})
		if !errors.Is(err, execution.ErrRunClosed) {
			t.Fatalf("append after terminal in the same tx: err = %v, want ErrRunClosed", err)
		}
		if events, err := manager.GetEvents(sameTxRunID); err != nil || len(events) != 0 {
			t.Errorf("rolled back tx left events %d err=%v", len(events), err)
		}
		events, err := manager.GetEvents(runID)
		if err != nil {
			t.Fatal(err)
		}
		if len(events) != 2 || events[1].ID != completedID {
			t.Errorf("closed run history changed: %d events", len(events))
		}
	})
}

// RunReadUnderLockContract 断言 ExecutionTx.GetRunForUpdate 的语义：读到本事务自己的写入；读到在它之前提交的、
// 改了同一个 Run 的事务的结果，即使本事务在那个事务提交之前就已开始；Run 不存在时返回 ErrRunNotFound。
//
// 并发用例里第一个事务改 Run 的策略并保持打开，第二个事务在它提交之前开始并读这个 Run。内存后端让第二个
// 事务等到第一个提交才进入；MySQL 上第二个事务在 execution state 行锁上等待，拿到锁后再读 Run。
func RunReadUnderLockContract(t *testing.T, newManager func(t *testing.T) execution.ExecutionManager) {
	t.Run("SeesOwnWrites", func(t *testing.T) {
		manager := newManager(t)
		const runID = "run-own-writes"
		createContractRun(t, manager, runID)
		inTx(t, manager, func(tx execution.ExecutionTx) error {
			run, err := tx.GetRunForUpdate(runID)
			if err != nil {
				return err
			}
			if err := run.OverridePolicy(execution.RunPolicyPinned); err != nil {
				return err
			}
			if err := tx.UpdateRun(run); err != nil {
				return err
			}
			reread, err := tx.GetRunForUpdate(runID)
			if err != nil {
				return err
			}
			if reread.Policy != execution.RunPolicyPinned || reread.PolicySource != execution.PolicySourceRunOverride {
				t.Errorf("run read in the tx misses its own update: policy=%q source=%q", reread.Policy, reread.PolicySource)
			}
			const createdID = "run-created-in-lock-tx"
			if err := tx.CreateRun(execution.NewRun(execution.NewRunOptions{ID: createdID, WorkflowID: "wf-" + createdID, WorkflowConfigVersion: "v1", CreatedAt: contractNow})); err != nil {
				return err
			}
			if created, err := tx.GetRunForUpdate(createdID); err != nil || created.WorkflowConfigVersion != "v1" {
				t.Errorf("run created in the tx: %+v err=%v", created, err)
			}
			return nil
		})
	})

	t.Run("MissingRun", func(t *testing.T) {
		manager := newManager(t)
		err := manager.RunInTx(func(tx execution.ExecutionTx) error {
			_, err := tx.GetRunForUpdate("run-missing")
			return err
		})
		if !errors.Is(err, execution.ErrRunNotFound) {
			t.Errorf("err = %v, want execution.ErrRunNotFound", err)
		}
	})

	t.Run("SeesWriteCommittedWhileWaiting", func(t *testing.T) {
		manager := newManager(t)
		const runID = "run-concurrent-policy"
		run := execution.NewRun(execution.NewRunOptions{ID: runID, WorkflowID: "wf-" + runID, WorkflowConfigVersion: "v1", CreatedAt: contractNow})
		if err := run.ApplyStartPolicy(execution.RunPolicyAutoUpgrade, ""); err != nil {
			t.Fatal(err)
		}
		createRun(t, manager, run)
		firstHolds, releaseFirst := make(chan struct{}), make(chan struct{})
		firstDone := make(chan error, 1)
		go func() {
			firstDone <- manager.RunInTx(func(tx execution.ExecutionTx) error {
				run, err := tx.GetRunForUpdate(runID)
				if err != nil {
					return err
				}
				if err := run.OverridePolicy(execution.RunPolicyPinned); err != nil {
					return err
				}
				if err := tx.UpdateRun(run); err != nil {
					return err
				}
				close(firstHolds)
				<-releaseFirst
				return nil
			})
		}()
		select {
		case <-firstHolds:
		case err := <-firstDone:
			t.Fatalf("first transaction ended before holding the lock: %v", err)
		}
		var seen *execution.Run
		secondDone := make(chan error, 1)
		go func() {
			secondDone <- manager.RunInTx(func(tx execution.ExecutionTx) error {
				var err error
				seen, err = tx.GetRunForUpdate(runID)
				return err
			})
		}()
		// 第二个事务在第一个提交之前开始：给它时间进入等待。
		time.Sleep(50 * time.Millisecond)
		close(releaseFirst)
		if err := <-firstDone; err != nil {
			t.Fatal(err)
		}
		if err := <-secondDone; err != nil {
			t.Fatal(err)
		}
		if seen.Policy != execution.RunPolicyPinned || seen.PolicySource != execution.PolicySourceRunOverride {
			t.Errorf("second transaction read a stale run: policy=%q source=%q, want %q/%q",
				seen.Policy, seen.PolicySource, execution.RunPolicyPinned, execution.PolicySourceRunOverride)
		}
	})

	// 并发的读改写各自在锁下读取 Run 再合并自己的搜索属性：每个事务都看到前面提交的合并结果，一个都不丢。
	t.Run("ConcurrentReadModifyWritesKeepEveryWrite", func(t *testing.T) {
		manager := newManager(t)
		const runID = "run-concurrent-upserts"
		createContractRun(t, manager, runID)
		const writers = 8
		errs := make(chan error, writers)
		var wg sync.WaitGroup
		for i := range writers {
			wg.Go(func() {
				errs <- manager.RunInTx(func(tx execution.ExecutionTx) error {
					run, err := tx.GetRunForUpdate(runID)
					if err != nil {
						return err
					}
					branch, err := execution.DeserializeBranchToken(run.BranchToken)
					if err != nil {
						return err
					}
					return execution.UpsertSearchAttributes(tx, run, branch.TreeID, branch.BranchID, map[string]string{fmt.Sprintf("writer-%d", i): "done"})
				})
			})
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			if err != nil {
				t.Errorf("concurrent read-modify-write: %v", err)
			}
		}
		run, err := manager.Get(runID)
		if err != nil {
			t.Fatal(err)
		}
		if len(run.SearchAttributes) != writers {
			t.Errorf("search attributes after %d concurrent writers = %v: a writer read a stale run", writers, run.SearchAttributes)
		}
	})
}
