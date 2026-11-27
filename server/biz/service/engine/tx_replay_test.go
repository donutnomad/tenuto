package engine

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

var errReplayAttempt = errors.New("first attempt rolled back for replay")

// replayOnceManager 模拟 MySQL 后端的死锁重放：replay 打开时，下一个 RunInTx 先执行一次 fn 并回滚，运行 between
// （另一个事务在两次尝试之间提交），再用新事务把 fn 整体执行一次。
type replayOnceManager struct {
	execution.ExecutionManager
	replay  bool
	between func()
}

func (m *replayOnceManager) RunInTx(fn func(tx execution.ExecutionTx) error) error {
	if !m.replay {
		return m.ExecutionManager.RunInTx(fn)
	}
	m.replay = false
	err := m.ExecutionManager.RunInTx(func(tx execution.ExecutionTx) error {
		if err := fn(tx); err != nil {
			return err
		}
		return errReplayAttempt
	})
	if !errors.Is(err, errReplayAttempt) {
		return err
	}
	if m.between != nil {
		m.between()
	}
	return m.ExecutionManager.RunInTx(fn)
}

// 事务闭包被重放、且重放走了另一条分支时，写到外层的结果不能残留上一次尝试的值：第一次尝试终止了 Run 后回滚，
// 两次尝试之间另一个事务终止了它，重放读到 Run 已结束（父关闭策略的非 strict 终止直接返回）。这次调用没有写入
// 终态，不能执行关闭的副作用（停掉这个 Run 在本进程的定时器、向父 Run 传播）。
func TestReplayedTransactionDoesNotKeepFirstAttemptResult(t *testing.T) {
	ctx := context.Background()
	inner := memory.NewExecutionManager()
	mgr := &replayOnceManager{ExecutionManager: inner}
	srv := assembleServer(mgr, newTestTaskQueue(), newTestTaskQueue())
	t.Cleanup(srv.Stop)
	const runID = "run-replay-terminate"
	seedRunningWorkflowWithActivity(t, srv, mgr, runID)
	run, err := inner.Get(runID)
	if err != nil {
		t.Fatal(err)
	}
	ref := timerRef{Kind: timerWorkflowExecution, RunID: runID}
	srv.timers.Arm(ref, time.Now().Add(time.Hour))

	mgr.replay = true
	mgr.between = func() {
		if err := inner.RunInTx(func(tx execution.ExecutionTx) error {
			current, err := tx.GetRunForUpdate(runID)
			if err != nil {
				return err
			}
			treeID, branchID, err := runBranchInfoFromRun(current)
			if err != nil {
				return err
			}
			_, err = execution.Terminate(tx, current, treeID, branchID, "terminated by another transaction")
			return err
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := srv.terminateRun(ctx, run.Namespace, run.WorkflowID, runID, execution.ParentCloseReason, false); err != nil {
		t.Fatal(err)
	}
	if !timerArmed(srv, ref) {
		t.Fatal("the replayed terminate kept the rolled-back attempt's result and ran close side effects it did not commit")
	}
}
