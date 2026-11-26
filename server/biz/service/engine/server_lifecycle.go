package engine

import (
	"context"
	"log/slog"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
)

// cancelRun 是不拿 keyed lock 的协作式取消；拿锁入口见 CancelWorkflow。
// 父关闭策略 RequestCancel 对子 run 调用它。
// CheckCancellation 是事务外读取，只能在 RunInTx 外调用；已请求过取消时幂等返回 nil，不再追加事件。
func (s *Server) cancelRun(runID, reason string) error {
	already, err := s.runtime.CheckCancellation(runID)
	if err != nil {
		return err
	}
	if already {
		return nil
	}
	if err := s.telemetry.runInTx(context.Background(), "request_cancellation", s.mgr, func(tx execution.ExecutionTx) error {
		run, err := tx.GetRunForUpdate(runID)
		if err != nil {
			return err
		}
		treeID, branchID, err := runBranchInfoFromRun(run)
		if err != nil {
			return err
		}
		return execution.RequestCancellation(tx, run, treeID, branchID, reason)
	}); err != nil {
		return err
	}
	return s.scheduleWorkflowTask(context.Background(), runID)
}

// CancelWorkflow 请求协作式取消请求寻址的 Run（runID 为空时按 workflowID 解析 current），拿 per-(namespace, workflowID) 锁。
// 给了 runID 时先在 lease 外校验 Run（含 history 去重），已请求过取消则幂等返回 nil。
// 只给 workflowID 时 current 在 lease 内解析，终态判定用 run.Status（RunInTx 内不重入 mgr.GetEvents，避免死锁）；
// 重复的取消请求由 per-workflow 锁串行，WFT 处理对它们幂等。
func (s *Server) CancelWorkflow(ctx context.Context, namespace, workflowID, runID, reason string) error {
	if runID != "" {
		ref, err := s.resolveRunRef(namespace, workflowID, runID)
		if err != nil {
			return err
		}
		already, err := s.runtime.CheckCancellation(ref.ID)
		if err != nil {
			return err
		}
		if already {
			return nil
		}
	}
	return s.withWorkflowLease(ctx, namespace, workflowID, runID, func(tx execution.ExecutionTx, run *execution.Run) (LeaseAction, error) {
		// runBranchInfoFromRun 是纯内存版（不走 mgr.Get），避免在 RunInTx 内重入存储导致死锁。
		treeID, branchID, err := runBranchInfoFromRun(run)
		if err != nil {
			return LeaseAction{}, err
		}
		if e := execution.RequestCancellation(tx, run, treeID, branchID, reason); e != nil {
			return LeaseAction{}, e
		}
		return LeaseAction{ScheduleWFT: true}, nil
	})
}

// terminateWorkflowAfterCommit 是 Terminate 事务提交后的副作用：关闭收尾（runClosed）、向父传播 close。
// 这些是内存/跨 run 操作，不能进事务（propagateChildWorkflowClose 内部自带 RunInTx，memory 单锁不支持
// 嵌套）。
func (s *Server) terminateWorkflowAfterCommit(run *execution.Run, reason string) error {
	s.runClosed(run.ID)
	return s.propagateChildWorkflowClose(context.Background(), run.ID, run.ParentRunID, history.EventChildWorkflowTerminated, nil, reason, nil)
}

// TerminateWorkflow 终止请求寻址的 Run（runID 为空时按 workflowID 解析 current），拿 per-(namespace, workflowID) 锁。
// 给了 runID 时先在 lease 外读 execution state 判终态；只给 workflowID 时 current 在 lease 内解析，
// 终态判定用 run.Status（RunInTx 内不重入 mgr.GetEvents/reconcile，避免死锁）。
func (s *Server) TerminateWorkflow(ctx context.Context, namespace, workflowID, runID, reason string) error {
	if runID != "" {
		ref, err := s.resolveRunRef(namespace, workflowID, runID)
		if err != nil {
			return err
		}
		if err := s.runtime.CheckTermination(ref.ID); err != nil {
			return err
		}
	}
	return s.terminateRun(ctx, namespace, workflowID, runID, execution.TerminationReason(reason), true)
}

// terminateRun 终止寻址的 Run。strict 为 true 时已结束的 Run 返回错误；为 false 时（父关闭策略）已结束的 Run 不做处理。
func (s *Server) terminateRun(ctx context.Context, namespace, workflowID, runID, reason string, strict bool) error {
	var (
		inserted   bool
		runToClose *execution.Run
	)
	err := s.withWorkflowLease(ctx, namespace, workflowID, runID, func(tx execution.ExecutionTx, run *execution.Run) (LeaseAction, error) {
		// fn 可能被重放（死锁、换 current 重试）：上一次尝试的结果作废，Run 已结束的分支不残留 inserted。
		inserted, runToClose = false, nil
		if err := execution.EnsureTerminable(run); err != nil {
			if strict {
				return LeaseAction{}, err
			}
			return LeaseAction{}, nil
		}
		// runBranchInfoFromRun 是纯内存版（不走 mgr.Get），避免在 RunInTx 内重入存储导致死锁。
		treeID, branchID, err := runBranchInfoFromRun(run)
		if err != nil {
			return LeaseAction{}, err
		}
		ins, e := execution.Terminate(tx, run, treeID, branchID, reason)
		if e != nil {
			return LeaseAction{}, e
		}
		inserted = ins
		runToClose = run // 闭包捕获传出，供锁内事务外的副作用使用
		return LeaseAction{}, nil
	})
	if err != nil {
		return err
	}
	if !inserted {
		return nil
	}
	// 副作用在 withWorkflowLease 返回后执行——此刻 keyed lock 已释放（lease 内部 defer release）。
	// 之所以安全：终态（Terminated 事件 + run.Status）已在 lease 的事务内原子提交，且副作用本身幂等
	// （停 timer 重复调用无害，propagateChildWorkflowClose 自带 RunInTx 幂等），
	// 故无需在锁内执行。propagateChildWorkflowClose 内部还会自开 RunInTx，留在锁外亦避免嵌套事务死锁。
	return s.terminateWorkflowAfterCommit(runToClose, reason)
}

// runClosed 是 Run 终态提交之后的收尾，每条关闭路径都调用它，幂等：停掉这个 Run 在本进程的全部内存定时器，
// 唤醒等待它的调用方（Update 结果、Run 结束），按父关闭策略处理它仍在运行的子 Workflow。
// 父关闭策略没有做完（进程崩溃或出错）时，由子 Run 的 reconcileRun 补做（recoverParentClosePolicy）。
func (s *Server) runClosed(runID string) {
	s.timers.StopRun(runID)
	s.runWatch.Notify(runID)
	if err := s.enforceParentClosePolicy(context.Background(), runID); err != nil {
		s.logger.Error("parent close policy incomplete; reconciler will finish it", slog.String("run_id", runID), slog.Any("err", err))
	}
}

// failWorkflow 让 run 以失败终结：按 retry policy 决定建重试 run 还是直接落终态。
// 不重试时 WorkflowFailed 事件与 Run 的 Failed 状态在同一事务提交；重试时 WorkflowFailed 事件与重试 run
// 在同一事务提交。
func (s *Server) failWorkflow(runID string, failure protocol.Failure, timeoutType string) error {
	ctx := context.Background()
	failed, retryRunID, err := s.prepareWorkflowFailure(runID, failure, timeoutType)
	if err != nil {
		return err
	}
	// appendFailed 在事务内锁下读取 Run，追加 WorkflowFailed；不重试时 Run 的 Failed 状态随它提交。
	var run *execution.Run
	appendFailed := func(tx execution.ExecutionTx) (bool, error) {
		var err error
		if run, err = tx.GetRunForUpdate(runID); err != nil {
			return false, err
		}
		treeID, branchID, err := runBranchInfoFromRun(run)
		if err != nil {
			return false, err
		}
		return execution.AppendWorkflowFailed(tx, run, treeID, branchID, failed)
	}
	if retryRunID == "" {
		inserted := false
		if err := s.telemetry.runInTx(ctx, "fail_workflow", s.mgr, func(tx execution.ExecutionTx) error {
			var err error
			inserted, err = appendFailed(tx)
			return err
		}); err != nil || !inserted {
			return err
		}
		return s.workflowFailedAfterCommit(run, failure)
	}
	retry, err := s.prepareRetryRun(ctx, runID, retryRunID)
	if err != nil {
		return err
	}
	// 并发幂等：WorkflowFailed 终态事件先到先得，只有插入成功的一方建重试 run。
	inserted := false
	if err := s.telemetry.runInTx(ctx, "start_retry_run", s.mgr, func(tx execution.ExecutionTx) error {
		var err error
		if inserted, err = appendFailed(tx); err != nil || !inserted {
			return err
		}
		_, err = execution.CommitRetryRun(tx, retry.commit)
		return err
	}); err != nil {
		return err
	}
	if inserted {
		s.activateRetryRun(retry)
	}
	return nil
}

// workflowCompletedAfterCommit 是 WorkflowCompleted 与 Run 的终态提交后的副作用：释放本进程资源，向父 Run 传播关闭。
func (s *Server) workflowCompletedAfterCommit(run *execution.Run, output map[string]any) error {
	s.runClosed(run.ID)
	return s.propagateChildWorkflowClose(context.Background(), run.ID, run.ParentRunID, history.EventChildWorkflowCompleted, output, "", nil)
}

// workflowFailedAfterCommit 把结构化失败同时写进子工作流传播事件：父 run 的 ChildWorkflowFailed
// 因此与 WorkflowFailed 同形，父侧 future 不会只剩 message。
func (s *Server) workflowFailedAfterCommit(run *execution.Run, failure protocol.Failure) error {
	s.runClosed(run.ID)
	return s.propagateChildWorkflowClose(context.Background(), run.ID, run.ParentRunID, history.EventChildWorkflowFailed, nil, failure.Message, &failure)
}

func (s *Server) workflowCancelledAfterCommit(run *execution.Run) error {
	s.runClosed(run.ID)
	return s.propagateChildWorkflowClose(context.Background(), run.ID, run.ParentRunID, history.EventChildWorkflowCancelled, nil, "", nil)
}

// workflowFailure 取出 CmdFailWorkflow 的结构化失败；普通 error 只有 message，Type 为空。
func workflowFailure(cmd history.Command) protocol.Failure {
	if cmd.WorkflowFailure != nil {
		return *cmd.WorkflowFailure
	}
	return protocol.Failure{Message: cmd.Error}
}
