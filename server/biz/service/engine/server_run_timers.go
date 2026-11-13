package engine

import (
	"context"
	"time"

	programpkg "github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
)

// applyRunTimeouts 按 Run 采用的配置补齐截止时间，返回 Run 是否有改动；新 Run 在建 Run 的事务之前调用。
func applyRunTimeouts(run *execution.Run, wf *programpkg.WorkflowConfig, now time.Time) bool {
	executionTimeout, runTimeout := configTimeouts(wf)
	return run.ApplyTimeouts(executionTimeout, runTimeout, now)
}

// configTimeouts 是配置声明的执行超时与 Run 超时，解析规则见 execution.ConfiguredTimeout。
func configTimeouts(wf *programpkg.WorkflowConfig) (executionTimeout, runTimeout time.Duration) {
	if wf == nil {
		return 0, 0
	}
	return execution.ConfiguredTimeout(wf.ExecutionTimeout), execution.ConfiguredTimeout(wf.RunTimeout)
}

// armRunTimers 按 Run 持久化的截止时间挂进程内 timer。
func (s *Server) armRunTimers(run *execution.Run) {
	if run.DeadlineAt != nil {
		s.timers.Arm(timerRef{Kind: timerWorkflowExecution, RunID: run.ID}, *run.DeadlineAt)
	}
	if run.RunDeadlineAt != nil {
		s.timers.Arm(timerRef{Kind: timerWorkflowRun, RunID: run.ID}, *run.RunDeadlineAt)
	}
}

// restoreRunDeadlines 是启动恢复：Run 缺少配置声明的截止时间时（例如升级到声明了超时的 Program）从现在起算，
// 在事务内按锁下读到的 Run 写回，返回写回后的 Run；配置没有声明超时时返回 nil。
func (s *Server) restoreRunDeadlines(runID string) (*execution.Run, error) {
	wf, err := s.runWorkflowConfig(runID)
	if err != nil {
		return nil, err
	}
	executionTimeout, runTimeout := configTimeouts(wf)
	if executionTimeout <= 0 && runTimeout <= 0 {
		return nil, nil
	}
	var restored *execution.Run
	err = s.telemetry.runInTx(context.Background(), "restore_run_deadlines", s.mgr, func(tx execution.ExecutionTx) error {
		run, err := tx.GetRunForUpdate(runID)
		if err != nil {
			return err
		}
		restored = run
		return execution.RestoreRunTimeouts(tx, run, executionTimeout, runTimeout, s.clock.Now())
	})
	return restored, err
}

func (s *Server) handleWorkflowTimeout(runID string) {
	// timer 在独立 goroutine 触发，改 run 状态/写 run 事件须与并发 WFT/signal 同 workflowID 串行，
	// 故先拿 per-(namespace, workflowID) keyed lock（对标 Temporal timer fire 也拿 workflow lock）。
	// 锁外按 runID 取 namespace+workflowID；本 handler 下游（failWorkflow/startRetryRun）均不拿
	// keyed lock，且派 WFT 仅异步入队，故同 workflowID 不重入。timer 无请求 ctx，用 Background。
	_, release, err := s.lockRunWorkflow(context.Background(), runID)
	if err != nil {
		return
	}
	defer release()

	status, err := s.GetRunStatus(runID)
	if err != nil || status != history.StatusRunning {
		return
	}
	// 资源释放在 failWorkflow 提交终态之后进行；提交失败时保留现场，由到期扫描重试。
	if err := s.failWorkflow(runID, protocol.Failure{Message: "workflow execution timeout"}, TimeoutTypeExecution); err != nil {
		s.logger.Error("fail workflow on execution timeout failed", "run_id", runID, "err", err)
	}
}

func (s *Server) handleRunTimeout(runID string) {
	// 同 handleWorkflowTimeout：timer goroutine 改 run 状态须拿 per-(namespace, workflowID) keyed lock 串行化。
	// 下游 failWorkflow 不拿 keyed lock、派 WFT 仅异步入队，同 workflowID 不重入。
	_, release, err := s.lockRunWorkflow(context.Background(), runID)
	if err != nil {
		return
	}
	defer release()

	status, err := s.GetRunStatus(runID)
	if err != nil || status != history.StatusRunning {
		return
	}
	// run timeout 对标 Temporal 也走 retry 决策：failWorkflow 统一决定重试或终结，并在提交后释放资源。
	if err := s.failWorkflow(runID, protocol.Failure{Message: "workflow run timeout"}, TimeoutTypeRun); err != nil {
		s.logger.Error("fail workflow on run timeout failed", "run_id", runID, "err", err)
	}
}
