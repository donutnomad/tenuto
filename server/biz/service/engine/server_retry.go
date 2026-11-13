package engine

import (
	"context"
	"fmt"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"

	"github.com/donutnomad/tenuto/server/biz/domain/execution"
)

// prepareWorkflowFailure 在事务外决定 run 失败后重试还是终结，返回要落库的 WorkflowFailed 事件。
// 配置了 RetryPolicy 时事件的 RetryState 记录这次决定：重试时为 InProgress，不再重试时说明原因
// （NonRetryableFailure、MaximumAttemptsReached、Timeout）。重试时 retryRunID 非空，事件带 NewExecutionRunID：
// 事件落库后，建重试 run 这一步由 startRetryRun 完成，失败时 reconcileRun 按事件里的 NewExecutionRunID 重做。
func (s *Server) prepareWorkflowFailure(runID string, failure protocol.Failure, timeoutType string) (failed history.Event, retryRunID string, err error) {
	failed, err = history.NewWorkflowFailedFailureEvent(failure, timeoutType)
	if err != nil {
		return history.Event{}, "", err
	}
	run, err := s.mgr.Get(runID)
	if err != nil {
		return history.Event{}, "", err
	}
	wf, cfgErr := s.runWorkflowConfig(runID)
	if cfgErr != nil || wf == nil || wf.RetryPolicy == nil {
		return failed, "", nil
	}
	should, _, retryState := execution.DecideWorkflowRetry(run, wf.RetryPolicy, failure, s.clock.Now())
	failed.WorkflowFailed.RetryState = retryState
	if !should {
		return failed, "", nil
	}
	retryRunID = execution.NewID()
	failed.WorkflowFailed.NewExecutionRunID = retryRunID
	return failed, retryRunID, nil
}

// retryRunStart 是事务外备好的重试 run：commit 在事务内提交，backoff 用于提交后挂内存 timer。
type retryRunStart struct {
	commit  execution.RetryRunCommit
	backoff time.Duration
}

// prepareRetryRun 备好重试 run、它的 started 事件与延迟首个 WFT 的 backoff timer。
// 对标 Temporal SetupNewWorkflowForRetryOrCron + WorkflowBackoffTimerTask。重试 run 按事务外读到的旧 run 构造；
// 提交它的事务（execution.CommitRetryRun）在锁下重读旧 run，重试 run 的有效策略取那一次读取。
//
// 铁律：runInTx 闭包内绝不能做非事务读——事务已占住一条连接，闭包内走 s.db 的读要再取一条；
// 连接池耗尽时它永久等连接而自死锁，且读不到本事务的未提交写入。所有读都在这里完成。
func (s *Server) prepareRetryRun(ctx context.Context, oldRunID, retryRunID string) (retryRunStart, error) {
	oldRun, err := s.mgr.Get(oldRunID)
	if err != nil {
		return retryRunStart{}, err
	}
	wfCfg, err := s.runWorkflowConfig(oldRunID)
	if err != nil {
		return retryRunStart{}, err
	}
	if wfCfg == nil || wfCfg.RetryPolicy == nil {
		return retryRunStart{}, fmt.Errorf("retry run %s: workflow retry policy missing for run %s", retryRunID, oldRunID)
	}
	// 照 Temporal：重试只继承旧 Run 启动时的搜索属性，运行期写入的值不带。
	oldEvents, err := s.mgr.GetEvents(oldRunID)
	if err != nil {
		return retryRunStart{}, err
	}
	startAttrs, err := execution.StartSearchAttributes(oldEvents)
	if err != nil {
		return retryRunStart{}, fmt.Errorf("retry run %s: run %s: %w", retryRunID, oldRunID, err)
	}
	now := s.clock.Now()
	newRun := execution.NewRetryRun(oldRun, retryRunID, now, startAttrs)
	applyRunTimeouts(newRun, wfCfg, now)
	_, _, startedEvent, err := execution.PrepareStartedRun(newRun, wfCfg.Name, execution.ExecutionTimeoutMs(wfCfg.ExecutionTimeout))
	if err != nil {
		return retryRunStart{}, err
	}
	backoff := execution.WorkflowRetryBackoff(wfCfg.RetryPolicy, oldRun.Attempt)
	oldTreeID, oldBranchID, err := runBranchInfoFromRun(oldRun)
	if err != nil {
		return retryRunStart{}, err
	}
	return retryRunStart{
		backoff: backoff,
		commit: execution.RetryRunCommit{
			OldRunID: oldRun.ID, OldTreeID: oldTreeID, OldBranchID: oldBranchID,
			NewRun: newRun, StartedEvent: startedEvent,
			TimerState: execution.NewRetryBackoffTimer(retryRunID, backoff, now, s.telemetry.inject(ctx)),
			// WorkflowFailed 事件由调用方落库（prepareWorkflowFailure 构造）。
			FailureAlreadyAppended: true,
		},
	}, nil
}

// activateRetryRun 是重试 run 提交后的进程内动作：释放旧 run 的资源，挂新 run 的内存 timer。
// 崩溃恢复由 recoverDurableTimers 与 reconcileRun 重挂 Run 截止时间覆盖。
func (s *Server) activateRetryRun(start retryRunStart) {
	s.runClosed(start.commit.OldRunID)
	timer := start.commit.TimerState
	s.startDurableTimer(timer.RunID, timer.ScheduledEventID, timer.Deadline)
	// 新 run 继承 execution deadline，run deadline 随新 run 写入。
	s.armRunTimers(start.commit.NewRun)
}

// startRetryRun 为已落 WorkflowFailed（带 NewExecutionRunID）的旧 run 建重试 run：
// 同一事务内把旧 run 置 Failed、切 current、建新 run 并持久化 backoff timer。
func (s *Server) startRetryRun(ctx context.Context, oldRunID, retryRunID string) error {
	start, err := s.prepareRetryRun(ctx, oldRunID, retryRunID)
	if err != nil {
		return err
	}
	if err := s.telemetry.runInTx(ctx, "start_retry_run", s.mgr, func(tx execution.ExecutionTx) error {
		_, err := execution.CommitRetryRun(tx, start.commit)
		return err
	}); err != nil {
		return err
	}
	s.activateRetryRun(start)
	return nil
}
