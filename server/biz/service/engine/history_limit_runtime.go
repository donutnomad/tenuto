package engine

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
)

const historyLimitFailureType = "HistoryLimitExceeded"

// runInTx 执行一个引擎事务，超限的处理见 closeAtHistoryLimit。Workflow Task 完成走 closeWorkflowForHistoryLimit，
// 保留本次完成事件。
func (s *Server) runInTx(ctx context.Context, operation string, fn func(execution.ExecutionTx) error) error {
	return s.closeAtHistoryLimit(ctx, s.telemetry.runInTx(ctx, operation, s.mgr, fn))
}

// closeAtHistoryLimit 原样返回 err。err 是某个 Run 的 History 超过硬上限时，这个 Run 写不进任何非关闭事件
// （WorkflowTaskScheduled、Signal、Update、Activity 与 Timer 结果），不会再推进：先用关闭预留把它以
// HistoryLimitExceeded 失败关闭（对标 Temporal 超限时强制结束 Run），调用方照常拿到原错误。
func (s *Server) closeAtHistoryLimit(ctx context.Context, err error) error {
	var limitErr *history.HistoryLimitExceededError
	if errors.As(err, &limitErr) && limitErr.RunID != "" {
		if closeErr := s.failRunAtHistoryLimit(ctx, limitErr); closeErr != nil {
			s.logger.Error("close run at history limit failed", slog.String("run_id", limitErr.RunID), slog.Any("err", closeErr))
		}
	}
	return err
}

// failRunAtHistoryLimit 以 WorkflowFailed(HistoryLimitExceeded) 关闭 History 超限的 Run。终态事件用关闭预留写入；
// 关闭的后续处理（timer、子 Workflow 的父关闭策略、向父 Run 传播）与其他失败关闭相同。Run 已结束时不做处理。
func (s *Server) failRunAtHistoryLimit(ctx context.Context, limitErr *history.HistoryLimitExceededError) error {
	failure := protocol.Failure{Message: limitErr.Error(), Type: historyLimitFailureType}
	var run *execution.Run
	inserted := false
	if err := s.telemetry.runInTx(context.WithoutCancel(ctx), "fail_history_limit", s.mgr, func(tx execution.ExecutionTx) error {
		var err error
		if run, err = tx.GetRunForUpdate(limitErr.RunID); err != nil {
			return err
		}
		if history.IsTerminalStatus(run.Status) {
			return nil
		}
		treeID, branchID, err := runBranchInfoFromRun(run)
		if err != nil {
			return err
		}
		failed, err := history.NewWorkflowFailedFailureEvent(failure, "")
		if err != nil {
			return err
		}
		inserted, err = execution.AppendWorkflowFailed(tx, run, treeID, branchID, failed)
		return err
	}); err != nil || !inserted {
		return err
	}
	s.logger.Warn("run closed at history limit", slog.String("run_id", run.ID), slog.String("reason", failure.Message))
	return s.workflowFailedAfterCommit(run, failure)
}

func (s *Server) closeWorkflowForHistoryLimit(ctx context.Context, runID, treeID, branchID string, closeEvent history.Event, limitErr *history.HistoryLimitExceededError) error {
	message := "workflow history limit exceeded"
	if limitErr != nil {
		message = limitErr.Error()
	}
	var run *execution.Run
	var result execution.WorkflowTaskCompletionResult
	postCommitCtx := context.WithoutCancel(ctx)
	if err := s.telemetry.runInTx(postCommitCtx, "close_history_limit", s.mgr, func(tx execution.ExecutionTx) error {
		var err error
		if run, err = tx.GetRunForUpdate(runID); err != nil {
			return err
		}
		result, err = execution.CloseWorkflowTaskAtHistoryLimit(tx, run, treeID, branchID, closeEvent, message, historyLimitFailureType)
		return err
	}); err != nil {
		return fmt.Errorf("close workflow after history limit: %w", err)
	}
	if !result.ClosedInserted {
		return nil
	}
	return s.workflowFailedAfterCommit(run, protocol.Failure{Message: message, Type: historyLimitFailureType})
}
