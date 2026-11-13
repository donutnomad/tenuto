package engine

import (
	"context"
	"fmt"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
)

const historyLimitFailureType = "HistoryLimitExceeded"

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
