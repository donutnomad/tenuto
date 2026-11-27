package engine

import (
	"context"
	"fmt"

	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
)

// runBranchInfo 返回 Run 的 History tree 与 branch。
func (s *Server) runBranchInfo(runID string) (treeID, branchID string, err error) {
	run, err := s.mgr.Get(runID)
	if err != nil {
		return "", "", err
	}
	return runBranchInfoFromRun(run)
}

// runBranchInfoFromRun 从 Run 的 branch token 解出 tree 与 branch。每个 Run 创建时都带 branch token，
// 解不出来说明数据损坏。
func runBranchInfoFromRun(run *execution.Run) (treeID, branchID string, err error) {
	branch, err := execution.DeserializeBranchToken(run.BranchToken)
	if err != nil {
		return "", "", fmt.Errorf("run %s: %w", run.ID, err)
	}
	return branch.TreeID, branch.BranchID, nil
}

func (s *Server) appendWorkflowTaskScheduled(runID, treeID, branchID string, event history.Event) (int64, bool, error) {
	if event.WorkflowTaskScheduled == nil {
		event.WorkflowTaskScheduled = &history.WorkflowTaskScheduledEvent{}
	}
	eventID, inserted, err := execution.AppendEvent(s.mgr, runID, treeID, branchID, event)
	return eventID, inserted, s.closeAtHistoryLimit(context.Background(), err)
}

// workflowAlreadyTerminated 报告 Run 是否已有终态，读 execution state。
func (s *Server) workflowAlreadyTerminated(runID string) (bool, error) {
	status, err := s.GetRunStatus(runID)
	return history.IsTerminalStatus(status), err
}

func (s *Server) GetEvents(runID string) ([]history.Event, error) {
	return s.mgr.GetEvents(runID)
}

// GetWorkflowExecutionState 返回 Run 的 execution state，当前状态一律从这里读取。
func (s *Server) GetWorkflowExecutionState(runID string) (*execution.WorkflowExecutionState, error) {
	return s.mgr.GetWorkflowExecutionState(runID)
}

// GetRunStatus 返回 execution state 投影的状态；Run 不存在时返回的错误满足 errors.Is(err, ErrRunNotFound)。
func (s *Server) GetRunStatus(runID string) (history.Status, error) {
	state, err := s.mgr.GetWorkflowExecutionState(runID)
	if err != nil {
		return "", err
	}
	return state.Status, nil
}
