package memory

import (
	"fmt"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/donutnomad/tenuto/server/biz/domain/clock"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/infra/systemclock"
)

type WorkflowExecutionStateStore struct {
	mu     sync.RWMutex
	states map[string]*execution.WorkflowExecutionState
	// clock 给 sticky、激活与 Run 同步写入的时间字段取当前时刻。
	clock clock.Clock
}

func (s *WorkflowExecutionStateStore) AssignStickyExecution(runID string, executionGeneration, expectedStickyGeneration int64, workerIdentity, workerBuildID string) (int64, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	state, ok := s.states[runID]
	if !ok {
		return 0, false, fmt.Errorf("workflow execution state %s: %w", runID, execution.ErrRunNotFound)
	}
	assigned := state.AssignSticky(executionGeneration, expectedStickyGeneration, workerIdentity, workerBuildID, s.clock.Now())
	state.ClearPatch()
	return state.StickyGeneration, assigned, nil
}

func (s *WorkflowExecutionStateStore) ClearStickyExecution(runID string, executionGeneration, expectedStickyGeneration int64) (int64, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	state, ok := s.states[runID]
	if !ok {
		return 0, false, fmt.Errorf("workflow execution state %s: %w", runID, execution.ErrRunNotFound)
	}
	cleared := state.ClearSticky(executionGeneration, expectedStickyGeneration, s.clock.Now())
	state.ClearPatch()
	return state.StickyGeneration, cleared, nil
}

func NewWorkflowExecutionStateStore() *WorkflowExecutionStateStore {
	return &WorkflowExecutionStateStore{states: make(map[string]*execution.WorkflowExecutionState), clock: systemclock.Clock{}}
}

func (s *WorkflowExecutionStateStore) GetWorkflowExecutionState(runID string) (*execution.WorkflowExecutionState, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	state, ok := s.states[runID]
	if !ok {
		return nil, fmt.Errorf("workflow execution state %s: %w", runID, execution.ErrRunNotFound)
	}
	return execution.CloneWorkflowExecutionState(state), nil
}

func (s *WorkflowExecutionStateStore) ListWorkflowExecutionStates(runIDs []string) (map[string]*execution.WorkflowExecutionState, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make(map[string]*execution.WorkflowExecutionState, len(runIDs))
	for _, runID := range runIDs {
		if state, ok := s.states[runID]; ok {
			result[runID] = execution.CloneWorkflowExecutionState(state)
		}
	}
	return result, nil
}

// beginWorkflowActivation 在已提交的 state 上执行 BeginActivation，由事务提交时调用。
func (s *WorkflowExecutionStateStore) beginWorkflowActivation(runID string, scheduledEventID, executionGeneration, stickyGeneration int64, activationID string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	state, ok := s.states[runID]
	if !ok {
		return false, fmt.Errorf("workflow execution state %s: %w", runID, execution.ErrRunNotFound)
	}
	begun := state.BeginActivation(scheduledEventID, executionGeneration, stickyGeneration, activationID, s.clock.Now())
	state.ClearPatch()
	return begun, nil
}

func (s *WorkflowExecutionStateStore) put(state *execution.WorkflowExecutionState) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.states[state.RunID] = execution.CloneWorkflowExecutionState(state)
}

func (s *WorkflowExecutionStateStore) updateFromRun(run *execution.Run) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	state, ok := s.states[run.ID]
	if !ok {
		return fmt.Errorf("workflow execution state %s: %w", run.ID, execution.ErrRunNotFound)
	}
	state.SyncFromRun(run, s.clock.Now())
	return nil
}

// ListExpiredStickyExecutions 按 runID 顺序返回 sticky schedule-to-start 期限已过、仍有 open WFT 的 execution state。
func (s *WorkflowExecutionStateStore) ListExpiredStickyExecutions(now time.Time, limit int) ([]*execution.WorkflowExecutionState, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var expired []*execution.WorkflowExecutionState
	for _, runID := range slices.Sorted(maps.Keys(s.states)) {
		state := s.states[runID]
		if state.StickyWorkerIdentity == "" || state.StickyExpiresAt == nil || state.StickyValidAt(now) || state.OpenWorkflowTaskEventID == 0 {
			continue
		}
		expired = append(expired, execution.CloneWorkflowExecutionState(state))
		if limit > 0 && len(expired) == limit {
			break
		}
	}
	return expired, nil
}
