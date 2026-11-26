package memory

import (
	"cmp"
	"slices"
	"sync"

	"github.com/donutnomad/tenuto/server/biz/domain/execution"
)

var _ execution.ChildWorkflowStateStore = (*ChildWorkflowStateStore)(nil)

type ChildWorkflowStateStore struct {
	mu     sync.RWMutex
	states map[string]map[int64]*execution.ChildWorkflowState // runID -> scheduledEventID -> state
}

func NewChildWorkflowStateStore() *ChildWorkflowStateStore {
	return &ChildWorkflowStateStore{states: make(map[string]map[int64]*execution.ChildWorkflowState)}
}

func (s *ChildWorkflowStateStore) GetChildWorkflowState(runID string, scheduledEventID int64) (*execution.ChildWorkflowState, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	byRun := s.states[runID]
	if byRun == nil {
		return nil, false, nil
	}
	st, ok := byRun[scheduledEventID]
	if !ok {
		return nil, false, nil
	}
	return cloneChildWorkflowState(st), true, nil
}

// UpsertChildWorkflowState 在 state 不存在时整份保存；已存在时只应用经 setter 改过的字段，与 gorm 后端一致。
func (s *ChildWorkflowStateStore) UpsertChildWorkflowState(state *execution.ChildWorkflowState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	next := cloneChildWorkflowState(state)
	if existing := s.states[state.RunID][state.ScheduledEventID]; existing != nil {
		applyPatch(existing, next, state.ExportPatch())
	} else {
		if s.states[state.RunID] == nil {
			s.states[state.RunID] = make(map[int64]*execution.ChildWorkflowState)
		}
		next.ClearPatch()
		s.states[state.RunID][state.ScheduledEventID] = next
	}
	state.ClearPatch()
	return nil
}

func (s *ChildWorkflowStateStore) ListChildWorkflowStates(runID string) ([]*execution.ChildWorkflowState, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	byRun := s.states[runID]
	out := make([]*execution.ChildWorkflowState, 0, len(byRun))
	for _, st := range byRun {
		out = append(out, cloneChildWorkflowState(st))
	}
	slices.SortFunc(out, func(a, b *execution.ChildWorkflowState) int {
		return cmp.Compare(a.ScheduledEventID, b.ScheduledEventID)
	})
	return out, nil
}

func (s *ChildWorkflowStateStore) ListOpenChildWorkflowStates(runIDs []string) ([]*execution.ChildWorkflowState, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []*execution.ChildWorkflowState
	for _, runID := range slices.Compact(slices.Sorted(slices.Values(runIDs))) {
		for _, st := range s.states[runID] {
			if !st.IsTerminal() {
				out = append(out, cloneChildWorkflowState(st))
			}
		}
	}
	slices.SortFunc(out, func(a, b *execution.ChildWorkflowState) int {
		return cmp.Or(cmp.Compare(a.RunID, b.RunID), cmp.Compare(a.ScheduledEventID, b.ScheduledEventID))
	})
	return out, nil
}

func (s *ChildWorkflowStateStore) GetChildWorkflowStates(keys []execution.ChildWorkflowKey) (map[execution.ChildWorkflowKey]*execution.ChildWorkflowState, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[execution.ChildWorkflowKey]*execution.ChildWorkflowState, len(keys))
	for _, key := range keys {
		if st := s.states[key.RunID][key.ScheduledEventID]; st != nil {
			out[key] = cloneChildWorkflowState(st)
		}
	}
	return out, nil
}

func cloneChildWorkflowState(st *execution.ChildWorkflowState) *execution.ChildWorkflowState {
	if st == nil {
		return nil
	}
	cloned := *st
	cloned.Input = cloneAnyMap(st.Input)
	cloned.Output = cloneAnyMap(st.Output)
	return &cloned
}
