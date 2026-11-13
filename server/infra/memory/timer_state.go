package memory

import (
	"cmp"
	"slices"
	"sync"
	"time"

	"github.com/donutnomad/tenuto/server/biz/domain/execution"
)

var _ execution.TimerStateStore = (*TimerStateStore)(nil)

type TimerStateStore struct {
	mu     sync.RWMutex
	states map[string]map[int64]*execution.TimerState // runID -> scheduledEventID -> state
}

func NewTimerStateStore() *TimerStateStore {
	return &TimerStateStore{states: make(map[string]map[int64]*execution.TimerState)}
}

func (s *TimerStateStore) GetTimerState(runID string, scheduledEventID int64) (*execution.TimerState, bool, error) {
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
	return cloneTimerState(st), true, nil
}

// UpsertTimerState 在 state 不存在时整份保存；已存在时只应用经 setter 改过的字段，与 gorm 后端一致。
func (s *TimerStateStore) UpsertTimerState(state *execution.TimerState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	next := cloneTimerState(state)
	if existing := s.states[state.RunID][state.ScheduledEventID]; existing != nil {
		applyPatch(existing, next, state.ExportPatch())
	} else {
		if s.states[state.RunID] == nil {
			s.states[state.RunID] = make(map[int64]*execution.TimerState)
		}
		next.ClearPatch()
		s.states[state.RunID][state.ScheduledEventID] = next
	}
	state.ClearPatch()
	return nil
}

// closeScheduled 把 runID 仍为 Scheduled 的 timer 置为 RunClosed。
func (s *TimerStateStore) closeScheduled(runID string, now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, st := range s.states[runID] {
		if st.IsScheduled() {
			st.Status = execution.TimerStatusEnums.RunClosed
			st.UpdatedAt = now
		}
	}
}

func (s *TimerStateStore) ListTimerStates(runID string) ([]*execution.TimerState, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	byRun := s.states[runID]
	out := make([]*execution.TimerState, 0, len(byRun))
	for _, st := range byRun {
		out = append(out, cloneTimerState(st))
	}
	slices.SortFunc(out, func(a, b *execution.TimerState) int {
		return cmp.Compare(a.ScheduledEventID, b.ScheduledEventID)
	})
	return out, nil
}

func cloneTimerState(st *execution.TimerState) *execution.TimerState {
	if st == nil {
		return nil
	}
	cloned := *st
	cloned.TraceContext = cloneStringMap(st.TraceContext)
	return &cloned
}
