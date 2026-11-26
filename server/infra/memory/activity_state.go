package memory

import (
	"cmp"
	"slices"
	"sync"
	"time"

	"github.com/donutnomad/tenuto/server/biz/domain/execution"
)

var _ execution.ActivityStateStore = (*ActivityStateStore)(nil)

type ActivityStateStore struct {
	mu     sync.RWMutex
	states map[string]map[int64]*execution.ActivityState // runID -> scheduledEventID -> state
}

func NewActivityStateStore() *ActivityStateStore {
	return &ActivityStateStore{states: make(map[string]map[int64]*execution.ActivityState)}
}

func (s *ActivityStateStore) GetActivityState(runID string, scheduledEventID int64) (*execution.ActivityState, bool, error) {
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
	return cloneActivityState(st), true, nil
}

// closeOpen 把 runID 仍为 Scheduled 或 Started 的 Activity 置为 RunClosed。
func (s *ActivityStateStore) closeOpen(runID string, now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, st := range s.states[runID] {
		if !st.IsTerminal() {
			st.Status = execution.ActivityStatusEnums.RunClosed
			st.UpdatedAt = now
		}
	}
}

// UpsertActivityState 在 state 不存在时整份保存；已存在时只应用经 setter 改过的字段，与 gorm 后端一致。
func (s *ActivityStateStore) UpsertActivityState(state *execution.ActivityState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	next := cloneActivityState(state)
	if existing := s.states[state.RunID][state.ScheduledEventID]; existing != nil {
		applyPatch(existing, next, state.ExportPatch())
	} else {
		if s.states[state.RunID] == nil {
			s.states[state.RunID] = make(map[int64]*execution.ActivityState)
		}
		next.ClearPatch()
		s.states[state.RunID][state.ScheduledEventID] = next
	}
	state.ClearPatch()
	return nil
}

func (s *ActivityStateStore) ListActivityStates(runID string) ([]*execution.ActivityState, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	byRun := s.states[runID]
	out := make([]*execution.ActivityState, 0, len(byRun))
	for _, st := range byRun {
		out = append(out, cloneActivityState(st))
	}
	slices.SortFunc(out, func(a, b *execution.ActivityState) int {
		return cmp.Compare(a.ScheduledEventID, b.ScheduledEventID)
	})
	return out, nil
}

func cloneActivityState(st *execution.ActivityState) *execution.ActivityState {
	if st == nil {
		return nil
	}
	cloned := *st
	cloned.LastHeartbeat = cloneAnyMap(st.LastHeartbeat)
	cloned.Input = cloneAnyMap(st.Input)
	cloned.TraceContext = cloneStringMap(st.TraceContext)
	if st.StartedTime != nil {
		t := *st.StartedTime
		cloned.StartedTime = &t
	}
	if st.LastHeartbeatAt != nil {
		t := *st.LastHeartbeatAt
		cloned.LastHeartbeatAt = &t
	}
	if st.NextAttemptAt != nil {
		t := *st.NextAttemptAt
		cloned.NextAttemptAt = &t
	}
	if st.RetryPolicy != nil {
		rp := *st.RetryPolicy
		if st.RetryPolicy.NonRetryableErrors != nil {
			rp.NonRetryableErrors = append([]string(nil), st.RetryPolicy.NonRetryableErrors...)
		}
		if st.RetryPolicy.NonRetryableErrorTypes != nil {
			rp.NonRetryableErrorTypes = append([]string(nil), st.RetryPolicy.NonRetryableErrorTypes...)
		}
		cloned.RetryPolicy = &rp
	}
	return &cloned
}
