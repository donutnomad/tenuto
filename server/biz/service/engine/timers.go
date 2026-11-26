package engine

import (
	"context"
	"sync"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/server/biz/domain/clock"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
)

type timerKind uint8

const (
	timerWorkflowExecution timerKind = iota + 1
	timerWorkflowRun
	timerWorkflowTaskRetry
	timerWorkflowTaskTimeout
	timerDurable
	timerActivityScheduleToClose
	timerActivityScheduleToStart
	timerActivityStartToClose
	timerActivityHeartbeat
	timerActivityRetry
)

// activityTimeoutKinds 是 Activity 四类超时的 timer 种类及其超时种类。
var activityTimeoutKinds = map[timerKind]protocol.TimeoutType{
	timerActivityScheduleToClose: protocol.TimeoutTypeScheduleToClose,
	timerActivityScheduleToStart: protocol.TimeoutTypeScheduleToStart,
	timerActivityStartToClose:    protocol.TimeoutTypeStartToClose,
	timerActivityHeartbeat:       protocol.TimeoutTypeHeartbeat,
}

// activityAttemptTimerKinds 是随 attempt 更替的 Activity timer；schedule_to_close 贯穿全部 attempt。
var activityAttemptTimerKinds = []timerKind{
	timerActivityScheduleToStart,
	timerActivityStartToClose,
	timerActivityHeartbeat,
	timerActivityRetry,
}

// timerRef 定位一个进程内 timer。run 级 timer 的 ScheduledEventID 为 0。
type timerRef struct {
	Kind             timerKind
	RunID            string
	ScheduledEventID int64
}

func activityTimerRef(kind timerKind, runID string, scheduledEventID int64) timerRef {
	return timerRef{Kind: kind, RunID: runID, ScheduledEventID: scheduledEventID}
}

// timerSet 保存进程内全部 timer 句柄，每个 timer 只有键与截止时间。
// 到期时先把条目摘除，再在锁外调用 fire；业务状态由 fire 重读持久化存储。
// 锁顺序：keyedMu -> timerSet.mu。fire 在未持 timerSet.mu 时执行，可以再取 keyedMu 与 Arm/Stop。
type timerSet struct {
	clock clock.Clock
	fire  func(timerRef)

	mu   sync.Mutex
	runs map[string]map[timerRef]*armedTimer
}

type armedTimer struct {
	deadline time.Time
	timer    clock.Timer
}

func newTimerSet(clk clock.Clock, fire func(timerRef)) *timerSet {
	return &timerSet{clock: clk, fire: fire, runs: make(map[string]map[timerRef]*armedTimer)}
}

// Arm 让 ref 在 deadline 到期，替换同键的旧 timer。deadline 已过时立即到期。
func (t *timerSet) Arm(ref timerRef, deadline time.Time) {
	entry := &armedTimer{deadline: deadline}
	t.mu.Lock()
	defer t.mu.Unlock()
	run := t.runs[ref.RunID]
	if run == nil {
		run = make(map[timerRef]*armedTimer)
		t.runs[ref.RunID] = run
	}
	if old := run[ref]; old != nil {
		old.timer.Stop()
	}
	// 回调先取 t.mu，此时 entry.timer 已赋值、条目已登记。
	entry.timer = t.clock.AfterFunc(deadline.Sub(t.clock.Now()), func() {
		if t.take(ref, entry) {
			t.fire(ref)
		}
	})
	run[ref] = entry
}

// take 摘除到期条目；条目已被 Stop 或被新的 Arm 替换时返回 false。
func (t *timerSet) take(ref timerRef, entry *armedTimer) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	run := t.runs[ref.RunID]
	if run[ref] != entry {
		return false
	}
	t.remove(run, ref)
	return true
}

func (t *timerSet) remove(run map[timerRef]*armedTimer, ref timerRef) {
	delete(run, ref)
	if len(run) == 0 {
		delete(t.runs, ref.RunID)
	}
}

func (t *timerSet) Stop(refs ...timerRef) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, ref := range refs {
		run := t.runs[ref.RunID]
		if entry := run[ref]; entry != nil {
			entry.timer.Stop()
			t.remove(run, ref)
		}
	}
}

// StopRun 停掉一个 run 的全部 timer。
func (t *timerSet) StopRun(runID string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, entry := range t.runs[runID] {
		entry.timer.Stop()
	}
	delete(t.runs, runID)
}

func (t *timerSet) StopAll() {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, run := range t.runs {
		for _, entry := range run {
			entry.timer.Stop()
		}
	}
	t.runs = make(map[string]map[timerRef]*armedTimer)
}

// Deadline 返回 ref 待到期 timer 的截止时间。
func (t *timerSet) Deadline(ref timerRef) (time.Time, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	entry := t.runs[ref.RunID][ref]
	if entry == nil {
		return time.Time{}, false
	}
	return entry.deadline, true
}

// fireTimer 是全部进程内 timer 的到期入口。timer 只带键，各 handler 重读持久化状态后决定是否生效。
// timer 没有请求 ctx，用 Background。
func (s *Server) fireTimer(ref timerRef) {
	ctx := context.Background()
	switch ref.Kind {
	case timerWorkflowExecution:
		s.handleWorkflowTimeout(ref.RunID)
	case timerWorkflowRun:
		s.handleRunTimeout(ref.RunID)
	case timerWorkflowTaskRetry:
		s.handleWorkflowTaskRetryFired(ctx, ref.RunID)
	case timerWorkflowTaskTimeout:
		s.handleWorkflowTaskTimeoutFired(ctx, ref.RunID, ref.ScheduledEventID)
	case timerDurable:
		s.handleTimerFired(ctx, ref.RunID, ref.ScheduledEventID)
	case timerActivityRetry:
		s.handleActivityRetryFired(ctx, ref.RunID, ref.ScheduledEventID)
	default:
		if reason, ok := activityTimeoutKinds[ref.Kind]; ok {
			s.handleActivityTimeout(ref.RunID, ref.ScheduledEventID, reason)
		}
	}
}

// armActivityTimeouts 按持久化状态排定该 Activity 当前生效的超时 timer，停掉已不适用的。
// armOverdue 为 false 时跳过已过期的截止时间，留给 FireOverdueTimers 按宽限期接管。
func (s *Server) armActivityTimeouts(st *execution.ActivityState, armOverdue bool) {
	for kind, reason := range activityTimeoutKinds {
		ref := activityTimerRef(kind, st.RunID, st.ScheduledEventID)
		deadline, ok := execution.ActivityTimeoutDeadline(st, reason)
		switch {
		case !ok:
			s.timers.Stop(ref)
		case armOverdue || deadline.After(s.clock.Now()):
			s.timers.Arm(ref, deadline)
		}
	}
}

func (s *Server) stopActivityAttemptTimers(runID string, scheduledEventID int64) {
	refs := make([]timerRef, 0, len(activityAttemptTimerKinds))
	for _, kind := range activityAttemptTimerKinds {
		refs = append(refs, activityTimerRef(kind, runID, scheduledEventID))
	}
	s.timers.Stop(refs...)
}

func (s *Server) stopActivityTimers(runID string, scheduledEventID int64) {
	s.stopActivityAttemptTimers(runID, scheduledEventID)
	s.timers.Stop(activityTimerRef(timerActivityScheduleToClose, runID, scheduledEventID))
}

// handleActivityRetryFired 在重试退避到期后按最新状态重新派发（携带新 attempt 与 stamp）。
func (s *Server) handleActivityRetryFired(ctx context.Context, runID string, scheduledEventID int64) {
	st, ok, err := s.mgr.GetActivityState(runID, scheduledEventID)
	if err != nil || !ok || !st.IsScheduled() {
		return
	}
	_, _ = s.redispatchActivityTask(ctx, s.activityTaskFromState(st))
}

// handleWorkflowTaskRetryFired 在 WFT 失败退避到期后重调度，trace 接续最近一次 WorkflowTaskFailed。
func (s *Server) handleWorkflowTaskRetryFired(ctx context.Context, runID string) {
	if state, err := s.mgr.GetWorkflowExecutionState(runID); err == nil && state != nil {
		ctx = s.telemetry.extract(ctx, state.WorkflowTask.LastFailureTraceContext)
	}
	if err := s.resumeWorkflowTask(ctx, runID); err != nil {
		s.logger.Error("reschedule workflow task after workflow task failure failed", "run_id", runID, "err", err)
	}
}
