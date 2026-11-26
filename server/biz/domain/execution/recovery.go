package execution

import (
	"time"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
)

type RunRecoveryAction uint8

const (
	RunRecoveryResume RunRecoveryAction = iota
	RunRecoveryWorkflowTimeout
	RunRecoveryRunTimeout
)

// RecoveryActionFor selects the recovery path of a Running Run. Workflow timeout
// takes precedence when both deadlines have elapsed.
func RecoveryActionFor(current *Run, now time.Time) RunRecoveryAction {
	if WorkflowDeadlineExpired(current, now) {
		return RunRecoveryWorkflowTimeout
	}
	if RunDeadlineExpired(current, now) {
		return RunRecoveryRunTimeout
	}
	return RunRecoveryResume
}

type ActivityRecoveryDisposition uint8

const (
	ActivityRecoverySkip ActivityRecoveryDisposition = iota
	ActivityRecoveryRestoreTimers
	ActivityRecoveryRedispatch
	// ActivityRecoveryAwaitRetry：重试退避尚未到期，只重挂退避 timer，到期后再派发。
	ActivityRecoveryAwaitRetry
)

// ActivityRecoveryFor keeps the persisted state authoritative during recovery.
func ActivityRecoveryFor(state *ActivityState, now time.Time) ActivityRecoveryDisposition {
	if state == nil || state.IsTerminal() {
		return ActivityRecoverySkip
	}
	if state.RetryBackoffPending(now) {
		return ActivityRecoveryAwaitRetry
	}
	if state.IsScheduled() {
		return ActivityRecoveryRedispatch
	}
	if state.IsStarted() {
		return ActivityRecoveryRestoreTimers
	}
	return ActivityRecoverySkip
}

const activityRetryStateTimeout = "Timeout"

// activityTimeoutTypes lists every timeout type in reporting priority order.
var activityTimeoutTypes = []protocol.TimeoutType{
	protocol.TimeoutTypeScheduleToClose,
	protocol.TimeoutTypeStartToClose,
	protocol.TimeoutTypeHeartbeat,
	protocol.TimeoutTypeScheduleToStart,
}

// OverdueActivityTimeout reports the first activity timeout whose persisted
// deadline is before cutoff.
func OverdueActivityTimeout(state *ActivityState, cutoff time.Time) (protocol.TimeoutType, bool) {
	for _, timeoutType := range activityTimeoutTypes {
		if deadline, ok := ActivityTimeoutDeadline(state, timeoutType); ok && deadline.Before(cutoff) {
			return timeoutType, true
		}
	}
	return "", false
}

// ActivityTimeoutAt 返回 Activity 当前已生效的各类超时里最早的截止时间；没有生效的超时时返回 nil。
// MySQL 后端把它存成 activity state 的 timeout_at 列，供 ListRecoveryRunIDs 按到期时间索引查询。
func ActivityTimeoutAt(state *ActivityState) *time.Time {
	var earliest *time.Time
	for _, timeoutType := range activityTimeoutTypes {
		if deadline, ok := ActivityTimeoutDeadline(state, timeoutType); ok && (earliest == nil || deadline.Before(*earliest)) {
			earliest = &deadline
		}
	}
	return earliest
}

// RecoveryCandidateActivity 报告 activity 是否让所属 Run 成为 ListRecoveryRunIDs 的候选：Scheduled 的 attempt
// 每轮都要检查派发与重试退避，Started 的只在最早的超时截止时间早于 dueBefore 时检查。
func RecoveryCandidateActivity(state *ActivityState, dueBefore time.Time) bool {
	if state.IsScheduled() {
		return true
	}
	timeoutAt := ActivityTimeoutAt(state)
	return state.IsStarted() && timeoutAt != nil && timeoutAt.Before(dueBefore)
}

// ActivityTimeoutDeadline returns the persisted deadline of one timeout type,
// reporting false when that type is not armed in the current state.
// ScheduleToStart guards every attempt, counted from the moment the attempt
// becomes dispatchable (see ActivityState.DispatchableAt); it closes the
// Activity without retry.
func ActivityTimeoutDeadline(state *ActivityState, timeoutType protocol.TimeoutType) (time.Time, bool) {
	if state == nil || state.IsTerminal() {
		return time.Time{}, false
	}
	deadline := func(base time.Time, timeoutMs int64) (time.Time, bool) {
		return base.Add(time.Duration(timeoutMs) * time.Millisecond), timeoutMs > 0
	}
	startedAt := state.ScheduledTime
	if state.StartedTime != nil {
		startedAt = *state.StartedTime
	}
	switch timeoutType {
	case protocol.TimeoutTypeScheduleToClose:
		return deadline(state.ScheduledTime, state.ScheduleToCloseTimeoutMs)
	case protocol.TimeoutTypeStartToClose:
		if state.IsStarted() {
			return deadline(startedAt, state.StartToCloseTimeoutMs)
		}
	case protocol.TimeoutTypeHeartbeat:
		if state.IsStarted() {
			if state.LastHeartbeatAt != nil && state.LastHeartbeatAt.After(startedAt) {
				startedAt = *state.LastHeartbeatAt
			}
			return deadline(startedAt, state.HeartbeatTimeoutMs)
		}
	case protocol.TimeoutTypeScheduleToStart:
		if state.IsScheduled() {
			return deadline(state.DispatchableAt(), state.ScheduleToStartTimeoutMs)
		}
	}
	return time.Time{}, false
}

type ChildRecoveryAction uint8

const (
	ChildRecoverySkip ChildRecoveryAction = iota
	ChildRecoveryStart
	ChildRecoveryPropagateClose
)

func ChildRecoveryFor(state *ChildWorkflowState, childExists bool, childStatus history.Status) ChildRecoveryAction {
	if state == nil || state.IsTerminal() {
		return ChildRecoverySkip
	}
	if !childExists {
		return ChildRecoveryStart
	}
	if history.IsTerminalStatus(childStatus) {
		return ChildRecoveryPropagateClose
	}
	return ChildRecoverySkip
}

func TimerRecoveryNeeded(state *TimerState) bool {
	return state != nil && !state.IsTerminal()
}
