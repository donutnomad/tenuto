package execution

import (
	"time"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
)

type ActivityTransition struct {
	RunID            string
	TreeID           string
	BranchID         string
	ScheduledEventID int64
	Attempt          int
	Stamp            int64
	Now              time.Time
}

type ActivityFailureResult struct {
	Applied    bool
	RetryState *ActivityState
	Terminal   bool
}

// CompleteActivity updates History and the Activity's current state together.
// A stale attempt leaves the transaction unchanged.
func CompleteActivity(tx ExecutionTx, completion ActivityTransition, output map[string]any) (bool, error) {
	state, ok, err := currentActivityAttempt(tx, completion)
	if err != nil || !ok {
		return false, err
	}
	if state.StartedEventID == TransientEventID {
		if err := backfillTransientStarted(tx, completion, state); err != nil {
			return false, err
		}
	}
	_, _, err = tx.AppendIdempotent(completion.RunID, completion.TreeID, completion.BranchID, history.NewActivityCompletedEvent(history.ActivityCompletedEvent{
		ActivityID: state.ActivityID, ScheduledEventID: completion.ScheduledEventID,
		StartedEventID: state.StartedEventID, Attempt: history.NormalizeActivityAttempt(completion.Attempt),
		ActivityType: state.ActivityType, TaskQueue: state.TaskQueue,
		Identity: state.WorkerIdentity, Output: output,
	}))
	if err != nil {
		return false, err
	}
	state.setStatus(ActivityStatusEnums.Completed)
	state.setUpdatedAt(completion.Now)
	if err := tx.UpdateActivityState(state); err != nil {
		return false, err
	}
	return true, nil
}

// FailActivity either schedules the next stamped attempt or closes the
// Activity. The caller starts retry timers or schedules a Workflow Task after
// the transaction commits.
//
// A cancel-requested Activity is not retried. A failure of type CanceledError
// on a cancel-requested Activity is the Worker acknowledging the cancellation
// (Temporal RespondActivityTaskCanceled): it closes the Activity as
// ActivityCancelled with the failure's details.
func FailActivity(tx ExecutionTx, completion ActivityTransition, failure protocol.Failure) (ActivityFailureResult, error) {
	state, ok, err := currentActivityAttempt(tx, completion)
	if err != nil || !ok {
		return ActivityFailureResult{}, err
	}
	if state.CancelRequested && failure.Type == protocol.CanceledFailureType {
		if err := cancelActivity(tx, completion, state, 0, failure.Details); err != nil {
			return ActivityFailureResult{}, err
		}
		return ActivityFailureResult{Applied: true, Terminal: true}, nil
	}
	// 超时只由 Server 判定，Worker 上报的失败不带超时种类。
	failure.TimeoutType = ""
	return failActivity(tx, completion, state, failure)
}

// TimeoutActivity applies a fired timeout after confirming, against the locked
// persisted state, that this timeout type's deadline has passed. Timers can
// outlive the state they were armed from: another replica may have recorded
// the Start or heartbeat that moved the deadline. A trigger ahead of the
// persisted deadline leaves the transaction unchanged.
func TimeoutActivity(tx ExecutionTx, completion ActivityTransition, timeoutType protocol.TimeoutType) (ActivityFailureResult, error) {
	state, ok, err := currentActivityAttempt(tx, completion)
	if err != nil || !ok {
		return ActivityFailureResult{}, err
	}
	if deadline, ok := ActivityTimeoutDeadline(state, timeoutType); !ok || deadline.After(completion.Now) {
		return ActivityFailureResult{}, nil
	}
	failure := protocol.Failure{Message: "activity " + string(timeoutType) + " timeout", TimeoutType: timeoutType}
	return failActivity(tx, completion, state, failure)
}

// failActivity 关闭当前 attempt：按生效的重试策略排定下一次 attempt，或以终态失败关闭 Activity。
// failure.TimeoutType 非空表示这次 attempt 超时。
func failActivity(tx ExecutionTx, completion ActivityTransition, state *ActivityState, failure protocol.Failure) (ActivityFailureResult, error) {
	var err error
	if state.StartedEventID == TransientEventID {
		if err := backfillTransientStarted(tx, completion, state); err != nil {
			return ActivityFailureResult{}, err
		}
	}
	policy := EffectiveActivityRetryPolicy(state.RetryPolicy)
	retryState := ActivityRetryState(state, failure)
	// ScheduleToClose bounds the whole Activity including retries, and
	// ScheduleToStart means no Worker picked the task up in time, which another
	// attempt on the same queue does not change. Both close the Activity even
	// when the retry policy has attempts left.
	closesActivity := failure.TimeoutType == protocol.TimeoutTypeScheduleToClose || failure.TimeoutType == protocol.TimeoutTypeScheduleToStart
	if closesActivity && retryState == "" {
		retryState = activityRetryStateTimeout
	}
	attempt := history.NormalizeActivityAttempt(completion.Attempt)
	nextAttemptAt := completion.Now.Add(activityRetryDelay(policy, attempt+1))
	// 下一次 attempt 会落在 ScheduleToClose 之后时不再重试。
	if deadline, ok := scheduleToCloseDeadline(state); retryState == "" && ok && !nextAttemptAt.Before(deadline) {
		retryState = activityRetryStateTimeout
	}
	if failure.TimeoutType != "" {
		_, _, err = tx.AppendIdempotent(completion.RunID, completion.TreeID, completion.BranchID, history.NewActivityTimedOutEvent(history.ActivityTimedOutEvent{
			ActivityID: state.ActivityID, ScheduledEventID: completion.ScheduledEventID,
			StartedEventID: state.StartedEventID, Attempt: attempt,
			Stamp: state.Stamp, ActivityType: state.ActivityType, TaskQueue: state.TaskQueue,
			TimeoutType: failure.TimeoutType, RetryState: retryState,
			Failure: &failure, LastHeartbeatDetails: state.LastHeartbeat,
		}))
		if err != nil {
			return ActivityFailureResult{}, err
		}
	}
	if retryState == "" {
		_, _, err = tx.AppendIdempotent(completion.RunID, completion.TreeID, completion.BranchID, history.NewActivityAttemptFailedEvent(history.ActivityAttemptFailedEvent{
			ActivityID: state.ActivityID, ScheduledEventID: completion.ScheduledEventID, Attempt: attempt,
			Stamp: state.Stamp, Error: failure.Message, Failure: &failure,
		}))
		if err != nil {
			return ActivityFailureResult{}, err
		}
		state.setAttempt(attempt + 1)
		state.setStamp(state.Stamp + 1)
		state.setStartedEventID(EmptyEventID)
		state.setStartRequestID("")
		state.setStartedTime(nil)
		state.setNextAttemptAt(&nextAttemptAt)
		state.setRetryLastFailure(failure.Message)
		state.setStatus(ActivityStatusEnums.Scheduled)
		state.setUpdatedAt(completion.Now)
		if err := tx.UpdateActivityState(state); err != nil {
			return ActivityFailureResult{}, err
		}
		copy := *state
		return ActivityFailureResult{Applied: true, RetryState: &copy}, nil
	}
	_, _, err = tx.AppendIdempotent(completion.RunID, completion.TreeID, completion.BranchID, history.NewActivityFailedEvent(history.ActivityFailedEvent{
		ActivityID: state.ActivityID, ScheduledEventID: completion.ScheduledEventID,
		StartedEventID: state.StartedEventID, Attempt: attempt,
		ActivityType: state.ActivityType, TaskQueue: state.TaskQueue,
		Identity: state.WorkerIdentity, Error: failure.Message,
		RetryState: retryState, Failure: &failure,
	}))
	if err != nil {
		return ActivityFailureResult{}, err
	}
	state.setStatus(ActivityStatusEnums.Failed)
	state.setRetryLastFailure(failure.Message)
	state.setUpdatedAt(completion.Now)
	if err := tx.UpdateActivityState(state); err != nil {
		return ActivityFailureResult{}, err
	}
	return ActivityFailureResult{Applied: true, Terminal: true}, nil
}

// scheduleToCloseDeadline 返回整个 Activity 必须结束的时间；没有设置 ScheduleToCloseTimeout 时 ok 为 false。
func scheduleToCloseDeadline(state *ActivityState) (time.Time, bool) {
	if state.ScheduleToCloseTimeoutMs <= 0 {
		return time.Time{}, false
	}
	return state.ScheduledTime.Add(time.Duration(state.ScheduleToCloseTimeoutMs) * time.Millisecond), true
}

func currentActivityAttempt(tx ExecutionTx, completion ActivityTransition) (*ActivityState, bool, error) {
	state, ok, err := tx.GetActivityStateForUpdate(completion.RunID, completion.ScheduledEventID)
	if err != nil || !ok || state.IsTerminal() || state.Stamp != completion.Stamp || state.Attempt != history.NormalizeActivityAttempt(completion.Attempt) {
		return nil, false, err
	}
	return state, true, nil
}

func backfillTransientStarted(tx ExecutionTx, completion ActivityTransition, state *ActivityState) error {
	event := history.NewActivityStartedEvent(history.ActivityStartedEvent{
		ActivityID: state.ActivityID, ScheduledEventID: state.ScheduledEventID,
		Attempt: history.NormalizeActivityAttempt(completion.Attempt), Stamp: state.Stamp,
		ActivityType: state.ActivityType, TaskQueue: state.TaskQueue,
		WorkerIdentity: state.WorkerIdentity, WorkerBuildID: state.WorkerBuildID, LastError: state.RetryLastFailure,
	})
	if state.StartedTime != nil && !state.StartedTime.IsZero() {
		event.Timestamp = *state.StartedTime
	}
	id, inserted, err := tx.AppendIdempotent(state.RunID, completion.TreeID, completion.BranchID, event)
	if err != nil {
		return err
	}
	if inserted {
		state.setStartedEventID(id)
	}
	return nil
}

// ActivityRetryState 按生效的重试策略计算 activity 停止重试的原因，供 ActivityFailedEvent/ActivityTimedOutEvent
// 使用；仍可重试时返回空串。
func ActivityRetryState(state *ActivityState, failure protocol.Failure) string {
	if state.CancelRequested {
		return ActivityCancelRetryState
	}
	policy := EffectiveActivityRetryPolicy(state.RetryPolicy)
	if NonRetryableFailure(failure, policy.NonRetryableErrorTypes, policy.NonRetryableErrors) {
		return "NonRetryableFailure"
	}
	if policy.MaxAttempts > 0 && state.Attempt >= policy.MaxAttempts {
		return "MaximumAttemptsReached"
	}
	return ""
}
