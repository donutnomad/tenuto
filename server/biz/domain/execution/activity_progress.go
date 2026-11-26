package execution

import (
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
)

// StartActivity records the current attempt's start. When the effective retry
// policy allows another attempt, the start stays transient until the attempt
// closes, when it is backfilled into History together with the outcome; an
// Activity limited to one attempt writes ActivityStarted immediately.
func StartActivity(tx ExecutionTx, start ActivityTransition, workerIdentity, workerBuildID, requestID string) (bool, error) {
	state, ok, err := currentActivityAttempt(tx, start)
	if err != nil || !ok || state.StartedEventID != EmptyEventID {
		return false, err
	}
	if state.TargetWorkerIdentity != "" && state.TargetWorkerIdentity != workerIdentity {
		return false, protocol.InvalidRequestf("activity targets worker %q; caller is %q", state.TargetWorkerIdentity, workerIdentity)
	}
	startedID := TransientEventID
	if EffectiveActivityRetryPolicy(state.RetryPolicy).MaxAttempts == 1 {
		id, inserted, err := tx.AppendIdempotent(start.RunID, start.TreeID, start.BranchID, history.NewActivityStartedEvent(history.ActivityStartedEvent{
			ActivityID: state.ActivityID, ScheduledEventID: start.ScheduledEventID,
			Attempt: history.NormalizeActivityAttempt(start.Attempt), Stamp: state.Stamp,
			ActivityType: state.ActivityType, TaskQueue: state.TaskQueue,
			WorkerIdentity: workerIdentity, WorkerBuildID: workerBuildID, LastError: state.RetryLastFailure,
		}))
		if err != nil {
			return false, err
		}
		if inserted {
			startedID = id
		}
	}
	state.setStartedEventID(startedID)
	state.setStartedTime(&start.Now)
	state.setNextAttemptAt(nil)
	state.setWorkerIdentity(workerIdentity)
	state.setWorkerBuildID(workerBuildID)
	state.setStartRequestID(requestID)
	state.setStatus(ActivityStatusEnums.Started)
	state.setUpdatedAt(start.Now)
	if err := tx.UpdateActivityState(state); err != nil {
		return false, err
	}
	return true, nil
}

// RecordActivityHeartbeat 把心跳详情与时间记在 Activity state 上，不写 History（Temporal 同样只记在
// pending activity info 里）。心跳超时从 LastHeartbeatAt 起算；重试与重新派发的 attempt 从
// LastHeartbeat 读到详情；attempt 超时时详情随 ActivityTimedOut 写入 History。返回更新后的 state；
// 过期的 attempt 返回 nil，事务不变。
func RecordActivityHeartbeat(tx ExecutionTx, heartbeat ActivityTransition, details map[string]any) (*ActivityState, error) {
	state, ok, err := currentActivityAttempt(tx, heartbeat)
	if err != nil || !ok {
		return nil, err
	}
	state.setLastHeartbeat(details)
	state.setLastHeartbeatAt(&heartbeat.Now)
	state.setUpdatedAt(heartbeat.Now)
	if err := tx.UpdateActivityState(state); err != nil {
		return nil, err
	}
	return state, nil
}

// ActivityRelease is the outcome of ReleaseActivity. Redispatch is the state
// to dispatch again; Cancelled reports that the released attempt belonged to a
// cancel-requested Activity, which closed as ActivityCancelled instead.
type ActivityRelease struct {
	Redispatch *ActivityState
	Cancelled  bool
}

// ReleaseActivity hands a started attempt back to the queue without a result:
// the Worker that started it stopped before the Activity finished. The attempt
// number and the retry budget stay unchanged; the stamp advances so a late
// result from the released execution is rejected as stale. The attempt is
// dispatchable again at release.Now, and the last heartbeat details are kept
// for the next execution. A cancel-requested Activity is not dispatched again:
// it closes as ActivityCancelled. A stale or unstarted attempt leaves the
// transaction unchanged. The caller redispatches, or schedules a Workflow Task
// for a cancelled Activity, after the transaction commits.
func ReleaseActivity(tx ExecutionTx, release ActivityTransition) (ActivityRelease, error) {
	state, ok, err := currentActivityAttempt(tx, release)
	if err != nil || !ok || !state.IsStarted() {
		return ActivityRelease{}, err
	}
	if state.CancelRequested {
		return ActivityRelease{Cancelled: true}, cancelActivity(tx, release, state, 0, nil)
	}
	state.setStamp(state.Stamp + 1)
	state.setStartedEventID(EmptyEventID)
	state.setStartRequestID("")
	state.setStartedTime(nil)
	state.setWorkerIdentity("")
	state.setWorkerBuildID("")
	state.setNextAttemptAt(&release.Now)
	state.setStatus(ActivityStatusEnums.Scheduled)
	state.setUpdatedAt(release.Now)
	if err := tx.UpdateActivityState(state); err != nil {
		return ActivityRelease{}, err
	}
	copy := *state
	return ActivityRelease{Redispatch: &copy}, nil
}
