package execution

import (
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
)

type ActivityReset struct {
	ActivityTransition
	Identity       string
	Reason         string
	ResetHeartbeat bool
}

// ResetActivity records the reset and advances the Activity stamp atomically.
// The caller redispatches the returned state after the transaction commits.
func ResetActivity(tx ExecutionTx, reset ActivityReset) (*ActivityState, error) {
	state, ok, err := tx.GetActivityStateForUpdate(reset.RunID, reset.ScheduledEventID)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, protocol.InvalidRequestf("cannot reset activity %d: no activity state found", reset.ScheduledEventID)
	}
	if state.IsTerminal() {
		return nil, protocol.InvalidRequestf("cannot reset activity %d: already in terminal state; use Reset Workflow instead", reset.ScheduledEventID)
	}
	// Workflow 已请求取消的 Activity 不再开始新的 attempt。
	if state.CancelRequested {
		return nil, protocol.InvalidRequestf("cannot reset activity %d: the workflow requested its cancellation", reset.ScheduledEventID)
	}
	_, err = tx.Append(reset.RunID, reset.TreeID, reset.BranchID, history.NewActivityResetEvent(history.ActivityResetEvent{
		ActivityID: state.ActivityID, ScheduledEventID: reset.ScheduledEventID,
		Attempt: state.Attempt, Identity: reset.Identity, Reason: reset.Reason,
		ResetHeartbeat: reset.ResetHeartbeat,
	}))
	if err != nil {
		return nil, err
	}
	state.setAttempt(1)
	state.setStamp(state.Stamp + 1)
	state.setStartedEventID(EmptyEventID)
	state.setStartedTime(nil)
	state.setNextAttemptAt(&reset.Now)
	state.setActivityReset(true)
	state.setStatus(ActivityStatusEnums.Scheduled)
	if reset.ResetHeartbeat {
		state.setLastHeartbeat(nil)
		state.setLastHeartbeatAt(nil)
	}
	state.setUpdatedAt(reset.Now)
	if err := tx.UpdateActivityState(state); err != nil {
		return nil, err
	}
	copy := *state
	return &copy, nil
}

// ResetActivityOptions 是 Reset Activity 请求的可选项。
type ResetActivityOptions struct {
	ResetHeartbeat bool
	Identity       string
	Reason         string
}
