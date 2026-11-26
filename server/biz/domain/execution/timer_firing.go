package execution

import (
	"time"

	"github.com/donutnomad/tenuto/protocol/history"
)

type TimerFire struct {
	RunID            string
	TreeID           string
	BranchID         string
	ScheduledEventID int64
	Kind             TimerKind
	Now              time.Time
}

// FireTimer records the durable timer transition in the caller's transaction.
// User timers append a History event; retry backoff timers remain internal.
//
// The Run's execution state is locked before the timer row, in the same order
// as a Workflow Task that cancels the timer and as closing the Run.
func FireTimer(tx ExecutionTx, request TimerFire) (bool, error) {
	if found, err := lockExecutionState(tx, request.RunID); err != nil || !found {
		return false, err
	}
	state, found, err := tx.GetTimerStateForUpdate(request.RunID, request.ScheduledEventID)
	if err != nil || !found || state.IsTerminal() || state.Kind != request.Kind {
		return false, err
	}
	if request.Kind == TimerKindEnums.UserTimer {
		if _, err := tx.Append(request.RunID, request.TreeID, request.BranchID,
			history.NewTimerFiredEvent(state.TimerID, request.ScheduledEventID)); err != nil {
			return false, err
		}
	}
	state.setStatus(TimerStatusEnums.Fired)
	state.setUpdatedAt(request.Now)
	if err := tx.UpdateTimerState(state); err != nil {
		return false, err
	}
	return true, nil
}
