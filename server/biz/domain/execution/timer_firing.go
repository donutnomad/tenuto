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
func FireTimer(tx ExecutionTx, request TimerFire) (bool, error) {
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
