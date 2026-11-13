package execution_test

import (
	"testing"
	"time"

	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

func TestFireTimerKeepsHistoryAndStateAtomic(t *testing.T) {
	for _, kind := range []execution.TimerKind{execution.TimerKindEnums.UserTimer, execution.TimerKindEnums.RetryBackoff} {
		t.Run(string(kind), func(t *testing.T) {
			store := memory.NewExecutionManager()
			candidate := execution.NewRun(execution.NewRunOptions{ID: "run-1", WorkflowID: "flow", WorkflowConfigVersion: "v1"})
			tree, branch, started, err := execution.PrepareStartedRun(candidate, "Flow", 0)
			if err != nil {
				t.Fatal(err)
			}
			state := &execution.TimerState{RunID: candidate.ID, ScheduledEventID: 2, TimerID: "pause", Kind: kind, Status: execution.TimerStatusEnums.Scheduled}
			if err := store.RunInTx(func(tx execution.ExecutionTx) error {
				if err := execution.CommitStartedRun(tx, candidate, "", tree, branch, started); err != nil {
					return err
				}
				return tx.UpsertTimerState(state)
			}); err != nil {
				t.Fatal(err)
			}
			request := execution.TimerFire{RunID: candidate.ID, TreeID: tree, BranchID: branch, ScheduledEventID: state.ScheduledEventID, Kind: kind, Now: time.Now()}
			for attempt := range 2 {
				var fired bool
				if err := store.RunInTx(func(tx execution.ExecutionTx) error {
					var fireErr error
					fired, fireErr = execution.FireTimer(tx, request)
					return fireErr
				}); err != nil || fired != (attempt == 0) {
					t.Fatalf("attempt=%d fired=%v err=%v", attempt, fired, err)
				}
			}
			stored, found, err := store.GetTimerState(candidate.ID, state.ScheduledEventID)
			if err != nil || !found || stored.Status != execution.TimerStatusEnums.Fired {
				t.Fatalf("timer=%+v found=%v err=%v", stored, found, err)
			}
			events, err := store.GetEvents(candidate.ID)
			wantEvents := 1
			if kind == execution.TimerKindEnums.UserTimer {
				wantEvents++
			}
			if err != nil || len(events) != wantEvents {
				t.Fatalf("history=%+v err=%v", events, err)
			}
		})
	}
}
