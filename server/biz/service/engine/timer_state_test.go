package engine

import (
	"testing"
	"time"

	"github.com/donutnomad/tenuto/server/biz/domain/execution"
)

// Happy Path: 新建一个 scheduled TimerState，约定初始值 Status=Scheduled、非终态。
func TestNewTimerStateDefaults(t *testing.T) {
	st := &execution.TimerState{
		RunID:            "run-1",
		ScheduledEventID: 10,
		TimerID:          "t1",
		Deadline:         time.Now().Add(time.Minute),
		Status:           execution.TimerStatusEnums.Scheduled,
	}
	if !st.IsScheduled() {
		t.Fatalf("want scheduled, got %q", st.Status)
	}
	if st.IsTerminal() {
		t.Fatal("scheduled timer must not be terminal")
	}
}

// Happy Path: fired 是终态。
func TestTimerStateTerminalPredicates(t *testing.T) {
	fired := &execution.TimerState{Status: execution.TimerStatusEnums.Fired}
	if !fired.IsTerminal() || fired.IsScheduled() {
		t.Fatalf("fired predicate mismatch: terminal=%v scheduled=%v", fired.IsTerminal(), fired.IsScheduled())
	}
}
