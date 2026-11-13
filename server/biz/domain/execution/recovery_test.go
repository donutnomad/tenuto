package execution_test

import (
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
)

func TestRecoveryActionFor(t *testing.T) {
	now := time.Date(2026, time.October, 27, 10, 0, 0, 0, time.UTC)
	past := now.Add(-time.Second)
	future := now.Add(time.Second)
	tests := []struct {
		name    string
		current *execution.Run
		want    execution.RunRecoveryAction
	}{
		{name: "resume", current: &execution.Run{DeadlineAt: &future}, want: execution.RunRecoveryResume},
		{name: "workflow deadline", current: &execution.Run{DeadlineAt: &past}, want: execution.RunRecoveryWorkflowTimeout},
		{name: "run deadline", current: &execution.Run{RunDeadlineAt: &past}, want: execution.RunRecoveryRunTimeout},
		{name: "workflow timeout precedence", current: &execution.Run{DeadlineAt: &past, RunDeadlineAt: &past}, want: execution.RunRecoveryWorkflowTimeout},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := execution.RecoveryActionFor(test.current, now); got != test.want {
				t.Fatalf("RecoveryActionFor() = %v, want %v", got, test.want)
			}
		})
	}
}

func TestActivityRecoveryFor(t *testing.T) {
	tests := []struct {
		name       string
		state      *execution.ActivityState
		wantAction execution.ActivityRecoveryDisposition
	}{
		{name: "nil", wantAction: execution.ActivityRecoverySkip},
		{name: "scheduled", state: &execution.ActivityState{Status: execution.ActivityStatusEnums.Scheduled}, wantAction: execution.ActivityRecoveryRedispatch},
		{name: "started", state: &execution.ActivityState{Status: execution.ActivityStatusEnums.Started}, wantAction: execution.ActivityRecoveryRestoreTimers},
		{name: "completed", state: &execution.ActivityState{Status: execution.ActivityStatusEnums.Completed}, wantAction: execution.ActivityRecoverySkip},
		{name: "failed", state: &execution.ActivityState{Status: execution.ActivityStatusEnums.Failed}, wantAction: execution.ActivityRecoverySkip},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := execution.ActivityRecoveryFor(test.state, time.Now()); got != test.wantAction {
				t.Fatalf("ActivityRecoveryFor() = %v, want %v", got, test.wantAction)
			}
		})
	}
}

func TestExecutionStateTracksWorkflowStarted(t *testing.T) {
	tests := []struct {
		name   string
		events []history.Event
		want   bool
	}{
		{name: "empty history", want: false},
		{name: "history without start", events: []history.Event{history.NewWorkflowTaskScheduledEvent(100)}, want: false},
		{name: "already started", events: []history.Event{history.NewWorkflowStartedEvent(nil, "", "", "wf", "Workflow", 0)}, want: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			for i := range test.events {
				test.events[i].ID = int64(i + 1)
			}
			state, err := execution.BuildWorkflowExecutionState(&execution.Run{ID: "run", Status: history.StatusRunning}, test.events)
			if err != nil {
				t.Fatal(err)
			}
			if state.HasWorkflowStarted != test.want {
				t.Fatalf("HasWorkflowStarted = %t, want %t", state.HasWorkflowStarted, test.want)
			}
		})
	}
}

func TestChildRecoveryFor(t *testing.T) {
	open := &execution.ChildWorkflowState{Status: execution.ChildWorkflowStatusEnums.Started}
	closed := &execution.ChildWorkflowState{Status: execution.ChildWorkflowStatusEnums.Completed}
	tests := []struct {
		name        string
		state       *execution.ChildWorkflowState
		childExists bool
		childStatus history.Status
		want        execution.ChildRecoveryAction
	}{
		{name: "missing state", want: execution.ChildRecoverySkip},
		{name: "closed association", state: closed, want: execution.ChildRecoverySkip},
		{name: "missing child run", state: open, want: execution.ChildRecoveryStart},
		{name: "running child", state: open, childExists: true, childStatus: history.StatusRunning, want: execution.ChildRecoverySkip},
		{name: "completed child", state: open, childExists: true, childStatus: history.StatusCompleted, want: execution.ChildRecoveryPropagateClose},
		{name: "failed child", state: open, childExists: true, childStatus: history.StatusFailed, want: execution.ChildRecoveryPropagateClose},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := execution.ChildRecoveryFor(test.state, test.childExists, test.childStatus); got != test.want {
				t.Fatalf("ChildRecoveryFor() = %v, want %v", got, test.want)
			}
		})
	}
}

func TestTimerRecoveryNeeded(t *testing.T) {
	tests := []struct {
		name  string
		state *execution.TimerState
		want  bool
	}{
		{name: "missing", want: false},
		{name: "scheduled", state: &execution.TimerState{Status: execution.TimerStatusEnums.Scheduled}, want: true},
		{name: "fired", state: &execution.TimerState{Status: execution.TimerStatusEnums.Fired}, want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := execution.TimerRecoveryNeeded(test.state); got != test.want {
				t.Fatalf("TimerRecoveryNeeded() = %t, want %t", got, test.want)
			}
		})
	}
}

func TestOverdueActivityTimeout(t *testing.T) {
	cutoff := time.Date(2026, 11, 9, 12, 0, 0, 0, time.UTC)
	const minuteMs = int64(time.Minute / time.Millisecond)
	past := cutoff.Add(-2 * time.Minute)
	future := cutoff.Add(-30 * time.Second)
	tests := []struct {
		name  string
		state execution.ActivityState
		want  protocol.TimeoutType
	}{
		{"Happy Path: schedule to close", execution.ActivityState{Status: execution.ActivityStatusEnums.Started, ScheduledTime: past, ScheduleToCloseTimeoutMs: minuteMs}, protocol.TimeoutTypeScheduleToClose},
		{"Happy Path: schedule to start", execution.ActivityState{Status: execution.ActivityStatusEnums.Scheduled, Attempt: 1, ScheduledTime: past, ScheduleToStartTimeoutMs: minuteMs}, protocol.TimeoutTypeScheduleToStart},
		{"Happy Path: start to close", execution.ActivityState{Status: execution.ActivityStatusEnums.Started, ScheduledTime: past, StartedTime: &past, StartToCloseTimeoutMs: minuteMs}, protocol.TimeoutTypeStartToClose},
		{"Happy Path: heartbeat", execution.ActivityState{Status: execution.ActivityStatusEnums.Started, ScheduledTime: past, StartedTime: &past, HeartbeatTimeoutMs: minuteMs}, protocol.TimeoutTypeHeartbeat},
		{"Edge Case: recent heartbeat", execution.ActivityState{Status: execution.ActivityStatusEnums.Started, ScheduledTime: past, StartedTime: &past, LastHeartbeatAt: &future, HeartbeatTimeoutMs: minuteMs}, ""},
		{"Edge Case: retry attempt counts schedule to start from its backoff", execution.ActivityState{Status: execution.ActivityStatusEnums.Scheduled, Attempt: 2, ScheduledTime: past, NextAttemptAt: &future, ScheduleToStartTimeoutMs: minuteMs}, ""},
		{"Happy Path: retry attempt schedule to start", execution.ActivityState{Status: execution.ActivityStatusEnums.Scheduled, Attempt: 2, ScheduledTime: past.Add(-time.Hour), NextAttemptAt: &past, ScheduleToStartTimeoutMs: minuteMs}, protocol.TimeoutTypeScheduleToStart},
		{"Edge Case: reset counts schedule to start from the reset", execution.ActivityState{Status: execution.ActivityStatusEnums.Scheduled, Attempt: 1, ActivityReset: true, ScheduledTime: past, NextAttemptAt: &future, ScheduleToStartTimeoutMs: minuteMs}, ""},
		{"Edge Case: not yet due", execution.ActivityState{Status: execution.ActivityStatusEnums.Scheduled, Attempt: 1, ScheduledTime: future, ScheduleToStartTimeoutMs: minuteMs}, ""},
		{"Edge Case: terminal", execution.ActivityState{Status: execution.ActivityStatusEnums.Completed, ScheduledTime: past, ScheduleToCloseTimeoutMs: minuteMs}, ""},
		{"Edge Case: no timeouts configured", execution.ActivityState{Status: execution.ActivityStatusEnums.Started, ScheduledTime: past, StartedTime: &past}, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, overdue := execution.OverdueActivityTimeout(&tt.state, cutoff)
			if got != tt.want || overdue != (tt.want != "") {
				t.Fatalf("got (%q, %v), want %q", got, overdue, tt.want)
			}
		})
	}
}
