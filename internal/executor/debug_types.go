package executor

import "github.com/donutnomad/tenuto/sdk/workflow"

type DebugWaitingActivity struct {
	ActivityID string
	Type       string
}

type DebugActivity struct {
	ActivityID string
	Type       string
	Status     string
	Input      map[string]any
	Output     map[string]any
	Error      string
}

type DebugEffect struct {
	Kind             string
	Phase            string
	RoutineID        uint64
	ActivityID       string
	Type             string
	ChildID          string
	ChildRunID       string
	ChildWorkflowID  string
	ScheduledEventID int64
}

type DebugEffectHook func(DebugEffect)

type DebugReplayResult struct {
	Blocked           bool
	WaitingActivities map[uint64]DebugWaitingActivity
	Activities        []DebugActivity
	Info              workflow.Info
}
