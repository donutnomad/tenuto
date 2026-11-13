package execution

import "time"

type WaitingType string

const (
	WaitingWorkflowTask  WaitingType = "workflow_task"
	WaitingActivity      WaitingType = "activity"
	WaitingTimer         WaitingType = "timer"
	WaitingChildWorkflow WaitingType = "child_workflow"
)

type WaitingState struct {
	Type             WaitingType `json:"type,omitempty"`
	ActivityID       string      `json:"activity_id,omitempty"`
	ChildID          string      `json:"child_id,omitempty"`
	ActivityType     string      `json:"activity_type,omitempty"`
	TaskQueue        string      `json:"task_queue,omitempty"`
	TimerID          string      `json:"timer_id,omitempty"`
	ChildRunID       string      `json:"child_run_id,omitempty"`
	ScheduledEventID int64       `json:"scheduled_event_id,omitempty"`
}

type BlockedInfo struct {
	Blocked             bool       `json:"blocked"`
	ConsecutiveFailures int        `json:"consecutive_failures,omitempty"`
	Cause               string     `json:"cause,omitempty"`
	Reason              string     `json:"reason,omitempty"`
	Since               *time.Time `json:"since,omitempty"`
}
