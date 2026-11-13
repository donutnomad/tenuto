package execution

import (
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol/history"
)

func TestWorkflowTaskActionsAlignsEventsAcrossFailureCommand(t *testing.T) {
	commands := []history.Command{
		{Type: history.CmdScheduleActivity, StartToCloseTimeout: time.Minute, ActivityID: "first"},
		{Type: history.CmdFailWorkflow},
		{Type: history.CmdScheduleTimer, TimerID: "later"},
	}
	result := WorkflowTaskCompletionResult{CommandEvents: []WorkflowTaskCommandEventResult{
		{EventID: 10, Inserted: true},
		{EventID: 11, Inserted: false},
	}}
	actions, err := WorkflowTaskActions(commands, result)
	if err != nil {
		t.Fatal(err)
	}
	if len(actions) != 3 || actions[0].Event.EventID != 10 || actions[1].Event.EventID != 0 ||
		actions[2].Index != 2 || actions[2].Event.EventID != 11 || actions[2].Event.Inserted {
		t.Fatalf("actions=%+v", actions)
	}
}

func TestWorkflowTaskActionsRejectsMissingCommittedEvent(t *testing.T) {
	_, err := WorkflowTaskActions([]history.Command{{Type: history.CmdScheduleActivity, StartToCloseTimeout: time.Minute}}, WorkflowTaskCompletionResult{})
	if err == nil {
		t.Fatal("expected missing command event error")
	}
}
