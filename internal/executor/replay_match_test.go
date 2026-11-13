package executor

import (
	"errors"
	"testing"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
)

func TestMatchReplayWithHistoryPreservesTypedFailure(t *testing.T) {
	commands := []history.Command{{Type: history.CmdScheduleActivity, ActivityID: "first", ActivityType: "work.v2"}}
	history := []history.Event{{ActivityScheduled: &history.ActivityScheduledEvent{ActivityID: "first", ActivityType: "work.v1"}}}
	err := MatchReplayWithHistory(commands, history)
	var mismatch NonDeterministicError
	if !errors.As(err, &mismatch) {
		t.Fatalf("replay mismatch type = %T, want NonDeterministicError", err)
	}
}

func TestMatchReplayWithHistoryComparesCanonicalUpdatePayload(t *testing.T) {
	commands := []history.Command{{
		Type: history.CmdCompleteWorkflowUpdate, Name: "save", UpdateID: "u1",
		UpdateOutcome: &protocol.UpdateOutcome{Success: &protocol.Payload{
			Encoding: protocol.PayloadEncodingJSONPlainV1, Data: []byte(`{"b":2,"a":1}`),
		}},
	}}
	history := []history.Event{{WorkflowUpdateCompleted: &history.WorkflowUpdateCompletedEvent{
		Name: "save", UpdateID: "u1", Outcome: &protocol.UpdateOutcome{Success: &protocol.Payload{
			Encoding: protocol.PayloadEncodingJSONPlainV1, Data: []byte(`{"a":1,"b":2}`),
		}},
	}}}
	if err := MatchReplayWithHistory(commands, history); err != nil {
		t.Fatalf("canonical update payload mismatch: %v", err)
	}
}
