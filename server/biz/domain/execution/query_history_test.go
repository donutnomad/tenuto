package execution_test

import (
	"bytes"
	"testing"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
)

func TestProjectWorkflowQueryBuildsHistoryProjection(t *testing.T) {
	current := execution.NewRun(execution.NewRunOptions{
		ID: "run-1", WorkflowID: "payment", SearchAttributes: map[string]string{"region": "west", "tier": "gold"},
	})
	accepted := history.NewWorkflowUpdateAcceptedEvent("approve", "update-1", nil)
	accepted.ID = 2
	successPayload, err := protocol.NewJSONPayload(map[string]any{"approved": true})
	if err != nil {
		t.Fatalf("NewJSONPayload: %v", err)
	}
	completed := history.Event{WorkflowUpdateCompleted: &history.WorkflowUpdateCompletedEvent{
		Name: "approve", UpdateID: "update-1",
		Outcome: &history.UpdateOutcome{Success: &successPayload},
	}}
	completed.ID = 3
	signaled := history.NewWorkflowSignaledEvent("refresh", map[string]any{"key": "k1"})
	signaled.ID = 4
	attrs := history.NewSearchAttributesUpsertedEvent(map[string]any{"region": "east"})
	attrs.ID = 5
	activity := history.NewActivityScheduledEvent(history.ActivityScheduledEvent{ActivityID: "charge", ActivityType: "Charge", TaskQueue: "payments"})
	activity.ID = 6
	events := []history.Event{
		accepted, completed, signaled, attrs, activity,
	}

	state, err := execution.BuildWorkflowExecutionState(current, events)
	if err != nil {
		t.Fatal(err)
	}
	result := execution.ProjectWorkflowQuery(current, "state", state, events)
	if result.Run != current || result.QueryName != "state" || result.HistoryLength != len(events) {
		t.Fatalf("query identity = run %p, name %q, length %d", result.Run, result.QueryName, result.HistoryLength)
	}
	if result.SearchAttributes["region"] != "east" || result.SearchAttributes["tier"] != "gold" {
		t.Fatalf("search attributes = %#v", result.SearchAttributes)
	}
	if result.CurrentWaiting.Type != execution.WaitingActivity || result.CurrentWaiting.ActivityID != "charge" {
		t.Fatalf("waiting state = %+v", result.CurrentWaiting)
	}
	if len(result.Signals) != 1 || result.Signals[0].EventID != 4 || result.Signals[0].Name != "refresh" {
		t.Fatalf("signals = %+v", result.Signals)
	}
	if len(result.Updates) != 1 || result.Updates[0].UpdateID != "update-1" || result.Updates[0].CompletedEventID != 3 {
		t.Fatalf("updates = %+v", result.Updates)
	}
	if outcome := result.Updates[0].Outcome; outcome == nil || outcome.Success == nil || !bytes.Contains(outcome.Success.Data, []byte("true")) {
		t.Fatalf("update outcome = %+v", result.Updates[0].Outcome)
	}
}
