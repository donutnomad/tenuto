package execution

import (
	"testing"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
)

type updateRulesTx struct {
	workflowTaskTestTx
	writes int
}

func (tx *updateRulesTx) AppendIdempotent(string, string, string, history.Event) (int64, bool, error) {
	tx.writes++
	return 42, true, nil
}

func TestAcceptOrReuseUpdate(t *testing.T) {
	current := &Run{ID: "run-1", Status: history.StatusRunning}
	tx := &updateRulesTx{}
	result, scheduled, err := AcceptOrReuseUpdate(tx, current, "tree", "branch", "rename", "u1", nil, history.WorkflowUpdateRecord{}, false)
	if err != nil || !scheduled || tx.writes != 1 || result.AcceptedEventID != 42 || result.Stage != protocol.UpdateReachedStageEnums.Accepted {
		t.Fatalf("new update: result=%+v scheduled=%t writes=%d err=%v", result, scheduled, tx.writes, err)
	}
	if result.RunID != "run-1" {
		t.Fatalf("accepted update RunID = %q, want the run that accepted it", result.RunID)
	}
	existing := history.WorkflowUpdateRecord{UpdateID: "u1", AcceptedEventID: 42}
	result, scheduled, err = AcceptOrReuseUpdate(tx, current, "tree", "branch", "rename", "u1", nil, existing, true)
	if err != nil || scheduled || tx.writes != 1 || result.AcceptedEventID != 42 {
		t.Fatalf("accepted update: result=%+v scheduled=%t writes=%d err=%v", result, scheduled, tx.writes, err)
	}
	if result.RunID != "run-1" {
		t.Fatalf("recorded update RunID = %q, want the run that holds it", result.RunID)
	}
	existing.CompletedEventID = 43
	successPayload, err := protocol.NewJSONPayload(map[string]any{"name": "new"})
	if err != nil {
		t.Fatalf("NewJSONPayload: %v", err)
	}
	existing.Outcome = &history.UpdateOutcome{Success: &successPayload}
	result, scheduled, err = AcceptOrReuseUpdate(tx, current, "tree", "branch", "rename", "u1", nil, existing, true)
	if err != nil || scheduled || tx.writes != 1 || result.Stage != protocol.UpdateReachedStageEnums.Completed || result.Outcome == nil || result.Outcome.Success == nil || string(result.Outcome.Success.Data) != string(successPayload.Data) {
		t.Fatalf("completed update: result=%+v scheduled=%t writes=%d err=%v", result, scheduled, tx.writes, err)
	}
}

type tokenStateStore struct{ state *WorkflowExecutionState }

func (s tokenStateStore) GetWorkflowExecutionState(string) (*WorkflowExecutionState, error) {
	return s.state, nil
}

func TestValidateWorkflowTaskTokenFence(t *testing.T) {
	task := protocol.Task{
		Type: protocol.TaskTypeWorkflow, RunID: "run-1", ScheduledEventID: 7,
		ExecutionGeneration: 2, StickyGeneration: 3, ActivationID: "activation-1",
		WorkflowTaskRoute: protocol.WorkflowTaskRouteSticky,
		WorkerIdentity:    "worker-a", WorkerBuildID: "build-a",
	}
	state := &WorkflowExecutionState{
		ExecutionGeneration: 2, StickyGeneration: 3, CurrentActivationID: "activation-1",
		StickyWorkerIdentity: "worker-a", StickyWorkerBuildID: "build-a",
	}
	if err := ValidateWorkflowTaskToken(task, "run-1", 7, tokenStateStore{state}); err != nil {
		t.Fatal(err)
	}
	state.StickyWorkerBuildID = "build-b"
	if err := ValidateWorkflowTaskToken(task, "run-1", 7, tokenStateStore{state}); err == nil {
		t.Fatal("stale sticky owner was accepted")
	}
	state.StickyWorkerBuildID = "build-a"
	state.ExecutionGeneration++
	if err := ValidateWorkflowTaskToken(task, "run-1", 7, tokenStateStore{state}); err == nil {
		t.Fatal("stale execution generation was accepted")
	}
}

func TestSearchAttributesFromHistoryWithEmptyBase(t *testing.T) {
	events := []history.Event{history.NewSearchAttributesUpsertedEvent(map[string]any{"order": "o1"})}
	attrs := SearchAttributesFromHistory(nil, events)
	if attrs["order"] != "o1" {
		t.Fatalf("attributes = %+v", attrs)
	}
}

// Run 关闭时已接受未完成的 Update 由终态事件以 AcceptedUpdateCompletedWorkflow 失败结束；
// 已完成的保留自己的结果，Run 未关闭时停在 Accepted。
func TestUpdateRecordsFailPendingUpdatesOnClose(t *testing.T) {
	success, err := protocol.NewJSONPayload(true)
	if err != nil {
		t.Fatal(err)
	}
	events := []history.Event{
		{ID: 1, WorkflowStarted: &history.WorkflowStartedEvent{}},
		{ID: 2, WorkflowUpdateAccepted: &history.WorkflowUpdateAcceptedEvent{UpdateID: "done", Name: "rename"}},
		{ID: 3, WorkflowUpdateAccepted: &history.WorkflowUpdateAcceptedEvent{UpdateID: "pending", Name: "rename"}},
		{ID: 4, WorkflowUpdateCompleted: &history.WorkflowUpdateCompletedEvent{UpdateID: "done", Name: "rename", Outcome: &protocol.UpdateOutcome{Success: &success}}},
	}
	open := UpdateRecordsFromEvents(events)
	if open[1].CompletedEventID != 0 || open[1].Outcome != nil {
		t.Fatalf("open run: pending update = %+v, want accepted only", open[1])
	}
	closed := UpdateRecordsFromEvents(append(events, history.Event{ID: 5, WorkflowTerminated: &history.WorkflowTerminatedEvent{}}))
	if closed[0].CompletedEventID != 4 || closed[0].Outcome.Success == nil {
		t.Fatalf("closed run: completed update = %+v, want its own result", closed[0])
	}
	pending := closed[1]
	if pending.CompletedEventID != 5 || pending.Outcome == nil || pending.Outcome.Failure == nil ||
		pending.Outcome.Failure.Type != protocol.UpdateFailureTypeWorkflowClosed || !pending.Outcome.Failure.NonRetryable {
		t.Fatalf("closed run: pending update = %+v, want failed by the terminal event", pending)
	}
}
