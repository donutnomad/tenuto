package persistence

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/compiler/sdkgen"
	"github.com/donutnomad/tenuto/internal/executor/interpreter"
	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/program/programtest"
	"github.com/donutnomad/tenuto/protocol"
	historypkg "github.com/donutnomad/tenuto/protocol/history"
	activitysdk "github.com/donutnomad/tenuto/sdk/activity"
	"github.com/donutnomad/tenuto/server/biz/domain/dispatch"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/biz/domain/execution/executiontest"
	"github.com/donutnomad/tenuto/server/biz/service/engine"
	"github.com/donutnomad/tenuto/server/infra/persistence/executionrepo"
)

func TestRunRowPreservesWorkflowType(t *testing.T) {
	store := newTestStore(t)
	run := &execution.Run{
		ID: "run-typed", Namespace: "demo", WorkflowID: "create-rec-1", WorkflowType: "approval-workflow",
		WorkflowConfigVersion: "v1", Status: historypkg.StatusRunning, CreatedAt: time.Now(),
	}
	if err := store.Executions.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(rootBranch(t, run)) }); err != nil {
		t.Fatal(err)
	}
	decoded, err := store.Executions.Get(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.WorkflowID != run.WorkflowID || decoded.WorkflowType != run.WorkflowType {
		t.Fatalf("decoded identity = %+v", decoded)
	}
}

func TestScheduledEventRowsPreserveTraceContext(t *testing.T) {
	store := newTestStore(t)
	carrier := map[string]string{
		"traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
		"baggage":     "tenant=acme",
	}
	events := []historypkg.Event{
		{WorkflowTaskScheduled: &historypkg.WorkflowTaskScheduledEvent{TraceContext: carrier}},
		{WorkflowTaskFailed: &historypkg.WorkflowTaskFailedEvent{ScheduledEventID: 1, TraceContext: carrier}},
		{ActivityScheduled: &historypkg.ActivityScheduledEvent{ActivityID: "charge", Input: map[string]any{"amount": 12}, TraceContext: carrier}},
		{TimerScheduled: &historypkg.TimerScheduledEvent{TimerID: "retry", DelayInput: map[string]any{"delayMs": 1000}, TraceContext: carrier}},
	}
	for index, event := range events {
		runID := fmt.Sprintf("run-otel-%d", index)
		createTestRun(t, store, runID)
		if _, err := store.Executions.Append(runID, runID, runID, event); err != nil {
			t.Fatal(err)
		}
		decoded, err := store.Executions.GetEvents(runID)
		if err != nil {
			t.Fatal(err)
		}
		got := eventTraceContext(decoded[0])
		if got["traceparent"] != carrier["traceparent"] || got["baggage"] != carrier["baggage"] {
			t.Fatalf("%s trace context = %#v", event.EventType(), got)
		}
	}
}

func eventTraceContext(event historypkg.Event) map[string]string {
	switch {
	case event.WorkflowTaskScheduled != nil:
		return event.WorkflowTaskScheduled.TraceContext
	case event.WorkflowTaskFailed != nil:
		return event.WorkflowTaskFailed.TraceContext
	case event.ActivityScheduled != nil:
		return event.ActivityScheduled.TraceContext
	case event.TimerScheduled != nil:
		return event.TimerScheduled.TraceContext
	default:
		return nil
	}
}

func createTestRun(t *testing.T, store *Store, runID string) {
	t.Helper()
	if err := store.Executions.RunInTx(func(tx execution.ExecutionTx) error {
		return tx.CreateRun(rootBranch(t, &execution.Run{
			ID:         runID,
			WorkflowID: "test-wf",
			Status:     historypkg.StatusRunning,
			CreatedAt:  time.Now(),
		}))
	}); err != nil {
		t.Fatal(err)
	}
}

func TestRunStoreRoundTrip(t *testing.T) {
	store := newTestStore(t)

	deadline := time.Now().Add(time.Hour).UTC().Truncate(time.Millisecond)
	runDeadline := time.Now().Add(30 * time.Minute).UTC().Truncate(time.Millisecond)
	run := &execution.Run{
		ID:                    "run-1",
		WorkflowID:            "wf-1",
		Status:                historypkg.StatusRunning,
		Input:                 map[string]any{"order_id": "o-1"},
		SearchAttributes:      map[string]string{"business_id": "biz-1"},
		WorkflowConfigVersion: "v1",
		CreatedAt:             time.Now().UTC().Truncate(time.Millisecond),
		DeadlineAt:            &deadline,
		RunDeadlineAt:         &runDeadline,
		Attempt:               2,
		Initiator:             execution.InitiatorEnums.Retry,
	}
	if err := store.Executions.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(rootBranch(t, run)) }); err != nil {
		t.Fatal(err)
	}

	got, err := store.Executions.Get(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != run.ID || got.WorkflowID != run.WorkflowID || got.Status != run.Status {
		t.Fatalf("unexpected run: %+v", got)
	}
	if got.Input["order_id"] != "o-1" {
		t.Fatalf("unexpected input: %+v", got.Input)
	}
	if got.WorkflowConfigVersion != "v1" {
		t.Fatalf("unexpected workflow config version: %q", got.WorkflowConfigVersion)
	}
	if got.SearchAttributes["business_id"] != "biz-1" {
		t.Fatalf("unexpected search attributes: %+v", got.SearchAttributes)
	}
	if got.DeadlineAt == nil || !got.DeadlineAt.Equal(deadline) {
		t.Fatalf("unexpected DeadlineAt: %v, want %v", got.DeadlineAt, deadline)
	}
	if got.RunDeadlineAt == nil || !got.RunDeadlineAt.Equal(runDeadline) {
		t.Fatalf("unexpected RunDeadlineAt: %v, want %v", got.RunDeadlineAt, runDeadline)
	}
	if got.Attempt != 2 {
		t.Fatalf("unexpected Attempt: got %d, want 2", got.Attempt)
	}
	if got.Initiator != execution.InitiatorEnums.Retry {
		t.Fatalf("unexpected Initiator: got %q, want %q", got.Initiator, execution.InitiatorEnums.Retry)
	}

	executiontest.UpdateRun(t, store.Executions, run.ID, func(run *execution.Run) error {
		return run.MarkTerminal(historypkg.StatusCompleted, map[string]any{"ok": true})
	})
	updated, err := store.Executions.Get(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if updated.Status != historypkg.StatusCompleted || updated.Output["ok"] != true {
		t.Fatalf("unexpected updated run: %+v", updated)
	}
}

func TestWorkflowConfigStoreUsesContentVersionAndDeduplicates(t *testing.T) {
	store := newTestStore(t)
	wf := &program.WorkflowConfig{
		ID:      "wf-1",
		Program: programtest.Noop(),
		Activities: map[string]program.ActivityRef{
			"step": {Symbol: "step", Type: "complete"},
		},
	}

	first, err := store.Definitions.SaveWorkflowConfigInNamespace("", wf)
	if err != nil {
		t.Fatal(err)
	}
	second, err := store.Definitions.SaveWorkflowConfigInNamespace("", wf)
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatalf("expected same config version, got %+v and %+v", first, second)
	}

	var count int64
	if err := store.db.Table("tenuto_configs").Count(&count).Error; err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("expected one persisted config, got %d", count)
	}

	wf.Name = "changed"
	third, err := store.Definitions.SaveWorkflowConfigInNamespace("", wf)
	if err != nil {
		t.Fatal(err)
	}
	if third.Version == first.Version {
		t.Fatalf("expected changed config to get a new version, got %q", third.Version)
	}

	loaded, err := store.Definitions.GetWorkflowConfig(third)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Name != "changed" {
		t.Fatalf("unexpected loaded workflow config: %+v", loaded)
	}
}

func TestActivityWorkerVersionUpsertRemovesStaleRoutes(t *testing.T) {
	store := newTestStore(t)
	now := time.Now()
	first := protocol.ActivityWorkerVersion{
		Identity:      "custodian-worker-1",
		BuildID:       "custodian@v1",
		TaskQueues:    []string{"custodian"},
		ActivityTypes: []string{"custodian.Capture", "custodian.Refund"},
	}
	if _, err := store.Dispatches.UpsertActivityWorkerVersionInNamespace(protocol.DefaultNamespace, first, now, time.Minute); err != nil {
		t.Fatal(err)
	}
	second := protocol.ActivityWorkerVersion{
		Identity:      "custodian-worker-1",
		BuildID:       "custodian@v1",
		TaskQueues:    []string{"custodian"},
		ActivityTypes: []string{"custodian.Capture"},
	}
	if _, err := store.Dispatches.UpsertActivityWorkerVersionInNamespace(protocol.DefaultNamespace, second, now.Add(time.Second), time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := store.Dispatches.HeartbeatActivityWorkerVersionInNamespace(protocol.DefaultNamespace, "custodian-worker-1", "custodian@v1", now.Add(2*time.Second), time.Minute); err != nil {
		t.Fatal(err)
	}

	workers, err := store.Dispatches.ListActiveActivityWorkerVersions(now.Add(3 * time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if len(workers) != 1 || len(workers[0].ActivityTypes) != 1 || workers[0].ActivityTypes[0] != "custodian.Capture" {
		t.Fatalf("expected stale activity type to be removed, got %+v", workers)
	}
}

func TestEventStoreAppendsGlobalIDsAndReadsRunHistoryInOrder(t *testing.T) {
	store := newTestStore(t)
	createTestRun(t, store, "run-1")
	createTestRun(t, store, "run-2")

	first, err := store.Executions.Append("run-1", "run-1", "run-1", historypkg.NewWorkflowStartedEvent(map[string]any{"request_id": "r-1"}, "", "", "", "", 0))
	if err != nil {
		t.Fatal(err)
	}
	second, err := store.Executions.Append("run-1", "run-1", "run-1", historypkg.NewActivityCompletedEvent(historypkg.ActivityCompletedEvent{
		ActivityID:       "step-1",
		ScheduledEventID: first,
		Output:           map[string]any{"amount": int64(42)},
	}))
	if err != nil {
		t.Fatal(err)
	}
	third, err := store.Executions.Append("run-2", "run-2", "run-2", historypkg.NewWorkflowStartedEvent(nil, "", "", "", "", 0))
	if err != nil {
		t.Fatal(err)
	}
	if first <= 0 || second != first+1 {
		t.Fatalf("expected per-branch increasing event IDs, got %d %d", first, second)
	}
	if third != 1 {
		t.Fatalf("expected new branch to start at 1, got %d", third)
	}

	history, err := store.Executions.GetEvents("run-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 2 {
		t.Fatalf("expected 2 events, got %d", len(history))
	}
	if history[0].ID != first || history[1].ID != second {
		t.Fatalf("unexpected event order: %+v", history)
	}
	if history[1].ActivityCompleted.Output["amount"] != int64(42) {
		t.Fatalf("unexpected event output: %+v", history[1].ActivityCompleted)
	}
}

func TestActivityEventStorePreservesIntegerTokens(t *testing.T) {
	store := newTestStore(t)
	createTestRun(t, store, "run-activity-integers")
	maximum := ^uint64(0)
	if _, err := store.Executions.Append("run-activity-integers", "run-activity-integers", "run-activity-integers",
		historypkg.NewActivityScheduledEvent(historypkg.ActivityScheduledEvent{
			ActivityID: "typed", Input: map[string]any{"count": int64(9007199254740993)},
		})); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Executions.Append("run-activity-integers", "run-activity-integers", "run-activity-integers",
		historypkg.NewActivityCompletedEvent(historypkg.ActivityCompletedEvent{
			ScheduledEventID: 1, Output: map[string]any{"maximum": maximum, "ratio": 1.25},
		})); err != nil {
		t.Fatal(err)
	}

	history, err := store.Executions.GetEvents("run-activity-integers")
	if err != nil {
		t.Fatal(err)
	}
	if got := history[0].ActivityScheduled.Input["count"]; got != int64(9007199254740993) {
		t.Fatalf("activity input count = %#v", got)
	}
	if got := history[1].ActivityCompleted.Output["maximum"]; got != maximum {
		t.Fatalf("activity output maximum = %#v", got)
	}
	if got := history[1].ActivityCompleted.Output["ratio"]; got != float64(1.25) {
		t.Fatalf("activity output ratio = %#v", got)
	}
}

// 直接写进 event_json 的整数读回是 int64，与写入路径经过的 Go 类型无关。
func TestEventJSONDecodesIntegersAsInt64(t *testing.T) {
	store := newTestStore(t)
	row := &executionrepo.EventPO{
		RunID: "raw-json", TreeID: "raw-json", BranchID: "raw-json", NodeID: 1,
		Type:      string(historypkg.EventActivityScheduled),
		EventJSON: []byte(`{"id":1,"timestamp":"2026-01-02T03:04:05Z","activityScheduled":{"activityId":"step","input":{"count":42}}}`),
		Timestamp: time.Now(),
	}
	createTestRun(t, store, row.RunID)
	if err := store.db.Create(row).Error; err != nil {
		t.Fatal(err)
	}
	events, err := store.Executions.GetEvents(row.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if got := events[0].ActivityScheduled.Input["count"]; got != int64(42) {
		t.Fatalf("count = %#v", got)
	}
}

func TestTypedActivityUint64SurvivesSDKHistoryAndOutArg(t *testing.T) {
	const maximum = ^uint64(0)
	registry := activitysdk.NewRegistry()
	type activityResult struct {
		Count uint64 `json:"count"`
	}
	activitysdk.RegisterActivity(registry, "activities.Fetch", "", func(context.Context, map[string]any) (activityResult, error) {
		return activityResult{Count: maximum}, nil
	})
	activity, err := registry.Get("activities.Fetch")
	if err != nil {
		t.Fatal(err)
	}
	activityOutput, err := activity(context.Background(), map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	if activityOutput["count"] != uint64(maximum) {
		t.Fatalf("SDK activity output = %#v", activityOutput)
	}

	generated, err := sdkgen.GenerateFromSource([]byte(`package flows

import "time"
import workflow "github.com/donutnomad/tenuto/sdk/workflow"
import "example.com/app/activities"

type ActivityResult struct {
	Count uint64 `+"`json:\"count\"`"+`
}

func InspectWorkflow(ctx workflow.Context, input map[string]any) error {
	var result ActivityResult
	ready := false
	if err := workflow.SetUpdateHandler(ctx, "inspect", func(ctx workflow.Context) (ActivityResult, error) {
		_ = workflow.Await(ctx, func() bool { return ready })
		return result, nil
	}); err != nil {
		return err
	}
	if err := workflow.ExecuteActivity(ctx, activities.Fetch, input, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &result); err != nil {
		return err
	}
	ready = true
	return nil
}
`), sdkgen.Options{WorkflowID: "typed-uint64", FuncName: "InspectWorkflow", Filename: "typed_uint64.go"})
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	node := generated.Workflow
	started := historypkg.NewWorkflowStartedEvent(map[string]any{}, "", "", "typed-uint64-run", "InspectWorkflow", 0)
	accepted := historypkg.NewWorkflowUpdateAcceptedEvent("inspect", "typed-uint64-update", protocol.Payloads{})
	accepted.ID = 2
	commands, err := interpreter.NewWorkflowExecutor(node, []historypkg.Event{started, accepted}).Execute()
	if err != nil {
		t.Fatalf("initial execution: %v", err)
	}
	var schedule *historypkg.Command
	for index := range commands {
		if commands[index].Type == historypkg.CmdScheduleActivity {
			schedule = &commands[index]
			break
		}
	}
	if schedule == nil {
		t.Fatalf("activity schedule missing: %+v", commands)
	}

	store := newTestStore(t)
	createTestRun(t, store, "typed-uint64-run")
	for _, event := range []historypkg.Event{
		started,
		accepted,
		historypkg.NewActivityScheduledEvent(historypkg.ActivityScheduledEvent{
			ActivityID: schedule.ActivityID, ActivityType: schedule.ActivityType, Input: schedule.Input,
		}),
		historypkg.NewActivityCompletedEvent(historypkg.ActivityCompletedEvent{
			ScheduledEventID: 3, Output: activityOutput,
		}),
	} {
		if _, err := store.Executions.Append("typed-uint64-run", "typed-uint64-run", "typed-uint64-run", event); err != nil {
			t.Fatal(err)
		}
	}
	history, err := store.Executions.GetEvents("typed-uint64-run")
	if err != nil {
		t.Fatal(err)
	}
	if history[3].ActivityCompleted.Output["count"] != uint64(maximum) {
		t.Fatalf("stored activity output = %#v", history[3].ActivityCompleted.Output)
	}
	commands, err = interpreter.NewWorkflowExecutor(node, history).Execute()
	if err != nil {
		t.Fatalf("replay execution: %v", err)
	}
	for _, command := range commands {
		if command.Type != historypkg.CmdCompleteWorkflowUpdate || command.UpdateOutcome == nil || command.UpdateOutcome.Success == nil {
			continue
		}
		var result activityResult
		if err := command.UpdateOutcome.Success.Decode(&result); err != nil {
			t.Fatal(err)
		}
		if result.Count != maximum {
			t.Fatalf("OutArg result = %#v", result)
		}
		return
	}
	t.Fatalf("typed update completion missing: %+v", commands)
}

func TestEventStorePersistsWorkflowInteractionFields(t *testing.T) {
	store := newTestStore(t)
	createTestRun(t, store, "run-1")

	if _, err := store.Executions.Append("run-1", "run-1", "run-1", historypkg.NewWorkflowSignaledEvent("payment_received", map[string]any{"amount": int64(42)})); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Executions.Append("run-1", "run-1", "run-1", historypkg.NewWorkflowUpdateCompletedOutcomeEvent("set_priority", "update-1", protocol.UpdateOutcome{
		Success: func() *protocol.Payload {
			payload, err := protocol.NewJSONPayload(map[string]any{"priority": "high"})
			if err != nil {
				t.Fatal(err)
			}
			return &payload
		}(),
	})); err != nil {
		t.Fatal(err)
	}

	history, err := store.Executions.GetEvents("run-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 2 {
		t.Fatalf("expected 2 events, got %+v", history)
	}
	if history[0].WorkflowSignaled == nil || history[0].WorkflowSignaled.Name != "payment_received" || history[0].WorkflowSignaled.Input["amount"] != int64(42) {
		t.Fatalf("unexpected signal event: %+v", history[0])
	}
	completed := history[1].WorkflowUpdateCompleted
	if completed == nil || completed.Name != "set_priority" || completed.UpdateID != "update-1" ||
		completed.Outcome == nil || completed.Outcome.Success == nil {
		t.Fatalf("unexpected update event: %+v", history[1])
	}
	if !bytes.Contains(completed.Outcome.Success.Data, []byte("high")) {
		t.Fatalf("success payload lost: %s", completed.Outcome.Success.Data)
	}
}

func TestEventStoreRoundTripsLegacyWorkflowUpdateFailure(t *testing.T) {
	store := newTestStore(t)
	createTestRun(t, store, "run-update-failure")

	if _, err := store.Executions.Append("run-update-failure", "run-update-failure", "run-update-failure",
		historypkg.NewWorkflowUpdateCompletedOutcomeEvent("create", "update-1", protocol.UpdateOutcome{
			Failure: &protocol.Failure{Message: "record creation failed"},
		})); err != nil {
		t.Fatalf("append update failure: %v", err)
	}

	history, err := store.Executions.GetEvents("run-update-failure")
	if err != nil {
		t.Fatalf("get history: %v", err)
	}
	if len(history) != 1 || history[0].WorkflowUpdateCompleted == nil {
		t.Fatalf("expected one update completed event, got %+v", history)
	}
	if got := history[0].WorkflowUpdateCompleted.Outcome.Failure.Message; got != "record creation failed" {
		t.Fatalf("failure round-trip = %q, want %q", got, "record creation failed")
	}
}

func TestEventStoreRoundTripsWorkflowUpdatePayloads(t *testing.T) {
	store := newTestStore(t)
	createTestRun(t, store, "run-update-typed")
	arg, err := protocol.NewJSONPayload(map[string]any{"record_id": "r-1"})
	if err != nil {
		t.Fatal(err)
	}
	result, err := protocol.NewJSONPayload(uint64(18446744073709551615))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Executions.Append("run-update-typed", "run-update-typed", "run-update-typed",
		historypkg.NewWorkflowUpdateAcceptedEvent("create", "update-typed", protocol.Payloads{arg})); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Executions.Append("run-update-typed", "run-update-typed", "run-update-typed",
		historypkg.NewWorkflowUpdateCompletedOutcomeEvent("create", "update-typed", protocol.UpdateOutcome{Success: &result})); err != nil {
		t.Fatal(err)
	}

	history, err := store.Executions.GetEvents("run-update-typed")
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 2 || history[0].WorkflowUpdateAccepted.Args == nil || len(*history[0].WorkflowUpdateAccepted.Args) != 1 {
		t.Fatalf("update args missing: %+v", history)
	}
	completed := history[1].WorkflowUpdateCompleted
	if completed == nil || completed.Outcome == nil || completed.Outcome.Success == nil || string(completed.Outcome.Success.Data) != "18446744073709551615" {
		t.Fatalf("update outcome missing: %+v", history)
	}
}

func TestEventStoreRoundTripsWorkflowUpdateFailureDetails(t *testing.T) {
	store := newTestStore(t)
	createTestRun(t, store, "run-update-typed-failure")
	detail, err := protocol.NewJSONPayload(map[string]any{"record_id": "r-1", "retryable": false})
	if err != nil {
		t.Fatal(err)
	}
	outcome := protocol.UpdateOutcome{Failure: &protocol.Failure{
		Message: "record creation failed",
		Type:    "CreateRecordError",
		Details: protocol.Payloads{detail},
	}}
	if _, err := store.Executions.Append("run-update-typed-failure", "run-update-typed-failure", "run-update-typed-failure",
		historypkg.NewWorkflowUpdateCompletedOutcomeEvent("create", "update-failure", outcome)); err != nil {
		t.Fatal(err)
	}

	history, err := store.Executions.GetEvents("run-update-typed-failure")
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 1 || history[0].WorkflowUpdateCompleted == nil || history[0].WorkflowUpdateCompleted.Outcome == nil {
		t.Fatalf("failure outcome missing: %+v", history)
	}
	failure := history[0].WorkflowUpdateCompleted.Outcome.Failure
	if failure == nil || failure.Type != "CreateRecordError" || failure.Message != "record creation failed" || len(failure.Details) != 1 {
		t.Fatalf("failure details lost: %+v", failure)
	}
}

// Update 的入参随整条事件存进 event_json，载荷字节经 JSON 的 base64 原样往返。
func TestUpdateAcceptedArgsRoundTrip(t *testing.T) {
	arg, err := protocol.NewJSONPayload(map[string]any{"record_id": "r-1", "amount": int64(9007199254740993)})
	if err != nil {
		t.Fatal(err)
	}
	store := newTestStore(t)
	createTestRun(t, store, "run-args")
	if _, err := store.Executions.Append("run-args", "run-args", "run-args",
		historypkg.NewWorkflowUpdateAcceptedEvent("create", "update-1", protocol.Payloads{arg})); err != nil {
		t.Fatal(err)
	}
	events, err := store.Executions.GetEvents("run-args")
	if err != nil {
		t.Fatal(err)
	}
	accepted := events[0].WorkflowUpdateAccepted
	if accepted == nil || accepted.Args == nil || len(*accepted.Args) != 1 {
		t.Fatalf("accepted args lost: %+v", events[0])
	}
	if got := (*accepted.Args)[0]; got.Encoding != arg.Encoding || !bytes.Equal(got.Data, arg.Data) {
		t.Fatalf("arg = %s (%s), want %s (%s)", got.Data, got.Encoding, arg.Data, arg.Encoding)
	}
}

// 不合法的 Update 载荷在写入前被拒绝，两个后端共用 execution.NormalizeEventFailures。
func TestUpdateAcceptedRejectsInvalidPayload(t *testing.T) {
	store := newTestStore(t)
	createTestRun(t, store, "run-bad-args")
	_, err := store.Executions.Append("run-bad-args", "run-bad-args", "run-bad-args",
		historypkg.NewWorkflowUpdateAcceptedEvent("create", "update-1", protocol.Payloads{{Encoding: "binary/unknown", Data: []byte("x")}}))
	if err == nil {
		t.Fatal("payload with an unsupported encoding should be rejected")
	}
	if events, _ := store.Executions.GetEvents("run-bad-args"); len(events) != 0 {
		t.Fatalf("rejected payload left %d events", len(events))
	}
}

func TestEventStorePersistsWorkflowConfigVersion(t *testing.T) {
	store := newTestStore(t)
	createTestRun(t, store, "parent-run")

	if _, err := store.Executions.Append("parent-run", "parent-run", "parent-run", historypkg.NewChildWorkflowStartedEvent("child-flow", "child", "child-run", "cfg-v1", nil)); err != nil {
		t.Fatal(err)
	}

	history, err := store.Executions.GetEvents("parent-run")
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 1 || history[0].ChildWorkflowStarted == nil || history[0].ChildWorkflowStarted.WorkflowConfigVersion != "cfg-v1" {
		t.Fatalf("workflow config version was not persisted: %+v", history)
	}
}

// ActivityScheduled 没有去重键：ActivityID 在前一个同名 Activity 结束后可以复用，
// 重复提交由 WorkflowTaskCompleted 的去重拦截。
func TestEventStoreAppendsRepeatedActivityScheduled(t *testing.T) {
	store := newTestStore(t)
	createTestRun(t, store, "run-1")
	event := historypkg.NewActivityScheduledEvent(historypkg.ActivityScheduledEvent{ActivityID: "charge", Input: map[string]any{"value": "first"}})

	firstID, inserted, err := store.Executions.AppendIdempotent("run-1", "run-1", "run-1", event)
	if err != nil {
		t.Fatal(err)
	}
	if !inserted {
		t.Fatal("expected first schedule event to be inserted")
	}
	secondID, inserted, err := store.Executions.AppendIdempotent("run-1", "run-1", "run-1", event)
	if err != nil {
		t.Fatal(err)
	}
	if !inserted || secondID == firstID {
		t.Fatalf("expected a second schedule event row, got id %d (first %d, inserted %v)", secondID, firstID, inserted)
	}

	history, err := store.Executions.GetEvents("run-1")
	if err != nil {
		t.Fatal(err)
	}
	if got := countStoreEvents(history, historypkg.EventActivityScheduled); got != 2 {
		t.Fatalf("expected two scheduled events, got %d", got)
	}
}

func TestEventStoreDeduplicatesActivityLifecycleByScheduledEvent(t *testing.T) {
	store := newTestStore(t)
	createTestRun(t, store, "run-1")

	startedID, inserted, err := store.Executions.AppendIdempotent("run-1", "run-1", "run-1", historypkg.NewActivityStartedEvent(historypkg.ActivityStartedEvent{
		ScheduledEventID: 10,
	}))
	if err != nil {
		t.Fatal(err)
	}
	if !inserted {
		t.Fatal("expected first started event to be inserted")
	}
	duplicateStartedID, inserted, err := store.Executions.AppendIdempotent("run-1", "run-1", "run-1", historypkg.NewActivityStartedEvent(historypkg.ActivityStartedEvent{
		ScheduledEventID: 10,
	}))
	if err != nil {
		t.Fatal(err)
	}
	if inserted || duplicateStartedID != startedID {
		t.Fatalf("expected duplicate started event to reuse %d, got inserted=%v id=%d", startedID, inserted, duplicateStartedID)
	}
	secondAttemptID, inserted, err := store.Executions.AppendIdempotent("run-1", "run-1", "run-1", historypkg.NewActivityStartedEvent(historypkg.ActivityStartedEvent{
		ScheduledEventID: 10,
		Attempt:          2,
	}))
	if err != nil {
		t.Fatal(err)
	}
	if !inserted || secondAttemptID == startedID {
		t.Fatalf("expected second attempt start to be inserted, got inserted=%v id=%d", inserted, secondAttemptID)
	}

	completedID, inserted, err := store.Executions.AppendIdempotent("run-1", "run-1", "run-1", historypkg.NewActivityCompletedEvent(historypkg.ActivityCompletedEvent{
		ScheduledEventID: 10,
		Output:           map[string]any{"ok": true},
	}))
	if err != nil {
		t.Fatal(err)
	}
	if !inserted {
		t.Fatal("expected first close event to be inserted")
	}
	failedID, inserted, err := store.Executions.AppendIdempotent("run-1", "run-1", "run-1", historypkg.NewActivityFailedEvent(historypkg.ActivityFailedEvent{
		ScheduledEventID: 10,
		Error:            "late failure",
		Failure:          &protocol.Failure{Message: "late failure"},
	}))
	if err != nil {
		t.Fatal(err)
	}
	if inserted || failedID != completedID {
		t.Fatalf("expected duplicate close event to reuse %d, got inserted=%v id=%d", completedID, inserted, failedID)
	}

	history, err := store.Executions.GetEvents("run-1")
	if err != nil {
		t.Fatal(err)
	}
	if countStoreEvents(history, historypkg.EventActivityStarted) != 2 || countStoreEvents(history, historypkg.EventActivityCompleted) != 1 || countStoreEvents(history, historypkg.EventActivityFailed) != 0 {
		t.Fatalf("unexpected activity lifecycle history: %+v", history)
	}
}

func TestEventStoreDeduplicatesActivityAttemptEventsByTypeAndAttempt(t *testing.T) {
	store := newTestStore(t)
	createTestRun(t, store, "run-1")

	timeoutID, inserted, err := store.Executions.AppendIdempotent("run-1", "run-1", "run-1", historypkg.NewActivityTimedOutEvent(historypkg.ActivityTimedOutEvent{
		ScheduledEventID: 10,
		Attempt:          1,
		TimeoutType:      protocol.TimeoutTypeStartToClose,
		Failure:          &protocol.Failure{Message: "timeout", TimeoutType: protocol.TimeoutTypeStartToClose},
	}))
	if err != nil {
		t.Fatal(err)
	}
	if !inserted {
		t.Fatal("expected first timeout event to be inserted")
	}
	duplicateTimeoutID, inserted, err := store.Executions.AppendIdempotent("run-1", "run-1", "run-1", historypkg.NewActivityTimedOutEvent(historypkg.ActivityTimedOutEvent{
		ScheduledEventID: 10,
		Attempt:          1,
		TimeoutType:      protocol.TimeoutTypeStartToClose,
		Failure:          &protocol.Failure{Message: "timeout", TimeoutType: protocol.TimeoutTypeStartToClose},
	}))
	if err != nil {
		t.Fatal(err)
	}
	if inserted || duplicateTimeoutID != timeoutID {
		t.Fatalf("expected duplicate timeout to reuse %d, got inserted=%v id=%d", timeoutID, inserted, duplicateTimeoutID)
	}

	failureID, inserted, err := store.Executions.AppendIdempotent("run-1", "run-1", "run-1", historypkg.NewActivityAttemptFailedEvent(historypkg.ActivityAttemptFailedEvent{
		ScheduledEventID: 10,
		Attempt:          1,
		Error:            "temporary",
		Failure:          &protocol.Failure{Message: "temporary"},
	}))
	if err != nil {
		t.Fatal(err)
	}
	if !inserted || failureID == timeoutID {
		t.Fatalf("expected failed attempt event to use a distinct dedupe key, got inserted=%v id=%d", inserted, failureID)
	}

	secondAttemptID, inserted, err := store.Executions.AppendIdempotent("run-1", "run-1", "run-1", historypkg.NewActivityTimedOutEvent(historypkg.ActivityTimedOutEvent{
		ScheduledEventID: 10,
		Attempt:          2,
		TimeoutType:      protocol.TimeoutTypeStartToClose,
		Failure:          &protocol.Failure{Message: "timeout", TimeoutType: protocol.TimeoutTypeStartToClose},
	}))
	if err != nil {
		t.Fatal(err)
	}
	if !inserted || secondAttemptID == timeoutID {
		t.Fatalf("expected timeout for another attempt to be inserted, got inserted=%v id=%d", inserted, secondAttemptID)
	}

	history, err := store.Executions.GetEvents("run-1")
	if err != nil {
		t.Fatal(err)
	}
	if countStoreEvents(history, historypkg.EventActivityTimedOut) != 2 || countStoreEvents(history, historypkg.EventActivityAttemptFailed) != 1 {
		t.Fatalf("unexpected activity attempt history: %+v", history)
	}
}

func TestEventStoreDeduplicatesWorkflowTaskLifecycleByScheduledEvent(t *testing.T) {
	store := newTestStore(t)
	createTestRun(t, store, "run-1")

	startedID, inserted, err := store.Executions.AppendIdempotent("run-1", "run-1", "run-1", historypkg.NewWorkflowTaskStartedEvent(10, "", "", 0))
	if err != nil {
		t.Fatal(err)
	}
	if !inserted {
		t.Fatal("expected first workflow task started event to be inserted")
	}
	duplicateStartedID, inserted, err := store.Executions.AppendIdempotent("run-1", "run-1", "run-1", historypkg.NewWorkflowTaskStartedEvent(10, "", "", 0))
	if err != nil {
		t.Fatal(err)
	}
	if inserted || duplicateStartedID != startedID {
		t.Fatalf("expected duplicate workflow task started event to reuse %d, got inserted=%v id=%d", startedID, inserted, duplicateStartedID)
	}

	completedID, inserted, err := store.Executions.AppendWorkflowTaskClosed("run-1", "run-1", "run-1", historypkg.NewWorkflowTaskCompletedEvent(10, ""))
	if err != nil {
		t.Fatal(err)
	}
	if !inserted {
		t.Fatal("expected first workflow task close event to be inserted")
	}
	timedOutID, inserted, err := store.Executions.AppendWorkflowTaskClosed("run-1", "run-1", "run-1", historypkg.NewWorkflowTaskTimedOutEvent(10, "late timeout"))
	if err != nil {
		t.Fatal(err)
	}
	if inserted || timedOutID != completedID {
		t.Fatalf("expected duplicate workflow task close event to reuse %d, got inserted=%v id=%d", completedID, inserted, timedOutID)
	}

	history, err := store.Executions.GetEvents("run-1")
	if err != nil {
		t.Fatal(err)
	}
	if countStoreEvents(history, historypkg.EventWorkflowTaskStarted) != 1 || countStoreEvents(history, historypkg.EventWorkflowTaskCompleted) != 1 || countStoreEvents(history, historypkg.EventWorkflowTaskTimedOut) != 0 {
		t.Fatalf("unexpected workflow task lifecycle history: %+v", history)
	}
}

func TestEventStoreDeduplicatesOpenWorkflowTaskAndReleasesOnClose(t *testing.T) {
	store := newTestStore(t)
	createTestRun(t, store, "run-1")

	firstID, inserted, err := store.Executions.AppendWorkflowTaskScheduled("run-1", "run-1", "run-1", historypkg.NewWorkflowTaskScheduledEvent(0))
	if err != nil {
		t.Fatal(err)
	}
	if !inserted {
		t.Fatal("expected first workflow task schedule to be inserted")
	}
	duplicateID, inserted, err := store.Executions.AppendWorkflowTaskScheduled("run-1", "run-1", "run-1", historypkg.NewWorkflowTaskScheduledEvent(0))
	if err != nil {
		t.Fatal(err)
	}
	if inserted || duplicateID != firstID {
		t.Fatalf("expected open workflow task to be reused, got inserted=%v id=%d", inserted, duplicateID)
	}
	if _, inserted, err := store.Executions.AppendWorkflowTaskClosed("run-1", "run-1", "run-1", historypkg.NewWorkflowTaskCompletedEvent(firstID, "")); err != nil {
		t.Fatal(err)
	} else if !inserted {
		t.Fatal("expected workflow task close to be inserted")
	}

	secondID, inserted, err := store.Executions.AppendWorkflowTaskScheduled("run-1", "run-1", "run-1", historypkg.NewWorkflowTaskScheduledEvent(0))
	if err != nil {
		t.Fatal(err)
	}
	if !inserted || secondID == firstID {
		t.Fatalf("expected close to release open workflow task key, got inserted=%v id=%d", inserted, secondID)
	}

	history, err := store.Executions.GetEvents("run-1")
	if err != nil {
		t.Fatal(err)
	}
	if countStoreEvents(history, historypkg.EventWorkflowTaskScheduled) != 2 {
		t.Fatalf("expected two workflow task schedules across two rounds, got %+v", history)
	}
}

func TestEventStoreCompletesWorkflowTaskWithCommandEventsInTemporalOrder(t *testing.T) {
	store := newTestStore(t)
	createTestRun(t, store, "run-1")

	scheduledID, inserted, err := store.Executions.AppendWorkflowTaskScheduled("run-1", "run-1", "run-1", historypkg.NewWorkflowTaskScheduledEvent(0))
	if err != nil {
		t.Fatal(err)
	}
	if !inserted {
		t.Fatal("expected workflow task schedule to be inserted")
	}

	result, err := store.Executions.AppendWorkflowTaskCompletedWithEvents("run-1", "run-1", "run-1", historypkg.NewWorkflowTaskCompletedEvent(scheduledID, ""), []historypkg.Event{
		historypkg.NewActivityScheduledEvent(historypkg.ActivityScheduledEvent{ActivityID: "activity-1", Input: map[string]any{"value": "ok"}}),
	})
	if err != nil {
		t.Fatal(err)
	}
	if !result.ClosedInserted {
		t.Fatal("expected workflow task close to be inserted")
	}
	if len(result.CommandEvents) != 1 || !result.CommandEvents[0].Inserted {
		t.Fatalf("expected one inserted command event, got %+v", result.CommandEvents)
	}

	history, err := store.Executions.GetEvents("run-1")
	if err != nil {
		t.Fatal(err)
	}
	wantTypes := []historypkg.EventType{
		historypkg.EventWorkflowTaskScheduled,
		historypkg.EventWorkflowTaskCompleted,
		historypkg.EventActivityScheduled,
	}
	if len(history) != len(wantTypes) {
		t.Fatalf("expected %d history events, got %+v", len(wantTypes), history)
	}
	for i, want := range wantTypes {
		if history[i].EventType() != want {
			t.Fatalf("history[%d] = %s, want %s; history=%+v", i, history[i].EventType(), want, history)
		}
	}
	if result.CommandEvents[0].EventID != history[2].ID {
		t.Fatalf("expected command event id %d, got %d", history[2].ID, result.CommandEvents[0].EventID)
	}
}

func TestEventStoreRollsBackWorkflowTaskCompletionWhenCommandEventFails(t *testing.T) {
	store := newTestStore(t)
	createTestRun(t, store, "run-1")

	scheduledID, inserted, err := store.Executions.AppendWorkflowTaskScheduled("run-1", "run-1", "run-1", historypkg.NewWorkflowTaskScheduledEvent(0))
	if err != nil {
		t.Fatal(err)
	}
	if !inserted {
		t.Fatal("expected workflow task schedule to be inserted")
	}

	_, err = store.Executions.AppendWorkflowTaskCompletedWithEvents("run-1", "run-1", "run-1", historypkg.NewWorkflowTaskCompletedEvent(scheduledID, ""), []historypkg.Event{
		historypkg.NewActivityScheduledEvent(historypkg.ActivityScheduledEvent{ActivityID: "activity-1", Input: map[string]any{"bad": func() {}}}),
	})
	if err == nil {
		t.Fatal("expected command event marshal error")
	}

	history, err := store.Executions.GetEvents("run-1")
	if err != nil {
		t.Fatal(err)
	}
	if countStoreEvents(history, historypkg.EventWorkflowTaskCompleted) != 0 || countStoreEvents(history, historypkg.EventActivityScheduled) != 0 {
		t.Fatalf("expected workflow task completion transaction to roll back, got %+v", history)
	}

	reusedID, inserted, err := store.Executions.AppendWorkflowTaskScheduled("run-1", "run-1", "run-1", historypkg.NewWorkflowTaskScheduledEvent(0))
	if err != nil {
		t.Fatal(err)
	}
	if inserted || reusedID != scheduledID {
		t.Fatalf("expected original workflow task to remain open, got inserted=%v id=%d", inserted, reusedID)
	}
}

func TestEventStoreDeduplicatesWorkflowTerminalEventByRun(t *testing.T) {
	store := newTestStore(t)
	createTestRun(t, store, "run-1")

	firstID, inserted, err := store.Executions.AppendIdempotent("run-1", "run-1", "run-1", historypkg.NewWorkflowCompletedEvent(map[string]any{"ok": true}))
	if err != nil {
		t.Fatal(err)
	}
	if !inserted {
		t.Fatal("expected first terminal event to be inserted")
	}

	secondID, inserted, err := store.Executions.AppendIdempotent("run-1", "run-1", "run-1", historypkg.NewWorkflowFailedEvent("late failure", ""))
	if err != nil {
		t.Fatal(err)
	}
	if inserted {
		t.Fatal("expected duplicate terminal event to reuse existing row")
	}
	if secondID != firstID {
		t.Fatalf("expected duplicate terminal id %d, got %d", firstID, secondID)
	}

	history, err := store.Executions.GetEvents("run-1")
	if err != nil {
		t.Fatal(err)
	}
	if countWorkflowTerminalEvents(history) != 1 {
		t.Fatalf("expected one terminal workflow event, got %+v", history)
	}
	if history[0].EventType() != historypkg.EventWorkflowCompleted {
		t.Fatalf("expected first terminal event to win, got %+v", history[0])
	}
}

func TestTaskDispatchStoreClaimsRetriesAndSkipsDispatched(t *testing.T) {
	store := newTestStore(t)
	task := protocol.Task{
		Type:             protocol.TaskTypeActivity,
		RunID:            "run-dispatch",
		ScheduledEventID: 10,
		Attempt:          1,
		ActivityID:       "capture",
		ActivityType:     "custodian.Capture",
		TaskQueue:        "custodian",
	}

	firstLease, acquired, err := store.Dispatches.AcquireTaskDispatch(task, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if !acquired || firstLease.LeaseID == "" {
		t.Fatalf("expected first dispatch lease, got acquired=%v lease=%+v", acquired, firstLease)
	}

	_, acquired, err = store.Dispatches.AcquireTaskDispatch(task, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if acquired {
		t.Fatal("expected active dispatch lease to block duplicate acquisition")
	}

	if err := store.Dispatches.MarkTaskDispatchFailed(task, firstLease.LeaseID, "queue down"); err != nil {
		t.Fatal(err)
	}
	failed, ok, err := store.Dispatches.GetTaskDispatch(task)
	if err != nil {
		t.Fatal(err)
	}
	if !ok || failed.Status != dispatch.StatusFailed || failed.Attempts != 1 || failed.LastError != "queue down" {
		t.Fatalf("unexpected failed dispatch state: ok=%v state=%+v", ok, failed)
	}

	secondLease, acquired, err := store.Dispatches.AcquireTaskDispatch(task, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if !acquired || secondLease.LeaseID == firstLease.LeaseID {
		t.Fatalf("expected retry dispatch lease, got acquired=%v lease=%+v", acquired, secondLease)
	}
	if err := store.Dispatches.MarkTaskDispatchDispatched(task, secondLease.LeaseID); err != nil {
		t.Fatal(err)
	}

	_, acquired, err = store.Dispatches.AcquireTaskDispatch(task, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if acquired {
		t.Fatal("expected dispatched task to skip duplicate acquisition")
	}
	dispatched, ok, err := store.Dispatches.GetTaskDispatch(task)
	if err != nil {
		t.Fatal(err)
	}
	if !ok || dispatched.Status != dispatch.StatusDispatched || dispatched.Attempts != 2 || dispatched.LastDispatchedAt.IsZero() {
		t.Fatalf("unexpected dispatched state: ok=%v state=%+v", ok, dispatched)
	}
}

func TestActivityWorkerVersionStorePersistsHeartbeatAndFiltersExpiry(t *testing.T) {
	store := newTestStore(t)
	now := time.Date(2026, 6, 9, 12, 0, 0, 0, time.UTC)
	worker := protocol.ActivityWorkerVersion{
		Identity:      "custodian-worker-1",
		BuildID:       "custodian@v1",
		TaskQueues:    []string{"custodian"},
		ActivityTypes: []string{"custodian.Capture"},
	}

	saved, err := store.Dispatches.UpsertActivityWorkerVersionInNamespace(protocol.DefaultNamespace, worker, now, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if saved.LastHeartbeatAt != now || !saved.ExpiresAt.Equal(now.Add(time.Minute)) {
		t.Fatalf("unexpected saved worker version: %+v", saved)
	}

	active, err := store.Dispatches.ListActiveActivityWorkerVersions(now.Add(30 * time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if len(active) != 1 || active[0].ActivityTypes[0] != "custodian.Capture" || active[0].TaskQueues[0] != "custodian" {
		t.Fatalf("unexpected active worker versions: %+v", active)
	}

	heartbeatAt := now.Add(40 * time.Second)
	if err := store.Dispatches.HeartbeatActivityWorkerVersionInNamespace(protocol.DefaultNamespace, "custodian-worker-1", "custodian@v1", heartbeatAt, time.Minute); err != nil {
		t.Fatal(err)
	}
	active, err = store.Dispatches.ListActiveActivityWorkerVersions(now.Add(90 * time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if len(active) != 1 || !active[0].LastHeartbeatAt.Equal(heartbeatAt) || !active[0].ExpiresAt.Equal(heartbeatAt.Add(time.Minute)) {
		t.Fatalf("expected heartbeat to extend active route, got %+v", active)
	}

	expired, err := store.Dispatches.ListActiveActivityWorkerVersions(now.Add(2 * time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if len(expired) != 0 {
		t.Fatalf("expected expired worker routes to be filtered, got %+v", expired)
	}
}

func TestActivityWorkerVersionStoreKeepsMultipleBuildsForSameActivity(t *testing.T) {
	store := newTestStore(t)
	now := time.Date(2026, 6, 9, 12, 0, 0, 0, time.UTC)

	if _, err := store.Dispatches.UpsertActivityWorkerVersionInNamespace(protocol.DefaultNamespace, protocol.ActivityWorkerVersion{
		Identity:      "custodian-worker-v1",
		BuildID:       "custodian@v1",
		TaskQueues:    []string{"custodian"},
		ActivityTypes: []string{"custodian.Capture"},
		Default:       true,
	}, now, time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Dispatches.UpsertActivityWorkerVersionInNamespace(protocol.DefaultNamespace, protocol.ActivityWorkerVersion{
		Identity:      "custodian-worker-v2",
		BuildID:       "custodian@v2",
		TaskQueues:    []string{"custodian"},
		ActivityTypes: []string{"custodian.Capture"},
		Weight:        25,
	}, now, time.Minute); err != nil {
		t.Fatal(err)
	}

	active, err := store.Dispatches.ListActiveActivityWorkerVersions(now.Add(30 * time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if len(active) != 2 {
		t.Fatalf("expected two active worker versions, got %+v", active)
	}
	seen := map[string]protocol.ActivityWorkerVersion{}
	for _, worker := range active {
		seen[worker.BuildID] = worker
	}
	if !seen["custodian@v1"].Default || seen["custodian@v2"].Weight != 25 {
		t.Fatalf("unexpected worker versions: %+v", active)
	}
}

func TestActivityWorkerVersionStorePersistsAllTaskQueues(t *testing.T) {
	store := newTestStore(t)
	now := time.Date(2026, 6, 9, 12, 0, 0, 0, time.UTC)
	if _, err := store.Dispatches.UpsertActivityWorkerVersionInNamespace(protocol.DefaultNamespace, protocol.ActivityWorkerVersion{
		Identity:      "custodian-worker-multi-queue",
		BuildID:       "custodian@multi",
		TaskQueues:    []string{"custodian", "custodian-priority"},
		ActivityTypes: []string{"custodian.Capture"},
	}, now, time.Minute); err != nil {
		t.Fatal(err)
	}

	active, err := store.Dispatches.ListActiveActivityWorkerVersions(now.Add(30 * time.Second))
	if err != nil {
		t.Fatal(err)
	}
	queues := map[string]bool{}
	for _, worker := range active {
		queues[worker.TaskQueues[0]] = true
	}
	if len(active) != 2 || !queues["custodian"] || !queues["custodian-priority"] {
		t.Fatalf("expected one worker route per task queue, got %+v", active)
	}
}

func TestActivityWorkerVersionHeartbeatUsesRegisteredTTL(t *testing.T) {
	store := newTestStore(t)
	now := time.Date(2026, 6, 9, 12, 0, 0, 0, time.UTC)
	if _, err := store.Dispatches.UpsertActivityWorkerVersionInNamespace(protocol.DefaultNamespace, protocol.ActivityWorkerVersion{
		Identity:      "custodian-worker-ttl",
		BuildID:       "custodian@ttl",
		TaskQueues:    []string{"custodian"},
		ActivityTypes: []string{"custodian.Capture"},
		HeartbeatTTL:  "2m",
	}, now, 2*time.Minute); err != nil {
		t.Fatal(err)
	}

	heartbeatAt := now.Add(30 * time.Second)
	if err := store.Dispatches.HeartbeatActivityWorkerVersionInNamespace(protocol.DefaultNamespace, "custodian-worker-ttl", "custodian@ttl", heartbeatAt, protocol.DefaultActivityWorkerVersionTTL); err != nil {
		t.Fatal(err)
	}
	active, err := store.Dispatches.ListActiveActivityWorkerVersions(heartbeatAt.Add(90 * time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if len(active) != 1 || !active[0].ExpiresAt.Equal(heartbeatAt.Add(2*time.Minute)) {
		t.Fatalf("expected heartbeat to preserve registered ttl, got %+v", active)
	}
}

func TestGormStore_ActivityResetRoundTrip(t *testing.T) {
	// Integration: ActivityReset 事件写入 gormstore 列式存储后能完整读回
	store := newTestStore(t)
	createTestRun(t, store, "run-reset")

	_, err := store.Executions.Append("run-reset", "run-reset", "run-reset", historypkg.NewActivityResetEvent(historypkg.ActivityResetEvent{
		ActivityID: "settle", ScheduledEventID: 114, Attempt: 3, Identity: "u", Reason: "r", ResetHeartbeat: true,
	}))
	if err != nil {
		t.Fatal(err)
	}

	history, err := store.Executions.GetEvents("run-reset")
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 1 {
		t.Fatalf("expected 1 event, got %d", len(history))
	}
	got := history[0].ActivityReset
	if got == nil {
		t.Fatal("ActivityReset is nil after round-trip")
	}
	if got.ActivityID != "settle" {
		t.Errorf("StepID = %q, want %q", got.ActivityID, "settle")
	}
	if got.ScheduledEventID != 114 {
		t.Errorf("ScheduledEventID = %d, want 114", got.ScheduledEventID)
	}
	if got.Attempt != 3 {
		t.Errorf("Attempt = %d, want 3", got.Attempt)
	}
	if got.Identity != "u" {
		t.Errorf("Identity = %q, want %q", got.Identity, "u")
	}
	if got.Reason != "r" {
		t.Errorf("Reason = %q, want %q", got.Reason, "r")
	}
	if !got.ResetHeartbeat {
		t.Errorf("ResetHeartbeat = false, want true")
	}
}

// newTestStore 返回独立 MySQL 库上的 Store；没有开启 MySQL 后端测试时跳过 t。
func newTestStore(t *testing.T) *Store {
	t.Helper()
	return newMySQLStore(t, Config{})
}

func countStoreEvents(history []historypkg.Event, typ historypkg.EventType) int {
	count := 0
	for i := range history {
		if history[i].EventType() == typ {
			count++
		}
	}
	return count
}

func TestEventStorePersistsWorkflowFailedTimeoutType(t *testing.T) {
	// Integration: WorkflowFailed 事件的 TimeoutType 必须在 gorm 持久化后完整取回，
	// 避免生产重启后 TimeoutType 丢失导致超时类型统计失效。
	store := newTestStore(t)
	createTestRun(t, store, "run-timeout")

	ev := historypkg.NewWorkflowFailedEvent("workflow run timeout", engine.TimeoutTypeRun)
	if _, err := store.Executions.Append("run-timeout", "run-timeout", "run-timeout", ev); err != nil {
		t.Fatal(err)
	}

	history, err := store.Executions.GetEvents("run-timeout")
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 1 {
		t.Fatalf("expected 1 event, got %d", len(history))
	}
	got := history[0]
	if got.WorkflowFailed == nil {
		t.Fatal("expected WorkflowFailed event")
	}
	if got.WorkflowFailed.Error != "workflow run timeout" {
		t.Fatalf("unexpected Error: %q", got.WorkflowFailed.Error)
	}
	if got.WorkflowFailed.TimeoutType != engine.TimeoutTypeRun {
		t.Fatalf("unexpected TimeoutType: %q, want %q", got.WorkflowFailed.TimeoutType, engine.TimeoutTypeRun)
	}
}

// Happy Path: WorkflowStarted 事件的 Attempt/Initiator 字段 gorm 持久化后完整取回。
func TestEventStoreWorkflowStartedAttemptInitiatorRoundTrip(t *testing.T) {
	store := newTestStore(t)
	createTestRun(t, store, "run-retry-started")

	ev := historypkg.NewWorkflowStartedEvent(nil, "first-run", "prev-run", "wf-1", "Workflow", 0)
	ev.WorkflowStarted.Attempt = 3
	ev.WorkflowStarted.Initiator = execution.InitiatorEnums.Retry
	if _, err := store.Executions.Append("run-retry-started", "run-retry-started", "run-retry-started", ev); err != nil {
		t.Fatal(err)
	}

	history, err := store.Executions.GetEvents("run-retry-started")
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 1 || history[0].WorkflowStarted == nil {
		t.Fatalf("expected 1 WorkflowStarted event, got %+v", history)
	}
	got := history[0].WorkflowStarted
	if got.Attempt != 3 {
		t.Fatalf("Attempt round-trip failed: got %d, want 3", got.Attempt)
	}
	if got.Initiator != execution.InitiatorEnums.Retry {
		t.Fatalf("Initiator round-trip failed: got %q, want %q", got.Initiator, execution.InitiatorEnums.Retry)
	}
}

func countWorkflowTerminalEvents(history []historypkg.Event) int {
	count := 0
	for i := range history {
		switch history[i].EventType() {
		case historypkg.EventWorkflowCompleted, historypkg.EventWorkflowFailed, historypkg.EventWorkflowCancelled, historypkg.EventWorkflowTerminated:
			count++
		}
	}
	return count
}

// Edge Case: 两个调用方各持一份快照，先后更新不同字段。后提交的一方只写自己改过的字段，
// 先提交的字段保留（多副本下的 Server 实例之间没有共享的进程内锁）。
func TestRunUpdateWritesOnlyChangedFields(t *testing.T) {
	store := newTestStore(t)
	createTestRun(t, store, "run-stale")
	stale, err := store.Executions.Get("run-stale")
	if err != nil {
		t.Fatal(err)
	}
	executiontest.UpdateRun(t, store.Executions, "run-stale", func(run *execution.Run) error {
		run.MarkConfigError("contract mismatch")
		return nil
	})
	if err := store.Executions.RunInTx(func(tx execution.ExecutionTx) error {
		stale.ApplyTimeouts(time.Hour, 0, time.Now())
		return tx.UpdateRun(stale)
	}); err != nil {
		t.Fatal(err)
	}
	got, err := store.Executions.Get("run-stale")
	if err != nil {
		t.Fatal(err)
	}
	if got.ConfigError != "contract mismatch" || got.DeadlineAt == nil {
		t.Fatalf("run after two updates: config_error=%q deadline=%v", got.ConfigError, got.DeadlineAt)
	}
}

// rootBranch 给测试 Run 补上 root branch token：tree 与 branch 都取 Run ID，
// 与 execution.PrepareStartedRun 为新 Run 生成的 token 一致。
func rootBranch(t testing.TB, run *execution.Run) *execution.Run {
	t.Helper()
	token, err := execution.SerializeBranchToken(&execution.HistoryBranch{TreeID: run.ID, BranchID: run.ID})
	if err != nil {
		t.Fatal(err)
	}
	run.BranchToken = token
	return run
}
