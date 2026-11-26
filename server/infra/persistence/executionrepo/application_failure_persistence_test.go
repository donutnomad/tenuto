package executionrepo

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
)

func matrixFailurePayload() protocol.Payloads {
	return protocol.Payloads{{
		Encoding: protocol.PayloadEncodingJSONPlainV1,
		Data:     []byte(`"card-1"`),
	}}
}

func eventPOByType(t *testing.T, store *Repository, runID string, eventType history.EventType) EventPO {
	t.Helper()
	var row EventPO
	if err := store.db.Where("run_id = ? AND type = ?", runID, string(eventType)).First(&row).Error; err != nil {
		t.Fatalf("load %s row: %v", eventType, err)
	}
	return row
}

// ActivityFailed 的结构化失败逐字段落库/回读，Error 仍是 message 镜像。
func TestActivityFailedFailureRoundTrip(t *testing.T) {
	store := newTestStore(t)
	createTestRun(t, store, "run-activity-failure")
	cause := &protocol.Failure{Message: "inner boom", Type: "InnerType", NonRetryable: true}
	want := protocol.Failure{
		Message:      "card declined",
		Type:         "PaymentDeclined",
		NonRetryable: true,
		Details:      matrixFailurePayload(),
		Cause:        cause,
	}
	event := history.NewActivityFailedEvent(history.ActivityFailedEvent{
		ActivityID: "charge", ScheduledEventID: 7, Attempt: 2, ActivityType: "charge",
		Error: want.Message, Failure: &want,
	})
	if _, err := store.Append("run-activity-failure", "run-activity-failure", "run-activity-failure", event); err != nil {
		t.Fatalf("Append: %v", err)
	}

	events, err := store.GetEvents("run-activity-failure")
	if err != nil {
		t.Fatalf("GetEvents: %v", err)
	}
	if len(events) != 1 || events[0].ActivityFailed == nil {
		t.Fatalf("events = %+v", events)
	}
	failed := events[0].ActivityFailed
	if failed.Failure == nil {
		t.Fatal("ActivityFailed.Failure was not persisted")
	}
	if !reflect.DeepEqual(*failed.Failure, want) {
		t.Fatalf("failure round-trip = %+v, want %+v", *failed.Failure, want)
	}
	if failed.Failure.Type != "PaymentDeclined" || !failed.Failure.NonRetryable {
		t.Fatalf("failure scalars = %+v", failed.Failure)
	}
	if len(failed.Failure.Details) != 1 || failed.Failure.Details[0].Encoding != protocol.PayloadEncodingJSONPlainV1 {
		t.Fatalf("failure details = %+v", failed.Failure.Details)
	}
	if failed.Failure.Cause == nil || failed.Failure.Cause.Type != "InnerType" || !failed.Failure.Cause.NonRetryable {
		t.Fatalf("failure cause = %+v", failed.Failure.Cause)
	}

	if failed.Error != "card declined" {
		t.Fatalf("error = %q, want message mirror", failed.Error)
	}
	row := eventPOByType(t, store, "run-activity-failure", history.EventActivityFailed)
	if !strings.Contains(string(row.EventJSON), `"failure"`) {
		t.Fatalf("event_json does not carry a failure payload: %s", row.EventJSON)
	}
	if row.EventSizeBytes != int64(len(row.EventJSON)) {
		t.Fatalf("event_size_bytes = %d, event_json is %d bytes", row.EventSizeBytes, len(row.EventJSON))
	}
}

// WorkflowFailed 带结构化失败可回读；message-only 失败保持 Type 为空。终态之后 History 不再接受事件，两个失败各写一个 Run。
func TestWorkflowFailedFailureRoundTrip(t *testing.T) {
	store := newTestStore(t)
	createTestRun(t, store, "run-workflow-failure")
	createTestRun(t, store, "run-workflow-failure-plain")
	structured := protocol.Failure{Message: "card declined", Type: "PaymentDeclined", Details: matrixFailurePayload()}
	structuredEvent, err := history.NewWorkflowFailedFailureEvent(structured, "")
	if err != nil {
		t.Fatalf("NewWorkflowFailedFailureEvent: %v", err)
	}
	if _, err := store.Append("run-workflow-failure", "run-workflow-failure", "run-workflow-failure", structuredEvent); err != nil {
		t.Fatalf("Append structured: %v", err)
	}
	legacy := protocol.Failure{Message: "plain boom"}
	legacyEvent, err := history.NewWorkflowFailedFailureEvent(legacy, "")
	if err != nil {
		t.Fatalf("NewWorkflowFailedFailureEvent: %v", err)
	}
	if _, err := store.Append("run-workflow-failure-plain", "run-workflow-failure-plain", "run-workflow-failure-plain", legacyEvent); err != nil {
		t.Fatalf("Append message-only: %v", err)
	}

	events, readErr := store.GetEvents("run-workflow-failure")
	if readErr != nil {
		t.Fatalf("GetEvents: %v", readErr)
	}
	plainEvents, readErr := store.GetEvents("run-workflow-failure-plain")
	if readErr != nil {
		t.Fatalf("GetEvents: %v", readErr)
	}
	if len(events) != 1 || len(plainEvents) != 1 {
		t.Fatalf("events = %d and %d, want 1 each", len(events), len(plainEvents))
	}
	events = append(events, plainEvents...)
	first := events[0].WorkflowFailed
	if first == nil || first.Failure == nil {
		t.Fatalf("first WorkflowFailed lost its failure: %+v", events[0])
	}
	if !reflect.DeepEqual(*first.Failure, structured) {
		t.Fatalf("structured round-trip = %+v, want %+v", *first.Failure, structured)
	}
	if first.Error != "card declined" {
		t.Fatalf("structured error mirror = %q", first.Error)
	}

	second := events[1].WorkflowFailed
	if second == nil || second.Failure == nil {
		t.Fatalf("second WorkflowFailed lost its failure: %+v", events[1])
	}
	if second.Failure.Type != "" || second.Failure.NonRetryable || len(second.Failure.Details) != 0 || second.Failure.Cause != nil {
		t.Fatalf("message-only failure shape = %+v", *second.Failure)
	}
	if second.Failure.Message != "plain boom" || second.Error != "plain boom" {
		t.Fatalf("message-only scalars: %+v error=%q", *second.Failure, second.Error)
	}
}

// event_json 为空或不是事件 JSON 的行已损坏，读取时报错。
func TestEventRowWithoutPayloadIsRejected(t *testing.T) {
	for _, payload := range [][]byte{nil, []byte("not json"), []byte(`{"id":1} trailing`)} {
		row := EventPO{
			RunID: "run-failure", TreeID: "run-failure", BranchID: "run-failure", NodeID: 1,
			Type: string(history.EventWorkflowFailed), Timestamp: time.Now(), EventJSON: payload,
		}
		if _, err := rowToEvent(&row); err == nil {
			t.Fatalf("event row with payload %q must be rejected", payload)
		}
	}
}

// Update completed 的 outcome 随整条事件存进 event_json：结构化失败与普通失败都能读回。
func TestUpdateCompletedOutcomeRoundTrip(t *testing.T) {
	store := newTestStore(t)
	createTestRun(t, store, "run-update-failure")

	structured := protocol.UpdateOutcome{Failure: &protocol.Failure{
		Message: "card declined", Type: "PaymentDeclined", NonRetryable: true,
	}}
	plain := protocol.UpdateOutcome{Failure: &protocol.Failure{Message: "transient"}}
	for updateID, outcome := range map[string]*protocol.UpdateOutcome{"u-structured": &structured, "u-plain": &plain} {
		if _, err := store.Append("run-update-failure", "run-update-failure", "run-update-failure", history.Event{
			Timestamp: time.Now(),
			WorkflowUpdateCompleted: &history.WorkflowUpdateCompletedEvent{
				Name: "approve", UpdateID: updateID, Outcome: outcome,
			},
		}); err != nil {
			t.Fatalf("Append %s: %v", updateID, err)
		}
	}

	events, err := store.GetEvents("run-update-failure")
	if err != nil {
		t.Fatalf("GetEvents: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("events = %d, want 2", len(events))
	}
	for _, event := range events {
		completed := event.WorkflowUpdateCompleted
		if completed == nil || completed.Outcome == nil || completed.Outcome.Failure == nil {
			t.Fatalf("outcome lost: %+v", event)
		}
		want := plain
		if completed.UpdateID == "u-structured" {
			want = structured
		}
		if !reflect.DeepEqual(*completed.Outcome, want) {
			t.Fatalf("%s outcome = %+v, want %+v", completed.UpdateID, *completed.Outcome.Failure, *want.Failure)
		}
	}
}

// Child 终态失败与 ActivityFailed/WorkflowFailed 同形落库。
func TestChildWorkflowFailedCarriesStructuredFailure(t *testing.T) {
	store := newTestStore(t)
	createTestRun(t, store, "run-child-failure")
	want := protocol.Failure{
		Message: "child declined", Type: "ChildDeclined", NonRetryable: true,
		Details: matrixFailurePayload(),
		Cause:   &protocol.Failure{Message: "inner", Type: "InnerType"},
	}
	event := history.NewChildWorkflowFailedEvent("child-1", "run-child", 11, want)
	event.Timestamp = time.Now()
	if _, err := store.Append("run-child-failure", "run-child-failure", "run-child-failure", event); err != nil {
		t.Fatalf("Append: %v", err)
	}

	events, err := store.GetEvents("run-child-failure")
	if err != nil {
		t.Fatalf("GetEvents: %v", err)
	}
	if len(events) != 1 || events[0].ChildWorkflowFailed == nil {
		t.Fatalf("events = %+v", events)
	}
	failed := events[0].ChildWorkflowFailed
	if failed.Failure == nil || !reflect.DeepEqual(*failed.Failure, want) {
		t.Fatalf("child failure round-trip = %+v, want %+v", failed.Failure, want)
	}
	if failed.Error != want.Message {
		t.Fatalf("error column mirror = %q, want %q", failed.Error, want.Message)
	}
	if failed.ChildID != "child-1" || failed.ChildRunID != "run-child" || failed.ScheduledEventID != 11 {
		t.Fatalf("child identity lost: %+v", failed)
	}
}

// 超限载荷在落库前明确失败：与 memory 后端同一份 Normalize 实现给出同一类错误。
func TestAppendRejectsOversizedFailurePayload(t *testing.T) {
	store := newTestStore(t)
	createTestRun(t, store, "run-oversize")
	payloads := make(protocol.Payloads, 0, 1025)
	for i := 0; i < 1025; i++ {
		payloads = append(payloads, protocol.Payload{
			Encoding: protocol.PayloadEncodingJSONPlainV1,
			Data:     []byte(`0`),
		})
	}
	oversize := protocol.Failure{Message: "too many", Type: "Bulk", Details: payloads}
	event := history.Event{
		Timestamp:      time.Now(),
		ActivityFailed: &history.ActivityFailedEvent{ActivityID: "charge", Error: oversize.Message, Failure: &oversize},
	}
	if _, err := store.Append("run-oversize", "run-oversize", "run-oversize", event); err == nil {
		t.Fatal("超限载荷必须失败")
	} else if !strings.Contains(err.Error(), "payload count exceeds") {
		t.Fatalf("错误信息应指出上限: %v", err)
	}
	events, err := store.GetEvents("run-oversize")
	if err != nil {
		t.Fatalf("GetEvents: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("超限载荷不应留下事件: %+v", events)
	}
}
