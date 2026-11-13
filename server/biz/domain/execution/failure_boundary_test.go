package execution

import (
	"strings"
	"testing"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
)

// unsortedPayload 是合法但非 canonical 的 JSON：normalize 会按键排序重编码，
// 用来证明边界真的做了 canonical 化而不是原样透传。
func unsortedPayload() protocol.Payloads {
	return protocol.Payloads{{
		Encoding: protocol.PayloadEncodingJSONPlainV1,
		Data:     []byte(`{"b":1,"a":2}`),
	}}
}

func oversizedPayloads() protocol.Payloads {
	payloads := make(protocol.Payloads, 0, 1025)
	for i := 0; i < 1025; i++ {
		payloads = append(payloads, protocol.Payload{
			Encoding: protocol.PayloadEncodingJSONPlainV1,
			Data:     []byte(`0`),
		})
	}
	return payloads
}

// 边界一致性的正向用例：Activity / Workflow / Child / Update outcome 四处失败载荷
// 全部走同一个 Normalize，details 被 canonical 化且逐字段保留。
func TestNormalizeEventFailuresCanonicalizesEverySlot(t *testing.T) {
	failure := protocol.Failure{
		Message:      "card declined",
		Type:         "PaymentDeclined",
		NonRetryable: true,
		Details:      unsortedPayload(),
		Cause:        &protocol.Failure{Message: "inner", Type: "InnerType"},
	}
	tests := []struct {
		name  string
		event history.Event
		get   func(history.Event) *protocol.Failure
	}{
		{
			name: "activity attempt failed",
			event: history.Event{ActivityAttemptFailed: &history.ActivityAttemptFailedEvent{
				ActivityID: "step", ScheduledEventID: 7, Attempt: 2, Error: failure.Message, Failure: &failure,
			}},
			get: func(e history.Event) *protocol.Failure { return e.ActivityAttemptFailed.Failure },
		},
		{
			name: "activity failed",
			event: history.Event{ActivityFailed: &history.ActivityFailedEvent{
				ActivityID: "step", ScheduledEventID: 7, Error: failure.Message, Failure: &failure,
			}},
			get: func(e history.Event) *protocol.Failure { return e.ActivityFailed.Failure },
		},
		{
			name:  "workflow failed",
			event: history.Event{WorkflowFailed: &history.WorkflowFailedEvent{Error: failure.Message, Failure: &failure}},
			get:   func(e history.Event) *protocol.Failure { return e.WorkflowFailed.Failure },
		},
		{
			name: "child workflow failed",
			event: history.Event{ChildWorkflowFailed: &history.ChildWorkflowFailedEvent{
				ChildID: "child", Error: failure.Message, Failure: &failure,
			}},
			get: func(e history.Event) *protocol.Failure { return e.ChildWorkflowFailed.Failure },
		},
		{
			name: "update outcome failed",
			event: history.Event{WorkflowUpdateCompleted: &history.WorkflowUpdateCompletedEvent{
				UpdateID: "u1",
				Outcome:  &history.UpdateOutcome{Failure: &failure},
			}},
			get: func(e history.Event) *protocol.Failure { return e.WorkflowUpdateCompleted.Outcome.Failure },
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			normalized, err := NormalizeEventFailures(test.event)
			if err != nil {
				t.Fatalf("NormalizeEventFailures: %v", err)
			}
			got := test.get(normalized)
			if got == nil {
				t.Fatalf("失败载荷丢失: %+v", normalized)
			}
			if got.Message != failure.Message || got.Type != failure.Type || !got.NonRetryable {
				t.Fatalf("失败字段被改写: %+v", got)
			}
			if len(got.Details) != 1 || string(got.Details[0].Data) != `{"a":2,"b":1}` {
				t.Fatalf("details 未 canonical 化: %q", got.Details[0].Data)
			}
			if got.Cause == nil || got.Cause.Type != "InnerType" {
				t.Fatalf("cause 丢失: %+v", got.Cause)
			}
		})
	}
}

// 超限载荷必须在边界明确失败，且返回零值事件——调用方因此在写状态/事件之前就能返回错误。
func TestNormalizeEventFailuresRejectsOversizeDetails(t *testing.T) {
	oversize := protocol.Failure{Message: "too many", Type: "Bulk", Details: oversizedPayloads()}
	events := map[string]history.Event{
		"activity failed": {ActivityFailed: &history.ActivityFailedEvent{Failure: &oversize}},
		"workflow failed": {WorkflowFailed: &history.WorkflowFailedEvent{Failure: &oversize}},
	}
	for name, event := range events {
		t.Run(name, func(t *testing.T) {
			normalized, err := NormalizeEventFailures(event)
			if err == nil {
				t.Fatalf("超限载荷必须失败: %+v", normalized)
			}
			if !strings.Contains(err.Error(), "payload count exceeds") {
				t.Fatalf("错误信息应指出上限: %v", err)
			}
		})
	}
}

// 失败类事件必须携带 Failure：缺失是协议错误，边界明确拒绝，不用字符串补造。
func TestNormalizeEventFailuresKeepsLegacyErrorOnlyHistory(t *testing.T) {
	events := map[string]history.Event{
		"activity failed":         {ActivityFailed: &history.ActivityFailedEvent{ActivityID: "step", Error: "boom"}},
		"activity attempt failed": {ActivityAttemptFailed: &history.ActivityAttemptFailedEvent{ActivityID: "step", Error: "boom"}},
		"activity timed out":      {ActivityTimedOut: &history.ActivityTimedOutEvent{ActivityID: "step"}},
		"workflow failed":         {WorkflowFailed: &history.WorkflowFailedEvent{Error: "boom"}},
		"child workflow failed":   {ChildWorkflowFailed: &history.ChildWorkflowFailedEvent{ChildID: "c", Error: "boom"}},
	}
	for name, event := range events {
		t.Run(name, func(t *testing.T) {
			if _, err := NormalizeEventFailures(event); err == nil {
				t.Fatal("缺 Failure 的失败事件必须被拒绝")
			}
		})
	}
}

// Update 完成事件必须携带 Outcome，且 Success/Failure 恰好一个。
func TestFailureBoundaryRequiresUpdateOutcome(t *testing.T) {
	if _, err := NormalizeEventFailures(history.Event{WorkflowUpdateCompleted: &history.WorkflowUpdateCompletedEvent{
		UpdateID: "u1", Outcome: &history.UpdateOutcome{},
	}}); err == nil {
		t.Fatal("空 outcome 必须被拒绝")
	}
	if _, err := NormalizeEventFailures(history.Event{WorkflowUpdateCompleted: &history.WorkflowUpdateCompletedEvent{
		UpdateID: "u1",
	}}); err == nil {
		t.Fatal("缺 outcome 的 completed 事件必须被拒绝")
	}
	successPayload, err := protocol.NewJSONPayload(map[string]any{"ok": true})
	if err != nil {
		t.Fatalf("NewJSONPayload: %v", err)
	}
	normalized, err := NormalizeEventFailures(history.Event{WorkflowUpdateCompleted: &history.WorkflowUpdateCompletedEvent{
		UpdateID: "u1", Outcome: &history.UpdateOutcome{Success: &successPayload},
	}})
	if err != nil {
		t.Fatalf("NormalizeEventFailures: %v", err)
	}
	if normalized.WorkflowUpdateCompleted.Outcome.Success == nil {
		t.Fatal("成功 outcome 应保留 Success")
	}
}

// Workflow 终态构造函数是失败载荷的入口校验点：超限在返回事件之前报错，
// 空 message 的 typed 失败仍然是失败（Error 镜像 message，可能为空）。
func TestNewWorkflowFailedFailureEventValidatesPayload(t *testing.T) {
	if _, err := history.NewWorkflowFailedFailureEvent(protocol.Failure{
		Message: "too many", Details: oversizedPayloads(),
	}, ""); err == nil {
		t.Fatal("超限载荷必须失败")
	}

	typed := protocol.Failure{Message: "", Type: "TypedEmptyMessage"}
	event, err := history.NewWorkflowFailedFailureEvent(typed, "")
	if err != nil {
		t.Fatalf("NewWorkflowFailedFailureEvent: %v", err)
	}
	if event.WorkflowFailed.Failure == nil || event.WorkflowFailed.Failure.Type != "TypedEmptyMessage" {
		t.Fatalf("空 message 的 typed 失败必须保留结构: %+v", event.WorkflowFailed)
	}
	if event.WorkflowFailed.Error != "" {
		t.Fatalf("Error 应镜像空 message: %q", event.WorkflowFailed.Error)
	}

	withoutDetails := protocol.Failure{Message: "boom", Type: "Plain"}
	event, err = history.NewWorkflowFailedFailureEvent(withoutDetails, "RunTimeout")
	if err != nil {
		t.Fatalf("NewWorkflowFailedFailureEvent: %v", err)
	}
	if event.WorkflowFailed.Failure == nil || event.WorkflowFailed.Failure.Details != nil {
		t.Fatalf("无 payload 写入面不应造出 details: %+v", event.WorkflowFailed.Failure)
	}
}

// 归一化只写拷贝：失败时调用方持有的 Event/Failure 原样不变（未 canonical 化、指针不变）。
func TestNormalizeEventFailuresLeavesCallerOwnedStateAlone(t *testing.T) {
	oversize := protocol.Failure{Message: "too many", Type: "Bulk", Details: oversizedPayloads()}
	original := &history.ActivityFailedEvent{ActivityID: "step", Error: "legacy text", Failure: &oversize}
	event := history.Event{ActivityFailed: original}
	originalFailure := original.Failure

	if _, err := NormalizeEventFailures(event); err == nil {
		t.Fatal("超限载荷必须失败")
	}
	if original.Failure != originalFailure {
		t.Fatal("失败路径不得替换调用方持有的 Failure 指针")
	}
	if len(original.Failure.Details) != len(oversize.Details) {
		t.Fatalf("失败路径不得改写调用方的 details: %d", len(original.Failure.Details))
	}
	if original.Error != "legacy text" {
		t.Fatalf("失败路径不得改写调用方的 Error: %q", original.Error)
	}

	// 成功路径同样只写拷贝，并且把 Error 镜像成 Failure.Message。
	failure := protocol.Failure{Message: "actual message", Type: "T"}
	caller := &history.ActivityFailedEvent{ActivityID: "step", Error: "stale text", Failure: &failure}
	normalized, err := NormalizeEventFailures(history.Event{ActivityFailed: caller})
	if err != nil {
		t.Fatalf("NormalizeEventFailures: %v", err)
	}
	if caller.Error != "stale text" || caller.Failure != &failure {
		t.Fatalf("调用方对象被改写: %+v", caller)
	}
	if normalized.ActivityFailed.Error != "actual message" {
		t.Fatalf("Error 投影未镜像 Failure.Message: %q", normalized.ActivityFailed.Error)
	}
}

// Child failure 事件与 Workflow 终态同形：Error 是 message 镜像，Failure 带完整结构。
func TestNewChildWorkflowFailedEventCarriesFailure(t *testing.T) {
	failure := protocol.Failure{
		Message: "child declined", Type: "ChildDeclined",
		Details: protocol.Payloads{{Encoding: protocol.PayloadEncodingJSONPlainV1, Data: []byte(`{"reason":"x"}`)}},
	}
	event := history.NewChildWorkflowFailedEvent("child-1", "run-child", 42, failure)
	failed := event.ChildWorkflowFailed
	if failed == nil {
		t.Fatal("缺少 ChildWorkflowFailed")
	}
	if failed.Error != failure.Message {
		t.Fatalf("Error = %q, want %q", failed.Error, failure.Message)
	}
	if failed.Failure == nil || failed.Failure.Type != "ChildDeclined" || len(failed.Failure.Details) != 1 {
		t.Fatalf("结构化失败丢失: %+v", failed.Failure)
	}
	if failed.ChildID != "child-1" || failed.ChildRunID != "run-child" || failed.ScheduledEventID != 42 {
		t.Fatalf("子流程标识丢失: %+v", failed)
	}
}

// ChildClose 是子终态写父历史的唯一入口：失败必须携带 Failure，缺失直接报协议错误。
func TestPropagateChildCloseCarriesFailure(t *testing.T) {
	failure := protocol.Failure{Message: "child declined", Type: "ChildDeclined", NonRetryable: true}
	event, status, err := childCloseEvent(ChildClose{
		Child: &Run{ID: "run-child", ParentRunID: "parent", ParentScheduledEventID: 9, ParentChildID: "child-1"},
		Type:  history.EventChildWorkflowFailed, Failure: &failure,
	})
	if err != nil {
		t.Fatalf("childCloseEvent: %v", err)
	}
	if event.ChildWorkflowFailed.Failure == nil || event.ChildWorkflowFailed.Failure.Type != "ChildDeclined" {
		t.Fatalf("ChildClose 丢弃了结构化失败: %+v", event.ChildWorkflowFailed)
	}
	if status != ChildWorkflowStatusEnums.Failed {
		t.Fatalf("status = %v, want failed", status)
	}

	if _, _, err := childCloseEvent(ChildClose{
		Child: &Run{ID: "run-child", ParentRunID: "parent", ParentScheduledEventID: 9, ParentChildID: "child-1"},
		Type:  history.EventChildWorkflowFailed,
	}); err == nil {
		t.Fatal("失败终态缺 Failure 必须报协议错误，不得用字符串补造")
	}
}
