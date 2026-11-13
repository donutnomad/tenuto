package history

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol"
)

func TestActivityScheduledEvent_JSONRoundTrip(t *testing.T) {
	// Happy Path: 带 timeout 和 retry policy 的事件序列化后能完整反序列化
	ev := NewActivityScheduledEvent(ActivityScheduledEvent{
		ActivityID: "step-1", ActivityType: "custodian.Reserve", TaskQueue: "default",
		StartToCloseTimeoutMs: 300000, HeartbeatTimeoutMs: 10000,
		RetryPolicy: &RetryPolicy{MaxAttempts: 3, InitialIntervalMs: 1000, BackoffCoefficient: 2, MaxIntervalMs: 100000},
	})
	data, err := json.Marshal(ev)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got Event
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.ActivityScheduled == nil {
		t.Fatal("activityScheduled nil after round-trip")
	}
	if got.ActivityScheduled.StartToCloseTimeoutMs != 300000 {
		t.Errorf("StartToClose = %d, want 300000", got.ActivityScheduled.StartToCloseTimeoutMs)
	}
	if got.ActivityScheduled.RetryPolicy == nil || got.ActivityScheduled.RetryPolicy.MaxAttempts != 3 {
		t.Errorf("RetryPolicy not preserved: %+v", got.ActivityScheduled.RetryPolicy)
	}
}

func TestActivityResetEvent_JSONRoundTrip(t *testing.T) {
	// Happy Path: ActivityReset 事件序列化后字段完整保留，EventType 正确
	ev := NewActivityResetEvent(ActivityResetEvent{
		ActivityID: "settle", ScheduledEventID: 114, Attempt: 3, Identity: "user-1", Reason: "manual retry", ResetHeartbeat: true,
	})
	if ev.EventType() != EventActivityReset {
		t.Fatalf("EventType = %s, want ActivityReset", ev.EventType())
	}
	data, err := json.Marshal(ev)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got Event
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.ActivityReset == nil || got.ActivityReset.ScheduledEventID != 114 || got.ActivityReset.Attempt != 3 || !got.ActivityReset.ResetHeartbeat {
		t.Errorf("ActivityReset not preserved: %+v", got.ActivityReset)
	}
}

func TestWorkflowUpdateEventJSONRoundTrip(t *testing.T) {
	arg, err := protocol.NewJSONPayload(json.RawMessage(`{"record_id":"r-1"}`))
	if err != nil {
		t.Fatal(err)
	}
	result, err := protocol.NewJSONPayload(json.RawMessage(`18446744073709551615`))
	if err != nil {
		t.Fatal(err)
	}
	events := []Event{
		NewWorkflowUpdateAcceptedEvent("create", "u-1", Payloads{arg}),
		NewWorkflowUpdateCompletedOutcomeEvent("create", "u-1", UpdateOutcome{Success: &result}),
	}

	raw, err := json.Marshal(events)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got []Event
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(got) != 2 || got[0].WorkflowUpdateAccepted.Args == nil || len(*got[0].WorkflowUpdateAccepted.Args) != 1 {
		t.Fatalf("accepted args missing: %+v", got)
	}
	if outcome := got[1].WorkflowUpdateCompleted.Outcome; outcome == nil || outcome.Success == nil || string(outcome.Success.Data) != "18446744073709551615" {
		t.Fatalf("completed outcome missing: %+v", got[1])
	}
}

func TestWorkflowUpdateZeroArgsPreservesWirePresence(t *testing.T) {
	event := NewWorkflowUpdateAcceptedEvent("create", "u-zero", nil)
	raw, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	var got Event
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	if got.WorkflowUpdateAccepted == nil || got.WorkflowUpdateAccepted.Args == nil || len(*got.WorkflowUpdateAccepted.Args) != 0 {
		t.Fatalf("zero args lost presence: json=%s event=%+v", raw, got)
	}
}

func TestToRetryPolicy(t *testing.T) {
	// Edge Case: nil 输入返回 nil
	if ToRetryPolicy(nil) != nil {
		t.Error("ToRetryPolicy(nil) should be nil")
	}
	// Happy Path: Duration 正确转毫秒，BackoffCoefficient=0 不丢失
	got := ToRetryPolicy(&protocol.ParsedRetryPolicy{
		MaxAttempts: 5, InitialInterval: time.Second, BackoffCoefficient: 0, MaxInterval: 100 * time.Second,
	})
	if got.InitialIntervalMs != 1000 {
		t.Errorf("InitialIntervalMs = %d, want 1000", got.InitialIntervalMs)
	}
	if got.MaxIntervalMs != 100000 {
		t.Errorf("MaxIntervalMs = %d, want 100000", got.MaxIntervalMs)
	}
	if got.BackoffCoefficient != 0 {
		t.Errorf("BackoffCoefficient = %v, want 0", got.BackoffCoefficient)
	}
}

// 取消事件的去重维度：每个 Activity 只有一个关闭事件（完成、失败、取消共用），Timer 与子 Workflow 的取消命令事件按目标去重。
func TestCancellationEventDedupeKeys(t *testing.T) {
	cases := []struct {
		event Event
		want  string
	}{
		{NewActivityCancelledEvent(ActivityCancelledEvent{ScheduledEventID: 7}), "activity_closed:7"},
		{NewActivityCompletedEvent(ActivityCompletedEvent{ScheduledEventID: 7}), "activity_closed:7"},
		{NewTimerCancelledEvent("#t1", 9), "timer_cancelled:#t1"},
		{NewChildWorkflowCancelRequestedEvent("#c1", "run-child", 11), "child_workflow_cancel_requested:#c1"},
	}
	for _, tc := range cases {
		got, ok := DedupeKey(tc.event)
		if !ok || got != tc.want {
			t.Errorf("DedupeKey(%s) = %q, %v; want %q", tc.event.EventType(), got, ok, tc.want)
		}
	}
	// ActivityID 关闭后可以再用：同一 ID 的调度与取消请求不去重。
	for _, event := range []Event{
		NewActivityScheduledEvent(ActivityScheduledEvent{ActivityID: "charge"}),
		NewActivityCancelRequestedEvent("charge", 7),
	} {
		if key, ok := DedupeKey(event); ok {
			t.Errorf("DedupeKey(%s) = %q, want no key", event.EventType(), key)
		}
	}
}
