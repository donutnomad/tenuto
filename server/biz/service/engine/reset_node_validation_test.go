package engine

import (
	"strings"
	"testing"

	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
)

// resolveResetNodeID 的 reset 锚点校验：reset 点必须落在 WorkflowTask 关闭事件（WFT 边界）上，
// 否则 fork+截断 history 会产生半截 task / 孤儿事件，破坏重放一致性。这里直接喂构造好的 events
// 走纯函数路径，覆盖合法边界 / 非边界被拒 / 超界被拒。

func eventWithID(id int64, payload history.Event) history.Event {
	payload.ID = id
	return payload
}

// resetTestEvents：一个 activity 在 WFT 中被调度的典型 history。
//
//	1 WorkflowTaskScheduled
//	2 WorkflowTaskStarted
//	3 WorkflowTaskCompleted（首个 WFT 关闭，合法 reset 点）
//	4 ActivityScheduled（非边界）
//	5 ActivityStarted（非边界）
//	6 ActivityCompleted（非边界）
//	7 WorkflowTaskScheduled
//	8 WorkflowTaskStarted
//	9 WorkflowTaskCompleted（第二个 WFT 关闭，合法 reset 点）
func resetTestEvents() []history.Event {
	return []history.Event{
		eventWithID(1, history.NewWorkflowTaskScheduledEvent(0)),
		eventWithID(2, history.NewWorkflowTaskStartedEvent(1, "w", "b", 0)),
		eventWithID(3, history.NewWorkflowTaskCompletedEvent(1, "")),
		eventWithID(4, history.NewActivityScheduledEvent(history.ActivityScheduledEvent{ActivityID: "a", ActivityType: "T"})),
		eventWithID(5, history.Event{ActivityStarted: &history.ActivityStartedEvent{ScheduledEventID: 4}}),
		eventWithID(6, history.Event{ActivityCompleted: &history.ActivityCompletedEvent{ScheduledEventID: 4}}),
		eventWithID(7, history.NewWorkflowTaskScheduledEvent(0)),
		eventWithID(8, history.NewWorkflowTaskStartedEvent(7, "w", "b", 0)),
		eventWithID(9, history.NewWorkflowTaskCompletedEvent(7, "")),
	}
}

func TestResolveResetNodeID_WFTCloseAccepted(t *testing.T) {
	// Happy Path: reset 到 WorkflowTaskCompleted（WFT 边界），返回该轮 Started 截断点
	events := resetTestEvents()

	for _, id := range []int64{3, 9} {
		got, err := execution.ResolveResetNodeID(events, execution.ResetWorkflowRequest{ResetEventID: id})
		if err != nil {
			t.Fatalf("ResetEventID=%d expected accepted, got error: %v", id, err)
		}
		if got != id-1 {
			t.Fatalf("ResetEventID=%d expected node id=%d, got %d", id, id-1, got)
		}
	}
}

func TestResolveResetNodeID_NonBoundaryRejected(t *testing.T) {
	// Error Case: reset 到非 WFT 边界事件（ActivityScheduled/Started/Completed）应被拒绝
	events := resetTestEvents()

	for _, id := range []int64{4, 5, 6} {
		_, err := execution.ResolveResetNodeID(events, execution.ResetWorkflowRequest{ResetEventID: id})
		if err == nil {
			t.Fatalf("ResetEventID=%d (non-boundary) expected rejection, got nil", id)
		}
		if !strings.Contains(err.Error(), "not a valid reset point") {
			t.Fatalf("ResetEventID=%d expected 'not a valid reset point' error, got: %v", id, err)
		}
	}
}

func TestResolveResetNodeID_ExceedsMaxRejected(t *testing.T) {
	// Error Case: reset_event_id 超过最大 event id 应被拒绝
	events := resetTestEvents()

	_, err := execution.ResolveResetNodeID(events, execution.ResetWorkflowRequest{ResetEventID: 99})
	if err == nil {
		t.Fatal("ResetEventID=99 (exceeds max) expected rejection, got nil")
	}
	if !strings.Contains(err.Error(), "exceeds max event id") {
		t.Fatalf("expected 'exceeds max event id' error, got: %v", err)
	}
}

// startedAnchorEvents：一个含完整 WFT 生命周期的 history，用于验证 Started 锚点解析。
//
//	1 WorkflowTaskScheduled
//	2 WorkflowTaskStarted   (ScheduledEventID=1)
//	3 WorkflowTaskCompleted (ScheduledEventID=1)   ← WFT-1 的 close
//	4 ActivityScheduled
//	5 WorkflowTaskScheduled
//	6 WorkflowTaskStarted   (ScheduledEventID=5)   ← pending WFT（无 close）
func startedAnchorEvents() []history.Event {
	return []history.Event{
		eventWithID(1, history.NewWorkflowTaskScheduledEvent(0)),
		eventWithID(2, history.NewWorkflowTaskStartedEvent(1, "w", "b", 0)),
		eventWithID(3, history.NewWorkflowTaskCompletedEvent(1, "")),
		eventWithID(4, history.NewActivityScheduledEvent(history.ActivityScheduledEvent{ActivityID: "a", ActivityType: "T"})),
		eventWithID(5, history.NewWorkflowTaskScheduledEvent(0)),
		eventWithID(6, history.NewWorkflowTaskStartedEvent(5, "w", "b", 0)),
	}
}

func TestResolveResetNodeID_StartedAnchor_ResolvesToCloseBoundary(t *testing.T) {
	// Happy Path: 锚点 Started(2) 所属 WFT 有 close 事件 Completed(3) → forkNodeID 取 2，
	// 与直接传 Completed(3) 严格等价
	events := startedAnchorEvents()

	gotStarted, err := execution.ResolveResetNodeID(events, execution.ResetWorkflowRequest{ResetEventID: 2})
	if err != nil {
		t.Fatalf("ResetEventID=2 (Started) expected accepted, got error: %v", err)
	}
	gotCompleted, err := execution.ResolveResetNodeID(events, execution.ResetWorkflowRequest{ResetEventID: 3})
	if err != nil {
		t.Fatalf("ResetEventID=3 (Completed) expected accepted, got error: %v", err)
	}
	if gotStarted != gotCompleted {
		t.Fatalf("Started(2) and Completed(3) must resolve to same forkNodeID, got %d vs %d", gotStarted, gotCompleted)
	}
	if gotStarted != 2 {
		t.Fatalf("Started(2) expected forkNodeID=2 (its WFT start), got %d", gotStarted)
	}
}

func TestResolveResetNodeID_StartedAnchor_PendingWFT_ResolvesToSelf(t *testing.T) {
	// Edge Case: 锚点 Started(6) 是 pending WFT（无 close）→ forkNodeID 取自身 6
	events := startedAnchorEvents()

	got, err := execution.ResolveResetNodeID(events, execution.ResetWorkflowRequest{ResetEventID: 6})
	if err != nil {
		t.Fatalf("ResetEventID=6 (pending Started) expected accepted, got error: %v", err)
	}
	if got != 6 {
		t.Fatalf("pending Started(6) expected forkNodeID=6 (self), got %d", got)
	}
}
