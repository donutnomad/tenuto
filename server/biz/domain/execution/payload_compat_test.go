package execution_test

import (
	"reflect"
	"strings"
	"testing"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
)

// RecordedPayloadPositions 列出 History 里之后还会被 Program 解码的载荷位置：关闭的 Activity 只留结果，
// 未关闭的 Activity 留输入；带失败的 Activity 留应用错误 detail。
func TestRecordedPayloadPositions(t *testing.T) {
	args := protocol.Payloads{{Encoding: "json/plain-v1", Data: []byte(`{}`)}, {Encoding: "json/plain-v1", Data: []byte(`{}`)}}
	events := []history.Event{
		{ID: 1, WorkflowStarted: &history.WorkflowStartedEvent{Input: map[string]any{"order_id": "o-1"}}},
		{ID: 2, WorkflowSignaled: &history.WorkflowSignaledEvent{Name: "approve"}},
		{ID: 3, ActivityScheduled: &history.ActivityScheduledEvent{ActivityID: "#a1", ActivityType: "orders.charge"}},
		{ID: 4, ActivityCompleted: &history.ActivityCompletedEvent{ScheduledEventID: 3, ActivityType: "orders.charge"}},
		{ID: 5, ActivityScheduled: &history.ActivityScheduledEvent{ActivityID: "#a2", ActivityType: "orders.refund"}},
		{ID: 6, ActivityFailed: &history.ActivityFailedEvent{ScheduledEventID: 5, Failure: &protocol.Failure{Message: "declined"}}},
		{ID: 7, ActivityScheduled: &history.ActivityScheduledEvent{ActivityID: "#a3", ActivityType: "orders.ship"}},
		{ID: 8, ChildWorkflowStarted: &history.ChildWorkflowStartedEvent{Name: "child-flow", ChildID: "#c1"}},
		{ID: 9, ChildWorkflowCompleted: &history.ChildWorkflowCompletedEvent{ChildID: "#c1"}},
		{ID: 10, WorkflowUpdateAccepted: &history.WorkflowUpdateAcceptedEvent{Name: "approve-update", Args: &args}},
		{ID: 11, MarkerRecorded: &history.MarkerRecordedEvent{MarkerName: "SideEffect", MarkerID: "se1"}},
		{ID: 12, MarkerRecorded: &history.MarkerRecordedEvent{MarkerName: "MutableSideEffect", MarkerID: "feature_flag_2"}},
		{ID: 13, MarkerRecorded: &history.MarkerRecordedEvent{MarkerName: "GetVersion", MarkerID: "change"}},
	}
	got := execution.RecordedPayloadPositions(events)
	want := map[program.PayloadPosition]bool{
		{Kind: program.PayloadWorkflowInput}:                                    true,
		{Kind: program.PayloadSignal, Name: "approve"}:                          true,
		{Kind: program.PayloadActivityResult, Name: "orders.charge"}:            true,
		{Kind: program.PayloadActivityInput, Name: "orders.ship"}:               true,
		{Kind: program.PayloadChildResult, Name: "child-flow"}:                  true,
		{Kind: program.PayloadUpdateArgument, Name: "approve-update"}:           true,
		{Kind: program.PayloadUpdateArgument, Name: "approve-update", Index: 1}: true,
		{Kind: program.PayloadSideEffect}:                                       true,
		{Kind: program.PayloadMutableSideEffect, Name: "feature_flag"}:          true,
		{Kind: program.PayloadErrorDetail}:                                      true,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("RecordedPayloadPositions() = %v\nwant %v", got, want)
	}
}

// PayloadConflictError 只报 History 里出现过的不兼容位置。
func TestPayloadConflictError(t *testing.T) {
	incompatibilities := []program.PayloadIncompatibility{
		{
			Position: program.PayloadPosition{Kind: program.PayloadSignal, Name: "approve"}, Current: "order.Approval", Candidate: "order2.Approval",
			Issues: []protocol.WireIncompatibility{{Path: "order_id", Reason: "field is missing in the new type (removed or renamed)"}},
		},
		{
			Position: program.PayloadPosition{Kind: program.PayloadSignal, Name: "cancel"}, Current: "order.Cancel", Candidate: "order2.Cancel",
			Issues: []protocol.WireIncompatibility{{Path: "reason", Reason: "type changed from string to int64"}},
		},
	}
	approved := []history.Event{{ID: 1, WorkflowSignaled: &history.WorkflowSignaledEvent{Name: "approve"}}}
	err := execution.PayloadConflictError("v2", incompatibilities, approved)
	if err == nil {
		t.Fatal("conflict on a recorded signal was not reported")
	}
	for _, want := range []string{
		"auto upgrade to v2 refused",
		`signal "approve": order.Approval -> order2.Approval: order_id: field is missing in the new type (removed or renamed)`,
		"set policy pinned",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error %q does not contain %q", err, want)
		}
	}
	if strings.Contains(err.Error(), "cancel") {
		t.Fatalf("error %q reports a signal that is not in history", err)
	}
	if err := execution.PayloadConflictError("v2", incompatibilities, nil); err != nil {
		t.Fatalf("conflict without recorded payloads: %v", err)
	}
	if err := execution.PayloadConflictError("v2", nil, approved); err != nil {
		t.Fatalf("no incompatibilities: %v", err)
	}
}

// PayloadConflictError 报未确定的解码处：History 里有同种载荷（名字已知时同名）时报，种类未知时总报；消息与确定位置
// 的不兼容分开写。
func TestPayloadConflictErrorReportsUnresolvedSites(t *testing.T) {
	unresolved := func(position program.PayloadPosition) program.PayloadIncompatibility {
		return program.PayloadIncompatibility{Position: position, UnresolvedIn: "candidate", Unresolved: &program.UnresolvedPayloadSite{
			Position: position, Type: "order.Approval", Path: "program[3].expr.Get", Reason: "the signal name is not a constant",
		}}
	}
	signal := unresolved(program.PayloadPosition{Kind: program.PayloadSignal})
	started := history.Event{ID: 1, WorkflowStarted: &history.WorkflowStartedEvent{}}
	approved := history.Event{ID: 2, WorkflowSignaled: &history.WorkflowSignaledEvent{Name: "approve"}}

	err := execution.PayloadConflictError("v2", []program.PayloadIncompatibility{signal}, []history.Event{started, approved})
	want := "auto upgrade to v2 refused: the payload check cannot prove that payloads recorded in history stay readable: " +
		"candidate program: a signal with a non-constant name decoded into order.Approval at program[3].expr.Get (the signal name is not a constant); signal types differ between the two versions; set policy pinned"
	if err == nil || !strings.HasPrefix(err.Error(), want) {
		t.Fatalf("error = %v, want prefix %q", err, want)
	}
	if err := execution.PayloadConflictError("v2", []program.PayloadIncompatibility{signal}, []history.Event{started}); err != nil {
		t.Fatalf("no signal in history: %v", err)
	}
	named := unresolved(program.PayloadPosition{Kind: program.PayloadSignal, Name: "cancel"})
	if err := execution.PayloadConflictError("v2", []program.PayloadIncompatibility{named}, []history.Event{started, approved}); err != nil {
		t.Fatalf("only another signal in history: %v", err)
	}
	unknown := unresolved(program.PayloadPosition{})
	if err := execution.PayloadConflictError("v2", []program.PayloadIncompatibility{unknown}, []history.Event{started}); err == nil {
		t.Fatal("a site of unknown kind was not reported")
	}
}
