package engine

import (
	"strings"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/internal/executor"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
)

// ============================================
// matchReplayWithHistory 比对器单测（表驱动）
// ============================================
// 测试策略:
// - Happy Path: 四类命令与事件全匹配 → nil
// - Error Case: 换 type / 历史多（missing）/ 命令多（extra）
// - Edge Case: 跳过表（version marker 双侧、终态事件、update 系）
// ============================================

func cmdSched(stepID, activityType string) history.Command {
	return history.Command{Type: history.CmdScheduleActivity, StartToCloseTimeout: time.Minute, ActivityID: stepID, ActivityType: activityType}
}

func evSchedM(stepID, activityType string) history.Event { // 与 batches_test.go 的 evSched(int64) 区分

	return history.Event{Timestamp: time.Now(), ActivityScheduled: &history.ActivityScheduledEvent{ActivityID: stepID, ActivityType: activityType}}
}

// Happy Path: 四类命令各一与对应事件顺序全匹配 → nil。
func TestMatchReplay_AllMatch(t *testing.T) {
	commands := []history.Command{
		cmdSched("#a1", "activities.A"),
		{Type: history.CmdScheduleTimer, TimerID: "#t1"},
		{Type: history.CmdStartChildWorkflow, ChildID: "#c1", WorkflowType: "child-wf"},
		{Type: history.CmdRecordMarker, MarkerName: "SideEffect", MarkerID: "se1"},
	}
	events := []history.Event{
		evSchedM("#a1", "activities.A"),
		{Timestamp: time.Now(), TimerScheduled: &history.TimerScheduledEvent{TimerID: "#t1"}},
		{Timestamp: time.Now(), ChildWorkflowStarted: &history.ChildWorkflowStartedEvent{ChildID: "#c1", Name: "child-wf"}},
		{Timestamp: time.Now(), MarkerRecorded: &history.MarkerRecordedEvent{MarkerName: "SideEffect", MarkerID: "se1"}},
	}
	if err := executor.MatchReplayWithHistory(commands, events); err != nil {
		t.Fatalf("全匹配应返回 nil, got %v", err)
	}
}

// Error Case: 同 StepID 不同 ActivityType → mismatch 错误。
func TestMatchReplay_ActivityTypeMismatch(t *testing.T) {
	commands := []history.Command{cmdSched("#a1", "activities.B")}
	events := []history.Event{evSchedM("#a1", "activities.A")}
	err := executor.MatchReplayWithHistory(commands, events)
	if err == nil || !strings.Contains(err.Error(), "history event is") {
		t.Fatalf("换 type 应报 mismatch(含 history event is), got %v", err)
	}
}

// Error Case: 历史多一个 scheduled（代码少调）→ missing replay command。
func TestMatchReplay_MissingCommand(t *testing.T) {
	commands := []history.Command{cmdSched("#a1", "activities.A")}
	events := []history.Event{evSchedM("#a1", "activities.A"), evSchedM("#a2", "activities.B")}
	err := executor.MatchReplayWithHistory(commands, events)
	if err == nil || !strings.Contains(err.Error(), "missing replay command") {
		t.Fatalf("历史多应报 missing replay command, got %v", err)
	}
}

// Error Case: 命令多一个（代码多调）→ extra replay command。
func TestMatchReplay_ExtraCommand(t *testing.T) {
	commands := []history.Command{cmdSched("#a1", "activities.A"), cmdSched("#a2", "activities.B")}
	events := []history.Event{evSchedM("#a1", "activities.A")}
	err := executor.MatchReplayWithHistory(commands, events)
	if err == nil || !strings.Contains(err.Error(), "extra replay command") {
		t.Fatalf("命令多应报 extra replay command, got %v", err)
	}
}

// Edge Case: 跳过表——version marker（双侧）、终态事件不参与
// 比对；update 命令已收紧为配对项（handler 协程 replay 批照常重放，
// Complete 命令与历史 WorkflowUpdateCompleted 按 UpdateID+Name 配对）。
func TestMatchReplay_SkipTable(t *testing.T) {
	commands := []history.Command{
		{Type: history.CmdRecordMarker, MarkerName: "GetVersion", MarkerID: "change-1"}, // 命令侧跳过
		cmdSched("#a1", "activities.A"),
		{Type: history.CmdCompleteWorkflowUpdate, Name: "u", UpdateID: "u1"}, // 配对项（收紧后不再跳过）
	}
	events := []history.Event{
		{Timestamp: time.Now(), MarkerRecorded: &history.MarkerRecordedEvent{MarkerName: "GetVersion", MarkerID: "change-1"}}, // 事件侧跳过
		evSchedM("#a1", "activities.A"),
		{Timestamp: time.Now(), WorkflowUpdateCompleted: &history.WorkflowUpdateCompletedEvent{Name: "u", UpdateID: "u1"}}, // 与命令侧配对
		{Timestamp: time.Now(), WorkflowCompleted: &history.WorkflowCompletedEvent{}},                                      // 终态跳过
	}
	if err := executor.MatchReplayWithHistory(commands, events); err != nil {
		t.Fatalf("跳过表项不参与比对、update 命令按 UpdateID 配对, got %v", err)
	}
	// update 配对失配要被抓住（UpdateID 漂移）。
	drifted := []history.Command{{Type: history.CmdCompleteWorkflowUpdate, Name: "u", UpdateID: "u-other"}}
	driftEvents := []history.Event{{Timestamp: time.Now(), WorkflowUpdateCompleted: &history.WorkflowUpdateCompletedEvent{Name: "u", UpdateID: "u1"}}}
	if err := executor.MatchReplayWithHistory(drifted, driftEvents); err == nil {
		t.Fatalf("UpdateID 失配应报非确定性")
	}
}

func TestMatchReplay_V2UpdateComparesCanonicalOutcome(t *testing.T) {
	replayPayload := protocol.Payload{Encoding: protocol.PayloadEncodingJSONPlainV1, Data: []byte(`{"b":2,"a":1}`)}
	historyPayload := protocol.Payload{Encoding: protocol.PayloadEncodingJSONPlainV1, Data: []byte("{\n  \"a\": 1, \"b\": 2\n}")}
	commands := []history.Command{{
		Type: history.CmdCompleteWorkflowUpdate, Name: "create", UpdateID: "u-v2",
		UpdateOutcome: &protocol.UpdateOutcome{Success: &replayPayload},
	}}
	events := []history.Event{{WorkflowUpdateCompleted: &history.WorkflowUpdateCompletedEvent{
		Name: "create", UpdateID: "u-v2", Outcome: &protocol.UpdateOutcome{Success: &historyPayload},
	}}}
	if err := executor.MatchReplayWithHistory(commands, events); err != nil {
		t.Fatalf("canonical outcomes should match: %v", err)
	}

	driftPayload, err := protocol.NewJSONPayload(map[string]any{"a": 1, "b": 3})
	if err != nil {
		t.Fatal(err)
	}
	events[0].WorkflowUpdateCompleted.Outcome = &protocol.UpdateOutcome{Success: &driftPayload}
	if err := executor.MatchReplayWithHistory(commands, events); err == nil {
		t.Fatal("v2 result drift must fail replay")
	}
}

func TestMatchReplay_V2UpdateComparesFailureDetails(t *testing.T) {
	detail, err := protocol.NewJSONPayload(map[string]any{"record_id": "r-1"})
	if err != nil {
		t.Fatal(err)
	}
	failure := protocol.Failure{Message: "failed", Type: "CreateError", Details: protocol.Payloads{detail}}
	historyFailure := failure
	commands := []history.Command{{
		Type: history.CmdCompleteWorkflowUpdate, Name: "create", UpdateID: "u-fail",
		UpdateOutcome: &protocol.UpdateOutcome{Failure: &failure},
	}}
	events := []history.Event{{WorkflowUpdateCompleted: &history.WorkflowUpdateCompletedEvent{
		Name: "create", UpdateID: "u-fail", Outcome: &protocol.UpdateOutcome{Failure: &historyFailure},
	}}}
	if err := executor.MatchReplayWithHistory(commands, events); err != nil {
		t.Fatalf("same failure should match: %v", err)
	}
	events[0].WorkflowUpdateCompleted.Outcome.Failure.Type = "OtherError"
	if err := executor.MatchReplayWithHistory(commands, events); err == nil {
		t.Fatal("failure type drift must fail replay")
	}
}
