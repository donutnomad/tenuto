package interpreter

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/internal/executor"
	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
)

// ============================================
// replay 确定性校验端到端注入测试
// ============================================
// 测试策略:
// - Happy Path: 无漂移多批全形态不误报（T6）
// - Error Case: 换 type / 少调 / 多调 / 换序 各漂移形态被抓（T7-T10）
// - Edge Case: GetVersion 跳过、CAN 豁免、onError 跳过、update-handler run 整体跳过（T11-T14）
// 多批历史构造照 wlang_prefix_consistency_test.go 先例（WFTCompleted 为批界）。
// ============================================

func wftc(id int64) history.Event {
	return history.Event{ID: id, Timestamp: time.Now(), WorkflowTaskCompleted: &history.WorkflowTaskCompletedEvent{}}
}

func assertNonDeterministic(t *testing.T, err error, wantSub string) {
	t.Helper()
	var nd executor.NonDeterministicError
	if !errors.As(err, &nd) {
		t.Fatalf("应返回 nonDeterministicError, got %v", err)
	}
	if !strings.Contains(err.Error(), wantSub) {
		t.Fatalf("错误应含 %q, got %v", wantSub, err)
	}
}

const rmSingleActivitySrc = `package flows
import (
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/activities"
)
func W(ctx workflow.Context, input map[string]any) error {
	f := workflow.ExecuteActivity(ctx, activities.A, input)
	var out map[string]any
	e := f.Get(ctx, &out)
	if e != nil {
		return e
	}
	return nil
}
`

// Happy Path (T6): 无漂移多批全形态——activity + replay 批内已 fired 的 timer + SideEffect
// marker + child，四类 shadow 分支与历史命令事件全配对，不误报、正常完成。
func TestReplayMatch_NoDrift_AllGreen(t *testing.T) {
	const srcAll = `package flows
import (
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/activities"
)
func W(ctx workflow.Context, input map[string]any) error {
	f := workflow.ExecuteActivity(ctx, activities.A, input)
	var out map[string]any
	e := f.Get(ctx, &out)
	if e != nil {
		return e
	}
	err := workflow.Sleep(ctx, 1000000)
	if err != nil {
		return err
	}
	var cfg map[string]any
	err2 := workflow.SideEffect(ctx, func(ctx workflow.Context) any { return map[string]any{"region": "sg"} }).Get(ctx, &cfg)
	if err2 != nil {
		return err2
	}
	var child map[string]any
	err3 := workflow.ExecuteChildWorkflow(ctx, "child-wf", input).Get(ctx, &child)
	if err3 != nil {
		return err3
	}
	return nil
}
`
	// 模拟真实 server 多轮落库：每轮 WFTC + 该轮决策的命令事件（位置无关分类归其决策批）。
	started := history.NewWorkflowStartedEvent(map[string]any{}, "", "", "", "", 0)
	events := []history.Event{
		started,
		wftc(2),
		{ID: 3, Timestamp: time.Now(), ActivityScheduled: &history.ActivityScheduledEvent{ActivityID: "#a1", ActivityType: "activities.A"}},
		{ID: 4, Timestamp: time.Now(), ActivityCompleted: &history.ActivityCompletedEvent{ScheduledEventID: 3, Output: map[string]any{"ok": true}}},
		wftc(5),
		{ID: 6, Timestamp: time.Now(), TimerScheduled: &history.TimerScheduledEvent{TimerID: "#t1"}},
		{ID: 7, Timestamp: time.Now(), TimerFired: &history.TimerFiredEvent{TimerID: "#t1"}},
		wftc(8),
		{ID: 9, Timestamp: time.Now(), MarkerRecorded: &history.MarkerRecordedEvent{MarkerName: "SideEffect", MarkerID: "#se1", Value: map[string]any{"value": map[string]any{"region": "sg"}}}},
		{ID: 10, Timestamp: time.Now(), ChildWorkflowStarted: &history.ChildWorkflowStartedEvent{ChildID: "#c1", Name: "child-wf"}},
		{ID: 11, Timestamp: time.Now(), ChildWorkflowCompleted: &history.ChildWorkflowCompletedEvent{ChildID: "#c1", Output: map[string]any{"done": true}}},
	}
	cmds, err := probeExecFix(t, srcAll, events)
	if err != nil {
		t.Fatalf("无漂移多批不应误报, got %v", err)
	}
	if !hasCompleteWorkflow(cmds) {
		t.Fatalf("应正常完成, got %+v", cmds)
	}
	t.Logf("✅ 全形态 shadow 与历史配对，无误报")
}

// Error Case (T7): 历史 scheduled(#a1, activities.A)，代码改调 activities.B → 换 type 漂移被抓。
func TestReplayMatch_ActivityTypeDrift_FailsWorkflow(t *testing.T) {
	const srcCallsB = `package flows
import (
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/activities"
)
func W(ctx workflow.Context, input map[string]any) error {
	f := workflow.ExecuteActivity(ctx, activities.B, input)
	var out map[string]any
	e := f.Get(ctx, &out)
	if e != nil {
		return e
	}
	return nil
}
`
	started := history.NewWorkflowStartedEvent(map[string]any{}, "", "", "", "", 0)
	sched := history.Event{ID: 2, Timestamp: time.Now(), ActivityScheduled: &history.ActivityScheduledEvent{ActivityID: "#a1", ActivityType: "activities.A"}}
	comp := history.Event{ID: 3, Timestamp: time.Now(), ActivityCompleted: &history.ActivityCompletedEvent{ScheduledEventID: 2, Output: map[string]any{"ok": true}}}

	_, err := probeExecFix(t, srcCallsB, []history.Event{started, sched, comp, wftc(4)})
	assertNonDeterministic(t, err, "history event is")
}

// Error Case (T8): 历史两个 scheduled，代码只调一个 → 少调漂移（missing replay command）。
func TestReplayMatch_RemovedCall_Detected(t *testing.T) {
	started := history.NewWorkflowStartedEvent(map[string]any{}, "", "", "", "", 0)
	sched1 := history.Event{ID: 2, Timestamp: time.Now(), ActivityScheduled: &history.ActivityScheduledEvent{ActivityID: "#a1", ActivityType: "activities.A"}}
	comp1 := history.Event{ID: 3, Timestamp: time.Now(), ActivityCompleted: &history.ActivityCompletedEvent{ScheduledEventID: 2, Output: map[string]any{"ok": true}}}
	sched2 := history.Event{ID: 4, Timestamp: time.Now(), ActivityScheduled: &history.ActivityScheduledEvent{ActivityID: "#a2", ActivityType: "activities.B"}}
	comp2 := history.Event{ID: 5, Timestamp: time.Now(), ActivityCompleted: &history.ActivityCompletedEvent{ScheduledEventID: 4, Output: map[string]any{"ok": true}}}

	// rmSingleActivitySrc 只调 activities.A（#a1）——历史却记录了两个调度。
	_, err := probeExecFix(t, rmSingleActivitySrc, []history.Event{started, sched1, comp1, sched2, comp2, wftc(6)})
	assertNonDeterministic(t, err, "missing replay command")
}

// Error Case (T9): 代码多调一个 → 多调漂移（extra replay command），且新命令不得混入上报通道
// （锚定分流修复：replay 批多调不再静默重复调度）。
func TestReplayMatch_AddedCall_Detected(t *testing.T) {
	const srcCallsTwo = `package flows
import (
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/activities"
)
func W(ctx workflow.Context, input map[string]any) error {
	f1 := workflow.ExecuteActivity(ctx, activities.A, input)
	var out map[string]any
	e1 := f1.Get(ctx, &out)
	if e1 != nil {
		return e1
	}
	f2 := workflow.ExecuteActivity(ctx, activities.B, input)
	var out2 map[string]any
	e2 := f2.Get(ctx, &out2)
	if e2 != nil {
		return e2
	}
	return nil
}
`
	started := history.NewWorkflowStartedEvent(map[string]any{}, "", "", "", "", 0)
	sched1 := history.Event{ID: 2, Timestamp: time.Now(), ActivityScheduled: &history.ActivityScheduledEvent{ActivityID: "#a1", ActivityType: "activities.A"}}
	comp1 := history.Event{ID: 3, Timestamp: time.Now(), ActivityCompleted: &history.ActivityCompletedEvent{ScheduledEventID: 2, Output: map[string]any{"ok": true}}}

	cmds, err := probeExecFix(t, srcCallsTwo, []history.Event{started, sched1, comp1, wftc(4)})
	assertNonDeterministic(t, err, "extra replay command")
	for _, c := range cmds {
		if c.Type == history.CmdScheduleActivity && c.ActivityID == "#a2" {
			t.Fatalf("replay 批多调的命令不得混入上报通道, got %+v", cmds)
		}
	}
}

// Error Case (T10): #a1/#a2 两调用类型对调 → 换序漂移（同位失配）。
func TestReplayMatch_ReorderedCalls_Detected(t *testing.T) {
	const srcSwapped = `package flows
import (
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/activities"
)
func W(ctx workflow.Context, input map[string]any) error {
	f1 := workflow.ExecuteActivity(ctx, activities.B, input)
	var out map[string]any
	e1 := f1.Get(ctx, &out)
	if e1 != nil {
		return e1
	}
	f2 := workflow.ExecuteActivity(ctx, activities.A, input)
	var out2 map[string]any
	e2 := f2.Get(ctx, &out2)
	if e2 != nil {
		return e2
	}
	return nil
}
`
	started := history.NewWorkflowStartedEvent(map[string]any{}, "", "", "", "", 0)
	sched1 := history.Event{ID: 2, Timestamp: time.Now(), ActivityScheduled: &history.ActivityScheduledEvent{ActivityID: "#a1", ActivityType: "activities.A"}}
	comp1 := history.Event{ID: 3, Timestamp: time.Now(), ActivityCompleted: &history.ActivityCompletedEvent{ScheduledEventID: 2, Output: map[string]any{"ok": true}}}
	sched2 := history.Event{ID: 4, Timestamp: time.Now(), ActivityScheduled: &history.ActivityScheduledEvent{ActivityID: "#a2", ActivityType: "activities.B"}}
	comp2 := history.Event{ID: 5, Timestamp: time.Now(), ActivityCompleted: &history.ActivityCompletedEvent{ScheduledEventID: 4, Output: map[string]any{"ok": true}}}

	_, err := probeExecFix(t, srcSwapped, []history.Event{started, sched1, comp1, sched2, comp2, wftc(6)})
	assertNonDeterministic(t, err, "history event is")
}

// Edge Case (T11): 含 GetVersion 的多批历史——version marker 双侧跳过，不误报。
func TestReplayMatch_GetVersion_Skipped(t *testing.T) {
	const srcGV = `package flows
import (
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/activities"
)
func W(ctx workflow.Context, input map[string]any) error {
	v := workflow.GetVersion(ctx, "change-1", 1, 2)
	if v < 0 {
		return nil
	}
	f := workflow.ExecuteActivity(ctx, activities.A, input)
	var out map[string]any
	e := f.Get(ctx, &out)
	if e != nil {
		return e
	}
	return nil
}
`
	started := history.NewWorkflowStartedEvent(map[string]any{}, "", "", "", "", 0)
	events := []history.Event{
		started,
		wftc(2),
		{ID: 3, Timestamp: time.Now(), MarkerRecorded: &history.MarkerRecordedEvent{MarkerName: "GetVersion", MarkerID: "change-1", Value: map[string]any{"version": int64(2)}}},
		{ID: 4, Timestamp: time.Now(), ActivityScheduled: &history.ActivityScheduledEvent{ActivityID: "#a1", ActivityType: "activities.A"}},
		{ID: 5, Timestamp: time.Now(), ActivityCompleted: &history.ActivityCompletedEvent{ScheduledEventID: 4, Output: map[string]any{"ok": true}}},
	}
	cmds, err := probeExecFix(t, srcGV, events)
	if err != nil {
		t.Fatalf("GetVersion marker 双侧跳过不应误报, got %v", err)
	}
	if !hasCompleteWorkflow(cmds) {
		t.Fatalf("应正常完成, got %+v", cmds)
	}
}

// Edge Case (T12): ContinueAsNew 在 replay 批产生 → CAN 豁免触发、比对整体跳过、不误报。
// （「重放已完成 run」的 Complete/Fail/Cancel 形态在 Execute 入口被 terminated 短路到不了
// matcher；CAN 不置 terminated，是唯一真实可达的豁免路径。）
func TestReplayMatch_CANInReplay_Exempt(t *testing.T) {
	const srcCAN = `package flows
import (
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
)
func W(ctx workflow.Context, input map[string]any) error {
	workflow.ContinueAsNew(ctx, "wf-next", input)
	return nil
}
`
	started := history.NewWorkflowStartedEvent(map[string]any{}, "", "", "", "", 0)
	// 历史含 WFTC 分界 → 批1 replay。replay 批中代码即跑到 CAN（进 replayCommands）——
	// 历史命令事件与 replayCommands 天然不对称（CAN 命令无对应命令事件），若无豁免必误报 extra。
	_, err := probeExecFix(t, srcCAN, []history.Event{started, wftc(2)})
	if err != nil {
		t.Fatalf("CAN-in-replay 应豁免比对, got %v", err)
	}
}

// Happy Path (T7，取代旧 T14 整体豁免): update 命令纳入比对后不误报——handler 协程在 replay 批
// 照常重放：R 类 marker 读回（键 update:<id>#ru1 专属桶）+ shadow
// RecordMarker + Complete 命令进 replayCommands，与历史 MarkerRecorded/WorkflowUpdateCompleted
// 配对；live 批新 update 的 handler 正常执行上报。旧的 hasUpdateHandlers 整体豁免已删除。
func TestReplayMatch_UpdateHandler_RunSkipped(t *testing.T) {
	handlerFn := map[string]any{
		"fn": map[string]any{
			"params": []any{
				[]any{"ctx", "workflow.Context"},
				[]any{"in", "map<string,any>"},
			},
			"returns": []any{"map<string,any>", "error"},
			"do": []any{
				map[string]any{"let": map[string]any{"u": map[string]any{"RandomUUID": []any{
					map[string]any{"pkg": "workflow"},
					map[string]any{"var": "ctx"},
				}}}},
				// 空 map 字面量编码为 {}，与历史的 Success 载荷一致（nil map 编码为 null）。
				map[string]any{"let": map[string]any{"out": map[string]any{"map": map[string]any{"type": []any{"string", "any"}, "entries": []any{}}}}},
				handlerReturn(map[string]any{"var": "out"}),
			},
		},
	}
	nodes := &program.WorkflowConfig{
		Type: "handler-uuid",
		Program: program.NewWLangProgram([]any{
			map[string]any{"expr": map[string]any{"SetUpdateHandler": []any{
				map[string]any{"pkg": "workflow"},
				map[string]any{"var": "ctx"},
				map[string]any{"literal": map[string]any{"type": "string", "value": "gen"}},
				handlerFn,
			}}},
		}),
	}

	// 轮 1（live 当年）：u-1 的 handler 产 RandomUUID marker（键 update:u-1#ru1）+ CompleteUpdate，
	// 命令随 WFTC₁ 落库归批 1。轮 2 历史：u-2 在批 2（live）新到。
	// replay 批 1：u-1 handler 重放——marker 读回 + shadow + Complete 进 replayCommands 与历史配对；
	// live 批 2：u-2 handler 首见 RandomUUID 合法记新值 + Complete 上报。
	started := history.NewWorkflowStartedEvent(nil, "", "", "", "", 0)
	emptyPayload, err := protocol.NewJSONPayload(map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	events := []history.Event{
		started,
		acceptedEvent(2, "gen", "u-1", map[string]any{"id": "x"}),
		wftc(3),
		{ID: 4, Timestamp: time.Now(), MarkerRecorded: &history.MarkerRecordedEvent{MarkerName: "RandomUUID", MarkerID: "update:u-1#ru1", Value: map[string]any{"value": "uuid-1"}}},
		{ID: 5, Timestamp: time.Now(), WorkflowUpdateCompleted: &history.WorkflowUpdateCompletedEvent{Name: "gen", UpdateID: "u-1", Outcome: &protocol.UpdateOutcome{Success: &emptyPayload}}},
		acceptedEvent(6, "gen", "u-2", map[string]any{"id": "y"}),
	}
	cmds, err := NewWorkflowExecutor(nodes, events).Execute()
	if err != nil {
		t.Fatalf("update 命令纳入比对后正常配对不应误报, got %v", err)
	}
	// live 侧锚：u-2 正常完成上报（u-1 的 Complete 在 replayCommands，不出现在 commands）。
	var sawU2 bool
	for _, c := range cmds {
		if c.Type == history.CmdCompleteWorkflowUpdate {
			if c.UpdateID == "u-1" {
				t.Fatalf("replay 批的 u-1 Complete 不得上报, got %+v", cmds)
			}
			if c.UpdateID == "u-2" {
				sawU2 = true
			}
		}
	}
	if !sawU2 {
		t.Fatalf("u-2 应被 handler 协程完成, got %+v", cmds)
	}
}
