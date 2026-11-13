package interpreter

import (
	"strings"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/internal/executor/coroutine"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
)

// ============================================
// Selector AddSignalFuture / AddChildFuture 端到端测试
//
// ============================================
// 测试策略:
// - T-E1 Happy Path: AddSignalFuture signal 已到 → 命中、回调内 Get 取值
// - T-E2 Happy Path: 跨批 signal → 批 k 未到、批 k+1 到达 → flush 桥唤醒命中（前缀一致性）
// - T-E3 Happy/Error: AddChildFuture child 完成 → 取结果；child 失败 → 回调内 Get 返回错误
// - T-E4 Edge Case:   child 发起上移——ExecuteChildWorkflow 后只 AddChildFuture 不 Get →
//                     CmdStartChildWorkflow 照发（发起上移锚点）
// - T-E6 Edge Case:   混合 selector（activity + signal + child）声明序与竞争护栏
// - T-E7 Error Case:  AddFuture 传 SideEffectFuture host 值 → evalErr
// - T-E8 Edge Case:   含 child 的多批历史 replay 无误报（shadow 上移后配对不变）
// ============================================

// selSignalSrc: WaitSignal + AddSignalFuture，回调内 Get 取 signal 值写外层变量。
const selSignalSrc = `package flows
import (
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
)
func W(ctx workflow.Context, input map[string]any) map[string]any {
	sf := workflow.WaitSignal(ctx, "go")
	got := ""
	sel := workflow.NewSelector(ctx)
	sel.AddSignalFuture(ctx, sf, func(f workflow.SignalFuture) {
		var out map[string]any
		if e := f.Get(ctx, &out); e == nil {
			got = "signal"
		}
	})
	sel.Select(ctx)
	return map[string]any{"got": got}
}
`

// T-E1 Happy Path: signal 已在历史里 → AddSignalFuture 命中、回调内 f.Get 取到值、工作流完成。
func TestSelectorSignalFuture_SignalArrived_CallbackRuns(t *testing.T) {
	started := history.NewWorkflowStartedEvent(map[string]any{"k": "v"}, "", "", "", "", 0)
	started.ID = 1
	events := []history.Event{
		started,
		{ID: 2, Timestamp: time.Now(), WorkflowSignaled: &history.WorkflowSignaledEvent{Name: "go", Input: map[string]any{"payload": int64(7)}}},
	}
	cmds, err := probeExecFix(t, selSignalSrc, events)
	if err != nil {
		t.Fatalf("execute err: %v", err)
	}
	cmd, ok := findCompleteCmd(cmds)
	if !ok {
		t.Fatalf("signal 已到应命中并完成, got %+v", cmds)
	}
	if cmd.Output["got"] != "signal" {
		t.Fatalf("AddSignalFuture 命中后回调应置 got=signal, got %v", cmd.Output)
	}
}

// T-E2 Happy Path（跨批 signal）: 批 1 signal 未到 → Select park；批 2 signal 到达 → flush 桥唤醒命中。
// 前缀一致性：批 1 推进只见批 1 事实（无 signal）→ 决策停在 Select；批 2 flush 后命中完成。
func TestSelectorSignalFuture_CrossBatch_FlushBridgeWakes(t *testing.T) {
	started := history.NewWorkflowStartedEvent(map[string]any{"k": "v"}, "", "", "", "", 0)
	started.ID = 1
	// 轮 1（live）：signal 未到 → Select park → 不完成。
	round1 := []history.Event{
		started,
		wftc(2),
	}
	cmds1, err := probeExecFix(t, selSignalSrc, round1)
	if err != nil {
		t.Fatalf("round1 execute err: %v", err)
	}
	if _, ok := findCompleteCmd(cmds1); ok {
		t.Fatalf("轮1 signal 未到不应完成, got %+v", cmds1)
	}

	// 轮 2（replay 批 0 + live 批 1）：批 1 signal 到达（非命令事件开新批）→ flush 桥唤醒命中。
	round2 := append(round1,
		history.Event{ID: 3, Timestamp: time.Now(), WorkflowSignaled: &history.WorkflowSignaledEvent{Name: "go", Input: map[string]any{"payload": int64(9)}}},
	)
	cmds2, err := probeExecFix(t, selSignalSrc, round2)
	if err != nil {
		t.Fatalf("round2 execute err: %v", err)
	}
	cmd, ok := findCompleteCmd(cmds2)
	if !ok {
		t.Fatalf("轮2 signal 到达后 flush 桥应唤醒命中并完成, got %+v", cmds2)
	}
	if cmd.Output["got"] != "signal" {
		t.Fatalf("跨批唤醒后回调应置 got=signal, got %v", cmd.Output)
	}
}

// selChildSrc: ExecuteChildWorkflow + AddChildFuture，回调内 Get 取结果/错误写外层变量。
const selChildSrc = `package flows
import (
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
)
func W(ctx workflow.Context, input map[string]any) map[string]any {
	cf := workflow.ExecuteChildWorkflow(ctx, "child-wf", input)
	got := ""
	sel := workflow.NewSelector(ctx)
	sel.AddChildFuture(ctx, cf, func(f workflow.ChildWorkflowFuture) {
		var out map[string]any
		if e := f.Get(ctx, &out); e != nil {
			got = "err:" + e.Error()
			return
		}
		got = "done"
	})
	sel.Select(ctx)
	return map[string]any{"got": got}
}
`

// T-E3 Happy Path: child 完成 → AddChildFuture 命中、回调内 Get 取结果、工作流完成。
func TestSelectorChildFuture_ChildCompleted_CallbackRuns(t *testing.T) {
	started := history.NewWorkflowStartedEvent(map[string]any{"k": "v"}, "", "", "", "", 0)
	started.ID = 1
	events := []history.Event{
		started,
		{ID: 2, Timestamp: time.Now(), ChildWorkflowStarted: &history.ChildWorkflowStartedEvent{ChildID: "#c1", Name: "child-wf"}},
		{ID: 3, Timestamp: time.Now(), ChildWorkflowCompleted: &history.ChildWorkflowCompletedEvent{ChildID: "#c1", Output: map[string]any{"ok": true}}},
	}
	cmds, err := probeExecFix(t, selChildSrc, events)
	if err != nil {
		t.Fatalf("execute err: %v", err)
	}
	cmd, ok := findCompleteCmd(cmds)
	if !ok {
		t.Fatalf("child 完成应命中并完成, got %+v", cmds)
	}
	if cmd.Output["got"] != "done" {
		t.Fatalf("AddChildFuture 命中后回调应置 got=done, got %v", cmd.Output)
	}
}

// T-E3 Error Case: child 失败 → 命中（失败也是 ready）、回调内 Get 返回历史错误。
func TestSelectorChildFuture_ChildFailed_GetReturnsError(t *testing.T) {
	started := history.NewWorkflowStartedEvent(map[string]any{"k": "v"}, "", "", "", "", 0)
	started.ID = 1
	events := []history.Event{
		started,
		{ID: 2, Timestamp: time.Now(), ChildWorkflowStarted: &history.ChildWorkflowStartedEvent{ChildID: "#c1", Name: "child-wf"}},
		{ID: 3, Timestamp: time.Now(), ChildWorkflowFailed: &history.ChildWorkflowFailedEvent{ChildID: "#c1", Error: "child-boom", Failure: &protocol.Failure{Message: "child-boom"}}},
	}
	cmds, err := probeExecFix(t, selChildSrc, events)
	if err != nil {
		t.Fatalf("execute err: %v", err)
	}
	cmd, ok := findCompleteCmd(cmds)
	if !ok {
		t.Fatalf("child 失败后回调捕获错误正常 return，应完成, got %+v", cmds)
	}
	if cmd.Output["got"] != "err:child-boom" {
		t.Fatalf("回调内 Get 应返回历史错误 child-boom, got %v", cmd.Output)
	}
}

// T-E4 Edge Case（child 发起上移锚点）: ExecuteChildWorkflow 后【只】AddChildFuture 不直接 Get
// → CmdStartChildWorkflow 照发（发起上移到调用点，旧「发起在 Get 内」会漏发）。
func TestSelectorChildFuture_ExecuteChildEmitsStartWithoutGet(t *testing.T) {
	started := history.NewWorkflowStartedEvent(map[string]any{"k": "v"}, "", "", "", "", 0)
	started.ID = 1
	// 无 child 事件：child 全 miss → ExecuteChildWorkflow 调用点应发 CmdStartChildWorkflow。
	events := []history.Event{started}
	cmds, err := probeExecFix(t, selChildSrc, events)
	if err != nil {
		t.Fatalf("execute err: %v", err)
	}
	var startFound bool
	for _, c := range cmds {
		if c.Type == history.CmdStartChildWorkflow && c.WorkflowID == "child-wf" {
			startFound = true
		}
	}
	if !startFound {
		t.Fatalf("ExecuteChildWorkflow 调用点应发起 CmdStartChildWorkflow（发起上移），got %+v", cmds)
	}
	// child 未 ready → 不完成。
	if _, ok := findCompleteCmd(cmds); ok {
		t.Fatalf("child 未 ready 不应完成, got %+v", cmds)
	}
}

// selMixedSrc: 混合 selector（activity + signal + child），声明序 activity→signal→child。
const selMixedExtSrc = `package flows
import (
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/activities"
)
func W(ctx workflow.Context, input map[string]any) map[string]any {
	af := workflow.ExecuteActivity(ctx, activities.A, input)
	sf := workflow.WaitSignal(ctx, "go")
	cf := workflow.ExecuteChildWorkflow(ctx, "child-wf", input)
	which := ""
	sel := workflow.NewSelector(ctx)
	sel.AddFuture(ctx, af, func(f workflow.ActivityFuture) {
		which = "activity"
	})
	sel.AddSignalFuture(ctx, sf, func(f workflow.SignalFuture) {
		which = "signal"
	})
	sel.AddChildFuture(ctx, cf, func(f workflow.ChildWorkflowFuture) {
		which = "child"
	})
	sel.Select(ctx)
	return map[string]any{"which": which}
}
`

// T-E6 Edge Case（混合 selector 声明序 + 竞争护栏）: activity/signal/child 三者都 ready →
// selector 按声明序取第一个 ready（activity 在最前）→ 命中 activity case。验证跨 future 类型的
// 声明序 tie-break（竞争护栏：多路同时 ready 时不因类型不同而乱序）。三者都 ready → 干净完成。
func TestSelectorMixed_AllReady_HitsFirstByDeclarationOrder(t *testing.T) {
	started := history.NewWorkflowStartedEvent(map[string]any{"k": "v"}, "", "", "", "", 0)
	started.ID = 1
	events := []history.Event{
		started,
		{ID: 2, Timestamp: time.Now(), ActivityScheduled: &history.ActivityScheduledEvent{ActivityID: "#a1", ActivityType: "activities.A"}},
		{ID: 3, Timestamp: time.Now(), ActivityCompleted: &history.ActivityCompletedEvent{ScheduledEventID: 2, Output: map[string]any{"a": true}}},
		{ID: 4, Timestamp: time.Now(), WorkflowSignaled: &history.WorkflowSignaledEvent{Name: "go", Input: map[string]any{"x": int64(1)}}},
		{ID: 5, Timestamp: time.Now(), ChildWorkflowStarted: &history.ChildWorkflowStartedEvent{ChildID: "#c1", Name: "child-wf"}},
		{ID: 6, Timestamp: time.Now(), ChildWorkflowCompleted: &history.ChildWorkflowCompletedEvent{ChildID: "#c1", Output: map[string]any{"c": true}}},
	}
	cmds, err := probeExecFix(t, selMixedExtSrc, events)
	if err != nil {
		t.Fatalf("execute err: %v", err)
	}
	cmd, ok := findCompleteCmd(cmds)
	if !ok {
		t.Fatalf("三者都 ready 应命中并完成, got %+v", cmds)
	}
	if cmd.Output["which"] != "activity" {
		t.Fatalf("三者都 ready → 声明序 tie-break 应命中最前的 activity, got %v", cmd.Output)
	}
}

// T-E7 Error Case: addFuture 传 SideEffectFuture host 值 → evalErr（type switch default 护栏）。
// SideEffectFuture 是同步语义（创建即 ready，无 select 意义）→ default 拒绝。
// 直接构造 host 调用锚 engine 运行时护栏（sdkgen 静态层不会翻出此形态）。
func TestSelectorFuture_SideEffectFuture_EvalErr(t *testing.T) {
	e := &WorkflowExecutor{}
	e.selectors = map[int]*selectorState{1: {sel: coroutine.NewSelector("te7")}}
	h := selectorHandle{executor: e, id: 1}
	h.AddFuture(workflowContext{}, SideEffectFuture{executor: e, id: "se1"}, nil)
	if e.evalErr == nil || !strings.Contains(e.evalErr.Error(), "unsupported future type") {
		t.Fatalf("SideEffectFuture 应记 evalErr（default 护栏拒绝同步语义 future）, got %v", e.evalErr)
	}
}

// T-E8 Edge Case（含 child 的多批历史 replay 无误报）: child 在批 1 start、批 2 completed →
// replay（批 0/1）+ live（批 2）无非确定性误报，shadow 上移后配对不变。selChildSrc 走 body Get 路径。
func TestSelectorChildFuture_MultiBatchReplay_NoFalsePositive(t *testing.T) {
	started := history.NewWorkflowStartedEvent(map[string]any{"k": "v"}, "", "", "", "", 0)
	started.ID = 1
	events := []history.Event{
		started,
		wftc(2),
		{ID: 3, Timestamp: time.Now(), ChildWorkflowStarted: &history.ChildWorkflowStartedEvent{ChildID: "#c1", Name: "child-wf"}},
		{ID: 4, Timestamp: time.Now(), ChildWorkflowCompleted: &history.ChildWorkflowCompletedEvent{ChildID: "#c1", Output: map[string]any{"ok": true}}},
	}
	cmds, err := probeExecFix(t, selChildSrc, events)
	if err != nil {
		t.Fatalf("多批含 child 的 replay 不应报错, got err: %v", err)
	}
	cmd, ok := findCompleteCmd(cmds)
	if !ok {
		t.Fatalf("多批 child 完成后应完成, got %+v", cmds)
	}
	if cmd.Output["got"] != "done" {
		t.Fatalf("child 完成回调应置 got=done, got %v", cmd.Output)
	}
}
