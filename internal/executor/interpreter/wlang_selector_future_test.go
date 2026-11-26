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
// Selector.AddFuture 端到端测试
// ============================================
// 测试策略:
// - Happy Path: T8 completed → 命中+回调内 Get 取历史 Output；T11 两 future 循环 Select 各命中一次
// - Error Case: T9 failed → 回调内 Get 返回历史错误；T16 AddFuture 传非 future → evalErr
// - Edge Case:  T10 只有 scheduled → park 不误判完成；T12 future 未 ready + channel 唤醒；
//               T14 跨批乱序 ready 前缀一致性（审查 C.3 核心）；T15 三 future 乱序消费序；
//               T17 同一 future 双 AddFuture 两 case 各命中一次
// ============================================

// 单 future 形态：Select 命中后回调内 Get，结果经 buffered channel 带回 root（回调闭包写外层变量）。
const selFutSingleSrc = `package flows
import (
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/activities"
)
func W(ctx workflow.Context, input map[string]any) map[string]any {
	f := workflow.ExecuteActivity(ctx, activities.A, input)
	var got map[string]any
	var errMsg string
	sel := workflow.NewSelector(ctx)
	sel.AddFuture(ctx, f, func(fut workflow.ActivityFuture) {
		var out map[string]any
		if e := fut.Get(ctx, &out); e != nil {
			errMsg = e.Error()
			return
		}
		got = out
	})
	sel.Select(ctx)
	if errMsg != "" {
		return map[string]any{"err": errMsg}
	}
	return map[string]any{"got": got}
}
`

// Happy Path（T8）: 历史含 completed → future case 命中、回调内 f.Get 取到历史 Output、工作流完成。
func TestSelectorFuture_ActivityCompleted_CallbackRuns(t *testing.T) {
	started := history.NewWorkflowStartedEvent(map[string]any{"k": "v"}, "", "", "", "", 0)
	started.ID = 1
	events := []history.Event{
		started,
		{ID: 2, Timestamp: time.Now(), ActivityScheduled: &history.ActivityScheduledEvent{ActivityID: "#a1", ActivityType: "activities.A"}},
		{ID: 3, Timestamp: time.Now(), ActivityCompleted: &history.ActivityCompletedEvent{ScheduledEventID: 2, Output: map[string]any{"answer": int64(42)}}},
	}
	cmds, err := probeExecFix(t, selFutSingleSrc, events)
	if err != nil {
		t.Fatalf("execute err: %v", err)
	}
	cmd, ok := findCompleteCmd(cmds)
	if !ok {
		t.Fatalf("应完成工作流, got %+v", cmds)
	}
	got, _ := cmd.Output["got"].(map[string]any)
	if got["answer"] != float64(42) {
		t.Fatalf("回调内 Get 应取到历史 Output(answer=42), got %v", cmd.Output)
	}
}

// Error Case（T9）: 历史含 failed → future case 命中（失败也是 ready），回调内 f.Get 返回历史错误。
func TestSelectorFuture_ActivityFailed_GetReturnsError(t *testing.T) {
	started := history.NewWorkflowStartedEvent(map[string]any{"k": "v"}, "", "", "", "", 0)
	started.ID = 1
	events := []history.Event{
		started,
		{ID: 2, Timestamp: time.Now(), ActivityScheduled: &history.ActivityScheduledEvent{ActivityID: "#a1", ActivityType: "activities.A"}},
		{ID: 3, Timestamp: time.Now(), ActivityFailed: &history.ActivityFailedEvent{ScheduledEventID: 2, Error: "boom-fut", Failure: &protocol.Failure{Message: "boom-fut"}}},
	}
	cmds, err := probeExecFix(t, selFutSingleSrc, events)
	if err != nil {
		t.Fatalf("execute err: %v", err)
	}
	cmd, ok := findCompleteCmd(cmds)
	if !ok {
		t.Fatalf("应完成工作流（错误被回调捕获后正常 return）, got %+v", cmds)
	}
	if cmd.Output["err"] != "boom-fut" {
		t.Fatalf("回调内 Get 应返回历史错误 boom-fut, got %v", cmd.Output)
	}
}

// Edge Case（T10）: 只有 scheduled（未完成）→ Select park，不误判完成、不产终态命令。
func TestSelectorFuture_NotReady_Blocks(t *testing.T) {
	started := history.NewWorkflowStartedEvent(map[string]any{"k": "v"}, "", "", "", "", 0)
	started.ID = 1
	events := []history.Event{
		started,
		{ID: 2, Timestamp: time.Now(), ActivityScheduled: &history.ActivityScheduledEvent{ActivityID: "#a1", ActivityType: "activities.A"}},
	}
	cmds, err := probeExecFix(t, selFutSingleSrc, events)
	if err != nil {
		t.Fatalf("execute err: %v", err)
	}
	if _, ok := findCompleteCmd(cmds); ok {
		t.Fatalf("future 未 ready 不应完成, got %+v", cmds)
	}
	if failed, msg := hasFailCmd(cmds); failed {
		t.Fatalf("不应失败, got %q", msg)
	}
}

// 双 future 形态（Temporal 经典 n-future 逐个消费）：循环 Select 两次，记录命中序。
const selFutTwoSrc = `package flows
import (
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/activities"
)
func W(ctx workflow.Context, input map[string]any) map[string]any {
	f1 := workflow.ExecuteActivity(ctx, activities.A, input)
	f2 := workflow.ExecuteActivity(ctx, activities.B, input)
	order := ""
	sel := workflow.NewSelector(ctx)
	sel.AddFuture(ctx, f1, func(fut workflow.ActivityFuture) {
		var out map[string]any
		if e := fut.Get(ctx, &out); e == nil {
			order = order + "1"
		}
	})
	sel.AddFuture(ctx, f2, func(fut workflow.ActivityFuture) {
		var out map[string]any
		if e := fut.Get(ctx, &out); e == nil {
			order = order + "2"
		}
	})
	for i := 0; i < 2; i += 1 {
		sel.Select(ctx)
	}
	return map[string]any{"order": order}
}
`

// Happy Path（T11）: 两 activity 都 completed（同批）→ 循环 Select 两回调各执行一次，声明序 f1→f2。
func TestSelectorFuture_TwoFutures_LoopSelect_ConsumesBoth(t *testing.T) {
	started := history.NewWorkflowStartedEvent(map[string]any{"k": "v"}, "", "", "", "", 0)
	started.ID = 1
	events := []history.Event{
		started,
		{ID: 2, Timestamp: time.Now(), ActivityScheduled: &history.ActivityScheduledEvent{ActivityID: "#a1", ActivityType: "activities.A"}},
		{ID: 3, Timestamp: time.Now(), ActivityScheduled: &history.ActivityScheduledEvent{ActivityID: "#a2", ActivityType: "activities.B"}},
		{ID: 4, Timestamp: time.Now(), ActivityCompleted: &history.ActivityCompletedEvent{ScheduledEventID: 2, Output: map[string]any{"ok": true}}},
		{ID: 5, Timestamp: time.Now(), ActivityCompleted: &history.ActivityCompletedEvent{ScheduledEventID: 3, Output: map[string]any{"ok": true}}},
	}
	cmds, err := probeExecFix(t, selFutTwoSrc, events)
	if err != nil {
		t.Fatalf("execute err: %v", err)
	}
	cmd, ok := findCompleteCmd(cmds)
	if !ok {
		t.Fatalf("应完成工作流, got %+v", cmds)
	}
	if cmd.Output["order"] != "12" {
		t.Fatalf("同批双 ready 应按声明序逐个消费(order=12), got %v", cmd.Output["order"])
	}
}

// Edge Case（T14，审查 C.3 核心——跨批乱序 ready 前缀一致性）:
// f2 完成事件在批 1、f1 在批 2 → live 序 f2→f1；轮 2 full replay 必须复现同序（order=21），
// 且轮 1（只有批 1 事实）的决策 order 前缀 "2" 与轮 2 一致。
func TestSelectorFuture_PrefixConsistency_OutOfOrderReady(t *testing.T) {
	started := history.NewWorkflowStartedEvent(map[string]any{"k": "v"}, "", "", "", "", 0)
	started.ID = 1
	// 轮 1（live）：只有 f2 完成 → 第一次 Select 命中 f2，第二次 Select park（f1 未 ready）→ 不完成。
	round1 := []history.Event{
		started,
		{ID: 2, Timestamp: time.Now(), ActivityScheduled: &history.ActivityScheduledEvent{ActivityID: "#a1", ActivityType: "activities.A"}},
		{ID: 3, Timestamp: time.Now(), ActivityScheduled: &history.ActivityScheduledEvent{ActivityID: "#a2", ActivityType: "activities.B"}},
		{ID: 4, Timestamp: time.Now(), ActivityCompleted: &history.ActivityCompletedEvent{ScheduledEventID: 3, Output: map[string]any{"from": "f2"}}},
	}
	cmds1, err := probeExecFix(t, selFutTwoSrc, round1)
	if err != nil {
		t.Fatalf("round1 execute err: %v", err)
	}
	if _, ok := findCompleteCmd(cmds1); ok {
		t.Fatalf("轮1 f1 未 ready 不应完成, got %+v", cmds1)
	}

	// 轮 2（replay）：轮 1 决策已提交（WFTC₁），其后 f1 完成（批 2）→ replay 必须先命中 f2 再 f1。
	round2 := append(round1,
		history.Event{ID: 5, Timestamp: time.Now(), WorkflowTaskCompleted: &history.WorkflowTaskCompletedEvent{}},
		history.Event{ID: 6, Timestamp: time.Now(), ActivityCompleted: &history.ActivityCompletedEvent{ScheduledEventID: 2, Output: map[string]any{"from": "f1"}}},
	)
	cmds2, err := probeExecFix(t, selFutTwoSrc, round2)
	if err != nil {
		t.Fatalf("round2 execute err: %v", err)
	}
	cmd, ok := findCompleteCmd(cmds2)
	if !ok {
		t.Fatalf("轮2 双 future 都 ready 应完成, got %+v", cmds2)
	}
	if cmd.Output["order"] != "21" {
		t.Fatalf("前缀不一致: live 序 f2→f1(order=21)，replay 得 %v ——整批灌表式漂移（声明序覆盖了 ready 序）", cmd.Output["order"])
	}
}

// 三 future 形态（T15）。
const selFutThreeSrc = `package flows
import (
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/activities"
)
func W(ctx workflow.Context, input map[string]any) map[string]any {
	f1 := workflow.ExecuteActivity(ctx, activities.A, input)
	f2 := workflow.ExecuteActivity(ctx, activities.B, input)
	f3 := workflow.ExecuteActivity(ctx, activities.C, input)
	order := ""
	sel := workflow.NewSelector(ctx)
	sel.AddFuture(ctx, f1, func(fut workflow.ActivityFuture) {
		var out map[string]any
		if e := fut.Get(ctx, &out); e == nil {
			order = order + "1"
		}
	})
	sel.AddFuture(ctx, f2, func(fut workflow.ActivityFuture) {
		var out map[string]any
		if e := fut.Get(ctx, &out); e == nil {
			order = order + "2"
		}
	})
	sel.AddFuture(ctx, f3, func(fut workflow.ActivityFuture) {
		var out map[string]any
		if e := fut.Get(ctx, &out); e == nil {
			order = order + "3"
		}
	})
	for i := 0; i < 3; i += 1 {
		sel.Select(ctx)
	}
	return map[string]any{"order": order}
}
`

// Edge Case（T15）: f3/f1/f2 的完成事件乱序分属三批 → 消费序 = ready 序（3,1,2），live/replay 一致。
func TestSelectorFuture_ThreeFutures_OutOfOrderConsumptionOrder(t *testing.T) {
	started := history.NewWorkflowStartedEvent(map[string]any{"k": "v"}, "", "", "", "", 0)
	started.ID = 1
	events := []history.Event{
		started,
		{ID: 2, Timestamp: time.Now(), ActivityScheduled: &history.ActivityScheduledEvent{ActivityID: "#a1", ActivityType: "activities.A"}},
		{ID: 3, Timestamp: time.Now(), ActivityScheduled: &history.ActivityScheduledEvent{ActivityID: "#a2", ActivityType: "activities.B"}},
		{ID: 4, Timestamp: time.Now(), ActivityScheduled: &history.ActivityScheduledEvent{ActivityID: "#a3", ActivityType: "activities.C"}},
		// 批 1：f3 完成
		{ID: 5, Timestamp: time.Now(), ActivityCompleted: &history.ActivityCompletedEvent{ScheduledEventID: 4, Output: map[string]any{"from": "f3"}}},
		{ID: 6, Timestamp: time.Now(), WorkflowTaskCompleted: &history.WorkflowTaskCompletedEvent{}},
		// 批 2：f1 完成
		{ID: 7, Timestamp: time.Now(), ActivityCompleted: &history.ActivityCompletedEvent{ScheduledEventID: 2, Output: map[string]any{"from": "f1"}}},
		{ID: 8, Timestamp: time.Now(), WorkflowTaskCompleted: &history.WorkflowTaskCompletedEvent{}},
		// 批 3：f2 完成
		{ID: 9, Timestamp: time.Now(), ActivityCompleted: &history.ActivityCompletedEvent{ScheduledEventID: 3, Output: map[string]any{"from": "f2"}}},
	}
	cmds, err := probeExecFix(t, selFutThreeSrc, events)
	if err != nil {
		t.Fatalf("execute err: %v", err)
	}
	cmd, ok := findCompleteCmd(cmds)
	if !ok {
		t.Fatalf("应完成工作流, got %+v", cmds)
	}
	if cmd.Output["order"] != "312" {
		t.Fatalf("消费序应 = ready 序(312), got %v", cmd.Output["order"])
	}
}

// 混合形态（T12）：future 未 ready + channel case 被兄弟 lane send 唤醒。
const selFutMixedSrc = `package flows
import (
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/activities"
)
func W(ctx workflow.Context, input map[string]any) map[string]any {
	f := workflow.ExecuteActivity(ctx, activities.A, input)
	ch := workflow.NewBufferedChannel(ctx, 1)
	wg := workflow.NewWaitGroup(ctx)
	wg.Go(ctx, func(ctx workflow.Context) {
		var o map[string]any
		if e := workflow.ExecuteActivity(ctx, activities.B, input).Get(ctx, &o); e == nil {
			ch.Send(ctx, o)
		}
	})
	which := ""
	sel := workflow.NewSelector(ctx)
	sel.AddFuture(ctx, f, func(fut workflow.ActivityFuture) {
		which = "future"
	})
	sel.AddReceive(ctx, ch, func(c workflow.Channel, more bool) {
		var got map[string]any
		c.Receive(ctx, &got)
		which = "channel"
	})
	sel.Select(ctx)
	return map[string]any{"which": which}
}
`

// Edge Case（T12）: future 的 activity 未完成、channel 被兄弟 lane send → Select 命中 channel case。
// main 在这一批返回，Workflow 随即完成；future 的 activity A 仍 pending，结果不再被消费。
func TestSelectorFuture_MixedWithChannelCase_ChannelWakes(t *testing.T) {
	started := history.NewWorkflowStartedEvent(map[string]any{"k": "v"}, "", "", "", "", 0)
	started.ID = 1
	events := []history.Event{
		started,
		{ID: 2, Timestamp: time.Now(), ActivityScheduled: &history.ActivityScheduledEvent{ActivityID: "#a1", ActivityType: "activities.A"}},
		{ID: 3, Timestamp: time.Now(), ActivityScheduled: &history.ActivityScheduledEvent{ActivityID: "/0#a1", ActivityType: "activities.B"}},
		{ID: 4, Timestamp: time.Now(), ActivityCompleted: &history.ActivityCompletedEvent{ScheduledEventID: 3, Output: map[string]any{"from": "B"}}},
	}
	cmds, err := probeExecFix(t, selFutMixedSrc, events)
	if err != nil {
		t.Fatalf("execute err: %v", err)
	}
	cmd, ok := findCompleteCmd(cmds)
	if !ok {
		t.Fatalf("main 已返回，应完成, got %+v", cmds)
	}
	if cmd.Output["which"] != "channel" {
		t.Fatalf("future 未 ready，Select 决策应是 channel, got %v", cmd.Output["which"])
	}
}

// Error Case（T16）: AddFuture 传非 future host 值（channel）→ evalErr（Execute 返回错误），不静默。
// sdkgen 静态层会拒绝非 ident（T13b），本测试锚 engine 运行时的 type switch default 护栏——
// 绕过 sdkgen 直接构造 WLang JSON 调 host AddFuture。
func TestSelectorFuture_UnsupportedType_EvalErr(t *testing.T) {
	// 直接用 wlang 层调用 host：ch 作第 2 参传给 AddFuture。
	src := `package flows
import (
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
)
func W(ctx workflow.Context, input map[string]any) error {
	ch := workflow.NewChannel(ctx)
	f := workflow.ExecuteActivity(ctx, activities.A, input)
	_ = f
	sel := workflow.NewSelector(ctx)
	sel.AddFuture(ctx, ch, func(fut workflow.ActivityFuture) {
	})
	sel.Select(ctx)
	return nil
}
`
	_ = src // sdkgen 会在静态层报错（ch 不是登记的 future 变量）——正是 T13b 锚定的路径。
	// 运行时护栏无法经 sdkgen 端到端到达（静态层先拦），此处直接断言 host 层行为：
	e := &WorkflowExecutor{}
	e.selectors = map[int]*selectorState{1: {sel: coroutine.NewSelector("t16")}}
	h := selectorHandle{executor: e, id: 1}
	h.AddFuture(workflowContext{}, channelHandle{}, nil)
	if e.evalErr == nil || !strings.Contains(e.evalErr.Error(), "unsupported future type") {
		t.Fatalf("非 ActivityFuture 应记 evalErr, got %v", e.evalErr)
	}
}

// 双 AddFuture 形态（T17）：同一 ActivityFuture 注册两个 case。
const selFutTwiceSrc = `package flows
import (
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/activities"
)
func W(ctx workflow.Context, input map[string]any) map[string]any {
	f := workflow.ExecuteActivity(ctx, activities.A, input)
	order := ""
	sel := workflow.NewSelector(ctx)
	sel.AddFuture(ctx, f, func(fut workflow.ActivityFuture) {
		order = order + "x"
	})
	sel.AddFuture(ctx, f, func(fut workflow.ActivityFuture) {
		order = order + "y"
	})
	for i := 0; i < 2; i += 1 {
		sel.Select(ctx)
	}
	return map[string]any{"order": order}
}
`

// Edge Case（T17）: 同一 ActivityFuture 双 AddFuture → 两 cofut 独立（各自 Set），
// 循环 Select 两 case 按声明序各命中一次（对齐 Temporal：同一 future 可注册多 case，各自独立命中）。
func TestSelectorFuture_SameFutureTwice_BothCasesHit(t *testing.T) {
	started := history.NewWorkflowStartedEvent(map[string]any{"k": "v"}, "", "", "", "", 0)
	started.ID = 1
	events := []history.Event{
		started,
		{ID: 2, Timestamp: time.Now(), ActivityScheduled: &history.ActivityScheduledEvent{ActivityID: "#a1", ActivityType: "activities.A"}},
		{ID: 3, Timestamp: time.Now(), ActivityCompleted: &history.ActivityCompletedEvent{ScheduledEventID: 2, Output: map[string]any{"ok": true}}},
	}
	cmds, err := probeExecFix(t, selFutTwiceSrc, events)
	if err != nil {
		t.Fatalf("execute err: %v", err)
	}
	cmd, ok := findCompleteCmd(cmds)
	if !ok {
		t.Fatalf("应完成工作流, got %+v", cmds)
	}
	if cmd.Output["order"] != "xy" {
		t.Fatalf("双 AddFuture 应各命中一次(order=xy), got %v", cmd.Output["order"])
	}
}
