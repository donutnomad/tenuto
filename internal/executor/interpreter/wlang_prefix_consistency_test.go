package interpreter

import (
	"github.com/donutnomad/tenuto/internal/executor"
	"github.com/donutnomad/tenuto/protocol/history"
	"testing"
	"time"
)

// ============================================
// 前缀一致性回归测试
// ============================================
// 测试策略:
// - Error Case 转正: 探针实锤场景——selector 两 channel case 跨轮乱序 ready，
//   整批灌表曾使轮 2 replay 命中与轮 1 live 不同的 case（which=A ≠ B，静默串数据）。
//   分批 apply 后轮 2 的批 1 推进只见批 1 事实 → 决策与 live 一致（which=B）。
// - 前缀断言: 轮 N 的 live 命令序必须是轮 N+1 replay 产出命令序的前缀。
// - update 批界: UpdateAccepted 开新批，等待 update 的决策点不得提前一批放行。
// ============================================

const prefixSelectorSrc = `package flows
import (
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/activities"
)
func W(ctx workflow.Context, input map[string]any) map[string]any {
	chA := workflow.NewBufferedChannel(ctx, 1)
	chB := workflow.NewBufferedChannel(ctx, 1)
	wg := workflow.NewWaitGroup(ctx)
	wg.Go(ctx, func(ctx workflow.Context) {
		var o map[string]any
		if e := workflow.ExecuteActivity(ctx, activities.A, input).Get(ctx, &o); e == nil {
			chA.Send(ctx, o)
		}
	})
	wg.Go(ctx, func(ctx workflow.Context) {
		var o map[string]any
		if e := workflow.ExecuteActivity(ctx, activities.B, input).Get(ctx, &o); e == nil {
			chB.Send(ctx, o)
		}
	})
	var which string
	sel := workflow.NewSelector(ctx)
	sel.AddReceive(ctx, chA, func(c workflow.Channel, more bool) {
		var got map[string]any
		c.Receive(ctx, &got)
		which = "A"
	})
	sel.AddReceive(ctx, chB, func(c workflow.Channel, more bool) {
		var got map[string]any
		c.Receive(ctx, &got)
		which = "B"
	})
	sel.Select(ctx)
	var out map[string]any
	if e := workflow.ExecuteActivity(ctx, activities.A, map[string]any{"which": which}).Get(ctx, &out); e != nil {
		return nil
	}
	return map[string]any{"which": which}
}
`

// prefixRound1 构造轮 1 历史（live）：只有 B 完成 → Select 应命中 chB。
func prefixRound1Events() []history.Event {
	started := history.NewWorkflowStartedEvent(map[string]any{"k": "v"}, "", "", "", "", 0)
	started.ID = 1
	return []history.Event{
		started,
		{ID: 2, Timestamp: time.Now(), ActivityScheduled: &history.ActivityScheduledEvent{ActivityID: "/0#a1", ActivityType: "activities.A"}},
		{ID: 3, Timestamp: time.Now(), ActivityScheduled: &history.ActivityScheduledEvent{ActivityID: "/1#a1", ActivityType: "activities.B"}},
		{ID: 4, Timestamp: time.Now(), ActivityCompleted: &history.ActivityCompletedEvent{ScheduledEventID: 3, Output: map[string]any{"from": "B"}}},
	}
}

// prefixRound2Events 构造轮 2 历史（replay）：轮 1 决策已提交（WFTC₁ + 决策产的 schedC），
// 其后 A 也完成了（append，含轮 2 决策界 WFTC₂ 与 compC）。
func prefixRound2Events() []history.Event {
	r1 := prefixRound1Events()
	return append(r1,
		history.Event{ID: 5, Timestamp: time.Now(), WorkflowTaskCompleted: &history.WorkflowTaskCompletedEvent{}},
		history.Event{ID: 6, Timestamp: time.Now(), ActivityScheduled: &history.ActivityScheduledEvent{ActivityID: "#a1", ActivityType: "activities.A"}},
		history.Event{ID: 7, Timestamp: time.Now(), ActivityCompleted: &history.ActivityCompletedEvent{ScheduledEventID: 2, Output: map[string]any{"from": "A"}}},
		history.Event{ID: 8, Timestamp: time.Now(), WorkflowTaskCompleted: &history.WorkflowTaskCompletedEvent{}},
		history.Event{ID: 9, Timestamp: time.Now(), ActivityCompleted: &history.ActivityCompletedEvent{ScheduledEventID: 6, Output: map[string]any{"ok": true}}},
	)
}

// Error Case 转正: 轮 2 full replay 的 Select 决策必须与轮 1 live 相同（which=B）。
// 整批灌表模型下曾实锤 which=A（laneA 在 replay 时提前 send，声明序命中 chA）。
func TestPrefixConsistency_SelectorChannelCase_ReplayMatchesLive(t *testing.T) {
	// 轮 1（live）：Select 命中 chB 后调度 root activity，Input.which=B 为 live 决策证据。
	cmds1, err := probeExecFix(t, prefixSelectorSrc, prefixRound1Events())
	if err != nil {
		t.Fatalf("round1 execute err: %v", err)
	}
	var round1Which any
	for _, c := range cmds1 {
		if c.Type == history.CmdScheduleActivity && c.ActivityID == "#a1" {
			round1Which = c.Input["which"]
		}
	}
	if round1Which != "B" {
		t.Fatalf("轮1 live 决策应为 which=B（只有 B 完成），got %v; cmds=%+v", round1Which, cmds1)
	}

	// 轮 2（replay）：A 的完成事件在轮 1 决策之后 append → 决策必须仍是 B。
	cmds2, err := probeExecFix(t, prefixSelectorSrc, prefixRound2Events())
	if err != nil {
		t.Fatalf("round2 execute err: %v", err)
	}
	cmd, ok := findCompleteCmd(cmds2)
	if !ok {
		t.Fatalf("轮2 应完成工作流, got %+v", cmds2)
	}
	if got := cmd.Output["which"]; got != "B" {
		t.Fatalf("前缀不一致: 轮1 live 命中 chB(which=B)，轮2 replay 得 which=%v ——replay 决策漂移（静默串数据）", got)
	}
}

// 前缀断言: 轮 1 live 产出的命令序是轮 2 replay 视角下已被历史核销的前缀——
// 轮 2 不得重发轮 1 已调度的 activity（幂等核销），新增命令只能是轮 2 的新决策。
func TestPrefixConsistency_CommandsArePrefix(t *testing.T) {
	cmds1, err := probeExecFix(t, prefixSelectorSrc, prefixRound1Events())
	if err != nil {
		t.Fatalf("round1 execute err: %v", err)
	}
	cmds2, err := probeExecFix(t, prefixSelectorSrc, prefixRound2Events())
	if err != nil {
		t.Fatalf("round2 execute err: %v", err)
	}
	// 轮 1 调度的 stepID 集（历史已含 sched 事件的除外——轮 1 输入已含 /0#a1、/1#a1）
	for _, c1 := range cmds1 {
		if c1.Type != history.CmdScheduleActivity {
			continue
		}
		// 轮 1 的每条新调度（#a1）在轮 2 中必须不再产出（已被历史事件 6 核销）
		for _, c2 := range cmds2 {
			if c2.Type == history.CmdScheduleActivity && c2.ActivityID == c1.ActivityID {
				t.Fatalf("轮2 重发了轮1 已调度并落库的 activity %q ——replay 未核销（批归属错位）", c1.ActivityID)
			}
		}
	}
}

// Edge Case: UpdateAccepted 开新批——等待 update 的决策点不得在其到达前一批放行。
// 历史: [Started, WFTC₁(批1: main park 于 update 等待)] [UpdateAccepted(批2)]
// 用「update 触发的 handler 在 replay 批不提前可见」的最小锚验证批界：
// 若 UpdateAccepted 被错误归入批 1（命令侧），批 1 推进时 handler 输入即可见，
// 工作流在批 1 就会走完 update 分支；正确分类下它属批 2。
func TestPrefixConsistency_UpdateAcceptedOpensNewBatch(t *testing.T) {
	// splitBatches 层面已锚定（TestSplitBatches_UpdateAccepted_IsExternal）；
	// 端到端锚: update handler 的 AcceptedEventID 顺序语义依赖 updateAccepted 表——
	// 分批下批 1 推进时表中不得含批 2 的 update。用 executor 内表状态断言。
	s := evStarted()
	s.ID = 1
	events := []history.Event{s, evWFTC(2), evSched(3, "#a1"), evUpdateAccepted(4, "upd"), evComp(5, 3)}
	batches := executor.SplitBatches(events)
	if len(batches) != 2 {
		t.Fatalf("应切 2 批, got %d: %v", len(batches), gotIDs(batches))
	}
	for _, ev := range batches[0] {
		if ev.WorkflowUpdateAccepted != nil {
			t.Fatalf("UpdateAccepted 被错误归入批 1（命令侧）——决策点会提前一批看到 update")
		}
	}
}
