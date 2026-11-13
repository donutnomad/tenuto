package interpreter

import (
	"testing"
	"time"

	"github.com/donutnomad/tenuto/compiler/sdkgen"
	"github.com/donutnomad/tenuto/protocol/history"
)

// ============================================
// wlang 共享 scope 的 park save/restore 回归测试
// ============================================
// 测试策略:
// - Error Case 转正: lane park（栈冻结、InvokeClosure defer 不执行）后 root 的语句
//   曾在 lane callScope 链上解析变量 → root 的 ctx 读到 lane 的 lanePath → laneKey 串桶。
//   修复 = park() 单点 save/restore。
// - Happy Path 对照: lane 正常 return（defer 恢复执行）时 root 键本就正确。
// - Edge Case: selector park 交错形态（scope 泄漏最初被发现的形态）。
// ============================================

func probeExecScope(t *testing.T, src string, events []history.Event) ([]history.Command, error) {
	t.Helper()
	result, err := sdkgen.GenerateFromSource([]byte(src), sdkgen.Options{WorkflowID: "scope-restore", FuncName: "W", Filename: "w.go"})
	if err != nil {
		t.Fatalf("translate: %v", err)
	}
	nodes := result.Workflow
	return NewWorkflowExecutor(nodes, events).Execute()
}

const scopeRestoreLaneParkSrc = `package flows
import (
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/activities"
)
func W(ctx workflow.Context, input map[string]any) error {
	wg := workflow.NewWaitGroup(ctx)
	wg.Go(ctx, func(ctx workflow.Context) {
		var o map[string]any
		if e := workflow.ExecuteActivity(ctx, activities.A, input).Get(ctx, &o); e != nil {
			return
		}
	})
	var out map[string]any
	if e := workflow.ExecuteActivity(ctx, activities.A, input).Get(ctx, &out); e != nil {
		return e
	}
	return nil
}
`

func assertRootStepID(t *testing.T, cmds []history.Command) {
	t.Helper()
	for _, c := range cmds {
		if c.Type == history.CmdScheduleActivity {
			if c.ActivityID != "#a1" {
				t.Fatalf("root 的 activity 键=%q, 应为 #a1（lane park 后 e.scope 未恢复 → root 在 lane scope 链解析 ctx）", c.ActivityID)
			}
			return
		}
	}
	t.Fatalf("未见 CmdScheduleActivity, got %+v", cmds)
}

// Error Case 转正: lane park（activity 未完成）后 root 调度 activity，键必须是 root 桶 #a1。
func TestScopeRestore_LaneParkThenRootActivity(t *testing.T) {
	started := history.NewWorkflowStartedEvent(map[string]any{"k": "v"}, "", "", "", "", 0)
	schedA := history.Event{ID: 2, Timestamp: time.Now(), ActivityScheduled: &history.ActivityScheduledEvent{ActivityID: "/0#a1"}}

	cmds, err := probeExecScope(t, scopeRestoreLaneParkSrc, []history.Event{started, schedA})
	if err != nil {
		t.Fatalf("execute err: %v", err)
	}
	assertRootStepID(t, cmds)
}

// Happy Path 对照: lane 正常 return（InvokeClosure defer 恢复 scope）时 root 键正确。
func TestScopeRestore_Control_LaneCompletes(t *testing.T) {
	started := history.NewWorkflowStartedEvent(map[string]any{"k": "v"}, "", "", "", "", 0)
	schedA := history.Event{ID: 2, Timestamp: time.Now(), ActivityScheduled: &history.ActivityScheduledEvent{ActivityID: "/0#a1"}}
	compA := history.Event{ID: 3, Timestamp: time.Now(), ActivityCompleted: &history.ActivityCompletedEvent{ScheduledEventID: 2, Output: map[string]any{"ok": true}}}

	cmds, err := probeExecScope(t, scopeRestoreLaneParkSrc, []history.Event{started, schedA, compA})
	if err != nil {
		t.Fatalf("execute err: %v", err)
	}
	assertRootStepID(t, cmds)
}

// Edge Case: selector park 交错形态（scope 泄漏最初被发现的形态）——
// root park 于 Select，laneA park 于 Get（e.scope 残留 laneA callScope），laneB send 唤醒 root，
// root 醒来后在 Select 之后调度 activity：键必须是 root 桶 #a1（曾实锤 /0#a2）。
func TestScopeRestore_SelectorParkInterleave(t *testing.T) {
	const src = `package flows
import (
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/activities"
)
func W(ctx workflow.Context, input map[string]any) error {
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
	sel := workflow.NewSelector(ctx)
	sel.AddReceive(ctx, chA, func(c workflow.Channel, more bool) {
		var got map[string]any
		c.Receive(ctx, &got)
	})
	sel.AddReceive(ctx, chB, func(c workflow.Channel, more bool) {
		var got map[string]any
		c.Receive(ctx, &got)
	})
	sel.Select(ctx)
	var out map[string]any
	if e := workflow.ExecuteActivity(ctx, activities.A, map[string]any{"after": "select"}).Get(ctx, &out); e != nil {
		return e
	}
	return nil
}
`
	started := history.NewWorkflowStartedEvent(map[string]any{"k": "v"}, "", "", "", "", 0)
	schedA := history.Event{ID: 2, Timestamp: time.Now(), ActivityScheduled: &history.ActivityScheduledEvent{ActivityID: "/0#a1"}}
	schedB := history.Event{ID: 3, Timestamp: time.Now(), ActivityScheduled: &history.ActivityScheduledEvent{ActivityID: "/1#a1"}}
	compB := history.Event{ID: 4, Timestamp: time.Now(), ActivityCompleted: &history.ActivityCompletedEvent{ScheduledEventID: 3, Output: map[string]any{"from": "B"}}}

	cmds, err := probeExecScope(t, src, []history.Event{started, schedA, schedB, compB})
	if err != nil {
		t.Fatalf("execute err: %v", err)
	}
	assertRootStepID(t, cmds)
}
