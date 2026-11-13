package interpreter

// ============================================
// 跨批 park 收敛回归测试
// ============================================
// 背景: 分批模型下协程跨批存活，Sleep/child Get 等检查-yield 循环在批 k park 前置的
// e.blocked=true 曾无人清除（单批模型每轮新建 executor 无感），批 k+1 命中返回后 blocked
// 残留 → 收敛判定永不产 CompleteWorkflow。修复 = progressed() 统一清 blocked。
// 测试策略:
// - Happy Path: activity/timer/child 各自跨批 park 后命中 → 正常完成（blocked 不残留）
// ============================================

import (
	"github.com/donutnomad/tenuto/protocol/history"
	"testing"
	"time"
)

func TestCrossBatchPark_Activity_Completes(t *testing.T) {
	started := history.NewWorkflowStartedEvent(map[string]any{}, "", "", "", "", 0)
	events := []history.Event{
		started,
		wftc(2),
		{ID: 3, Timestamp: time.Now(), ActivityScheduled: &history.ActivityScheduledEvent{ActivityID: "#a1", ActivityType: "activities.A"}},
		{ID: 4, Timestamp: time.Now(), ActivityCompleted: &history.ActivityCompletedEvent{ScheduledEventID: 3, Output: map[string]any{"ok": true}}},
	}
	cmds, err := probeExecFix(t, rmSingleActivitySrc, events)
	t.Logf("cmds=%+v err=%v", cmds, err)
	if err != nil || !hasCompleteWorkflow(cmds) {
		t.Fatalf("activity-only 多批应完成")
	}
}

const crossBatchTimerSrc = `package flows
import (
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
)
func W(ctx workflow.Context, input map[string]any) error {
	err := workflow.Sleep(ctx, 1000000)
	if err != nil {
		return err
	}
	return nil
}
`

func TestCrossBatchPark_Timer_Completes(t *testing.T) {
	started := history.NewWorkflowStartedEvent(map[string]any{}, "", "", "", "", 0)
	events := []history.Event{
		started,
		wftc(2),
		{ID: 3, Timestamp: time.Now(), TimerScheduled: &history.TimerScheduledEvent{TimerID: "#t1"}},
		{ID: 4, Timestamp: time.Now(), TimerFired: &history.TimerFiredEvent{TimerID: "#t1"}},
	}
	cmds, err := probeExecFix(t, crossBatchTimerSrc, events)
	t.Logf("cmds=%+v err=%v", cmds, err)
	if err != nil || !hasCompleteWorkflow(cmds) {
		t.Fatalf("timer-only 多批应完成")
	}
}

const crossBatchChildSrc = `package flows
import (
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
)
func W(ctx workflow.Context, input map[string]any) error {
	var child map[string]any
	err := workflow.ExecuteChildWorkflow(ctx, "child-wf", input).Get(ctx, &child)
	if err != nil {
		return err
	}
	return nil
}
`

func TestCrossBatchPark_Child_Completes(t *testing.T) {
	started := history.NewWorkflowStartedEvent(map[string]any{}, "", "", "", "", 0)
	events := []history.Event{
		started,
		wftc(2),
		{ID: 3, Timestamp: time.Now(), ChildWorkflowStarted: &history.ChildWorkflowStartedEvent{ChildID: "#c1", Name: "child-wf"}},
		{ID: 4, Timestamp: time.Now(), ChildWorkflowCompleted: &history.ChildWorkflowCompletedEvent{ChildID: "#c1", Output: map[string]any{"done": true}}},
	}
	cmds, err := probeExecFix(t, crossBatchChildSrc, events)
	t.Logf("cmds=%+v err=%v", cmds, err)
	if err != nil || !hasCompleteWorkflow(cmds) {
		t.Fatalf("child-only 多批应完成")
	}
}
