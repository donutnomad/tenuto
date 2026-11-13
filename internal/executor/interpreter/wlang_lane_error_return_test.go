package interpreter

import (
	"testing"
	"time"

	"github.com/donutnomad/tenuto/compiler/sdkgen"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
)

func probeExecFix(t *testing.T, src string, events []history.Event) ([]history.Command, error) {
	t.Helper()
	result, err := sdkgen.GenerateFromSource([]byte(src), sdkgen.Options{WorkflowID: "probe-fix", FuncName: "W", Filename: "w.go"})
	if err != nil {
		t.Fatalf("translate: %v", err)
	}
	nodes := result.Workflow
	return NewWorkflowExecutor(nodes, events).Execute()
}

func hasFailCmd(cmds []history.Command) (bool, string) {
	for _, c := range cmds {
		if c.Type == history.CmdFailWorkflow {
			return true, c.Error
		}
	}
	return false, ""
}

// 你原来问的写法:lane 捕获 error 到外层 errA + Wait 后 return errA。
const fixSrc = `package flows
import (
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/app/activities"
)
func W(ctx workflow.Context, input map[string]any) error {
	wg := workflow.NewWaitGroup(ctx)
	var errA, errB error
	wg.Go(ctx, func(ctx workflow.Context) {
		var r map[string]any
		if err := workflow.ExecuteActivity(ctx, activities.A, input).Get(ctx, &r); err != nil {
			errA = err
			return
		}
	})
	wg.Go(ctx, func(ctx workflow.Context) {
		var r map[string]any
		if err := workflow.ExecuteActivity(ctx, activities.B, input).Get(ctx, &r); err != nil {
			errB = err
			return
		}
	})
	wg.Wait(ctx)
	if errA != nil {
		return errA
	}
	if errB != nil {
		return errB
	}
	return nil
}
`

// 修复后:lane0 的 A 失败 → errA 捕获 → return errA → 应产 CmdFailWorkflow(Error="boom-A")。
func TestFix_LaneError_ReturnsFailWorkflow(t *testing.T) {
	started := history.NewWorkflowStartedEvent(map[string]any{"k": "v"}, "", "", "", "", 0)
	schedA := history.Event{ID: 2, Timestamp: time.Now(), ActivityScheduled: &history.ActivityScheduledEvent{ActivityID: "/0#a1"}}
	schedB := history.Event{ID: 3, Timestamp: time.Now(), ActivityScheduled: &history.ActivityScheduledEvent{ActivityID: "/1#a1"}}
	failA := history.Event{ID: 4, Timestamp: time.Now(), ActivityFailed: &history.ActivityFailedEvent{ScheduledEventID: 2, Error: "boom-A", Failure: &protocol.Failure{Message: "boom-A"}}}
	compB := history.Event{ID: 5, Timestamp: time.Now(), ActivityCompleted: &history.ActivityCompletedEvent{ScheduledEventID: 3, Output: map[string]any{"ok": true}}}

	cmds, err := probeExecFix(t, fixSrc, []history.Event{started, schedA, schedB, failA, compB})
	if err != nil {
		t.Fatalf("execute err: %v", err)
	}
	failed, msg := hasFailCmd(cmds)
	if !failed {
		t.Fatalf("lane0 A 失败、errA=err、return errA → 应产 CmdFailWorkflow,got %+v", cmds)
	}
	if msg != "boom-A" {
		t.Fatalf("CmdFailWorkflow.Error 应为 boom-A(lane 捕获的原始错误),got %q", msg)
	}
	t.Logf("✅ 修复生效:lane 捕获的 error 经 return 上浮为 CmdFailWorkflow(Error=%q)", msg)
}

// 对照:两 lane 都成功 → return nil → 仍产 CmdCompleteWorkflow(不误报失败)。
func TestFix_AllSucceed_StillCompletes(t *testing.T) {
	started := history.NewWorkflowStartedEvent(map[string]any{"k": "v"}, "", "", "", "", 0)
	schedA := history.Event{ID: 2, Timestamp: time.Now(), ActivityScheduled: &history.ActivityScheduledEvent{ActivityID: "/0#a1"}}
	schedB := history.Event{ID: 3, Timestamp: time.Now(), ActivityScheduled: &history.ActivityScheduledEvent{ActivityID: "/1#a1"}}
	compA := history.Event{ID: 4, Timestamp: time.Now(), ActivityCompleted: &history.ActivityCompletedEvent{ScheduledEventID: 2, Output: map[string]any{"ok": true}}}
	compB := history.Event{ID: 5, Timestamp: time.Now(), ActivityCompleted: &history.ActivityCompletedEvent{ScheduledEventID: 3, Output: map[string]any{"ok": true}}}

	cmds, err := probeExecFix(t, fixSrc, []history.Event{started, schedA, schedB, compA, compB})
	if err != nil {
		t.Fatalf("execute err: %v", err)
	}
	if failed, _ := hasFailCmd(cmds); failed {
		t.Fatalf("两 lane 都成功不应产 CmdFailWorkflow,got %+v", cmds)
	}
	if !hasCompleteWorkflow(cmds) {
		t.Fatalf("两 lane 都成功应产 CmdCompleteWorkflow,got %+v", cmds)
	}
	t.Logf("✅ 对照通过:全成功仍正常完成")
}

// workflow.Go 的 lane 出现运行时错误时本次 Workflow Task 失败，不能让 main 带着半截状态完成。
func TestLaneRuntimeErrorFailsWorkflowTask(t *testing.T) {
	const src = `package flows
import workflow "github.com/donutnomad/tenuto/sdk/workflow"
func W(ctx workflow.Context, input map[string]any) error {
	var m map[string]int
	workflow.Go(ctx, func(ctx workflow.Context) {
		m["x"] = 1
	})
	return nil
}
`
	started := history.NewWorkflowStartedEvent(map[string]any{"k": "v"}, "", "", "", "", 0)
	cmds, err := probeExecFix(t, src, []history.Event{started})
	if err == nil {
		t.Fatalf("lane runtime error must fail the workflow task, got commands %+v", cmds)
	}
	if hasCompleteWorkflow(cmds) {
		t.Fatalf("workflow must not complete after a lane runtime error: %+v", cmds)
	}
}
