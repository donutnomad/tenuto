package interpreter

import (
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol/history"
)

// ============================================
// 主函数返回即结束 Workflow
// ============================================
// 测试策略:
// - Happy Path: main 返回值 / 返回 error 时，仍阻塞在 signal、timer、Activity 上的 workflow.Go 协程被放弃，
//   本次 Workflow Task 直接产出 CmdCompleteWorkflow / CmdFailWorkflow
// - Edge Case:  main 返回前同批产出的命令（Activity 调度）保留在终态命令之前
// - Edge Case:  main 仍阻塞时不结束
// - Replay:     新语义产出的历史（终态事件紧跟 main 返回的批）重放后 query 可读、不再产命令
// ============================================

const mainReturnBlockedSignalLaneSrc = `package flows
import (
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
)
func W(ctx workflow.Context, input map[string]any) (map[string]any, error) {
	workflow.Go(ctx, func(ctx workflow.Context) {
		workflow.WaitSignal(ctx, "never").Get(ctx, nil)
	})
	return map[string]any{"done": true}, nil
}
`

const mainReturnErrorBlockedTimerLaneSrc = `package flows
import (
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
)
func W(ctx workflow.Context, input map[string]any) error {
	workflow.Go(ctx, func(ctx workflow.Context) {
		workflow.Sleep(ctx, 86400000000000)
	})
	return workflow.NewApplicationError("main failed", "MainFailed")
}
`

const mainReturnPendingActivityLaneSrc = `package flows
import (
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/activities"
)
func W(ctx workflow.Context, input map[string]any) (map[string]any, error) {
	workflow.Go(ctx, func(ctx workflow.Context) {
		var o map[string]any
		workflow.ExecuteActivity(ctx, activities.A, input).Get(ctx, &o)
	})
	return map[string]any{"done": true}, nil
}
`

const mainBlockedWithLaneSrc = `package flows
import (
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
)
func W(ctx workflow.Context, input map[string]any) (map[string]any, error) {
	workflow.Go(ctx, func(ctx workflow.Context) {
		workflow.WaitSignal(ctx, "never").Get(ctx, nil)
	})
	workflow.WaitSignal(ctx, "go").Get(ctx, nil)
	return map[string]any{"done": true}, nil
}
`

func mainReturnStarted() history.Event {
	started := history.NewWorkflowStartedEvent(map[string]any{"k": "v"}, "", "", "", "", 0)
	started.ID = 1
	return started
}

// Happy Path: main 返回值，协程阻塞在永不到达的 signal 上 → 本批直接完成，输出是 main 的返回值。
func TestMainReturn_CompletesWhileLaneBlockedOnSignal(t *testing.T) {
	cmds, err := probeExecFix(t, mainReturnBlockedSignalLaneSrc, []history.Event{mainReturnStarted()})
	if err != nil {
		t.Fatal(err)
	}
	cmd, ok := findCompleteCmd(cmds)
	if !ok || len(cmds) != 1 {
		t.Fatalf("main returned: want exactly CmdCompleteWorkflow, got %+v", cmds)
	}
	if cmd.Output["done"] != true {
		t.Fatalf("output = %v, want main's return value", cmd.Output)
	}
}

// Happy Path: main 返回 error，协程在 Sleep(24h) → 本批直接失败，timer 调度命令排在失败命令之前。
func TestMainReturn_FailsWhileLaneSleeps(t *testing.T) {
	cmds, err := probeExecFix(t, mainReturnErrorBlockedTimerLaneSrc, []history.Event{mainReturnStarted()})
	if err != nil {
		t.Fatal(err)
	}
	failed, message := hasFailCmd(cmds)
	if !failed || message != "main failed" {
		t.Fatalf("main returned an error: want CmdFailWorkflow(main failed), got %+v", cmds)
	}
	if len(cmds) != 2 || cmds[0].Type != history.CmdScheduleTimer || cmds[0].Delay != 24*time.Hour || cmds[1].Type != history.CmdFailWorkflow {
		t.Fatalf("want [ScheduleTimer(24h), FailWorkflow], got %+v", cmds)
	}
}

// Edge Case: 协程里调度的 Activity 尚未完成 → main 返回后仍然完成，调度命令保留在完成命令之前。
func TestMainReturn_CompletesWhileLaneActivityPending(t *testing.T) {
	cmds, err := probeExecFix(t, mainReturnPendingActivityLaneSrc, []history.Event{mainReturnStarted()})
	if err != nil {
		t.Fatal(err)
	}
	if len(cmds) != 2 || cmds[0].Type != history.CmdScheduleActivity || cmds[1].Type != history.CmdCompleteWorkflow {
		t.Fatalf("want [ScheduleActivity, CompleteWorkflow], got %+v", cmds)
	}
}

// Edge Case: main 自己还阻塞 → 不结束；main 的 signal 到达后的那一批才完成。
func TestMainReturn_BlockedMainDoesNotComplete(t *testing.T) {
	cmds, err := probeExecFix(t, mainBlockedWithLaneSrc, []history.Event{mainReturnStarted()})
	if err != nil {
		t.Fatal(err)
	}
	if len(cmds) != 0 {
		t.Fatalf("main is blocked: want no commands, got %+v", cmds)
	}
	events := []history.Event{
		mainReturnStarted(),
		{ID: 2, Timestamp: time.Now(), WorkflowTaskCompleted: &history.WorkflowTaskCompletedEvent{}},
		{ID: 3, Timestamp: time.Now(), WorkflowSignaled: &history.WorkflowSignaledEvent{Name: "go"}},
	}
	cmds, err = probeExecFix(t, mainBlockedWithLaneSrc, events)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := findCompleteCmd(cmds); !ok || len(cmds) != 1 {
		t.Fatalf("main returned after its signal: want CmdCompleteWorkflow, got %+v", cmds)
	}
}

// commandsBeforeCompletion 断言 commands 以 CmdCompleteWorkflow 结尾（main 在本批返回），
// 返回它之前的命令。
func commandsBeforeCompletion(t *testing.T, commands []history.Command) []history.Command {
	t.Helper()
	if len(commands) == 0 || commands[len(commands)-1].Type != history.CmdCompleteWorkflow {
		t.Fatalf("main returned in this batch: want the commands to end with CmdCompleteWorkflow, got %+v", commands)
	}
	return commands[:len(commands)-1]
}
