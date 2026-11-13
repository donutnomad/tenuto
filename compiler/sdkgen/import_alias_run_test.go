package sdkgen_test

import (
	"strings"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/compiler/sdkgen"
	"github.com/donutnomad/tenuto/internal/executor/interpreter"
	historypkg "github.com/donutnomad/tenuto/protocol/history"
)

// sdk/workflow 以任意别名导入时，生成的程序在解释器里与默认名 workflow 一样运行：
// workflow 包的函数（Sleep、GetInfo、ExecuteActivity）、类型（Context、ActivityOptions）与方法都能解析。
func TestWorkflowImportAliasRunsInInterpreter(t *testing.T) {
	src := `package flows

import (
	"time"

	wf "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/activities"
)

func W(ctx wf.Context, input map[string]any) (map[string]any, error) {
	if err := wf.Sleep(ctx, time.Second); err != nil {
		return nil, err
	}
	info := wf.GetInfo(ctx)
	var out map[string]any
	if err := wf.ExecuteActivity(ctx, activities.Reserve, input, wf.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &out); err != nil {
		return nil, err
	}
	return map[string]any{"length": info.GetCurrentHistoryLength(), "id": out["id"]}, nil
}
`
	result, err := sdkgen.GenerateFromSource([]byte(src), sdkgen.Options{WorkflowID: "alias-run", FuncName: "W", Filename: "alias.go"})
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	wf := result.Workflow
	events := []historypkg.Event{historypkg.NewWorkflowStartedEvent(map[string]any{}, "", "", "alias-run", "", 0)}
	events[0].ID = 1
	add := func(event historypkg.Event) {
		event.ID = int64(len(events) + 1)
		event.Timestamp = time.Unix(1_800_000_000, 0)
		events = append(events, event)
	}

	commands, err := interpreter.NewWorkflowExecutor(wf, events).Execute()
	if err != nil {
		t.Fatal(err)
	}
	if len(commands) != 1 || commands[0].Type != historypkg.CmdScheduleTimer {
		t.Fatalf("first task: want a timer, got %+v", commands)
	}
	add(historypkg.Event{WorkflowTaskCompleted: &historypkg.WorkflowTaskCompletedEvent{}})
	add(historypkg.NewTimerScheduledEvent(commands[0].TimerID, nil, commands[0].Delay.Milliseconds()))
	add(historypkg.NewTimerFiredEvent(commands[0].TimerID, int64(len(events))))

	commands, err = interpreter.NewWorkflowExecutor(wf, events).Execute()
	if err != nil {
		t.Fatal(err)
	}
	if len(commands) != 1 || commands[0].Type != historypkg.CmdScheduleActivity {
		t.Fatalf("second task: want an activity, got %+v", commands)
	}
	scheduled := commands[0]
	add(historypkg.Event{WorkflowTaskCompleted: &historypkg.WorkflowTaskCompletedEvent{}})
	add(historypkg.NewActivityScheduledEvent(historypkg.ActivityScheduledEvent{ActivityID: scheduled.ActivityID, ActivityType: scheduled.ActivityType}))
	add(historypkg.Event{ActivityCompleted: &historypkg.ActivityCompletedEvent{ActivityID: scheduled.ActivityID, ScheduledEventID: int64(len(events)), ActivityType: scheduled.ActivityType, Output: map[string]any{"id": "r-1"}}})

	commands, err = interpreter.NewWorkflowExecutor(wf, events).Execute()
	if err != nil {
		t.Fatal(err)
	}
	if len(commands) != 1 || commands[0].Type != historypkg.CmdCompleteWorkflow {
		t.Fatalf("third task: want completion, got %+v", commands)
	}
	if commands[0].Output["length"] == nil || commands[0].Output["id"] != "r-1" {
		t.Fatalf("output = %+v", commands[0].Output)
	}
}

// 以别名导入时，文件里另有名为 workflow 的标识符，改写成默认包名会改变语义：生成期带 file:line 拒绝。
func TestWorkflowImportAliasRejectsWorkflowIdentifier(t *testing.T) {
	src := `package flows

import wf "github.com/donutnomad/tenuto/sdk/workflow"

func W(ctx wf.Context, input map[string]any) error {
	workflow := "x"
	_ = workflow
	return wf.Sleep(ctx, 0)
}
`
	_, err := sdkgen.GenerateFromSource([]byte(src), sdkgen.Options{WorkflowID: "alias-conflict", FuncName: "W", Filename: "alias.go"})
	if err == nil || !strings.Contains(err.Error(), "alias.go:6:") || !strings.Contains(err.Error(), "标识符 workflow") {
		t.Fatalf("err = %v, want a diagnostic at alias.go:6", err)
	}
}
