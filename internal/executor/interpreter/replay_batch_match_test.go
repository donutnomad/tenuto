package interpreter

// 重放按 Workflow Task 逐批比对命令：新代码把一条命令挪到另一个 Workflow Task（例如把并行的两个 Activity 改成串行，
// 或反过来），即使命令的总顺序不变，旧 Run 在新 build 上重放也报非确定性。

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/internal/executor"
	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/protocol/history"
)

// v1：Reserve 与 Charge 并行调度。
const parallelReserveChargeSrc = `package flows

import (
	"time"

	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/app/activities"
)

func W(ctx workflow.Context, input map[string]any) (string, error) {
	opts := workflow.ActivityOptions{StartToCloseTimeout: time.Minute}
	fr := workflow.ExecuteActivity(ctx, activities.Reserve, input, opts)
	fc := workflow.ExecuteActivity(ctx, activities.Charge, input, opts)
	var r, c map[string]any
	if err := fr.Get(ctx, &r); err != nil {
		return "", err
	}
	if err := fc.Get(ctx, &c); err != nil {
		return "", err
	}
	return "v1", nil
}
`

// v2：先 Reserve，成功之后才 Charge（Charge 的输入依赖 Reserve 的结果）。没有用 GetVersion。
const sequentialReserveChargeSrc = `package flows

import (
	"time"

	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/app/activities"
)

func W(ctx workflow.Context, input map[string]any) (string, error) {
	opts := workflow.ActivityOptions{StartToCloseTimeout: time.Minute}
	var r map[string]any
	if err := workflow.ExecuteActivity(ctx, activities.Reserve, input, opts).Get(ctx, &r); err != nil {
		return "", err
	}
	var c map[string]any
	if err := workflow.ExecuteActivity(ctx, activities.Charge, r, opts).Get(ctx, &c); err != nil {
		return "", err
	}
	return "v2", nil
}
`

// runActivityWorkflow 用 build 执行 Run：每个 Workflow Task 把上一批调度的 Activity 按调度顺序依次完成一个，
// 直到 Run 结束，返回完整 History。
func runActivityWorkflow(t *testing.T, build *program.WorkflowConfig) []history.Event {
	t.Helper()
	now := time.Date(2026, 11, 27, 8, 0, 0, 0, time.UTC)
	var events []history.Event
	add := func(e history.Event) history.Event {
		e.ID = int64(len(events) + 1)
		e.Timestamp = now.Add(time.Duration(e.ID) * time.Second)
		events = append(events, e)
		return e
	}
	add(history.NewWorkflowStartedEvent(map[string]any{"order": "o-1"}, "", "", "", "", 0))
	type pending struct {
		activityID  string
		scheduledID int64
	}
	var open []pending
	for range 10 {
		scheduled := add(history.NewWorkflowTaskScheduledEvent(0))
		add(history.NewWorkflowTaskStartedEvent(scheduled.ID, "w", "", 0))
		commands, err := NewWorkflowExecutor(build, events).Execute()
		if err != nil {
			t.Fatal(err)
		}
		add(history.NewWorkflowTaskCompletedEvent(scheduled.ID, "w"))
		for _, command := range commands {
			switch command.Type {
			case history.CmdScheduleActivity:
				e := add(history.NewActivityScheduledEvent(history.ActivityScheduledEvent{
					ActivityID: command.ActivityID, ActivityType: command.ActivityType, Input: command.Input,
				}))
				open = append(open, pending{command.ActivityID, e.ID})
			case history.CmdCompleteWorkflow:
				add(history.NewWorkflowCompletedEvent(command.Output))
				return events
			}
		}
		if len(open) == 0 {
			t.Fatalf("Run 没有可完成的 Activity，History=%+v", events)
		}
		add(history.NewActivityCompletedEvent(history.ActivityCompletedEvent{ActivityID: open[0].activityID, ScheduledEventID: open[0].scheduledID, Output: map[string]any{"ok": true}}))
		open = open[1:]
	}
	t.Fatal("Run 没有在 10 个 Workflow Task 内结束")
	return nil
}

// replayLastTask 去掉最后一个 Workflow Task（Scheduled、Started、Completed 与它的命令事件），补上新的 Workflow Task，用 build 重放。
func replayLastTask(build *program.WorkflowConfig, events []history.Event) error {
	cut := len(events)
	for i, event := range events {
		if event.WorkflowTaskCompleted != nil {
			cut = i
		}
	}
	replay := append([]history.Event(nil), events[:cut-2]...)
	scheduled := history.NewWorkflowTaskScheduledEvent(0)
	scheduled.ID = int64(len(replay) + 1)
	started := history.NewWorkflowTaskStartedEvent(scheduled.ID, "w", "", 0)
	started.ID = scheduled.ID + 1
	_, err := NewWorkflowExecutor(build, append(replay, scheduled, started)).Execute()
	return err
}

func requireNondeterministicInTask(t *testing.T, err error, task string) {
	t.Helper()
	var nondeterministic executor.NonDeterministicError
	if !errors.As(err, &nondeterministic) || !strings.Contains(err.Error(), task) {
		t.Fatalf("err = %v, want nondeterministic error in %s", err, task)
	}
}

// v1 在第一个 Workflow Task 里并行调度 Reserve 与 Charge；v2 在第一个 Workflow Task 只调度 Reserve。v2 重放 v1 的
// History 时第一个 Workflow Task 少了 Charge。
func TestCommandMovedToLaterWorkflowTaskIsNondeterministic(t *testing.T) {
	events := runActivityWorkflow(t, compileSource(t, parallelReserveChargeSrc))
	err := replayLastTask(compileSource(t, sequentialReserveChargeSrc), events)
	requireNondeterministicInTask(t, err, "Workflow Task 1 ")
	if !strings.Contains(err.Error(), "missing replay command") {
		t.Fatalf("err = %v, want missing replay command", err)
	}
}

// v2 在第二个 Workflow Task 才调度 Charge；v1 重放 v2 的 History 时第一个 Workflow Task 多出 Charge。
func TestCommandMovedToEarlierWorkflowTaskIsNondeterministic(t *testing.T) {
	events := runActivityWorkflow(t, compileSource(t, sequentialReserveChargeSrc))
	err := replayLastTask(compileSource(t, parallelReserveChargeSrc), events)
	requireNondeterministicInTask(t, err, "Workflow Task 1 ")
	if !strings.Contains(err.Error(), "extra replay command") {
		t.Fatalf("err = %v, want extra replay command", err)
	}
}

// 同一个 build 重放自己的 History 不报错。
func TestReplaySameBuildMatchesPerWorkflowTask(t *testing.T) {
	for _, src := range []string{parallelReserveChargeSrc, sequentialReserveChargeSrc} {
		build := compileSource(t, src)
		if err := replayLastTask(build, runActivityWorkflow(t, build)); err != nil {
			t.Fatal(err)
		}
	}
}
