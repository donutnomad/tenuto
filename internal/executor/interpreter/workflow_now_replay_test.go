package interpreter

// workflow.Now 在一个 Workflow Task 里取该任务 WorkflowTaskStarted 的时间：live 执行（sticky 会话或非 sticky 的
// 全量路径）与之后从 History 冷重放读到同一个值。提交回执里的 WorkflowTaskCompleted 与命令事件带的是提交时刻，
// 不改变这个值。

import (
	"context"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/protocol/history"
)

const workflowNowSrc = `package flows

import (
	"time"

	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/app/activities"
)

type Out struct {
	StartMs int64
	Late    bool
}

func W(ctx workflow.Context, input map[string]any) (Out, error) {
	start := workflow.Now(ctx)
	deadline := start.Add(1500 * time.Millisecond)
	var out map[string]any
	if err := workflow.ExecuteActivity(ctx, activities.Charge, map[string]any{"start": start.UnixMilli()}, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &out); err != nil {
		return Out{}, err
	}
	return Out{StartMs: start.UnixMilli(), Late: workflow.Now(ctx).After(deadline)}, nil
}
`

var nowTestStart = time.Date(2026, 11, 10, 8, 0, 0, 0, time.UTC)

func nowTestEvent(id int64, offset time.Duration, event history.Event) history.Event {
	event.ID = id
	event.Timestamp = nowTestStart.Add(offset)
	return event
}

func nowTestCompleted(id int64, offset time.Duration, scheduledEventID, seenThroughEventID int64) history.Event {
	event := history.NewWorkflowTaskCompletedEvent(scheduledEventID, "worker-a")
	event.WorkflowTaskCompleted.SeenThroughEventID = seenThroughEventID
	return nowTestEvent(id, offset, event)
}

// nowTestFirstTask 是第一个 Workflow Task：00:00 started，Worker 用了 1s 才提交（00:01）。
func nowTestFirstTask() []history.Event {
	return []history.Event{
		nowTestEvent(1, 0, history.NewWorkflowStartedEvent(map[string]any{"k": "v"}, "", "", "", "", 0)),
		nowTestEvent(2, 0, history.NewWorkflowTaskScheduledEvent(0)),
		nowTestEvent(3, 0, history.NewWorkflowTaskStartedEvent(2, "worker-a", "", 0)),
	}
}

// nowTestHistory 返回第一个 Workflow Task 的提交事件与第二个 Workflow Task（Activity 在 00:02 完成）的事件。
func nowTestHistory(activity history.Command) (commit, second []history.Event) {
	commit = []history.Event{
		nowTestCompleted(4, time.Second, 2, 3),
		nowTestEvent(5, time.Second, history.NewActivityScheduledEvent(history.ActivityScheduledEvent{ActivityID: activity.ActivityID, ActivityType: activity.ActivityType})),
	}
	second = []history.Event{
		nowTestEvent(6, 2*time.Second, history.NewActivityCompletedEvent(history.ActivityCompletedEvent{ActivityID: activity.ActivityID, ActivityType: activity.ActivityType, ScheduledEventID: 5, Output: map[string]any{}})),
		nowTestEvent(7, 2*time.Second, history.NewWorkflowTaskScheduledEvent(0)),
		nowTestEvent(8, 2*time.Second, history.NewWorkflowTaskStartedEvent(7, "worker-a", "", 0)),
	}
	return commit, second
}

func TestWorkflowNowMatchesLiveAndReplayWithoutSticky(t *testing.T) {
	nodes := compileSource(t, workflowNowSrc)
	first := nowTestFirstTask()
	live, err := NewWorkflowExecutor(nodes, first).Execute()
	if err != nil {
		t.Fatal(err)
	}
	if len(live) != 1 || live[0].Type != history.CmdScheduleActivity {
		t.Fatalf("first task commands = %+v", live)
	}
	if got := commandInt64(t, live[0].Input["start"]); got != nowTestStart.UnixMilli() {
		t.Fatalf("live Now = %d, want WorkflowTaskStarted time %d", got, nowTestStart.UnixMilli())
	}
	// 非 sticky Worker 的第二个 Workflow Task 从完整 History 重放第一个任务。
	commit, second := nowTestHistory(live[0])
	full := append(append(append([]history.Event{}, first...), commit...), second...)
	commands, err := NewWorkflowExecutor(nodes, full).Execute()
	if err != nil {
		t.Fatal(err)
	}
	assertNowOutput(t, completedOutput(t, commands))
}

func TestWorkflowNowMatchesLiveAndReplayWithSticky(t *testing.T) {
	nodes := compileSource(t, workflowNowSrc)
	session := restoredNowSession(t, nodes)
	first := nowTestFirstTask()
	result := activateNowSession(t, session, "a1", first)
	if len(result.Commands) != 1 || commandInt64(t, result.Commands[0].Input["start"]) != nowTestStart.UnixMilli() {
		t.Fatalf("first task commands = %+v", result.Commands)
	}
	commit, second := nowTestHistory(result.Commands[0])
	commitNowSession(t, session, result, commit)
	stickyOut := completedOutput(t, activateNowSession(t, session, "a2", second).Commands)
	assertNowOutput(t, stickyOut)

	// Worker 重启之后从完整 History 冷重放，得到同样的输出。
	full := append(append(append([]history.Event{}, first...), commit...), second...)
	commands, err := NewWorkflowExecutor(nodes, full).Execute()
	if err != nil {
		t.Fatal(err)
	}
	assertNowOutput(t, completedOutput(t, commands))
}

const workflowNowBranchSrc = `package flows

import (
	"time"

	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/app/activities"
)

func W(ctx workflow.Context, input map[string]any) error {
	deadline := workflow.Now(ctx).Add(1500 * time.Millisecond)
	var out map[string]any
	if err := workflow.ExecuteActivity(ctx, activities.Charge, input, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &out); err != nil {
		return err
	}
	if workflow.Now(ctx).After(deadline) {
		return workflow.ExecuteActivity(ctx, activities.Refund, input, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &out)
	}
	return workflow.ExecuteActivity(ctx, activities.Ship, input, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &out)
}
`

// sticky Worker 按 live 的 Now 选了分支并提交；Worker 重启之后从 History 冷重放，走同一个分支，不报非确定性。
func TestWorkflowNowBranchReplaysAfterWorkerRestart(t *testing.T) {
	nodes := compileSource(t, workflowNowBranchSrc)
	session := restoredNowSession(t, nodes)
	first := nowTestFirstTask()
	result := activateNowSession(t, session, "a1", first)
	commit, second := nowTestHistory(result.Commands[0])
	commitNowSession(t, session, result, commit)
	branch := activateNowSession(t, session, "a2", second).Commands
	if len(branch) != 1 || branch[0].ActivityType != "activities.Refund" {
		t.Fatalf("second task commands = %+v, want Refund (2s after start > 1.5s)", branch)
	}
	full := append(append(append([]history.Event{}, first...), commit...), second...)
	full = append(full,
		nowTestCompleted(9, 2100*time.Millisecond, 7, 8),
		nowTestEvent(10, 2100*time.Millisecond, history.NewActivityScheduledEvent(history.ActivityScheduledEvent{ActivityID: branch[0].ActivityID, ActivityType: branch[0].ActivityType})),
		nowTestEvent(11, 3*time.Second, history.NewActivityCompletedEvent(history.ActivityCompletedEvent{ActivityID: branch[0].ActivityID, ActivityType: branch[0].ActivityType, ScheduledEventID: 10, Output: map[string]any{}})),
		nowTestEvent(12, 3*time.Second, history.NewWorkflowTaskScheduledEvent(0)),
		nowTestEvent(13, 3*time.Second, history.NewWorkflowTaskStartedEvent(12, "worker-b", "", 0)),
	)
	if _, err := NewWorkflowExecutor(nodes, full).Execute(); err != nil {
		t.Fatalf("cold replay of the sticky worker's history: %v", err)
	}
}

func assertNowOutput(t *testing.T, out map[string]any) {
	t.Helper()
	if got := commandInt64(t, out["StartMs"]); got != nowTestStart.UnixMilli() {
		t.Fatalf("StartMs = %d, want first WorkflowTaskStarted time %d", got, nowTestStart.UnixMilli())
	}
	if out["Late"] != true {
		t.Fatalf("Late = %v, want true (second task started 2s after the first)", out["Late"])
	}
}

func restoredNowSession(t *testing.T, nodes *program.WorkflowConfig) *workflowExecutionSession {
	t.Helper()
	session := NewExecutionSession(context.Background(), nodes, 1).(*workflowExecutionSession)
	t.Cleanup(func() { _ = session.Close() })
	if _, err := session.Restore(context.Background(), NewSliceHistoryIterator(nil)); err != nil {
		t.Fatal(err)
	}
	return session
}

func activateNowSession(t *testing.T, session *workflowExecutionSession, id string, events []history.Event) history.ActivationResult {
	t.Helper()
	result, err := session.Activate(context.Background(), history.WorkflowActivation{
		ActivationID: id, ExecutionGeneration: 1, Events: events, ThroughEventID: events[len(events)-1].ID,
	})
	if err != nil {
		t.Fatalf("activate %s: %v", id, err)
	}
	return result
}

func commitNowSession(t *testing.T, session *workflowExecutionSession, result history.ActivationResult, committed []history.Event) {
	t.Helper()
	if err := session.Commit(history.WorkflowTaskCompletionAck{
		Accepted:                true,
		ActivationID:            result.ActivationID,
		CommandDigest:           result.CommandDigest,
		ExecutionGeneration:     result.ExecutionGeneration,
		CommittedEvents:         committed,
		CommittedThroughEventID: committed[len(committed)-1].ID,
	}); err != nil {
		t.Fatalf("commit %s: %v", result.ActivationID, err)
	}
}

func completedOutput(t *testing.T, commands []history.Command) map[string]any {
	t.Helper()
	for _, command := range commands {
		if command.Type == history.CmdCompleteWorkflow {
			return command.Output
		}
	}
	t.Fatalf("no CompleteWorkflow in %+v", commands)
	return nil
}

func commandInt64(t *testing.T, value any) int64 {
	t.Helper()
	switch v := value.(type) {
	case interface{ Int64() (int64, error) }:
		n, err := v.Int64()
		if err != nil {
			t.Fatal(err)
		}
		return n
	case int64:
		return v
	case int:
		return int64(v)
	case float64:
		return int64(v)
	}
	t.Fatalf("not a number: %#v (%T)", value, value)
	return 0
}
