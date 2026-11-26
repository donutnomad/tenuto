package interpreter

// SignalFuture 在第一次被消费时（Get 返回或 Selector 回调执行）才取走该名称的下一条 Signal：Selector 循环里每轮
// 重新调用 WaitSignal，上一轮没被选中的 Future 不占序号，之后到达的 Signal 由新一轮的 Future 收到。

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol/history"
)

const signalSelectorLoopSrc = `package flows

import (
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
)

type Out struct {
	Ticks    int
	Approved bool
}

func W(ctx workflow.Context, input map[string]any) (Out, error) {
	ticks := 0
	approved := false
	for round := 0; round < 100 && !approved; round++ {
		sel := workflow.NewSelector(ctx)
		sel.AddSignalFuture(ctx, workflow.WaitSignal(ctx, "approve"), func(f workflow.SignalFuture) {
			var v map[string]any
			_ = f.Get(ctx, &v)
			approved = true
		})
		sel.AddSignalFuture(ctx, workflow.WaitSignal(ctx, "tick"), func(f workflow.SignalFuture) {
			var v map[string]any
			_ = f.Get(ctx, &v)
			ticks++
		})
		sel.Select(ctx)
	}
	return Out{Ticks: ticks, Approved: approved}, nil
}
`

func TestSignalSelectorLoopReissuingWaitSignalReceivesLaterSignal(t *testing.T) {
	add, wft := signalHistory(t, signalSelectorLoopSrc)
	add(history.NewWorkflowStartedEvent(nil, "", "", "", "", 0))
	wft()
	add(history.NewWorkflowSignaledEvent("tick", map[string]any{}))
	wft() // 第一轮 Select 取到 tick；第二轮重新调用 WaitSignal("approve")
	add(history.NewWorkflowSignaledEvent("approve", map[string]any{}))
	commands := wft() // History 里第一条也是唯一一条 approve 已到达
	for _, command := range commands {
		if command.Type == history.CmdCompleteWorkflow {
			return
		}
	}
	t.Fatalf("approve is in History but the workflow is still waiting (commands=%+v)", commands)
}

// signalHistory 编译 src，返回追加事件与执行一个 Workflow Task 的函数。每个 Workflow Task 都从头重放整份 History。
func signalHistory(t *testing.T, src string) (add func(history.Event), wft func() []history.Command) {
	t.Helper()
	nodes := compileSource(t, src)
	now := time.Date(2026, 11, 10, 8, 0, 0, 0, time.UTC)
	var events []history.Event
	add = func(e history.Event) {
		e.ID = int64(len(events) + 1)
		now = now.Add(time.Second)
		e.Timestamp = now
		events = append(events, e)
	}
	wft = func() []history.Command {
		t.Helper()
		add(history.Event{WorkflowTaskScheduled: &history.WorkflowTaskScheduledEvent{}})
		add(history.Event{WorkflowTaskStarted: &history.WorkflowTaskStartedEvent{ScheduledEventID: int64(len(events))}})
		commands, err := NewWorkflowExecutor(nodes, events).Execute()
		if err != nil {
			t.Fatal(err)
		}
		add(history.Event{WorkflowTaskCompleted: &history.WorkflowTaskCompletedEvent{}})
		return commands
	}
	return add, wft
}

const signalConsumeOrderSrc = `package flows

import (
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
)

type Out struct {
	First  string
	Second string
	Again  string
}

func W(ctx workflow.Context, input map[string]any) (Out, error) {
	early := workflow.WaitSignal(ctx, "x")
	late := workflow.WaitSignal(ctx, "x")
	var a, b, c map[string]any
	_ = late.Get(ctx, &a)
	_ = early.Get(ctx, &b)
	_ = late.Get(ctx, &c)
	return Out{First: a["v"].(string), Second: b["v"].(string), Again: c["v"].(string)}, nil
}
`

// Signal 按消费顺序分配：先 Get 的 Future 取第一条，与创建顺序无关；同一个 Future 再 Get 返回同一条。
func TestSignalFutureConsumesInGetOrder(t *testing.T) {
	add, wft := signalHistory(t, signalConsumeOrderSrc)
	add(history.NewWorkflowStartedEvent(nil, "", "", "", "", 0))
	add(history.NewWorkflowSignaledEvent("x", map[string]any{"v": "one"}))
	wft()
	add(history.NewWorkflowSignaledEvent("x", map[string]any{"v": "two"}))
	for _, command := range wft() {
		if command.Type == history.CmdCompleteWorkflow {
			if command.Output["First"] != "one" || command.Output["Second"] != "two" || command.Output["Again"] != "one" {
				t.Fatalf("output = %v, want First=one Second=two Again=one", command.Output)
			}
			return
		}
	}
	t.Fatal("workflow did not complete")
}

// 同一份 History 在 sticky 会话里逐个 Activation 执行，与每次冷重放得到相同的结果。
func TestSignalSelectorLoopInExecutionSession(t *testing.T) {
	session := NewExecutionSession(context.Background(), compileSource(t, signalSelectorLoopSrc), 1)
	t.Cleanup(func() { _ = session.Close() })
	if _, err := session.Restore(context.Background(), NewSliceHistoryIterator(nil)); err != nil {
		t.Fatal(err)
	}
	next := int64(1)
	activate := func(signal string) []history.Command {
		t.Helper()
		var events []history.Event
		if next == 1 {
			events = append(events, executionSessionEvent(next, history.NewWorkflowStartedEvent(nil, "", "", "", "", 0)))
			next++
		}
		if signal != "" {
			events = append(events, executionSessionEvent(next, history.NewWorkflowSignaledEvent(signal, map[string]any{})))
			next++
		}
		events = append(events,
			executionSessionEvent(next, history.Event{WorkflowTaskScheduled: &history.WorkflowTaskScheduledEvent{}}),
			executionSessionEvent(next+1, history.NewWorkflowTaskStartedEvent(next, "worker", "build", 0)))
		next += 2
		result, err := session.Activate(context.Background(), history.WorkflowActivation{
			ActivationID: fmt.Sprintf("activation-%d", next), ExecutionGeneration: 1, Events: events, ThroughEventID: next - 1,
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := session.Commit(history.WorkflowTaskCompletionAck{
			Accepted: true, CommittedThroughEventID: next,
			CommittedEvents:     []history.Event{executionSessionEvent(next, history.NewWorkflowTaskCompletedEvent(0, "worker"))},
			ActivationID:        result.ActivationID,
			CommandDigest:       result.CommandDigest,
			ExecutionGeneration: 1,
		}); err != nil {
			t.Fatal(err)
		}
		next++
		return result.Commands
	}
	activate("")
	activate("tick")
	for _, command := range activate("approve") {
		if command.Type == history.CmdCompleteWorkflow {
			if command.Output["Approved"] != true || fmt.Sprint(command.Output["Ticks"]) != "1" {
				t.Fatalf("output = %v, want Approved=true Ticks=1", command.Output)
			}
			return
		}
	}
	t.Fatal("approve is in History but the session is still waiting")
}
