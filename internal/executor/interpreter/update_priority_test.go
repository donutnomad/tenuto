package interpreter

import (
	"context"
	"testing"

	"github.com/donutnomad/tenuto/compiler/sdkgen"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
)

const updatePrioritySrc = `package flows

import workflow "github.com/donutnomad/tenuto/sdk/workflow"

type Sig struct {
	V string ` + "`json:\"v\"`" + `
}

func W(ctx workflow.Context, input map[string]any) (map[string]any, error) {
	order := ""
	if err := workflow.SetQueryHandler(ctx, "order", func(ctx workflow.Context, _ map[string]any) map[string]any {
		return map[string]any{"order": order}
	}); err != nil {
		return nil, err
	}
	if err := workflow.SetUpdateHandler(ctx, "u", func(ctx workflow.Context, v string) (string, error) {
		order += "U" + v + "|"
		return order, nil
	}); err != nil {
		return nil, err
	}
	for i := 0; i < 3; i++ {
		var s Sig
		if err := workflow.WaitSignal(ctx, "s").Get(ctx, &s); err != nil {
			return nil, err
		}
		order += "S" + s.V + "|"
	}
	return map[string]any{"order": order}, nil
}
`

// 同一批到达的 Signal 与 Update：Update handler 先于 main 运行，多个 handler 按事件顺序运行
// （对标 Temporal Go SDK 的 SDKPriorityUpdateHandling）。冷重放、sticky 会话与 Query 三条路径的顺序一致。
func TestUpdateHandlersRunBeforeMainDifferential(t *testing.T) {
	result, err := sdkgen.GenerateFromSource([]byte(updatePrioritySrc), sdkgen.Options{WorkflowType: "update-priority", FuncName: "W", Filename: "w.go"})
	if err != nil {
		t.Fatal(err)
	}
	wf := result.Workflow
	signal := func(id int64, v string) history.Event {
		return executionSessionEvent(id, history.NewWorkflowSignaledEvent("s", map[string]any{"v": v}))
	}
	update := func(id int64, updateID, v string) history.Event {
		arg, err := protocol.NewJSONPayload(v)
		if err != nil {
			t.Fatal(err)
		}
		return executionSessionEvent(id, acceptedArgsEvent(id, "u", updateID, protocol.Payloads{arg}))
	}
	first := []history.Event{
		executionSessionEvent(1, history.NewWorkflowStartedEvent(map[string]any{}, "", "", "", "", 0)),
		executionSessionEvent(2, history.NewWorkflowTaskScheduledEvent(0)),
		executionSessionEvent(3, history.NewWorkflowTaskStartedEvent(2, "worker", "build", 0)),
	}
	firstCompleted := []history.Event{executionSessionEvent(4, history.NewWorkflowTaskCompletedEvent(2, "worker"))}
	second := []history.Event{
		signal(5, "a"), update(6, "u-1", "1"), update(7, "u-2", "2"), signal(8, "b"),
		executionSessionEvent(9, history.NewWorkflowTaskScheduledEvent(0)),
		executionSessionEvent(10, history.NewWorkflowTaskStartedEvent(9, "worker", "build", 0)),
	}
	prefix := append(append(append([]history.Event(nil), first...), firstCompleted...), second...)
	commands, err := NewWorkflowExecutor(wf, prefix).Execute()
	if err != nil {
		t.Fatal(err)
	}
	if len(commands) != 2 || commands[0].UpdateID != "u-1" || commands[1].UpdateID != "u-2" {
		t.Fatalf("second workflow task commands = %+v, want both update completions in event order", commands)
	}
	secondCompleted := []history.Event{executionSessionEvent(11, history.NewWorkflowTaskCompletedEvent(9, "worker"))}
	for i, command := range commands {
		secondCompleted = append(secondCompleted, executionSessionEvent(int64(12+i), history.Event{WorkflowUpdateCompleted: &history.WorkflowUpdateCompletedEvent{
			Name: command.Name, UpdateID: command.UpdateID, Outcome: command.UpdateOutcome,
		}}))
	}
	third := []history.Event{
		signal(14, "c"),
		executionSessionEvent(15, history.NewWorkflowTaskScheduledEvent(0)),
		executionSessionEvent(16, history.NewWorkflowTaskStartedEvent(15, "worker", "build", 0)),
	}
	const want = `{"order":"U1|U2|Sa|Sb|Sc|"}`

	all := append(append(append([]history.Event(nil), prefix...), secondCompleted...), third...)
	coldCommands, err := NewWorkflowExecutor(wf, all).Execute()
	if cold := terminalOutcome(t, coldCommands, err); cold != want {
		t.Errorf("cold replay = %s, want %s", cold, want)
	}

	session := NewExecutionSession(context.Background(), wf, 1)
	t.Cleanup(func() { _ = session.Close() })
	if _, err := session.Restore(context.Background(), NewSliceHistoryIterator(nil)); err != nil {
		t.Fatal(err)
	}
	var sticky string
	for i, step := range []struct {
		events    []history.Event
		through   int64
		committed []history.Event
	}{{first, 3, firstCompleted}, {second, 10, secondCompleted}, {third, 16, nil}} {
		id := string(rune('a' + i))
		activation, err := session.Activate(context.Background(), history.WorkflowActivation{ActivationID: id, ExecutionGeneration: 1, Events: step.events, ThroughEventID: step.through})
		if step.committed == nil {
			sticky = terminalOutcome(t, activation.Commands, err)
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if err := session.Commit(history.WorkflowTaskCompletionAck{
			Accepted: true, CommittedThroughEventID: step.committed[len(step.committed)-1].ID, CommittedEvents: step.committed,
			ActivationID: id, CommandDigest: activation.CommandDigest, ExecutionGeneration: 1,
		}); err != nil {
			t.Fatal(err)
		}
	}
	if sticky != want {
		t.Errorf("sticky = %s, want %s", sticky, want)
	}

	queried, failed, message := QueryEvaluator{}.EvaluateQuery(context.Background(), wf, append(append([]history.Event(nil), prefix...), secondCompleted...), "order", nil)
	if failed || queried["order"] != "U1|U2|Sa|Sb|" {
		t.Errorf("query = %v (failed=%v %s), want U1|U2|Sa|Sb|", queried, failed, message)
	}
}

// Workflow 开始的同一批到达的 Update：main 尚未登记 handler，update 排队；main 登记 handler 时立即执行，
// 排在 main 本次推进之后（对标 Temporal Go SDK 登记 handler 时处理排队的 update）。本批结束仍未登记的以 UnknownUpdate 失败。
func TestQueuedUpdateRunsWhenHandlerRegistered(t *testing.T) {
	result, err := sdkgen.GenerateFromSource([]byte(updatePrioritySrc), sdkgen.Options{WorkflowType: "update-priority", FuncName: "W", Filename: "w.go"})
	if err != nil {
		t.Fatal(err)
	}
	arg, err := protocol.NewJSONPayload("1")
	if err != nil {
		t.Fatal(err)
	}
	commands, err := NewWorkflowExecutor(result.Workflow, []history.Event{
		executionSessionEvent(1, history.NewWorkflowStartedEvent(map[string]any{}, "", "", "", "", 0)),
		acceptedArgsEvent(2, "u", "u-1", protocol.Payloads{arg}),
		acceptedArgsEvent(3, "missing", "u-2", protocol.Payloads{arg}),
	}).Execute()
	if err != nil {
		t.Fatal(err)
	}
	if len(commands) != 2 || commands[0].UpdateID != "u-1" || commands[0].UpdateOutcome.Success == nil ||
		commands[1].UpdateID != "u-2" || commands[1].UpdateOutcome.Failure == nil || commands[1].UpdateOutcome.Failure.Type != unknownUpdateFailureType {
		t.Fatalf("commands = %+v, want u-1 handled and u-2 rejected as unknown", commands)
	}
}
