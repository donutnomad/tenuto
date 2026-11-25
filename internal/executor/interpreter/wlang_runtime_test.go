package interpreter

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/compiler/sdkgen"
	programpkg "github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
)

func TestWLangRuntimeSchedulesActivityAndReplaysCompletion(t *testing.T) {
	result, err := sdkgen.GenerateFromSource([]byte(`package flows

import (
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/app/activities"
)

func OrderWorkflow(ctx workflow.Context, input map[string]any) error {
	var reserve map[string]any
	return workflow.ExecuteActivity(ctx, activities.ReserveInventory, map[string]any{"order_id": input["order_id"]}).Get(ctx, &reserve)
}
`), sdkgen.Options{WorkflowType: "order-flow", FuncName: "OrderWorkflow", Filename: "order_workflow.go"})
	if err != nil {
		t.Fatal(err)
	}
	wf := result.Workflow

	started := history.NewWorkflowStartedEvent(map[string]any{"order_id": "o-1"}, "", "", "", "", 0)
	commands, err := NewWorkflowExecutor(wf, []history.Event{started}).Execute()
	if err != nil {
		t.Fatal(err)
	}
	if len(commands) != 1 || commands[0].Type != history.CmdScheduleActivity {
		t.Fatalf("expected schedule command, got %+v", commands)
	}
	if commands[0].ActivityID != "#a1" || commands[0].ActivityType != "activities.ReserveInventory" {
		t.Fatalf("unexpected activity command: %+v", commands[0])
	}
	if commands[0].Input["order_id"] != "o-1" {
		t.Fatalf("unexpected activity input: %+v", commands[0].Input)
	}

	completed := history.Event{ID: 2, Timestamp: time.Now(), ActivityCompleted: &history.ActivityCompletedEvent{ScheduledEventID: 1, Output: map[string]any{"reserve_id": "r-1"}}}
	scheduled := history.Event{ID: 1, Timestamp: time.Now(), ActivityScheduled: &history.ActivityScheduledEvent{ActivityID: "#a1", Input: commands[0].Input}}
	commands, err = NewWorkflowExecutor(wf, []history.Event{started, scheduled, completed}).Execute()
	if err != nil {
		t.Fatal(err)
	}
	if len(commands) != 1 || commands[0].Type != history.CmdCompleteWorkflow {
		t.Fatalf("expected workflow completion after replay, got %+v", commands)
	}
}

func TestWLangRuntimeUsesExecutionContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	nodes := &programpkg.WorkflowConfig{
		Type: "program",
		Program: programpkg.NewWLangProgram([]any{
			map[string]any{"let": map[string]any{"ok": map[string]any{"literal": map[string]any{"type": "boolean", "value": true}}}},
		}),
	}
	_, err := NewWorkflowExecutorWithContext(ctx, nodes, []history.Event{
		history.NewWorkflowStartedEvent(nil, "", "", "", "", 0),
	}).Execute()
	if err == nil || !strings.Contains(err.Error(), "context canceled") {
		t.Fatalf("expected WLang execution context cancellation, got %v", err)
	}
}

func TestWLangRuntimeRunsSDKGeneratedThreeArgExecuteActivity(t *testing.T) {
	src := []byte(`package flows

import (
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/app/activities"
)

	func OrderWorkflow(ctx workflow.Context, input map[string]any) error {
		var reserve map[string]any
		err := workflow.ExecuteActivity(ctx, activities.ReserveInventory, input).Get(ctx, &reserve)
		if err != nil {
			return err
		}
		return nil
	}
	`)
	result, err := sdkgen.GenerateFromSource(src, sdkgen.Options{
		WorkflowType: "sdk-order",
		FuncName:     "OrderWorkflow",
		Filename:     "order_workflow.go",
	})
	if err != nil {
		t.Fatal(err)
	}
	nodes := result.Workflow

	commands, err := NewWorkflowExecutor(nodes, []history.Event{
		history.NewWorkflowStartedEvent(map[string]any{"order_id": "o-1"}, "", "", "", "", 0),
	}).Execute()
	if err != nil {
		t.Fatal(err)
	}
	if len(commands) != 1 || commands[0].Type != history.CmdScheduleActivity {
		t.Fatalf("expected schedule command, got %+v", commands)
	}
	if commands[0].ActivityID != "#a1" || commands[0].ActivityType != "activities.ReserveInventory" {
		t.Fatalf("unexpected sdk activity command: %+v", commands[0])
	}
}

func TestWLangRuntimeRunsSDKGeneratedActivityTaskQueueOption(t *testing.T) {
	src := []byte(`package flows

import (
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/app/activities"
)

func RoutedWorkflow(ctx workflow.Context, input map[string]any) error {
	var reserve map[string]any
	err := workflow.ExecuteActivity(ctx, activities.ReserveInventory, input, workflow.ActivityOptions{
		TaskQueue: "custodian",
	}).Get(ctx, &reserve)
	if err != nil {
		return err
	}
	return nil
}
`)
	result, err := sdkgen.GenerateFromSource(src, sdkgen.Options{
		WorkflowType: "sdk-routed-order",
		FuncName:     "RoutedWorkflow",
		Filename:     "routed_order_workflow.go",
	})
	if err != nil {
		t.Fatal(err)
	}
	nodes := result.Workflow

	commands, err := NewWorkflowExecutor(nodes, []history.Event{
		history.NewWorkflowStartedEvent(map[string]any{"order_id": "o-1"}, "", "", "", "", 0),
	}).Execute()
	if err != nil {
		t.Fatal(err)
	}
	if len(commands) != 1 || commands[0].Type != history.CmdScheduleActivity {
		t.Fatalf("expected schedule command, got %+v", commands)
	}
	if commands[0].TaskQueue != "custodian" {
		t.Fatalf("unexpected task queue: %+v", commands[0])
	}
}

func TestWLangRuntimeWaitSignalBlocksUntilHistoryContainsSignal(t *testing.T) {
	src := []byte(`package flows

import workflow "github.com/donutnomad/tenuto/sdk/workflow"

func PaymentWorkflow(ctx workflow.Context, input map[string]any) error {
	var signal map[string]any
	err := workflow.WaitSignal(ctx, "payment_received").Get(ctx, &signal)
	if err != nil {
		return err
	}
	return nil
}
`)
	result, err := sdkgen.GenerateFromSource(src, sdkgen.Options{
		WorkflowType: "sdk-payment",
		FuncName:     "PaymentWorkflow",
		Filename:     "payment_workflow.go",
	})
	if err != nil {
		t.Fatal(err)
	}
	nodes := result.Workflow

	started := history.NewWorkflowStartedEvent(map[string]any{"order_id": "o-1"}, "", "", "", "", 0)
	commands, err := NewWorkflowExecutor(nodes, []history.Event{started}).Execute()
	if err != nil {
		t.Fatal(err)
	}
	if len(commands) != 0 {
		t.Fatalf("expected signal wait to keep workflow open without commands, got %+v", commands)
	}

	signaled := history.Event{ID: 2, Timestamp: time.Now(), WorkflowSignaled: &history.WorkflowSignaledEvent{Name: "payment_received", Input: map[string]any{"amount": int64(42)}}}
	commands, err = NewWorkflowExecutor(nodes, []history.Event{started, signaled}).Execute()
	if err != nil {
		t.Fatal(err)
	}
	if len(commands) != 1 || commands[0].Type != history.CmdCompleteWorkflow {
		t.Fatalf("expected workflow completion after signal replay, got %+v", commands)
	}
}

func TestWLangRuntimeWaitSignalConsumesSameNameSignalsInOrder(t *testing.T) {
	src := []byte(`package flows

import workflow "github.com/donutnomad/tenuto/sdk/workflow"

func TwoPaymentsWorkflow(ctx workflow.Context, input map[string]any) error {
	var first map[string]any
	err := workflow.WaitSignal(ctx, "payment_received").Get(ctx, &first)
	if err != nil {
		return err
	}
	var second map[string]any
	err = workflow.WaitSignal(ctx, "payment_received").Get(ctx, &second)
	if err != nil {
		return err
	}
	return nil
}
`)
	result, err := sdkgen.GenerateFromSource(src, sdkgen.Options{
		WorkflowType: "sdk-two-payments",
		FuncName:     "TwoPaymentsWorkflow",
		Filename:     "two_payments_workflow.go",
	})
	if err != nil {
		t.Fatal(err)
	}
	nodes := result.Workflow

	started := history.NewWorkflowStartedEvent(nil, "", "", "", "", 0)
	firstSignal := history.Event{ID: 2, Timestamp: time.Now(), WorkflowSignaled: &history.WorkflowSignaledEvent{Name: "payment_received", Input: map[string]any{"seq": int64(1)}}}
	commands, err := NewWorkflowExecutor(nodes, []history.Event{started, firstSignal}).Execute()
	if err != nil {
		t.Fatal(err)
	}
	if len(commands) != 0 {
		t.Fatalf("expected second signal wait to keep workflow open, got %+v", commands)
	}

	secondSignal := history.Event{ID: 3, Timestamp: time.Now(), WorkflowSignaled: &history.WorkflowSignaledEvent{Name: "payment_received", Input: map[string]any{"seq": int64(2)}}}
	commands, err = NewWorkflowExecutor(nodes, []history.Event{started, firstSignal, secondSignal}).Execute()
	if err != nil {
		t.Fatal(err)
	}
	if len(commands) != 1 || commands[0].Type != history.CmdCompleteWorkflow {
		t.Fatalf("expected workflow completion after second signal replay, got %+v", commands)
	}
}

func TestWLangRuntimeUpdateHandlerCompletesAcceptedUpdateBeforeContinuing(t *testing.T) {
	src := []byte(`package flows

import workflow "github.com/donutnomad/tenuto/sdk/workflow"

	func RiskWorkflow(ctx workflow.Context, input map[string]any) error {
		handled := false
		err := workflow.SetUpdateHandler(ctx, "risk_approved", func(ctx workflow.Context, approver string) (string, error) {
			handled = true
			return "handled by " + approver, nil
		})
		if err != nil {
			return err
		}
		workflow.Await(ctx, func() bool { return handled })
		return nil
	}
	`)
	result, err := sdkgen.GenerateFromSource(src, sdkgen.Options{
		WorkflowType: "sdk-risk",
		FuncName:     "RiskWorkflow",
		Filename:     "risk_workflow.go",
	})
	if err != nil {
		t.Fatal(err)
	}
	nodes := result.Workflow

	started := history.NewWorkflowStartedEvent(nil, "", "", "", "", 0)

	// 没有 update 时 body 停在 Await，不产命令。
	commands, err := NewWorkflowExecutor(nodes, []history.Event{started}).Execute()
	if err != nil {
		t.Fatal(err)
	}
	if len(commands) != 0 {
		t.Fatalf("expected workflow to wait for the update, got %+v", commands)
	}

	approver, err := protocol.NewJSONPayload("alice")
	if err != nil {
		t.Fatal(err)
	}
	accepted := acceptedArgsEvent(2, "risk_approved", "update-1", protocol.Payloads{approver})
	commands, err = NewWorkflowExecutor(nodes, []history.Event{started, accepted}).Execute()
	if err != nil {
		t.Fatal(err)
	}
	// handler 完成 update，main 随后返回：同一批产出 update 完成命令与 Workflow 完成命令。
	commands = commandsBeforeCompletion(t, commands)
	if len(commands) != 1 || commands[0].Type != history.CmdCompleteWorkflowUpdate {
		t.Fatalf("expected accepted update to be completed by its handler, got %+v", commands)
	}
	if commands[0].UpdateID != "update-1" || commands[0].Name != "risk_approved" || commands[0].UpdateOutcome == nil || commands[0].UpdateOutcome.Success == nil || commands[0].Output != nil {
		t.Fatalf("unexpected update completion command: %+v", commands[0])
	}
	var output string
	if err := commands[0].UpdateOutcome.Success.Decode(&output); err != nil || output != "handled by alice" {
		t.Fatalf("update outcome: output=%v err=%v", output, err)
	}

	completed := history.Event{ID: 4, Timestamp: time.Now(), WorkflowUpdateCompleted: &history.WorkflowUpdateCompletedEvent{Name: "risk_approved", UpdateID: "update-1", Outcome: commands[0].UpdateOutcome}}
	commands, err = NewWorkflowExecutor(nodes, []history.Event{started, accepted, wftCompletedEvent(3), completed}).Execute()
	if err != nil {
		t.Fatal(err)
	}
	if len(commands) != 1 || commands[0].Type != history.CmdCompleteWorkflow {
		t.Fatalf("expected workflow completion after update replay, got %+v", commands)
	}
}

func TestWLangRuntimeSleepSchedulesTimerAndReplaysFire(t *testing.T) {
	src := []byte(`package flows

import workflow "github.com/donutnomad/tenuto/sdk/workflow"

func TimerWorkflow(ctx workflow.Context, input map[string]any) error {
	err := workflow.Sleep(ctx, 1000000)
	if err != nil {
		return err
	}
	return nil
}
`)
	result, err := sdkgen.GenerateFromSource(src, sdkgen.Options{
		WorkflowType: "sdk-timer",
		FuncName:     "TimerWorkflow",
		Filename:     "timer_workflow.go",
	})
	if err != nil {
		t.Fatal(err)
	}
	nodes := result.Workflow

	started := history.NewWorkflowStartedEvent(nil, "", "", "", "", 0)
	commands, err := NewWorkflowExecutor(nodes, []history.Event{started}).Execute()
	if err != nil {
		t.Fatal(err)
	}
	if len(commands) != 1 || commands[0].Type != history.CmdScheduleTimer {
		t.Fatalf("expected schedule timer command, got %+v", commands)
	}
	if commands[0].TimerID != "#t1" || commands[0].Delay != time.Millisecond {
		t.Fatalf("unexpected timer command: %+v", commands[0])
	}

	scheduled := history.Event{ID: 2, Timestamp: time.Now(), TimerScheduled: &history.TimerScheduledEvent{TimerID: "#t1", DelayInput: map[string]any{"delay_ns": int64(time.Millisecond)}}}
	commands, err = NewWorkflowExecutor(nodes, []history.Event{started, scheduled}).Execute()
	if err != nil {
		t.Fatal(err)
	}
	if len(commands) != 0 {
		t.Fatalf("expected scheduled timer to keep workflow open, got %+v", commands)
	}

	fired := history.Event{ID: 3, Timestamp: time.Now(), TimerFired: &history.TimerFiredEvent{TimerID: "#t1", ScheduledEventID: scheduled.ID}}
	commands, err = NewWorkflowExecutor(nodes, []history.Event{started, scheduled, fired}).Execute()
	if err != nil {
		t.Fatal(err)
	}
	if len(commands) != 1 || commands[0].Type != history.CmdCompleteWorkflow {
		t.Fatalf("expected workflow completion after timer fire replay, got %+v", commands)
	}
}

func TestWLangRuntimeGetVersionRecordsMarkerAndReplaysValue(t *testing.T) {
	src := []byte(`package flows

import workflow "github.com/donutnomad/tenuto/sdk/workflow"

func VersionedWorkflow(ctx workflow.Context, input map[string]any) error {
	version := workflow.GetVersion(ctx, "change-a", 1, 3)
	if version < 1 {
		return nil
	}
	return nil
}
`)
	result, err := sdkgen.GenerateFromSource(src, sdkgen.Options{
		WorkflowType: "sdk-versioned",
		FuncName:     "VersionedWorkflow",
		Filename:     "versioned_workflow.go",
	})
	if err != nil {
		t.Fatal(err)
	}
	nodes := result.Workflow

	// Happy Path（同步返回，阶段2）：首个 WFT（isReplay=false）GetVersion 首见 → maxSupported=3 +
	// recordMarker（就地写 marker）+ 版本索引 + 同步返回 3，body 跑到底，main 返回即在本轮完成。
	started := history.NewWorkflowStartedEvent(nil, "", "", "", "", 0)
	commands, err := NewWorkflowExecutor(nodes, []history.Event{started}).Execute()
	if err != nil {
		t.Fatal(err)
	}
	commands = commandsBeforeCompletion(t, commands)
	if len(commands) != 2 || commands[0].Type != history.CmdRecordMarker || commands[1].Type != history.CmdUpsertSearchAttributes {
		t.Fatalf("expected record marker and search attribute commands, got %+v", commands)
	}
	if commands[0].MarkerName != "GetVersion" || commands[0].MarkerID != "change-a" {
		t.Fatalf("unexpected version marker command: %+v", commands[0])
	}
	if commands[0].Input["version"] != int64(3) {
		t.Fatalf("unexpected version marker input: %+v", commands[0].Input)
	}

	// 确定性 replay：marker 灌回 history（含 WorkflowTaskCompleted 表示上一轮已决策，isReplay=true），
	// GetVersion marker 命中读回同一 version=3 → body 跑到底、无新命令、产 CmdCompleteWorkflow。
	taskCompleted := history.NewWorkflowTaskCompletedEvent(0, "")
	marker := history.Event{ID: 2, Timestamp: time.Now(), MarkerRecorded: &history.MarkerRecordedEvent{MarkerName: "GetVersion", MarkerID: "change-a", Value: map[string]any{"version": int64(3)}}}
	commands, err = NewWorkflowExecutor(nodes, []history.Event{started, taskCompleted, marker}).Execute()
	if err != nil {
		t.Fatal(err)
	}
	if len(commands) != 1 || commands[0].Type != history.CmdCompleteWorkflow {
		t.Fatalf("expected workflow completion after version marker replay, got %+v", commands)
	}
}

func TestWLangRuntimeSideEffectAndRandomValuesReplayMarkers(t *testing.T) {
	src := []byte(`package flows

import workflow "github.com/donutnomad/tenuto/sdk/workflow"

func DeterministicWorkflow(ctx workflow.Context, input map[string]any) error {
	var side map[string]any
	err := workflow.SideEffect(ctx, func(ctx workflow.Context) any {
		return map[string]any{"region": "sg"}
	}).Get(ctx, &side)
	if err != nil {
		return err
	}
	uuidValue := workflow.RandomUUID(ctx)
	randomValue := workflow.RandomInt(ctx, 100)
	err = workflow.SideEffect(ctx, func(ctx workflow.Context) any {
		return map[string]any{"uuid": uuidValue, "n": randomValue}
	}).Get(ctx, &side)
	if err != nil {
		return err
	}
	return nil
}
`)
	result, err := sdkgen.GenerateFromSource(src, sdkgen.Options{
		WorkflowType: "sdk-deterministic",
		FuncName:     "DeterministicWorkflow",
		Filename:     "deterministic_workflow.go",
	})
	if err != nil {
		t.Fatal(err)
	}
	nodes := result.Workflow

	// Happy Path（同步返回，阶段2）：首个 WFT（仅 WorkflowStarted，isReplay=false）内 4 处 R 类原语
	// 全部同轮算值 + 就地写 marker + 产 CmdRecordMarker，body 一口气跑到底 → 4 条 marker 命令，
	// 不再是旧模型"每 park 一次只产 1 条 marker、下轮读回"。
	started := history.NewWorkflowStartedEvent(nil, "", "", "", "", 0)
	commands, err := NewWorkflowExecutor(nodes, []history.Event{started}).Execute()
	if err != nil {
		t.Fatal(err)
	}
	commands = commandsBeforeCompletion(t, commands)
	if len(commands) != 4 {
		t.Fatalf("expected 4 marker commands in a single WFT, got %+v", commands)
	}
	for _, c := range commands {
		if c.Type != history.CmdRecordMarker {
			t.Fatalf("expected all commands to be RecordMarker, got %+v", commands)
		}
	}
	// 声明序：SideEffect(#se1) → RandomUUID(#ru1) → RandomInt(#ri1) → SideEffect(#se2)。
	// SideEffect 闭包返回值包在 {"value": ...} 里（对齐 RandomUUID 包装），id 由 laneKey 自动分配。
	se1Val, ok := commands[0].Input["value"].(map[string]any)
	if commands[0].MarkerName != "SideEffect" || commands[0].MarkerID != "#se1" || !ok || se1Val["region"] != "sg" {
		t.Fatalf("unexpected first marker: %+v", commands[0])
	}
	if commands[1].MarkerName != "RandomUUID" || commands[1].MarkerID != "#ru1" || commands[1].Input["value"] == "" {
		t.Fatalf("unexpected uuid marker: %+v", commands[1])
	}
	if commands[2].MarkerName != "RandomInt" || commands[2].MarkerID != "#ri1" {
		t.Fatalf("unexpected int marker: %+v", commands[2])
	}
	se2Val, ok := commands[3].Input["value"].(map[string]any)
	if commands[3].MarkerName != "SideEffect" || commands[3].MarkerID != "#se2" || !ok {
		t.Fatalf("unexpected #se2 marker: %+v", commands[3])
	}
	// 同轮就地 marker 命中：第二个 SideEffect 闭包引用了 uuid/n 的当轮计算值，
	// 证明 RandomUUID/RandomInt 同轮返回了值供闭包捕获使用（非旧模型下轮才可读回）。
	if se2Val["uuid"] != commands[1].Input["value"] {
		t.Fatalf("#se2 uuid should reuse same-round RandomUUID value: %+v vs %+v", se2Val, commands[1].Input)
	}
	if int64FromAny(se2Val["n"]) != int64FromAny(commands[2].Input["value"]) {
		t.Fatalf("#se2 n should reuse same-round RandomInt value: %+v vs %+v", se2Val, commands[2].Input)
	}

	// 确定性 replay：把 4 条 marker 灌回 history（含一个 WorkflowTaskCompleted 表示上一轮已决策），
	// R 类全部 marker 命中读回同一值 → body 跑到底、无新 marker、产 CmdCompleteWorkflow。
	taskCompleted := history.NewWorkflowTaskCompletedEvent(0, "")
	replayEvents := []history.Event{
		started,
		taskCompleted,
		{ID: 2, Timestamp: time.Now(), MarkerRecorded: &history.MarkerRecordedEvent{MarkerName: "SideEffect", MarkerID: "#se1", Value: commands[0].Input}},
		{ID: 3, Timestamp: time.Now(), MarkerRecorded: &history.MarkerRecordedEvent{MarkerName: "RandomUUID", MarkerID: "#ru1", Value: commands[1].Input}},
		{ID: 4, Timestamp: time.Now(), MarkerRecorded: &history.MarkerRecordedEvent{MarkerName: "RandomInt", MarkerID: "#ri1", Value: commands[2].Input}},
		{ID: 5, Timestamp: time.Now(), MarkerRecorded: &history.MarkerRecordedEvent{MarkerName: "SideEffect", MarkerID: "#se2", Value: commands[3].Input}},
	}
	replayCommands, err := NewWorkflowExecutor(nodes, replayEvents).Execute()
	if err != nil {
		t.Fatal(err)
	}
	if len(replayCommands) != 1 || replayCommands[0].Type != history.CmdCompleteWorkflow {
		t.Fatalf("expected workflow completion after deterministic markers replay, got %+v", replayCommands)
	}
}

func TestRuntimeWorkflowAPINowUsesLatestHistoryTimestamp(t *testing.T) {
	startedAt := time.Now().Add(-time.Hour).Truncate(time.Millisecond)
	firedAt := startedAt.Add(30 * time.Second)
	executor := NewWorkflowExecutor(nil, []history.Event{
		history.NewWorkflowStartedEvent(nil, "", "", "", "", 0),
		{TimerFired: &history.TimerFiredEvent{}, Timestamp: firedAt},
	})
	// 分批模型下构造不再自动灌表；本测试绕过 Execute 裸读 currentTime，显式灌入
	//（单批历史：currentTime = 批尾事件时间 ≡ 旧全量 replay 的最末事件时间）。
	executor.applyBatch(executor.events)
	api := &runtimeWorkflowAPI{executor: executor}

	got := api.Now(workflowContext{}).Go().(workflowTime).t
	if !got.Equal(firedAt) {
		t.Fatalf("expected latest history timestamp %s, got %s", firedAt, got)
	}
}

func TestWLangRuntimeChildWorkflowSchedulesAndReplaysCompletion(t *testing.T) {
	src := []byte(`package flows

import workflow "github.com/donutnomad/tenuto/sdk/workflow"

func ParentWorkflow(ctx workflow.Context, input map[string]any) error {
	var child map[string]any
	err := workflow.ExecuteChildWorkflow(ctx, "child-flow", input).Get(ctx, &child)
	if err != nil {
		return err
	}
	return nil
}
`)
	result, err := sdkgen.GenerateFromSource(src, sdkgen.Options{
		WorkflowType: "sdk-parent",
		FuncName:     "ParentWorkflow",
		Filename:     "parent_workflow.go",
	})
	if err != nil {
		t.Fatal(err)
	}
	nodes := result.Workflow

	started := history.NewWorkflowStartedEvent(map[string]any{"order_id": "o-1"}, "", "", "", "", 0)
	commands, err := NewWorkflowExecutor(nodes, []history.Event{started}).Execute()
	if err != nil {
		t.Fatal(err)
	}
	if len(commands) != 1 || commands[0].Type != history.CmdStartChildWorkflow {
		t.Fatalf("expected child workflow command, got %+v", commands)
	}
	if commands[0].ChildID != "#c1" || commands[0].WorkflowType != "child-flow" {
		t.Fatalf("unexpected child command: %+v", commands[0])
	}

	childStarted := history.Event{ID: 2, Timestamp: time.Now(), ChildWorkflowStarted: &history.ChildWorkflowStartedEvent{Name: "child-flow", ChildID: "#c1", ChildRunID: "child-run-1", Input: map[string]any{"order_id": "o-1"}}}
	commands, err = NewWorkflowExecutor(nodes, []history.Event{started, childStarted}).Execute()
	if err != nil {
		t.Fatal(err)
	}
	if len(commands) != 0 {
		t.Fatalf("expected open child workflow to keep parent open, got %+v", commands)
	}

	childCompleted := history.Event{ID: 3, Timestamp: time.Now(), ChildWorkflowCompleted: &history.ChildWorkflowCompletedEvent{ChildID: "#c1", ChildRunID: "child-run-1", ScheduledEventID: childStarted.ID, Output: map[string]any{"ok": true}}}
	commands, err = NewWorkflowExecutor(nodes, []history.Event{started, childStarted, childCompleted}).Execute()
	if err != nil {
		t.Fatal(err)
	}
	if len(commands) != 1 || commands[0].Type != history.CmdCompleteWorkflow {
		t.Fatalf("expected parent completion after child replay, got %+v", commands)
	}
}

func TestWLangRuntimeChildWorkflowOptionsCarryParentClosePolicy(t *testing.T) {
	src := []byte(`package flows

import workflow "github.com/donutnomad/tenuto/sdk/workflow"

func ParentWorkflow(ctx workflow.Context, input map[string]any) error {
	abandoned := workflow.ExecuteChildWorkflow(ctx, "child-flow", input, workflow.ChildWorkflowOptions{ParentClosePolicy: workflow.ParentClosePolicyAbandon})
	cancelled := workflow.ExecuteChildWorkflow(ctx, "child-flow", input, workflow.ChildWorkflowOptions{ParentClosePolicy: workflow.ParentClosePolicyRequestCancel})
	defaulted := workflow.ExecuteChildWorkflow(ctx, "child-flow", input)
	var out map[string]any
	if err := abandoned.Get(ctx, &out); err != nil {
		return err
	}
	if err := cancelled.Get(ctx, &out); err != nil {
		return err
	}
	return defaulted.Get(ctx, &out)
}
`)
	result, err := sdkgen.GenerateFromSource(src, sdkgen.Options{
		WorkflowType: "sdk-parent-policy",
		FuncName:     "ParentWorkflow",
		Filename:     "parent_policy_workflow.go",
	})
	if err != nil {
		t.Fatal(err)
	}
	nodes := result.Workflow
	started := history.NewWorkflowStartedEvent(map[string]any{"order_id": "o-1"}, "", "", "", "", 0)
	commands, err := NewWorkflowExecutor(nodes, []history.Event{started}).Execute()
	if err != nil {
		t.Fatal(err)
	}
	want := []protocol.ParentClosePolicy{protocol.ParentClosePolicyAbandon, protocol.ParentClosePolicyRequestCancel, ""}
	if len(commands) != len(want) {
		t.Fatalf("expected %d child workflow commands, got %+v", len(want), commands)
	}
	for i, policy := range want {
		if commands[i].Type != history.CmdStartChildWorkflow || commands[i].ParentClosePolicy != policy {
			t.Fatalf("command %d = %+v, want StartChildWorkflow with policy %q", i, commands[i], policy)
		}
	}
}

func TestWLangRuntimeContinueAsNewCommand(t *testing.T) {
	src := []byte(`package flows

import workflow "github.com/donutnomad/tenuto/sdk/workflow"

func RollingWorkflow(ctx workflow.Context, input map[string]any) error {
	return workflow.ContinueAsNew(ctx, "rolling-flow", input)
}
`)
	result, err := sdkgen.GenerateFromSource(src, sdkgen.Options{
		WorkflowType: "sdk-rolling",
		FuncName:     "RollingWorkflow",
		Filename:     "rolling_workflow.go",
	})
	if err != nil {
		t.Fatal(err)
	}
	nodes := result.Workflow

	commands, err := NewWorkflowExecutor(nodes, []history.Event{
		history.NewWorkflowStartedEvent(map[string]any{"page": int64(10)}, "", "", "", "", 0),
	}).Execute()
	if err != nil {
		t.Fatal(err)
	}
	if len(commands) != 1 || commands[0].Type != history.CmdContinueAsNew {
		t.Fatalf("expected continue-as-new command, got %+v", commands)
	}
	if commands[0].WorkflowType != "rolling-flow" || commands[0].Input["page"] != int64(10) {
		t.Fatalf("unexpected continue-as-new command: %+v", commands[0])
	}
}
