package interpreter

import (
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/compiler/sdkgen"
	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/sdk/apperror"
)

// ============================================
// Workflow 取消（Temporal 语义）
// ============================================
// 测试策略：cancelSim 扮演 Server——每次 decide 都从头执行完整历史（重放已提交的批并与历史命令事件比对），
// 再把 live 批的命令按 Server 的规则落成命令事件。因此每个场景的每一步都同时验证重放产出相同的命令。
// - Happy Path: 取消请求交给代码，清理 Activity 在 disconnected context 上运行，返回取消错误 → CancelWorkflow
// - Happy Path: 取消后返回 nil → CompleteWorkflow；不理会取消、继续等待 → 不结束
// - Happy Path: WithCancel 作用域只取消其中一个 Activity；timer 随作用域取消；子 Workflow 收到取消请求
// - Happy Path: WaitForCancellation false 立即返回取消错误，true 等 ActivityCancelled
// - Edge Case: 已取消的 ctx 上发起的 Activity 与 timer 不发命令；同一批里发起又取消的命令被撤回
// - Edge Case: 没有取消请求时返回取消错误同样以 Cancelled 结束（对标 Temporal）
// - Error Case: 历史里的取消命令事件与代码不符 → 非确定性错误
// ============================================

type cancelSim struct {
	t      *testing.T
	wf     *program.WorkflowConfig
	events []history.Event
}

func newCancelSim(t *testing.T, src string) *cancelSim {
	t.Helper()
	result, err := sdkgen.GenerateFromSource([]byte(src), sdkgen.Options{WorkflowType: "cancel-wf", FuncName: "W", Filename: "w.go"})
	if err != nil {
		t.Fatalf("translate: %v", err)
	}
	sim := &cancelSim{t: t, wf: result.Workflow}
	sim.add(history.NewWorkflowStartedEvent(map[string]any{}, "", "", "", "", 0))
	return sim
}

func (s *cancelSim) add(events ...history.Event) {
	for _, event := range events {
		event.ID = int64(len(s.events) + 1)
		event.Timestamp = time.Unix(1_800_000_000, 0)
		s.events = append(s.events, event)
	}
}

// decide 执行一次 Workflow Task 并把命令落成历史；返回 live 批的命令。
func (s *cancelSim) decide() []history.Command {
	s.t.Helper()
	commands, err := NewWorkflowExecutor(s.wf, slices.Clone(s.events)).Execute()
	if err != nil {
		s.t.Fatalf("execute: %v", err)
	}
	s.add(history.Event{WorkflowTaskCompleted: &history.WorkflowTaskCompletedEvent{}})
	for _, command := range commands {
		switch command.Type {
		case history.CmdScheduleActivity:
			s.add(history.NewActivityScheduledEvent(history.ActivityScheduledEvent{ActivityID: command.ActivityID, ActivityType: command.ActivityType}))
		case history.CmdRequestCancelActivity:
			s.add(history.NewActivityCancelRequestedEvent(command.ActivityID, s.scheduled(command.ActivityID)))
		case history.CmdScheduleTimer:
			s.add(history.NewTimerScheduledEvent(command.TimerID, nil, command.Delay.Milliseconds()))
		case history.CmdCancelTimer:
			s.add(history.NewTimerCancelledEvent(command.TimerID, 0))
		case history.CmdStartChildWorkflow:
			s.add(history.NewChildWorkflowStartedEvent(command.WorkflowID, command.ChildID, "child-run", "", command.Input))
		case history.CmdRequestCancelChildWorkflow:
			s.add(history.NewChildWorkflowCancelRequestedEvent(command.ChildID, "child-run", 0))
		case history.CmdCompleteWorkflow:
			s.add(history.NewWorkflowCompletedEvent(command.Output))
		case history.CmdCancelWorkflow:
			s.add(history.NewWorkflowCancelledEvent(command.Details))
		case history.CmdFailWorkflow:
			s.add(history.NewWorkflowFailedEvent(command.Error, ""))
		default:
			s.t.Fatalf("unexpected command %s", command.Type)
		}
	}
	return commands
}

// scheduled 返回 stepID 的 ActivityScheduled 事件 ID。
func (s *cancelSim) scheduled(stepID string) int64 {
	for _, event := range s.events {
		if event.ActivityScheduled != nil && event.ActivityScheduled.ActivityID == stepID {
			return event.ID
		}
	}
	s.t.Fatalf("activity %s was never scheduled", stepID)
	return 0
}

func (s *cancelSim) completeActivity(stepID string) {
	s.add(history.NewActivityCompletedEvent(history.ActivityCompletedEvent{ActivityID: stepID, ScheduledEventID: s.scheduled(stepID), Output: map[string]any{"ok": true}}))
}

func (s *cancelSim) requestCancel() {
	s.add(history.NewWorkflowCancellationRequestedEvent("test"))
}

// describe 把命令写成 "Type(id)" 序列，便于整体比较。
func describe(commands []history.Command) string {
	parts := make([]string, 0, len(commands))
	for _, command := range commands {
		id := command.ActivityID + command.TimerID + command.ChildID
		parts = append(parts, string(command.Type)+"("+id+")")
	}
	return strings.Join(parts, " ")
}

func expectCommands(t *testing.T, got []history.Command, want string) {
	t.Helper()
	if describe(got) != want {
		t.Fatalf("commands = %q, want %q", describe(got), want)
	}
}

const cancelCleanupSrc = `package flows

import (
	"time"

	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/activities"
)

func W(ctx workflow.Context, input map[string]any) error {
	var out map[string]any
	err := workflow.ExecuteActivity(ctx, activities.Work, input, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &out)
	if workflow.IsCanceledError(err) {
		dctx, _ := workflow.NewDisconnectedContext(ctx)
		if cleanupErr := workflow.ExecuteActivity(dctx, activities.Cleanup, input, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(dctx, &out); cleanupErr != nil {
			return cleanupErr
		}
		return RESULT
	}
	return err
}
`

// 取消请求在灌入之后、协程推进之前送达：根作用域取消 → 请求取消 Activity → Future 立即返回取消错误，
// 代码在 disconnected context 上调度清理 Activity；返回取消错误时以 Cancelled 结束，details 保留。
func TestCancellation_CleanupInDisconnectedContextThenCancelled(t *testing.T) {
	sim := newCancelSim(t, strings.Replace(cancelCleanupSrc, "RESULT", `workflow.NewCanceledError("cleaned")`, 1))
	expectCommands(t, sim.decide(), "ScheduleActivity(#a1)")
	sim.requestCancel()
	expectCommands(t, sim.decide(), "RequestCancelActivity(#a1) ScheduleActivity(#a2)")
	sim.completeActivity("#a2")
	commands := sim.decide()
	expectCommands(t, commands, "CancelWorkflow()")
	if len(commands[0].Details) != 1 {
		t.Fatalf("cancel details = %+v, want the canceled error's details", commands[0].Details)
	}
	var detail string
	if err := commands[0].Details[0].Decode(&detail); err != nil || detail != "cleaned" {
		t.Fatalf("cancel detail = %q err=%v", detail, err)
	}
}

// 处理取消后返回 nil：Run 以 Completed 结束。
func TestCancellation_CleanupThenReturnNilCompletes(t *testing.T) {
	sim := newCancelSim(t, strings.Replace(cancelCleanupSrc, "RESULT", "nil", 1))
	sim.decide()
	sim.requestCancel()
	sim.decide()
	sim.completeActivity("#a2")
	expectCommands(t, sim.decide(), "CompleteWorkflow()")
}

// 不理会取消的代码继续运行：等待 Signal 不受取消影响，Signal 到达后正常完成。
func TestCancellation_IgnoredCancellationKeepsRunning(t *testing.T) {
	sim := newCancelSim(t, `package flows

import workflow "github.com/donutnomad/tenuto/sdk/workflow"

func W(ctx workflow.Context, input map[string]any) error {
	var in map[string]any
	return workflow.WaitSignal(ctx, "go").Get(ctx, &in)
}
`)
	expectCommands(t, sim.decide(), "")
	sim.requestCancel()
	expectCommands(t, sim.decide(), "")
	sim.add(history.NewWorkflowSignaledEvent("go", map[string]any{}))
	expectCommands(t, sim.decide(), "CompleteWorkflow()")
}

// WithCancel 作用域只取消它里面的 Activity：B 完成后取消作用域，A 立即以取消错误返回，Workflow 完成。
func TestCancellation_WithCancelScopeCancelsOneOfTwoActivities(t *testing.T) {
	sim := newCancelSim(t, `package flows

import (
	"time"

	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/activities"
)

func W(ctx workflow.Context, input map[string]any) error {
	opts := workflow.ActivityOptions{StartToCloseTimeout: time.Minute}
	cctx, cancel := workflow.WithCancel(ctx)
	a := workflow.ExecuteActivity(cctx, activities.A, input, opts)
	b := workflow.ExecuteActivity(ctx, activities.B, input, opts)
	var out map[string]any
	if err := b.Get(ctx, &out); err != nil {
		return err
	}
	cancel()
	if cctx.Err() == nil {
		return workflow.NewApplicationError("scope not cancelled", "Test")
	}
	if ctx.Err() != nil {
		return workflow.NewApplicationError("parent cancelled", "Test")
	}
	err := a.Get(ctx, &out)
	if !workflow.IsCanceledError(err) {
		return workflow.NewApplicationError("activity A not cancelled", "Test")
	}
	return nil
}
`)
	expectCommands(t, sim.decide(), "ScheduleActivity(#a1) ScheduleActivity(#a2)")
	sim.completeActivity("#a2")
	expectCommands(t, sim.decide(), "RequestCancelActivity(#a1) CompleteWorkflow()")
	// 已结束的 Run 重放到末尾仍与历史一致。
	if _, _, err := NewWorkflowExecutor(sim.wf, sim.events).ReplayForQuery(); err != nil {
		t.Fatalf("replay closed run: %v", err)
	}
}

// timer 随作用域取消：已提交的 timer 产 CancelTimer，Sleep 立即返回取消错误。
func TestCancellation_TimerCancelledByScope(t *testing.T) {
	sim := newCancelSim(t, `package flows

import (
	"time"

	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/activities"
)

func W(ctx workflow.Context, input map[string]any) error {
	cctx, cancel := workflow.WithCancel(ctx)
	done := false
	var sleepErr error
	workflow.Go(ctx, func(ctx workflow.Context) {
		sleepErr = workflow.Sleep(cctx, time.Hour)
		done = true
	})
	var out map[string]any
	if err := workflow.ExecuteActivity(ctx, activities.A, input, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &out); err != nil {
		return err
	}
	cancel()
	if err := workflow.Await(ctx, func() bool { return done }); err != nil {
		return err
	}
	if workflow.IsCanceledError(sleepErr) {
		return nil
	}
	return workflow.NewApplicationError("sleep was not cancelled", "Test")
}
`)
	first := sim.decide()
	if !strings.Contains(describe(first), "ScheduleTimer(#t1)") || !strings.Contains(describe(first), "ScheduleActivity(#a1)") {
		t.Fatalf("first task commands = %q", describe(first))
	}
	sim.completeActivity("#a1")
	expectCommands(t, sim.decide(), "CancelTimer(#t1) CompleteWorkflow()")
}

// 同一批里发起又被取消的命令不发给 Server；已取消的 ctx 上发起的 Activity 与 timer 不产命令。
func TestCancellation_CommandsCancelledBeforeSendAreWithdrawn(t *testing.T) {
	sim := newCancelSim(t, `package flows

import (
	"time"

	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/activities"
)

func W(ctx workflow.Context, input map[string]any) error {
	opts := workflow.ActivityOptions{StartToCloseTimeout: time.Minute}
	cctx, cancel := workflow.WithCancel(ctx)
	a := workflow.ExecuteActivity(cctx, activities.A, input, opts)
	cancel()
	var out map[string]any
	if err := a.Get(ctx, &out); !workflow.IsCanceledError(err) {
		return workflow.NewApplicationError("A not cancelled", "Test")
	}
	if err := workflow.ExecuteActivity(cctx, activities.B, input, opts).Get(ctx, &out); !workflow.IsCanceledError(err) {
		return workflow.NewApplicationError("B not cancelled", "Test")
	}
	if err := workflow.Sleep(cctx, time.Hour); !workflow.IsCanceledError(err) {
		return workflow.NewApplicationError("sleep not cancelled", "Test")
	}
	return workflow.ExecuteActivity(ctx, activities.C, input, opts).Get(ctx, &out)
}
`)
	expectCommands(t, sim.decide(), "ScheduleActivity(#a3)")
	sim.completeActivity("#a3")
	expectCommands(t, sim.decide(), "CompleteWorkflow()")
}

const waitForCancellationSrc = `package flows

import (
	"time"

	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/activities"
)

func W(ctx workflow.Context, input map[string]any) error {
	var out map[string]any
	return workflow.ExecuteActivity(ctx, activities.A, input, workflow.ActivityOptions{StartToCloseTimeout: time.Minute, HeartbeatTimeout: time.Second, WaitForCancellation: WAIT}).Get(ctx, &out)
}
`

// WaitForCancellation=false：取消请求一发出 Future 就返回取消错误。
func TestCancellation_ActivityWithoutWaitResolvesImmediately(t *testing.T) {
	sim := newCancelSim(t, strings.Replace(waitForCancellationSrc, "WAIT", "false", 1))
	sim.decide()
	sim.requestCancel()
	expectCommands(t, sim.decide(), "RequestCancelActivity(#a1) CancelWorkflow()")
}

// WaitForCancellation=true：Future 等 Activity 的结果；ActivityCancelled 到达后返回带 details 的取消错误。
func TestCancellation_ActivityWithWaitResolvesOnActivityCancelled(t *testing.T) {
	sim := newCancelSim(t, strings.Replace(waitForCancellationSrc, "WAIT", "true", 1))
	sim.decide()
	sim.requestCancel()
	expectCommands(t, sim.decide(), "RequestCancelActivity(#a1)")
	payload, err := protocol.NewJSONPayload("stopped at 40%")
	if err != nil {
		t.Fatal(err)
	}
	sim.add(history.NewActivityCancelledEvent(history.ActivityCancelledEvent{ActivityID: "#a1", ScheduledEventID: sim.scheduled("#a1"), Details: protocol.Payloads{payload}}))
	commands := sim.decide()
	expectCommands(t, commands, "CancelWorkflow()")
	if len(commands[0].Details) != 1 {
		t.Fatalf("workflow cancel details = %+v, want the activity's cancellation details", commands[0].Details)
	}
}

// WaitForCancellation=true 且取消确认之前 Activity 完成：完成结果胜出，Workflow 完成。
func TestCancellation_ActivityCompletionWinsOverCancellation(t *testing.T) {
	sim := newCancelSim(t, strings.Replace(waitForCancellationSrc, "WAIT", "true", 1))
	sim.decide()
	sim.requestCancel()
	sim.decide()
	sim.completeActivity("#a1")
	expectCommands(t, sim.decide(), "CompleteWorkflow()")
}

// 子 Workflow：WaitForCancellation=false 立即返回取消错误；true 等子 Workflow 结束（ChildWorkflowCancelled 是取消错误）。
func TestCancellation_ChildWorkflowCancelRequest(t *testing.T) {
	const src = `package flows

import workflow "github.com/donutnomad/tenuto/sdk/workflow"

func W(ctx workflow.Context, input map[string]any) error {
	var out map[string]any
	return workflow.ExecuteChildWorkflow(ctx, "child-wf", input, workflow.ChildWorkflowOptions{WaitForCancellation: WAIT}).Get(ctx, &out)
}
`
	t.Run("without wait", func(t *testing.T) {
		sim := newCancelSim(t, strings.Replace(src, "WAIT", "false", 1))
		expectCommands(t, sim.decide(), "StartChildWorkflow(#c1)")
		sim.requestCancel()
		expectCommands(t, sim.decide(), "RequestCancelChildWorkflow(#c1) CancelWorkflow()")
	})
	t.Run("with wait", func(t *testing.T) {
		sim := newCancelSim(t, strings.Replace(src, "WAIT", "true", 1))
		sim.decide()
		sim.requestCancel()
		expectCommands(t, sim.decide(), "RequestCancelChildWorkflow(#c1)")
		sim.add(history.NewChildWorkflowCancelledEvent("#c1", "child-run", 0))
		expectCommands(t, sim.decide(), "CancelWorkflow()")
	})
}

// ctx.Done 放进 Selector 与 Signal 一起等待；Await 在取消后返回取消错误。
func TestCancellation_DoneChannelInSelectorAndAwait(t *testing.T) {
	sim := newCancelSim(t, `package flows

import workflow "github.com/donutnomad/tenuto/sdk/workflow"

func W(ctx workflow.Context, input map[string]any) error {
	canceled := false
	sel := workflow.NewSelector(ctx)
	sel.AddReceive(ctx, ctx.Done(), func(ch workflow.Channel, more bool) { canceled = !more })
	sel.AddSignalFuture(ctx, workflow.WaitSignal(ctx, "go"), func(f workflow.SignalFuture) {})
	sel.Select(ctx)
	if !canceled {
		return workflow.NewApplicationError("signal won", "Test")
	}
	if err := workflow.Await(ctx, func() bool { return false }); err != nil {
		return err
	}
	return nil
}
`)
	expectCommands(t, sim.decide(), "")
	sim.requestCancel()
	expectCommands(t, sim.decide(), "CancelWorkflow()")
}

// 没有取消请求时返回取消错误同样以 Cancelled 结束（Temporal：SDK 对 CanceledError 产 CancelWorkflowExecution，
// Server 不检查取消请求）；其他错误仍是 FailWorkflow。
func TestCancellation_ReturnValueDecidesStatus(t *testing.T) {
	for name, tc := range map[string]struct{ ret, want string }{
		"canceled error":      {`workflow.NewCanceledError()`, "CancelWorkflow()"},
		"ErrCanceled":         {`workflow.ErrCanceled`, "CancelWorkflow()"},
		"application error":   {`workflow.NewApplicationError("boom", "Boom")`, "FailWorkflow()"},
		"canceled cause":      {`workflow.NewApplicationErrorWithCause("wrapped", "Wrap", workflow.NewCanceledError())`, "CancelWorkflow()"},
		"nil after no cancel": {`nil`, "CompleteWorkflow()"},
	} {
		t.Run(name, func(t *testing.T) {
			sim := newCancelSim(t, `package flows

import workflow "github.com/donutnomad/tenuto/sdk/workflow"

func W(ctx workflow.Context, input map[string]any) error {
	return `+tc.ret+`
}
`)
			expectCommands(t, sim.decide(), tc.want)
		})
	}
}

// 重放时代码没有在历史记录的位置取消 Activity：非确定性错误。
func TestCancellation_ReplayMismatchIsNonDeterministic(t *testing.T) {
	sim := newCancelSim(t, strings.Replace(waitForCancellationSrc, "WAIT", "true", 1))
	sim.decide()
	// 历史声称上一批请求取消了 #a1，但这段历史里没有取消请求，代码不会产出这条命令。
	sim.add(history.Event{WorkflowTaskCompleted: &history.WorkflowTaskCompletedEvent{}},
		history.NewActivityCancelRequestedEvent("#a1", sim.scheduled("#a1")))
	_, err := NewWorkflowExecutor(sim.wf, sim.events).Execute()
	assertNonDeterministic(t, err, "ActivityCancelRequested")
}

// canceledFailure 还原成 IsCanceledError 为 true 的错误。
func TestCancellation_CanceledFailureRoundTrip(t *testing.T) {
	if err := apperror.FromFailure(*canceledFailure(nil), apperror.JSONDecoder); !apperror.IsCanceled(err) {
		t.Fatalf("canceled failure decodes to %v, want a canceled error", err)
	}
}
