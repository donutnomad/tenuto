package engine

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/internal/worker"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/sdk/activity"

	"github.com/donutnomad/tenuto/compiler/sdkgen"
	programpkg "github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
)

// ============================================
// BlockWorkflow 错误分类出口测试
// ============================================
// 测试策略:
// - Happy Path: Execute 返回 nonDeterministicError（R 类护栏 replay 缺 marker，经 wlang safeCall
//   上浮，typed 传播已落地）→ worker 走 Block 路径调 HandleWorkflowTaskFailed，不产 CmdFailWorkflow。
// - 对照（Error Case）: 普通 execErr（用户 return err）→ 保持现状 HandleWorkflowTaskCompleted +
//   CmdFailWorkflow。
// ============================================

// blockClassifyWorkerAPI 是驱动 worker 错误分类的桩：返回预置的 wlang 配置 + replay 历史，
// 记录 worker 最终走 Block（HandleWorkflowTaskFailed）还是 Fail（HandleWorkflowTaskCompleted）。
type blockClassifyWorkerAPI struct {
	runID            string
	wf               *programpkg.WorkflowConfig
	events           []history.Event
	scheduledEventID int64
	acks             int
	nacks            int
	failed           int // HandleWorkflowTaskFailed 调用次数（Block 路径）
	failCmds         int // HandleWorkflowTaskCompleted 中含 CmdFailWorkflow 的次数（Fail 路径）
	timedOut         int
	failCause        string
}

func newBlockClassifyWorkerAPI(t *testing.T, src []byte, workflowID, funcName, filename string, replay bool) *blockClassifyWorkerAPI {
	t.Helper()
	result, err := sdkgen.GenerateFromSource(src, sdkgen.Options{WorkflowID: workflowID, FuncName: funcName, Filename: filename})
	if err != nil {
		t.Fatal(err)
	}
	wf := result.Workflow
	events := []history.Event{history.NewWorkflowStartedEvent(nil, "", "", "", "", 0)}
	scheduledEventID := int64(0)
	if replay {
		// 一个已 close 的旧 WFT（sched=100/completed）令 splitBatches 产 replay 批 → isReplay 触发
		// → R 类原语 replay 缺 marker 抛 nonDeterministicError。当前 open WFT（sched=200，未 close）
		// 是 worker 本轮处理的 task，避免 workflowTaskClosed 提前 ack 短路。
		events = append(events,
			history.NewWorkflowTaskCompletedEvent(100, ""),
			history.Event{ID: 200, WorkflowTaskScheduled: &history.WorkflowTaskScheduledEvent{}},
		)
		scheduledEventID = 200
	}
	return &blockClassifyWorkerAPI{runID: "run-block-classify", wf: wf, events: events, scheduledEventID: scheduledEventID}
}

func (a *blockClassifyWorkerAPI) PollWorkflowTask(ctx context.Context) (protocol.TaskLease, error) {
	return protocol.TaskLease{}, nil
}

func (a *blockClassifyWorkerAPI) PollActivityTask(ctx context.Context, taskQueue string) (protocol.TaskLease, error) {
	return protocol.TaskLease{}, nil
}

func (a *blockClassifyWorkerAPI) AckWorkflowTask(ctx context.Context, lease protocol.TaskLease) error {
	a.acks++
	return nil
}

func (a *blockClassifyWorkerAPI) NackWorkflowTask(ctx context.Context, lease protocol.TaskLease) error {
	a.nacks++
	return nil
}

func (a *blockClassifyWorkerAPI) AckActivityTask(ctx context.Context, lease protocol.TaskLease) error {
	return nil
}
func (a *blockClassifyWorkerAPI) NackActivityTask(ctx context.Context, lease protocol.TaskLease) error {
	return nil
}

func (a *blockClassifyWorkerAPI) GetWorkflowConfig(_ context.Context, runID, _ string) (*programpkg.WorkflowConfig, error) {
	return a.wf, nil
}

func (a *blockClassifyWorkerAPI) GetEvents(runID string) ([]history.Event, error) {
	return a.events, nil
}

func (a *blockClassifyWorkerAPI) GetRunStatus(runID string) (history.Status, error) {
	return history.StatusRunning, nil
}

func (a *blockClassifyWorkerAPI) HandleWorkflowTaskCompleted(ctx context.Context, runID string, scheduledEventID int64, commands []history.Command, workerIdentity string) error {
	for _, cmd := range commands {
		if cmd.Type == history.CmdFailWorkflow {
			a.failCmds++
		}
	}
	return nil
}

func (a *blockClassifyWorkerAPI) HandleWorkflowTaskTimedOut(ctx context.Context, runID string, scheduledEventID int64, errMsg string) error {
	a.timedOut++
	return nil
}

func (a *blockClassifyWorkerAPI) HandleWorkflowTaskFailed(ctx context.Context, runID string, scheduledEventID int64, cause, reason string) error {
	a.failed++
	a.failCause = cause
	return nil
}

func (a *blockClassifyWorkerAPI) GetActivityState(runID string, scheduledEventID int64) (*execution.ActivityState, bool, error) {
	return nil, false, nil
}
func (a *blockClassifyWorkerAPI) GetWorkflowExecutionState(runID string) (*execution.WorkflowExecutionState, error) {
	return workerAPIExecutionState(a, runID)
}

func (a *blockClassifyWorkerAPI) TaskRun(task protocol.Task) (*execution.Run, error) {
	return &execution.Run{ID: task.RunID, Namespace: task.Namespace}, nil
}

func (a *blockClassifyWorkerAPI) StartActivityAttempt(task protocol.Task, _ *execution.Run, startToCloseTimeout, heartbeatTimeout time.Duration) (time.Time, bool, error) {
	return time.Time{}, true, nil
}
func (a *blockClassifyWorkerAPI) CompleteActivityAttempt(ctx context.Context, _ *execution.Run, scheduledEventID int64, attempt int, stamp int64, output map[string]any) error {
	return nil
}
func (a *blockClassifyWorkerAPI) FailActivityAttempt(ctx context.Context, _ *execution.Run, scheduledEventID int64, attempt int, stamp int64, failure protocol.Failure) error {
	return nil
}
func (a *blockClassifyWorkerAPI) HandleActivityHeartbeat(runID string, scheduledEventID int64, attempt int, stamp int64, details map[string]any) error {
	return nil
}
func (a *blockClassifyWorkerAPI) CancelWorkflow(runID, reason string) error    { return nil }
func (a *blockClassifyWorkerAPI) TerminateWorkflow(runID, reason string) error { return nil }

// Happy Path: non-det 错误 → Block 路径（HandleWorkflowTaskFailed），不产 CmdFailWorkflow。
func TestWorkerClassifiesNonDeterministicError(t *testing.T) {
	src := []byte(`package flows

import workflow "github.com/donutnomad/tenuto/sdk/workflow"

func BlockGuardFlow(ctx workflow.Context, input map[string]any) error {
	var cfg map[string]any
	err := workflow.SideEffect(ctx, func(ctx workflow.Context) any { return map[string]any{"region": "sg"} }).Get(ctx, &cfg)
	if err != nil {
		return err
	}
	return nil
}
`)
	api := newBlockClassifyWorkerAPI(t, src, "sdk-block-guard", "BlockGuardFlow", "block_guard_flow.go", true)
	worker := assembleWorker(api, activity.NewRegistry(), worker.WorkerOptions{
		WorkflowConcurrency: 1,
		ActivityConcurrency: 1,
		WorkflowTaskTimeout: 5 * time.Second,
	})

	worker.HandleWorkflowTask(context.Background(), protocol.TaskLease{
		Task:  protocol.Task{Type: protocol.TaskTypeWorkflow, RunID: api.runID, ScheduledEventID: api.scheduledEventID},
		Token: "lease-token",
	})

	if api.failed != 1 {
		t.Fatalf("expected non-det error to route to Block (HandleWorkflowTaskFailed=1), got %d", api.failed)
	}
	if api.failCmds != 0 {
		t.Fatalf("expected non-det error NOT to produce CmdFailWorkflow, got %d", api.failCmds)
	}
	if api.failCause != history.WorkflowTaskFailedCauseNonDeterministic {
		t.Fatalf("expected cause %q, got %q", history.WorkflowTaskFailedCauseNonDeterministic, api.failCause)
	}
	if api.acks != 1 {
		t.Fatalf("expected lease ack after Block, got %d", api.acks)
	}
}

// Error Case（对照）: 普通用户错误（return err）→ Fail 路径（CmdFailWorkflow），不走 Block。
func TestWorkerKeepsFailWorkflowForOrdinaryError(t *testing.T) {
	src := []byte(`package flows

import (
	"time"
	"fmt"

	workflow "github.com/donutnomad/tenuto/sdk/workflow"
)

func OrdinaryErrorFlow(ctx workflow.Context, input map[string]any) error {
	return fmt.Errorf("boom")
}
`)
	// 无 replay 也应产 CmdFailWorkflow：body return err → workflowReturnErr → CmdFailWorkflow。
	api := newBlockClassifyWorkerAPI(t, src, "sdk-ordinary-error", "OrdinaryErrorFlow", "ordinary_error_flow.go", false)
	worker := assembleWorker(api, activity.NewRegistry(), worker.WorkerOptions{
		WorkflowConcurrency: 1,
		ActivityConcurrency: 1,
		WorkflowTaskTimeout: 5 * time.Second,
	})

	worker.HandleWorkflowTask(context.Background(), protocol.TaskLease{
		Task:  protocol.Task{Type: protocol.TaskTypeWorkflow, RunID: api.runID, ScheduledEventID: api.scheduledEventID},
		Token: "lease-token",
	})

	if api.failed != 0 {
		t.Fatalf("expected ordinary error NOT to route to Block, got HandleWorkflowTaskFailed=%d", api.failed)
	}
	if api.failCmds != 1 {
		t.Fatalf("expected ordinary error to produce CmdFailWorkflow, got %d", api.failCmds)
	}
}

func TestWorkerNacksUnsupportedProgramArtifactBeforeExecution(t *testing.T) {
	program := programpkg.WLangProgram{
		Schema:           programpkg.WLangProgramSchemaV2,
		Lang:             programpkg.WLangLanguageV1,
		Program:          []any{map[string]any{"return": map[string]any{"literal": map[string]any{"type": "null", "value": nil}}}},
		HandlerABISchema: programpkg.WLangHandlerABISchemaV1,
	}
	api := &blockClassifyWorkerAPI{
		runID: "run-capability",
		wf: &programpkg.WorkflowConfig{Program: program,
			Activities: map[string]programpkg.ActivityRef{}},
		events: []history.Event{history.NewWorkflowStartedEvent(nil, "", "", "", "", 0)},
	}
	worker := worker.NewWorkflowWorker(api, worker.WorkerOptions{
		WorkflowTaskTimeout:  5 * time.Second,
		WorkflowCapabilities: []string{},
	})
	worker.HandleWorkflowTask(context.Background(), protocol.TaskLease{
		Task:  protocol.Task{Type: protocol.TaskTypeWorkflow, RunID: api.runID},
		Token: "lease-token",
	})
	if api.nacks != 1 {
		t.Fatalf("unsupported worker should nack task, got %d nacks", api.nacks)
	}
	if api.failCmds != 0 || api.failed != 0 {
		t.Fatalf("capability mismatch mutated workflow: failCmds=%d failed=%d", api.failCmds, api.failed)
	}
}

func (*blockClassifyWorkerAPI) GetWorkflowActivation(context.Context, history.WorkflowActivationRequest) (history.WorkflowActivation, error) {
	return history.WorkflowActivation{}, errors.New("incremental activation is not used by this fake")
}

func (*blockClassifyWorkerAPI) CompleteWorkflowActivation(context.Context, history.WorkflowTaskCompletionRequest) (history.WorkflowTaskCompletionAck, error) {
	return history.WorkflowTaskCompletionAck{}, errors.New("incremental activation is not used by this fake")
}

func (*blockClassifyWorkerAPI) ReleaseActivityAttempt(context.Context, *execution.Run, int64, int, int64) error {
	return nil
}

func (*blockClassifyWorkerAPI) RefuseIncompatibleActivityClaim(context.Context, protocol.Task, *execution.Run) (bool, error) {
	return false, nil
}
