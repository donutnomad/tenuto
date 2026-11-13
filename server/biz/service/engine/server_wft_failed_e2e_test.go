package engine

import (
	"context"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/compiler/sdkgen"
	"github.com/donutnomad/tenuto/internal/worker"
	"github.com/donutnomad/tenuto/protocol"
	historypkg "github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

// B5 端到端（server 级）: 真实 Worker 处理一个 non-det replay 历史 → 经真实 Server 的 Block 路径
// 落 WorkflowTaskFailed 事件、run 不终结、无 WorkflowFailed 终态。
func TestNonDetError_BlocksInsteadOfFails_EndToEnd(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	workflowTasks := newTestTaskQueue()
	server := assembleServer(mgr, workflowTasks, newTestTaskQueue())
	server.wftBackoffBase = time.Millisecond

	// wlang 工作流：SideEffect（R 类原语）——replay 缺 marker 触发 nonDeterministicError。
	src := []byte(`package flows

import workflow "github.com/donutnomad/tenuto/sdk/workflow"

func E2EGuardFlow(ctx workflow.Context, input map[string]any) error {
	var cfg map[string]any
	err := workflow.SideEffect(ctx, func(ctx workflow.Context) any { return map[string]any{"region": "sg"} }).Get(ctx, &cfg)
	if err != nil {
		return err
	}
	return nil
}
`)
	result, err := sdkgen.GenerateFromSource(src, sdkgen.Options{WorkflowID: "wf-e2e-guard", FuncName: "E2EGuardFlow", Filename: "e2e_guard_flow.go"})
	if err != nil {
		t.Fatal(err)
	}
	wf := result.Workflow
	ref, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, wf)
	if err != nil {
		t.Fatal(err)
	}

	run := &execution.Run{
		ID:                    "run-e2e-block",
		WorkflowID:            ref.WorkflowID,
		WorkflowType:          ref.WorkflowID,
		WorkflowConfigVersion: ref.Version,
		Status:                historypkg.StatusRunning,
		CreatedAt:             time.Now(),
	}
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(run)) }); err != nil {
		t.Fatal(err)
	}
	// 构造 replay 历史：一个已 close 的旧 WFT 令 splitBatches 产 replay 批（isReplay 触发），
	// 当前 open WFT 是 worker 本轮处理的 task。
	if _, err := mgr.Append(run.ID, run.ID, run.ID, historypkg.NewWorkflowStartedEvent(nil, "", "", run.WorkflowID, "", 0)); err != nil {
		t.Fatal(err)
	}
	completeOpenWorkflowTask(t, mgr, run.ID)
	openScheduledID, err := mgr.Append(run.ID, run.ID, run.ID, historypkg.NewWorkflowTaskScheduledEvent(0))
	if err != nil {
		t.Fatal(err)
	}

	worker := worker.NewWorkflowWorker(server, worker.WorkerOptions{
		WorkflowConcurrency: 1,
		WorkflowTaskTimeout: 5 * time.Second,
	})
	state, err := mgr.GetWorkflowExecutionState(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	task, err := testTaskTokens().Attach(protocol.Task{
		Type: protocol.TaskTypeWorkflow, RunID: run.ID, ScheduledEventID: openScheduledID,
		ExecutionGeneration: state.ExecutionGeneration, StickyGeneration: state.StickyGeneration,
	})
	if err != nil {
		t.Fatal(err)
	}
	worker.HandleWorkflowTask(ctx, protocol.TaskLease{Task: task, Token: "lease-token"})

	history, err := mgr.GetEvents(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if countEngineEvents(history, historypkg.EventWorkflowTaskFailed) != 1 {
		t.Fatalf("expected non-det to land one WorkflowTaskFailed (Block), got history %+v", history)
	}
	if countEngineEvents(history, historypkg.EventWorkflowFailed) != 0 {
		t.Fatalf("expected Block NOT to produce terminal WorkflowFailed, got history %+v", history)
	}
	status, err := server.GetRunStatus(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if status != historypkg.StatusRunning {
		t.Fatalf("expected Block to keep run Running, got %s", status)
	}
	// Cause 标记为 non-det
	for _, ev := range history {
		if ev.WorkflowTaskFailed != nil && ev.WorkflowTaskFailed.Cause != historypkg.WorkflowTaskFailedCauseNonDeterministic {
			t.Fatalf("expected non_deterministic_error cause, got %q", ev.WorkflowTaskFailed.Cause)
		}
	}
}
