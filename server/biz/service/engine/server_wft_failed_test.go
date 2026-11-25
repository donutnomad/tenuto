package engine

import (
	"context"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/program/programtest"
	"github.com/donutnomad/tenuto/protocol"
	historypkg "github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

// ============================================
// HandleWorkflowTaskFailed（BlockWorkflow）服务端测试
// ============================================
// 测试策略:
// - B2 Happy Path: 落 WorkflowTaskFailed 事件（Cause 正确）+ backoff 后新 WFTScheduled；run 不终结。
// - B2 Edge Case: 同 scheduledEventID 二次调用幂等——不重复落 Failed 事件、不重复调度。
// - B6 Recovery: 「Running 且无 open WFT，末个 WFT lifecycle 是 Failed」→ 恢复扫描后新 WFTScheduled。
// ============================================

// B2 Happy Path: HandleWorkflowTaskFailed 落 Failed 事件，按默认退避重调度新 WFT，run 保持 Running。
// 假时钟推进：退避到期之前不调度，到期时恰好调度一次。
func TestHandleWorkflowTaskFailed_AppendsEventAndReschedules(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	server, clk := newFakeClockServer(mgr, newTestTaskQueue(), newTestTaskQueue())
	defer server.Stop()
	backoff := execution.WorkflowTaskFailureBackoff(1, 0, 0)

	run := &execution.Run{ID: "run-wft-failed", WorkflowID: "wf", WorkflowType: "wf", Status: historypkg.StatusRunning}
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(run)) }); err != nil {
		t.Fatal(err)
	}
	scheduledEventID, err := mgr.Append(run.ID, run.ID, run.ID, historypkg.NewWorkflowTaskScheduledEvent(0))
	if err != nil {
		t.Fatal(err)
	}

	if err := server.HandleWorkflowTaskFailed(server.WorkflowTaskContext(ctx, run.ID, scheduledEventID), run.ID, scheduledEventID, historypkg.WorkflowTaskFailedCauseNonDeterministic, "boom"); err != nil {
		t.Fatal(err)
	}

	history, err := mgr.GetEvents(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if countEngineEvents(history, historypkg.EventWorkflowTaskFailed) != 1 {
		t.Fatalf("expected one WorkflowTaskFailed event, got history %+v", history)
	}
	// Cause 正确
	var cause string
	for _, ev := range history {
		if ev.WorkflowTaskFailed != nil {
			cause = ev.WorkflowTaskFailed.Cause
		}
	}
	if cause != historypkg.WorkflowTaskFailedCauseNonDeterministic {
		t.Fatalf("expected cause %q, got %q", historypkg.WorkflowTaskFailedCauseNonDeterministic, cause)
	}
	// run 不终结
	status, err := server.GetRunStatus(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if status != historypkg.StatusRunning {
		t.Fatalf("expected Block to keep run Running, got %s", status)
	}
	if countEngineEvents(history, historypkg.EventWorkflowFailed) != 0 {
		t.Fatalf("expected no terminal WorkflowFailed event, got history %+v", history)
	}
	clk.Advance(backoff - time.Millisecond)
	if history, _ = mgr.GetEvents(run.ID); countEngineEvents(history, historypkg.EventWorkflowTaskScheduled) != 1 {
		t.Fatal("workflow task rescheduled before its backoff")
	}
	clk.Advance(time.Millisecond)
	if history, _ = mgr.GetEvents(run.ID); countEngineEvents(history, historypkg.EventWorkflowTaskScheduled) != 2 {
		t.Fatal("workflow task not rescheduled when its backoff expired")
	}
}

// B2 Edge Case: 同 scheduledEventID 二次调用幂等——只落一次 Failed，不重复调度。
func TestHandleWorkflowTaskFailed_IdempotentByScheduledEvent(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	server, clk := newFakeClockServer(mgr, newTestTaskQueue(), newTestTaskQueue())
	defer server.Stop()

	run := &execution.Run{ID: "run-wft-failed-idem", WorkflowID: "wf", WorkflowType: "wf", Status: historypkg.StatusRunning}
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(run)) }); err != nil {
		t.Fatal(err)
	}
	scheduledEventID, err := mgr.Append(run.ID, run.ID, run.ID, historypkg.NewWorkflowTaskScheduledEvent(0))
	if err != nil {
		t.Fatal(err)
	}

	if err := server.HandleWorkflowTaskFailed(server.WorkflowTaskContext(ctx, run.ID, scheduledEventID), run.ID, scheduledEventID, historypkg.WorkflowTaskFailedCauseNonDeterministic, "boom"); err != nil {
		t.Fatal(err)
	}
	if err := server.HandleWorkflowTaskFailed(server.WorkflowTaskContext(ctx, run.ID, scheduledEventID), run.ID, scheduledEventID, historypkg.WorkflowTaskFailedCauseNonDeterministic, "boom"); err != nil {
		t.Fatal(err)
	}

	// 推进到远超退避上限：只有一次 Failed（幂等），只有一次替补 WFTScheduled。
	clk.Advance(time.Hour)

	history, err := mgr.GetEvents(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if countEngineEvents(history, historypkg.EventWorkflowTaskFailed) != 1 {
		t.Fatalf("expected duplicate failure to close only original task once, got history %+v", history)
	}
	if countEngineEvents(history, historypkg.EventWorkflowTaskScheduled) != 2 {
		t.Fatalf("expected exactly one replacement workflow task, got history %+v", history)
	}
}

// B6 Recovery: 「Running 且无 open WFT，末个 WFT lifecycle 是 Failed」（模拟 AfterFunc 丢失/重启）
// → 恢复扫描后新 WFTScheduled 出现。
func TestRecoveryReschedulesRunningRunWithoutOpenWFT(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	workflowTasks := newTestTaskQueue()
	server := assembleServer(mgr, workflowTasks, newTestTaskQueue())
	server.wftBackoffBase = time.Millisecond

	ref, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{
		Type:    "wf-recover-block",
		Program: programtest.Noop(),
		Activities: map[string]program.ActivityRef{
			"step-1": {Symbol: "step-1", Type: "work"},
		},
	})

	if err != nil {
		t.Fatal(err)
	}
	run := &execution.Run{
		ID:                    "run-recover-block",
		WorkflowID:            ref.WorkflowID,
		WorkflowType:          ref.WorkflowID,
		WorkflowConfigVersion: ref.Version,
		Status:                historypkg.StatusRunning,
		CreatedAt:             time.Now(),
	}
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(run)) }); err != nil {
		t.Fatal(err)
	}
	// 构造 Block 死等态：WorkflowStarted → WFTScheduled → WFTFailed（无新 open WFT）。
	if _, err := mgr.Append(run.ID, run.ID, run.ID, historypkg.NewWorkflowStartedEvent(run.Input, "", "", run.WorkflowID, "", 0)); err != nil {
		t.Fatal(err)
	}
	scheduledEventID, err := mgr.Append(run.ID, run.ID, run.ID, historypkg.NewWorkflowTaskScheduledEvent(0))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.Append(run.ID, run.ID, run.ID, historypkg.NewWorkflowTaskFailedEvent(historypkg.WorkflowTaskFailedEvent{
		ScheduledEventID: scheduledEventID,
		Cause:            historypkg.WorkflowTaskFailedCauseNonDeterministic,
	})); err != nil {
		t.Fatal(err)
	}

	// 现状（无恢复扫描）: 该 run 死等，无新 WFTScheduled。恢复扫描应重调度。
	if err := server.RecoverRun(ctx, run.ID); err != nil {
		t.Fatal(err)
	}

	waitForEngineEventCount(t, mgr, run.ID, historypkg.EventWorkflowTaskScheduled, 2, time.Second)
}
