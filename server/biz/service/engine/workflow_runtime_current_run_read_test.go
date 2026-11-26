package engine

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

// ============================================
// 只读操作 runID 可空 → 解析 current run 测试
// ============================================
// 测试策略（对标 Temporal runId 可空）:
// - Happy Path: runID 非空 → 老路径行为完全不变
// - Happy Path: runID 空 + workflowID → 解析到 current
// - Edge Case: current 是终态 run（complete/terminate 后 current 仍指向它）→ 仍能查到
// - Error Case: runID 与 workflowID 皆空 → ErrRunOrWorkflowIDRequired
// - Error Case: workflowID 无任何 run → ErrNoCurrentRun
// ============================================

func newCurrentReadRuntime(t *testing.T) (*Server, execution.ExecutionManager) {
	t.Helper()
	mgr := memory.NewExecutionManager()
	server := assembleServer(mgr, newTestTaskQueue(), newTestTaskQueue())
	return server, mgr
}

func countSignaledEvents(events []history.Event) int {
	n := 0
	for _, e := range events {
		if e.WorkflowSignaled != nil {
			n++
		}
	}
	return n
}

// seedRun 建 Run 行；给了 events 时 Run 先以 Running 建好、写入 events，再落 status：存储拒绝在终态之后追加事件。
func seedRun(t *testing.T, mgr execution.ExecutionManager, id, workflowID string, status history.Status, isCurrent bool, events ...history.Event) {
	t.Helper()
	createStatus := status
	if len(events) > 0 {
		createStatus = history.StatusRunning
	}
	run := &execution.Run{
		ID:                    id,
		Namespace:             protocol.DefaultNamespace,
		WorkflowID:            workflowID,
		WorkflowType:          workflowID,
		WorkflowConfigVersion: "v1",
		Status:                createStatus,
		IsCurrent:             isCurrent,
		CreatedAt:             time.Now(),
	}
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(run)) }); err != nil {
		t.Fatal(err)
	}
	for _, event := range events {
		if _, err := mgr.Append(id, id, id, event); err != nil {
			t.Fatal(err)
		}
	}
	if createStatus == status {
		return
	}
	markRunTerminalForTest(t, mgr, id, status, nil)
}

func TestGetRun_RunIDPath_BehaviorUnchanged(t *testing.T) {
	// Happy Path: 传具体 runID（老路径），namespace/workflowID 留空，行为完全不变
	svc, mgr := newCurrentReadRuntime(t)
	seedRun(t, mgr, "run-1", "wf-a", history.StatusRunning, true)

	run, err := getRun(svc, "", "", "run-1")
	if err != nil {
		t.Fatal(err)
	}
	if run.ID != "run-1" {
		t.Fatalf("expected run-1, got %s", run.ID)
	}
}

func TestGetRun_RunIDEmpty_ResolvesCurrent(t *testing.T) {
	// Happy Path: runID 空 + workflowID → GetCurrentRunID 解析 current
	svc, mgr := newCurrentReadRuntime(t)
	// 旧代非 current，新代 current —— 解析必须落到 current
	seedRun(t, mgr, "run-old", "wf-a", history.StatusContinuedAsNew, false)
	seedRun(t, mgr, "run-current", "wf-a", history.StatusRunning, true)

	run, err := getRun(svc, protocol.DefaultNamespace, "wf-a", "")
	if err != nil {
		t.Fatal(err)
	}
	if run.ID != "run-current" {
		t.Fatalf("expected current run-current, got %s", run.ID)
	}
}

func TestGetRun_TerminalCurrent_StillResolvable(t *testing.T) {
	// Edge Case: current 指向终态 run（complete/terminate 后 current 不清）→ runID 空查 current 仍返回它
	svc, mgr := newCurrentReadRuntime(t)
	seedRun(t, mgr, "run-done", "wf-done", history.StatusCompleted, true)

	run, err := getRun(svc, protocol.DefaultNamespace, "wf-done", "")
	if err != nil {
		t.Fatalf("terminal current run must still be resolvable: %v", err)
	}
	if run.ID != "run-done" {
		t.Fatalf("expected run-done, got %s", run.ID)
	}
}

func TestGetRun_BothEmpty_BadRequest(t *testing.T) {
	// Error Case: runID 与 workflowID 皆空 → ErrRunOrWorkflowIDRequired
	svc, _ := newCurrentReadRuntime(t)

	_, err := getRun(svc, protocol.DefaultNamespace, "", "")
	if !errors.Is(err, execution.ErrRunOrWorkflowIDRequired) {
		t.Fatalf("expected ErrRunOrWorkflowIDRequired, got %v", err)
	}
}

func TestGetRun_NoCurrent(t *testing.T) {
	// Error Case: workflowID 存在但无任何 run → ErrNoCurrentRun
	svc, _ := newCurrentReadRuntime(t)

	_, err := getRun(svc, protocol.DefaultNamespace, "wf-missing", "")
	if !errors.Is(err, execution.ErrNoCurrentRun) {
		t.Fatalf("expected ErrNoCurrentRun, got %v", err)
	}
}

func TestGetEvents_RunIDEmpty_ResolvesCurrent(t *testing.T) {
	// Happy Path: GetEvents runID 空 + workflowID → 解析 current 并读其历史
	svc, mgr := newCurrentReadRuntime(t)
	seedRun(t, mgr, "run-evt", "wf-evt", history.StatusRunning, true)
	if _, err := mgr.Append("run-evt", "run-evt", "run-evt", history.NewWorkflowSignaledEvent("ping", nil)); err != nil {
		t.Fatal(err)
	}

	events, err := svc.View().GetEvents(protocol.DefaultNamespace, "wf-evt", "")
	if err != nil {
		t.Fatal(err)
	}
	if countSignaledEvents(events) != 1 {
		t.Fatalf("expected 1 signal event from resolved current run, got %+v", events)
	}
}

func TestGetEvents_RunIDPath_BehaviorUnchanged(t *testing.T) {
	// Happy Path: GetEvents 传具体 runID（老路径）行为不变
	svc, mgr := newCurrentReadRuntime(t)
	seedRun(t, mgr, "run-evt2", "wf-evt2", history.StatusRunning, true)
	if _, err := mgr.Append("run-evt2", "run-evt2", "run-evt2", history.NewWorkflowSignaledEvent("ping", nil)); err != nil {
		t.Fatal(err)
	}

	events, err := svc.View().GetEvents("", "", "run-evt2")
	if err != nil {
		t.Fatal(err)
	}
	if countSignaledEvents(events) != 1 {
		t.Fatalf("expected 1 signal event, got %+v", events)
	}
}

func TestGetEvents_BothEmpty_BadRequest(t *testing.T) {
	// Error Case: runID 与 workflowID 皆空 → ErrRunOrWorkflowIDRequired
	svc, _ := newCurrentReadRuntime(t)

	_, err := svc.View().GetEvents(protocol.DefaultNamespace, "", "")
	if !errors.Is(err, execution.ErrRunOrWorkflowIDRequired) {
		t.Fatalf("expected ErrRunOrWorkflowIDRequired, got %v", err)
	}
}

func TestQueryWorkflow_RunIDEmpty_ResolvesCurrent(t *testing.T) {
	// Happy Path: QueryWorkflow runID 空 + workflowID → 解析 current 并汇总状态
	ctx := context.Background()
	svc, mgr := newCurrentReadRuntime(t)
	seedRun(t, mgr, "run-q", "wf-q", history.StatusRunning, true)
	if _, err := mgr.Append("run-q", "run-q", "run-q", history.NewWorkflowSignaledEvent("payment", nil)); err != nil {
		t.Fatal(err)
	}

	result, err := svc.QueryWorkflow(ctx, protocol.DefaultNamespace, "wf-q", "", "state", nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.Run == nil || result.Run.ID != "run-q" {
		t.Fatalf("expected query on current run-q, got %+v", result.Run)
	}
	if len(result.Signals) != 1 || result.Signals[0].Name != "payment" {
		t.Fatalf("unexpected signals: %+v", result.Signals)
	}
}

func TestQueryWorkflow_TerminalCurrent_StillResolvable(t *testing.T) {
	// Edge Case: current 是终态（failed）run，runID 空 Query 仍找到它并汇总失败原因
	ctx := context.Background()
	svc, mgr := newCurrentReadRuntime(t)
	seedRun(t, mgr, "run-qf", "wf-qf", history.StatusFailed, true, history.NewWorkflowFailedEvent("boom", ""))

	result, err := svc.QueryWorkflow(ctx, protocol.DefaultNamespace, "wf-qf", "", "state", nil)
	if err != nil {
		t.Fatalf("terminal current run must still be queryable: %v", err)
	}
	if result.Run == nil || result.Run.ID != "run-qf" {
		t.Fatalf("expected query on terminal current run-qf, got %+v", result.Run)
	}
	if result.FailureReason != "boom" {
		t.Fatalf("expected failure reason boom, got %q", result.FailureReason)
	}
}

func TestQueryWorkflow_BothEmpty_BadRequest(t *testing.T) {
	// Error Case: runID 与 workflowID 皆空 → ErrRunOrWorkflowIDRequired
	ctx := context.Background()
	svc, _ := newCurrentReadRuntime(t)

	_, err := svc.QueryWorkflow(ctx, protocol.DefaultNamespace, "", "", "state", nil)
	if !errors.Is(err, execution.ErrRunOrWorkflowIDRequired) {
		t.Fatalf("expected ErrRunOrWorkflowIDRequired, got %v", err)
	}
}
