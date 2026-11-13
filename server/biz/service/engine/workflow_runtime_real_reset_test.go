package engine

import (
	"context"
	"testing"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/program/programtest"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

// ============================================
// 内存存储下的真 reset 路径测试（依赖 ForkBranch）
// ============================================
// 测试策略:
// - Happy Path: 带 ResetType=last 的 reset 走真 reset 路径（非 legacy）：新 run 含截断前缀
//   + WorkflowTaskFailed{Cause:"reset_workflow"} 事件
// - Happy Path: first vs last 截断点不同 → 新 run 的事件数不同（first 截到第一个 WFT close，last 截到最后一个）
// ============================================

// twoWFTCompletedRun：启动 run 并追加两个 WorkflowTaskCompleted（两个 WFT 边界），返回 runID。
// event 布局（per-run id 从 1 起）：
//
//	1 WorkflowTaskScheduled / 首个 WFT 相关（StartWorkflow 产生）
//	后续追加：一个 WorkflowTaskCompleted（first 边界）、一个 WorkflowTaskCompleted（last 边界）
func twoWFTCompletedRun(t *testing.T, ctx context.Context, svc *Server, mgr execution.ExecutionManager, wfID string) (string, int64, int64) {
	t.Helper()
	runID, err := startedRunID(svc.StartWorkflow(ctx, "", wfID, wfID, "", nil))
	if err != nil {
		t.Fatal(err)
	}
	firstWFT := completeOpenWorkflowTask(t, mgr, runID)
	// 中间塞一个非边界事件，使 first 与 last 截断点之间事件数有差异
	if _, err := mgr.Append(runID, runID, runID, history.NewActivityScheduledEvent(history.ActivityScheduledEvent{ActivityID: "a", ActivityType: "T"})); err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.Append(runID, runID, runID, history.Event{ActivityCompleted: &history.ActivityCompletedEvent{ScheduledEventID: firstWFT + 1}}); err != nil {
		t.Fatal(err)
	}
	lastWFT := completeOpenWorkflowTask(t, mgr, runID)
	return runID, firstWFT, lastWFT
}

func TestResetWorkflow_RealResetPath_WritesResetMarker(t *testing.T) {
	// Happy Path: 带 ResetType=last_workflow_task 的 reset 走真 reset，新 run 含 WorkflowTaskFailed{Cause:reset_workflow}
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	server := assembleServer(mgr, newTestTaskQueue(), newTestTaskQueue())

	if _, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{ID: "wf-real-reset", Program: programtest.Noop()}); err != nil {
		t.Fatal(err)
	}
	base, _, lastWFT := twoWFTCompletedRun(t, ctx, server, mgr, "wf-real-reset")

	newRunID, err := server.ResetWorkflow(ctx, execution.ResetWorkflowRequest{
		RunID:     base,
		Reason:    "real-reset",
		ResetType: execution.ResetTypeEnums.LastWorkflowTask,
	})
	if err != nil {
		t.Fatalf("reset: %v", err)
	}
	if newRunID == base {
		t.Fatal("expected a new run id distinct from base (real reset, not idempotent)")
	}

	events, err := mgr.GetEvents(newRunID)
	if err != nil {
		t.Fatal(err)
	}
	// 截断前缀（id <= lastWFT）+ 一个 reset marker。前缀长度应为 lastWFT 条。
	var resetMarkers, prefixCount int
	for _, e := range events {
		if e.EventType() == history.EventWorkflowTaskFailed && e.WorkflowTaskFailed != nil && e.WorkflowTaskFailed.Cause == "reset_workflow" {
			resetMarkers++
		} else if resetMarkers == 0 {
			prefixCount++
		}
	}
	if resetMarkers != 1 {
		t.Fatalf("expected exactly 1 reset_workflow marker in new run, got %d (events=%d)", resetMarkers, len(events))
	}
	if int64(prefixCount) != lastWFT-1 {
		t.Fatalf("expected truncated prefix of %d events (before last WFT close), got %d", lastWFT-1, prefixCount)
	}
}

func TestResetWorkflow_FirstVsLast_DifferentTruncation(t *testing.T) {
	// Happy Path: first reset 截到第一个 WFT close、last 截到最后一个 → 新 run 事件数不同
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	server := assembleServer(mgr, newTestTaskQueue(), newTestTaskQueue())

	// 两条 base 用独立 workflowID：同一 workflowID 至多一个 current/running（对标 Temporal），
	// reset 后 current 切到新 run，无法再为同 workflowID 首次 start 第二条 base。
	if _, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{ID: "wf-first-last-a", Program: programtest.Noop()}); err != nil {
		t.Fatal(err)
	}
	if _, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{ID: "wf-first-last-b", Program: programtest.Noop()}); err != nil {
		t.Fatal(err)
	}

	baseA, firstWFT, lastWFT := twoWFTCompletedRun(t, ctx, server, mgr, "wf-first-last-a")
	firstRun, err := server.ResetWorkflow(ctx, execution.ResetWorkflowRequest{RunID: baseA, ResetType: execution.ResetTypeEnums.FirstWorkflowTask})
	if err != nil {
		t.Fatal(err)
	}

	baseB, _, _ := twoWFTCompletedRun(t, ctx, server, mgr, "wf-first-last-b")
	lastRun, err := server.ResetWorkflow(ctx, execution.ResetWorkflowRequest{RunID: baseB, ResetType: execution.ResetTypeEnums.LastWorkflowTask})
	if err != nil {
		t.Fatal(err)
	}

	firstEvents, err := mgr.GetEvents(firstRun)
	if err != nil {
		t.Fatal(err)
	}
	lastEvents, err := mgr.GetEvents(lastRun)
	if err != nil {
		t.Fatal(err)
	}
	// first 截到 firstWFT 条前缀，last 截到 lastWFT 条前缀，二者必不同（中间隔了非边界事件）。
	if firstWFT == lastWFT {
		t.Fatalf("test precondition broken: firstWFT(%d) must differ from lastWFT(%d)", firstWFT, lastWFT)
	}
	if len(firstEvents) == len(lastEvents) {
		t.Fatalf("expected first(%d events) and last(%d events) reset to differ in event count", len(firstEvents), len(lastEvents))
	}
}

// ============================================
// reset 核心 3 步原子化测试（RunInTx：建新 run + reset_run_id + marker）
// ============================================
// 测试策略:
// - Happy Path: reset 成功后新 run 三要素全在（run 行 + 旧 run.ResetRunID + reset marker）
// - Error Case: 事务内第 3 步（落 marker）失败 → 整事务回滚，三要素全无（真原子）
// ============================================

func TestResetWorkflow_AtomicSuccess_AllThreePartsPresent(t *testing.T) {
	// Happy Path: reset 成功后，新 run 行存在 + 旧 run.ResetRunID 指向新 run + 新 run 含 reset marker，三者缺一不可
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	server := assembleServer(mgr, newTestTaskQueue(), newTestTaskQueue())

	if _, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{ID: "wf-atomic-ok", Program: programtest.Noop()}); err != nil {
		t.Fatal(err)
	}
	base, _, _ := twoWFTCompletedRun(t, ctx, server, mgr, "wf-atomic-ok")

	newRunID, err := server.ResetWorkflow(ctx, execution.ResetWorkflowRequest{
		RunID:     base,
		Reason:    "atomic-ok",
		ResetType: execution.ResetTypeEnums.LastWorkflowTask,
	})
	if err != nil {
		t.Fatalf("reset: %v", err)
	}

	// 1) 新 run 行存在
	newRun, err := mgr.Get(newRunID)
	if err != nil || newRun == nil {
		t.Fatalf("expected new run %s to exist, got run=%v err=%v", newRunID, newRun, err)
	}
	// 2) 旧 run.ResetRunID 指向新 run（SetResetRunID 已提交）
	oldRun, err := mgr.Get(base)
	if err != nil || oldRun == nil {
		t.Fatalf("get old run: %v", err)
	}
	if oldRun.ResetRunID != newRunID {
		t.Fatalf("expected old run ResetRunID=%s, got %q", newRunID, oldRun.ResetRunID)
	}
	// 3) 新 run 含 reset marker
	events, err := mgr.GetEvents(newRunID)
	if err != nil {
		t.Fatal(err)
	}
	var markers int
	for _, e := range events {
		if e.EventType() == history.EventWorkflowTaskFailed && e.WorkflowTaskFailed != nil && e.WorkflowTaskFailed.Cause == "reset_workflow" {
			markers++
		}
	}
	if markers != 1 {
		t.Fatalf("expected exactly 1 reset marker, got %d", markers)
	}
}

func TestResetWorkflow_AtomicRollback_MarkerAppendFails_NothingPersisted(t *testing.T) {
	// Error Case: 注入第 3 步（落 reset marker，类型 WorkflowTaskFailed）失败 → RunInTx 整体回滚，
	// 新 run 行 + 旧 run.ResetRunID + marker 全无（核心 3 步真原子，杜绝僵尸 run / 断链）
	ctx := context.Background()
	mgr := newFailingEventManager()
	server := assembleServer(mgr, newTestTaskQueue(), newTestTaskQueue())

	if _, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{ID: "wf-atomic-rb", Program: programtest.Noop()}); err != nil {
		t.Fatal(err)
	}
	base, _, _ := twoWFTCompletedRun(t, ctx, server, mgr, "wf-atomic-rb")

	// 注入：事务内 Append reset marker（WorkflowTaskFailed）失败。
	mgr.failOn(history.EventWorkflowTaskFailed)

	newRunID, err := server.ResetWorkflow(ctx, execution.ResetWorkflowRequest{
		RunID:     base,
		Reason:    "atomic-rollback",
		ResetType: execution.ResetTypeEnums.LastWorkflowTask,
	})
	if err == nil {
		t.Fatalf("expected reset to fail (marker append injected failure), got newRunID=%s", newRunID)
	}

	// 解除注入，以便后续断言用 Get/GetEvents 读取真实存储状态。
	mgr.failOn("")

	// 1) 新 run 行不应存在（CreateRun 已随事务回滚）
	if newRunID != "" {
		if run, _ := mgr.Get(newRunID); run != nil {
			t.Fatalf("expected no new run after rollback, but run %s exists", newRunID)
		}
	}
	// 2) 旧 run.ResetRunID 不应被设置（SetResetRunID 已随事务回滚）
	oldRun, err := mgr.Get(base)
	if err != nil || oldRun == nil {
		t.Fatalf("get old run: %v", err)
	}
	if oldRun.ResetRunID != "" {
		t.Fatalf("expected old run ResetRunID empty after rollback, got %q", oldRun.ResetRunID)
	}
	// 3) 不应有任何含 reset marker 的新 run 事件（marker Append 已随事务回滚）
	if newRunID != "" {
		events, _ := mgr.GetEvents(newRunID)
		for _, e := range events {
			if e.EventType() == history.EventWorkflowTaskFailed && e.WorkflowTaskFailed != nil && e.WorkflowTaskFailed.Cause == "reset_workflow" {
				t.Fatalf("expected no reset marker after rollback, but found one in run %s", newRunID)
			}
		}
	}
}
