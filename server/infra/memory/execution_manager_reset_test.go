package memory_test

import (
	"context"
	"testing"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/program/programtest"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/app/assembly"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/biz/service/engine"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

// ============================================
// 真 reset 路径端到端测试（跑在真实 memory store 上）
// ============================================
// 测试策略:
// - 本测试从包外用 memory.NewExecutionManager() 构造真实 Server，验证 memory 版
//   ForkBranch + EventStore.forkEventsTo 端到端正确（截断边界 + nextID 推进）。
// - Happy Path: ResetType=last_workflow_task 走真 reset → 新 run 含截断前缀 + 1 个
//   WorkflowTaskFailed{Cause:"reset_workflow"} marker，前缀长度 = 到 last WFT close 的事件数。
// - Happy Path: first vs last 截断点不同 → 新 run 事件数不同（证明 forkEventsTo 对 first/last
//   截断边界都正确）。
// ============================================

// twoWFTCompletedRun 启动 run 并完成两个 Workflow Task（两个 WFT 边界），返回 runID 与两个 close 事件 ID。
// 中间塞一个非边界事件，使 first 与 last 截断点之间事件数有差异。
func twoWFTCompletedRun(t *testing.T, ctx context.Context, svc *engine.Server, mgr *memory.ExecutionManager, wfID string) (runID string, firstWFT, lastWFT int64) {
	t.Helper()
	runID, err := startedRunID(svc.StartWorkflow(ctx, "", wfID, wfID, "", nil))
	if err != nil {
		t.Fatal(err)
	}
	appendEvent := func(event history.Event) int64 {
		id, err := mgr.Append(runID, runID, runID, event)
		if err != nil {
			t.Fatal(err)
		}
		return id
	}
	events, err := mgr.GetEvents(runID)
	if err != nil {
		t.Fatal(err)
	}
	var scheduled int64
	for _, e := range events {
		if e.WorkflowTaskScheduled != nil {
			scheduled = e.ID
		}
	}
	if scheduled == 0 {
		t.Fatal("started run has no scheduled workflow task")
	}
	appendEvent(history.NewWorkflowTaskStartedEvent(scheduled, "", "", 0))
	firstWFT = appendEvent(history.NewWorkflowTaskCompletedEvent(scheduled, ""))
	activityID := appendEvent(history.NewActivityScheduledEvent(history.ActivityScheduledEvent{ActivityID: "a", ActivityType: "T"}))
	appendEvent(history.Event{ActivityCompleted: &history.ActivityCompletedEvent{ScheduledEventID: activityID}})
	scheduled = appendEvent(history.NewWorkflowTaskScheduledEvent(0))
	appendEvent(history.NewWorkflowTaskStartedEvent(scheduled, "", "", 0))
	lastWFT = appendEvent(history.NewWorkflowTaskCompletedEvent(scheduled, ""))
	return runID, firstWFT, lastWFT
}

func TestMemoryResetWorkflow_RealResetPath_WritesResetMarker(t *testing.T) {
	// Happy Path: 带 ResetType=last_workflow_task 的 reset 在真实 memory store 上走真 reset，
	// 新 run 含 WorkflowTaskFailed{Cause:reset_workflow} marker + 截断前缀（长度 = lastWFT）。
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	server := assembly.NewServer(assembly.Options{Stores: assembly.Stores{Executions: mgr}, WorkflowQueue: memory.NewTaskQueue(16), ActivityRouter: memory.NewActivityQueueRouter(16)})
	defer server.Stop()

	if _, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{Type: "wf-mem-real-reset", Program: programtest.Noop()}); err != nil {
		t.Fatal(err)
	}
	base, _, lastWFT := twoWFTCompletedRun(t, ctx, server, mgr, "wf-mem-real-reset")

	newRunID, err := server.ResetWorkflow(ctx, execution.ResetWorkflowRequest{
		RunID:     base,
		Reason:    "mem-real-reset",
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
	var resetMarkers, prefixCount int
	var markerID, maxPrefixID int64
	for _, e := range events {
		if e.EventType() == history.EventWorkflowTaskFailed && e.WorkflowTaskFailed != nil && e.WorkflowTaskFailed.Cause == "reset_workflow" {
			resetMarkers++
			markerID = e.ID
		} else if resetMarkers == 0 {
			prefixCount++
			if e.ID > maxPrefixID {
				maxPrefixID = e.ID
			}
		}
	}
	if resetMarkers != 1 {
		t.Fatalf("expected exactly 1 reset_workflow marker in new run, got %d (events=%d)", resetMarkers, len(events))
	}
	if int64(prefixCount) != lastWFT-1 {
		t.Fatalf("expected truncated prefix of %d events (before last WFT close), got %d", lastWFT-1, prefixCount)
	}
	// nextID 推进断言：marker 经 Append(nextIDForRun) 写入，若 forkEventsTo 漏推进 nextID，
	// marker 会拿到与前缀冲突的 id（从 1 重新计数）。要求 marker id 严格大于前缀最大 id。
	if markerID <= maxPrefixID {
		t.Fatalf("expected reset marker id (%d) > max prefix id (%d): forkEventsTo must advance nextID for the new run", markerID, maxPrefixID)
	}
}

func TestMemoryResetWorkflow_FirstVsLast_DifferentTruncation(t *testing.T) {
	// Happy Path: first reset 截到第一个 WFT close、last 截到最后一个 → 新 run 事件数不同。
	// 证明 memory 版 forkEventsTo 的截断边界对 first/last 都正确。
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	server := assembly.NewServer(assembly.Options{Stores: assembly.Stores{Executions: mgr}, WorkflowQueue: memory.NewTaskQueue(16), ActivityRouter: memory.NewActivityQueueRouter(16)})
	defer server.Stop()

	// 两个 base run 用独立 workflowID：同一 workflowID 至多一个 current（对标 Temporal），
	// 第二个 StartWorkflow 同 workflowID 会与首个 current 冲突。
	if _, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{Type: "wf-mem-first-last-a", Program: programtest.Noop()}); err != nil {
		t.Fatal(err)
	}
	if _, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{Type: "wf-mem-first-last-b", Program: programtest.Noop()}); err != nil {
		t.Fatal(err)
	}

	baseA, firstWFT, lastWFT := twoWFTCompletedRun(t, ctx, server, mgr, "wf-mem-first-last-a")
	firstRun, err := server.ResetWorkflow(ctx, execution.ResetWorkflowRequest{RunID: baseA, ResetType: execution.ResetTypeEnums.FirstWorkflowTask})
	if err != nil {
		t.Fatal(err)
	}

	baseB, _, _ := twoWFTCompletedRun(t, ctx, server, mgr, "wf-mem-first-last-b")
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
	if firstWFT == lastWFT {
		t.Fatalf("test precondition broken: firstWFT(%d) must differ from lastWFT(%d)", firstWFT, lastWFT)
	}
	if len(firstEvents) == len(lastEvents) {
		t.Fatalf("expected first(%d events) and last(%d events) reset to differ in event count", len(firstEvents), len(lastEvents))
	}
}

// startedRunID 取 Server 启动结果里的 Run ID。
func startedRunID(run *execution.Run, err error) (string, error) {
	if err != nil {
		return "", err
	}
	return run.ID, nil
}
