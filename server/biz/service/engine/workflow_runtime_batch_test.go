package engine

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/program/programtest"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/batch"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/biz/usecase/batches"
	"github.com/donutnomad/tenuto/server/biz/view/executionview"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

// ============================================
// Batch Operations 测试
// ============================================
// 测试策略:
// - Happy Path: 批量 Cancel 3 个 running 的 Run，succeeded_count=3
// - Happy Path: 批量 Terminate 2 个 running 的 Run，全部终止
// - Happy Path: 启动批量操作后按 ID 查询，字段匹配
// - Happy Path: 启动 2 个批量操作后 List，返回全部
// - Edge Case: 已结束的 Run 对批量 Terminate 与 Cancel 计为 skipped
// ============================================

func TestStartBatchOperation_Cancel(t *testing.T) {
	// Happy Path: 启动 3 个 Run，批量 Cancel，验证 succeeded_count=3
	ctx := context.Background()
	server := newTestServer()

	// 每个 run 用独立 workflowID：同一 workflowID 至多一个 current/running（对标 Temporal），
	// 无法并存 3 个 running 同 workflowID 的 run。批量过滤改用 namespace+status。
	runIDs := make([]string, 3)
	for i := range 3 {
		wfID := fmt.Sprintf("wf-batch-%d", i)
		if _, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{ID: wfID, Program: programtest.Noop()}); err != nil {
			t.Fatal(err)
		}
		id, err := startedRunID(server.StartWorkflow(ctx, "", wfID, wfID, "", nil))
		if err != nil {
			t.Fatal(err)
		}
		runIDs[i] = id
	}

	op, err := driveBatch(server, batches.StartRequest{
		Namespace: "default",
		Type:      batch.TypeEnums.Cancel,
		Filter: execution.RunListFilter{
			Statuses: []history.Status{history.StatusRunning},
		},
		Reason: "batch cancel test",
	})
	if err != nil {
		t.Fatal(err)
	}
	if op.TotalCount != 3 {
		t.Fatalf("expected total_count=3, got %d", op.TotalCount)
	}
	if op.SucceededCount != 3 {
		t.Fatalf("expected succeeded_count=3, got %d", op.SucceededCount)
	}
	if op.FailedCount != 0 {
		t.Fatalf("expected failed_count=0, got %d", op.FailedCount)
	}
	if op.Status != batch.StatusEnums.Completed {
		t.Fatalf("expected status=completed, got %s", op.Status)
	}

	// Cancel is cooperative: runs remain running but have CancellationRequested event
	for _, runID := range runIDs {
		run, err := getRun(server, "", "", runID)
		if err != nil {
			t.Fatal(err)
		}
		if run.Status != history.StatusRunning {
			t.Fatalf("run %s: expected status=running (cancel is cooperative), got %s", runID, run.Status)
		}
		events, err := server.View().GetEvents("", "", runID)
		if err != nil {
			t.Fatal(err)
		}
		found := false
		for _, e := range events {
			if e.WorkflowCancellationRequested != nil {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("run %s: expected CancellationRequested event in history", runID)
		}
	}
}

func TestStartBatchOperation_Terminate(t *testing.T) {
	// Happy Path: 启动 2 个 Run，批量 Terminate，验证全部终止
	ctx := context.Background()
	server := newTestServer()

	// 每个 run 用独立 workflowID：同一 workflowID 至多一个 current/running（对标 Temporal），
	// 无法并存 2 个 running 同 workflowID 的 run。批量过滤改用 namespace+status。
	runIDs := make([]string, 2)
	for i := range 2 {
		wfID := fmt.Sprintf("wf-term-%d", i)
		if _, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{ID: wfID, Program: programtest.Noop()}); err != nil {
			t.Fatal(err)
		}
		id, err := startedRunID(server.StartWorkflow(ctx, "", wfID, wfID, "", nil))
		if err != nil {
			t.Fatal(err)
		}
		runIDs[i] = id
	}

	op, err := driveBatch(server, batches.StartRequest{
		Namespace: "default",
		Type:      batch.TypeEnums.Terminate,
		Filter: execution.RunListFilter{
			Statuses: []history.Status{history.StatusRunning},
		},
		Reason: "shutdown",
	})
	if err != nil {
		t.Fatal(err)
	}
	if op.SucceededCount != 2 {
		t.Fatalf("expected succeeded_count=2, got %d", op.SucceededCount)
	}
	if op.Status != batch.StatusEnums.Completed {
		t.Fatalf("expected status=completed, got %s", op.Status)
	}

	for _, runID := range runIDs {
		run, err := getRun(server, "", "", runID)
		if err != nil {
			t.Fatal(err)
		}
		if run.Status != history.StatusTerminated {
			t.Fatalf("run %s: expected status=terminated, got %s", runID, run.Status)
		}
	}
}

func TestBatchTerminateCountsClosedRunsAsSkipped(t *testing.T) {
	// Edge Case: 显式选中的 Run 已经结束（例如上一任驱动在保存进度前已终止它）计为 skipped，不算失败；
	// 批量取消对已结束的 Run 同样跳过
	ctx := context.Background()
	server := newTestServer()
	runIDs := make([]string, 3)
	for i := range 3 {
		wfID := fmt.Sprintf("wf-term-skip-%d", i)
		if _, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{ID: wfID, Program: programtest.Noop()}); err != nil {
			t.Fatal(err)
		}
		id, err := startedRunID(server.StartWorkflow(ctx, "", wfID, wfID, "", nil))
		if err != nil {
			t.Fatal(err)
		}
		runIDs[i] = id
	}
	if err := server.TerminateWorkflow(ctx, "", "", runIDs[1], "already done"); err != nil {
		t.Fatal(err)
	}

	for _, typ := range []batch.Type{batch.TypeEnums.Terminate, batch.TypeEnums.Cancel} {
		op, err := driveBatch(server, batches.StartRequest{Type: typ, Filter: execution.RunListFilter{RunIDs: runIDs}})
		if err != nil {
			t.Fatal(err)
		}
		wantSucceeded, wantSkipped := 2, 1
		if typ == batch.TypeEnums.Cancel {
			// 终止批量已关闭全部 Run。
			wantSucceeded, wantSkipped = 0, 3
		}
		if op.Status != batch.StatusEnums.Completed || op.SucceededCount != wantSucceeded || op.SkippedCount != wantSkipped || op.FailedCount != 0 || op.TotalCount != 3 {
			t.Fatalf("%s batch = %+v, want %d succeeded and %d skipped", typ, op, wantSucceeded, wantSkipped)
		}
	}
}

func TestGetBatchOperation(t *testing.T) {
	// Happy Path: 启动批量操作后按 namespace+ID 查询，验证字段匹配
	ctx := context.Background()
	server := newTestServer()

	if _, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{ID: "wf-get-batch", Program: programtest.Noop()}); err != nil {
		t.Fatal(err)
	}
	if _, err := server.StartWorkflow(ctx, "", "wf-get-batch", "wf-get-batch", "", nil); err != nil {
		t.Fatal(err)
	}

	op, err := driveBatch(server, batches.StartRequest{
		Namespace: "default",
		Type:      batch.TypeEnums.Cancel,
		Filter: execution.RunListFilter{
			WorkflowID: "wf-get-batch",
			Statuses:   []history.Status{history.StatusRunning},
		},
		Reason: "get-test",
	})
	if err != nil {
		t.Fatal(err)
	}

	got, err := server.Batches().Get("default", op.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != op.ID {
		t.Fatalf("expected id=%s, got %s", op.ID, got.ID)
	}
	if got.Type != batch.TypeEnums.Cancel {
		t.Fatalf("expected type=cancel, got %s", got.Type)
	}
	if got.Namespace != "default" {
		t.Fatalf("expected namespace=default, got %s", got.Namespace)
	}
	if got.Reason != "get-test" {
		t.Fatalf("expected reason=get-test, got %s", got.Reason)
	}
	if got.TotalCount != 1 {
		t.Fatalf("expected total_count=1, got %d", got.TotalCount)
	}
	if got.SucceededCount != 1 {
		t.Fatalf("expected succeeded_count=1, got %d", got.SucceededCount)
	}
}

func TestListBatchOperations(t *testing.T) {
	// Happy Path: 启动 2 个批量操作，按 namespace 列出，验证返回全部
	ctx := context.Background()
	server := newTestServer()

	// 每个 run 用独立 workflowID：同一 workflowID 至多一个 current/running（对标 Temporal）。
	// 批量过滤改用 namespace+status。
	for i := range 2 {
		wfID := fmt.Sprintf("wf-list-batch-%d", i)
		if _, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{ID: wfID, Program: programtest.Noop()}); err != nil {
			t.Fatal(err)
		}
		if _, err := server.StartWorkflow(ctx, "", wfID, wfID, "", nil); err != nil {
			t.Fatal(err)
		}
	}

	if _, err := driveBatch(server, batches.StartRequest{
		Namespace: "default",
		Type:      batch.TypeEnums.Cancel,
		Filter:    execution.RunListFilter{Statuses: []history.Status{history.StatusRunning}},
	}); err != nil {
		t.Fatal(err)
	}

	for i := range 2 {
		wfID := fmt.Sprintf("wf-list-batch-%d", i+2)
		if _, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{ID: wfID, Program: programtest.Noop()}); err != nil {
			t.Fatal(err)
		}
		if _, err := server.StartWorkflow(ctx, "", wfID, wfID, "", nil); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := driveBatch(server, batches.StartRequest{
		Namespace: "default",
		Type:      batch.TypeEnums.Terminate,
		Filter:    execution.RunListFilter{Statuses: []history.Status{history.StatusRunning}},
		Reason:    "second",
	}); err != nil {
		t.Fatal(err)
	}

	result, err := server.Batches().List(batches.ListFilter{Namespace: "default"})
	if err != nil {
		t.Fatal(err)
	}
	if result.Total != 2 {
		t.Fatalf("expected 2 batch ops, got %d", result.Total)
	}
	types := map[batch.Type]bool{result.Items[0].Type: true, result.Items[1].Type: true}
	if !types[batch.TypeEnums.Cancel] || !types[batch.TypeEnums.Terminate] {
		t.Fatalf("expected both cancel and terminate ops, got %+v", result.Items)
	}
}

// ============================================
// Reset Workflow 测试
// ============================================
// 测试策略:
// - Happy Path: 完成一个 Run 后 Reset，返回新 runID
// - Happy Path: Reset 仍在运行的 Run，自动终止后产生新 Run
// ============================================

func TestResetWorkflow_Success(t *testing.T) {
	// Happy Path: 完成一个 workflow run 后 Reset，验证返回新的 runID 且 workflowID 相同
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	server := assembleServer(mgr, newTestTaskQueue(), newTestTaskQueue())

	if _, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{ID: "wf-reset", Program: programtest.Noop()}); err != nil {
		t.Fatal(err)
	}
	runID, err := startedRunID(server.StartWorkflow(ctx, "", "wf-reset", "wf-reset", "", map[string]any{"key": "val"}))
	if err != nil {
		t.Fatal(err)
	}

	if err := server.HandleWorkflowTaskCompleted(server.WorkflowTaskContext(ctx, runID, openWorkflowTaskForTest(t, mgr, runID)), runID, openWorkflowTaskForTest(t, mgr, runID), nil, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.Append(runID, runID, runID, history.NewWorkflowCompletedEvent(nil)); err != nil {
		t.Fatal(err)
	}

	newRunID, err := server.ResetWorkflow(ctx, execution.ResetWorkflowRequest{RunID: runID, Reason: "retry", ResetType: execution.ResetTypeEnums.LastWorkflowTask})
	if err != nil {
		t.Fatal(err)
	}
	if newRunID == "" {
		t.Fatal("expected non-empty new run ID")
	}
	if newRunID == runID {
		t.Fatal("expected new run ID to differ from original")
	}

	newRun, err := getRun(server, "", "", newRunID)
	if err != nil {
		t.Fatal(err)
	}
	if newRun.WorkflowID != "wf-reset" {
		t.Fatalf("expected workflow_id=wf-reset, got %s", newRun.WorkflowID)
	}
}

func TestResetWorkflow_StillRunning(t *testing.T) {
	// Happy Path: Reset 一个仍在运行的 Run，应自动终止后重启
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	server := assembleServer(mgr, newTestTaskQueue(), newTestTaskQueue())

	if _, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{ID: "wf-reset-run", Program: programtest.Noop()}); err != nil {
		t.Fatal(err)
	}
	runID, err := startedRunID(server.StartWorkflow(ctx, "", "wf-reset-run", "wf-reset-run", "", nil))
	if err != nil {
		t.Fatal(err)
	}
	if err := server.HandleWorkflowTaskCompleted(server.WorkflowTaskContext(ctx, runID, openWorkflowTaskForTest(t, mgr, runID)), runID, openWorkflowTaskForTest(t, mgr, runID), nil, ""); err != nil {
		t.Fatal(err)
	}

	newRunID, err := server.ResetWorkflow(ctx, execution.ResetWorkflowRequest{RunID: runID, Reason: "retry", ResetType: execution.ResetTypeEnums.LastWorkflowTask})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if newRunID == "" || newRunID == runID {
		t.Fatal("expected a new run ID different from original")
	}
}

// ============================================
// Task Queues 测试
// ============================================
// 测试策略:
// - Happy Path: 注册 2 个 worker 使用不同 task queue，验证 list 返回两个队列
// - Edge Case: 无 worker 注册时返回空列表
// ============================================

func TestListTaskQueues(t *testing.T) {
	// Happy Path: 注册 2 个 worker 使用不同 task queue，验证 list 返回两个队列
	server := assembleServerWithoutTestWorker(memory.NewExecutionManager())

	workers := []protocol.ActivityWorkerVersion{
		{
			Namespace:     "default",
			Identity:      "worker-q1",
			BuildID:       "build-1",
			TaskQueues:    []string{"queue-alpha"},
			ActivityTypes: []string{"activity.Foo"},
			HeartbeatTTL:  "1m",
		},
		{
			Namespace:     "default",
			Identity:      "worker-q2",
			BuildID:       "build-2",
			TaskQueues:    []string{"queue-beta"},
			ActivityTypes: []string{"activity.Bar"},
			HeartbeatTTL:  "1m",
		},
	}
	for _, w := range workers {
		if err := server.RegisterActivityWorkerVersion(context.Background(), w); err != nil {
			t.Fatal(err)
		}
	}

	result, err := server.View().ListTaskQueues("default")
	if err != nil {
		t.Fatal(err)
	}
	if result.Total != 2 {
		t.Fatalf("expected 2 task queues, got %d", result.Total)
	}

	names := map[string]bool{}
	for _, item := range result.Items {
		names[item.Name] = true
		if item.Namespace != "default" {
			t.Fatalf("expected namespace=default, got %s", item.Namespace)
		}
		if item.ActiveWorkers != 1 {
			t.Fatalf("queue %s: expected 1 active worker, got %d", item.Name, item.ActiveWorkers)
		}
	}
	if !names["queue-alpha"] || !names["queue-beta"] {
		t.Fatalf("expected queue-alpha and queue-beta, got %v", names)
	}
}

func TestListTaskQueues_Empty(t *testing.T) {
	// Edge Case: 无 worker 注册时返回空列表
	server := assembleServerWithoutTestWorker(memory.NewExecutionManager())

	result, err := server.View().ListTaskQueues("default")
	if err != nil {
		t.Fatal(err)
	}
	if result.Total != 0 {
		t.Fatalf("expected 0 task queues, got %d", result.Total)
	}
	if len(result.Items) != 0 {
		t.Fatalf("expected empty items, got %+v", result.Items)
	}
}

// ============================================
// Namespaces 测试
// ============================================
// 测试策略:
// - Happy Path: 在 2 个 namespace 注册 workflow 并创建 Run，验证都被列出且计数正确
// - Edge Case: 新建 server 无任何数据时返回空列表
// ============================================

func TestListNamespaces(t *testing.T) {
	// Happy Path: 在 2 个 namespace 注册 workflow 并创建 Run，验证 namespace 列表和计数
	mgr := memory.NewExecutionManager()
	server := assembleServer(mgr, newTestTaskQueue(), newTestTaskQueue())

	if _, err := server.Definitions().RegisterWorkflowInNamespace("ns-x", &program.WorkflowConfig{ID: "wf-x", Program: programtest.Noop()}); err != nil {
		t.Fatal(err)
	}
	if _, err := server.Definitions().RegisterWorkflowInNamespace("ns-y", &program.WorkflowConfig{ID: "wf-y1", Program: programtest.Noop()}); err != nil {
		t.Fatal(err)
	}
	if _, err := server.Definitions().RegisterWorkflowInNamespace("ns-y", &program.WorkflowConfig{ID: "wf-y2", Program: programtest.Noop()}); err != nil {
		t.Fatal(err)
	}

	for i := range 2 {
		run := &execution.Run{
			ID:                    fmt.Sprintf("run-x-%d", i),
			Namespace:             "ns-x",
			WorkflowID:            "wf-x",
			WorkflowType:          "wf-x",
			WorkflowConfigVersion: "v1",
			Status:                history.StatusRunning,
			CreatedAt:             time.Now(),
		}
		if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(run)) }); err != nil {
			t.Fatal(err)
		}
	}
	for i := range 3 {
		run := &execution.Run{
			ID:                    fmt.Sprintf("run-y-%d", i),
			Namespace:             "ns-y",
			WorkflowID:            "wf-y1",
			WorkflowType:          "wf-y1",
			WorkflowConfigVersion: "v1",
			Status:                history.StatusRunning,
			CreatedAt:             time.Now(),
		}
		if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(run)) }); err != nil {
			t.Fatal(err)
		}
	}

	result, err := server.View().ListNamespaces()
	if err != nil {
		t.Fatal(err)
	}
	if result.Total < 2 {
		t.Fatalf("expected at least 2 namespaces, got %d", result.Total)
	}

	nsMap := make(map[string]executionview.NamespaceInfo)
	for _, info := range result.Items {
		nsMap[info.Name] = info
	}

	nsX, ok := nsMap["ns-x"]
	if !ok {
		t.Fatal("expected ns-x in namespace list")
	}
	if nsX.RunCount != 2 {
		t.Fatalf("ns-x: expected run_count=2, got %d", nsX.RunCount)
	}

	nsY, ok := nsMap["ns-y"]
	if !ok {
		t.Fatal("expected ns-y in namespace list")
	}
	if nsY.RunCount != 3 {
		t.Fatalf("ns-y: expected run_count=3, got %d", nsY.RunCount)
	}
	if nsY.WorkflowCount != 2 {
		t.Fatalf("ns-y: expected workflow_count=2, got %d", nsY.WorkflowCount)
	}
}

func TestListNamespaces_Empty(t *testing.T) {
	// Edge Case: 新建 server 无任何数据时返回空列表
	server := assembleServerWithoutTestWorker(memory.NewExecutionManager())

	result, err := server.View().ListNamespaces()
	if err != nil {
		t.Fatal(err)
	}
	if result.Total != 0 {
		t.Fatalf("expected 0 namespaces, got %d", result.Total)
	}
	if len(result.Items) != 0 {
		t.Fatalf("expected empty items, got %+v", result.Items)
	}
}
