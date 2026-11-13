package engine

import (
	"fmt"
	"strings"
	"testing"

	"context"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/program/programtest"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/batch"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/biz/usecase/batches"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

// ============================================
// Batch Reset 边界/容错测试
// ============================================
// 测试策略:
// - Error Case (容错): 一批 reset 部分失败（部分 run 无 WFT close），成功计入 SucceededCount、
//   失败计入 FailedCount 且记入 Errors，循环不中断（失败后续 run 继续处理），整批 Status=Completed
// - Edge Case: 同批两个 history 长度不同的 run，ResetType=last 各自截到自己的 last 边界（事件数不同）
// - Error Case: 整批全部 reset 失败（都无 WFT close），Status=Failed、SucceededCount=0、FailedCount=全部
// - Edge Case: filter 匹配 0 run，不 panic、TotalCount=0、SucceededCount=0、Status 合理(Completed)
// ============================================

// batchResetRunWithWFTs：注册独立 workflowID 并启动 run，追加 n 个 WFT close 边界（每个边界前插一个非边界事件），返回 runID。
// n=0 时仅启动（无 WFT close），用于构造 reset 失败场景。
// 用独立 workflowID：同一 workflowID 至多一个 current/running（对标 Temporal）。
func batchResetRunWithWFTs(t *testing.T, ctx context.Context, svc *Server, mgr execution.ExecutionManager, wfID string, n int) string {
	t.Helper()
	if _, err := svc.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{ID: wfID, Program: programtest.Noop()}); err != nil {
		t.Fatal(err)
	}
	runID, err := startedRunID(svc.StartWorkflow(ctx, "", wfID, wfID, "", nil))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		scheduled, err := mgr.Append(runID, runID, runID, history.NewActivityScheduledEvent(history.ActivityScheduledEvent{ActivityID: "a", ActivityType: "T"}))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := mgr.Append(runID, runID, runID, history.Event{ActivityCompleted: &history.ActivityCompletedEvent{ScheduledEventID: scheduled}}); err != nil {
			t.Fatal(err)
		}
		completeOpenWorkflowTask(t, mgr, runID)
	}
	return runID
}

func TestStartBatchOperation_BatchReset_PartialFailure_ContinuesAndCompletes(t *testing.T) {
	// Error Case (容错): 一批 4 个 run reset，其中 2 个无 WFT close 会失败。
	// 断言: 成功计入 SucceededCount、失败计入 FailedCount 且记入 Errors（含对应 run_id），
	// 循环不中断（失败的 run 不阻断后续成功的 run），整批 Status=Completed（非全失败）。
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	server := assembleServer(mgr, newTestTaskQueue(), newTestTaskQueue())

	// 交错排列：失败、成功、失败、成功 —— 验证失败不中断后续处理，且失败夹在成功之间也能继续
	failA := batchResetRunWithWFTs(t, ctx, server, mgr, "wf-batch-partial-faila", 0) // 无 WFT close → reset 失败
	okA := batchResetRunWithWFTs(t, ctx, server, mgr, "wf-batch-partial-oka", 1)     // 有 WFT close → reset 成功
	failB := batchResetRunWithWFTs(t, ctx, server, mgr, "wf-batch-partial-failb", 0) // 无 WFT close → reset 失败
	okB := batchResetRunWithWFTs(t, ctx, server, mgr, "wf-batch-partial-okb", 2)     // 有 WFT close → reset 成功

	op, err := driveBatch(server, batches.StartRequest{
		Namespace: "default",
		Type:      batch.TypeEnums.Reset,
		Filter:    execution.RunListFilter{RunIDs: []string{failA, okA, failB, okB}, IncludeTerminal: true},
		Reason:    "partial failure test",
		ResetType: execution.ResetTypeEnums.LastWorkflowTask,
	})
	if err != nil {
		t.Fatal(err)
	}

	if op.TotalCount != 4 {
		t.Fatalf("expected total_count=4, got %d", op.TotalCount)
	}
	if op.SucceededCount != 2 {
		t.Fatalf("expected succeeded_count=2, got %d (errors=%+v)", op.SucceededCount, op.Errors)
	}
	if op.FailedCount != 2 {
		t.Fatalf("expected failed_count=2, got %d", op.FailedCount)
	}
	// 整批不因部分失败而中断：成功与失败合计 == TotalCount（证明全部 run 都被处理，循环未提前 break/return）
	if op.SucceededCount+op.FailedCount != op.TotalCount {
		t.Fatalf("expected all runs processed (success+failure==total): success=%d failure=%d total=%d", op.SucceededCount, op.FailedCount, op.TotalCount)
	}
	if op.Status != batch.StatusEnums.Completed {
		t.Fatalf("expected status=completed (has successes), got %s", op.Status)
	}

	// op.Errors 含两个失败项，且 run_id 恰好是 failA / failB（成功的不应入 Errors）
	if len(op.Errors) != 2 {
		t.Fatalf("expected 2 errors, got %d: %+v", len(op.Errors), op.Errors)
	}
	errByRun := map[string]string{}
	for _, e := range op.Errors {
		errByRun[e.RunID] = e.Message
	}
	for _, id := range []string{failA, failB} {
		msg, ok := errByRun[id]
		if !ok {
			t.Fatalf("expected error entry for failed run %s, got errors=%+v", id, op.Errors)
		}
		if !strings.Contains(msg, "no workflow task close events found") {
			t.Fatalf("run %s: expected 'no workflow task close events found' error, got %q", id, msg)
		}
	}
	for _, id := range []string{okA, okB} {
		if _, ok := errByRun[id]; ok {
			t.Fatalf("successful run %s should not appear in Errors: %+v", id, op.Errors)
		}
	}

	// 成功的 run 确实发生了真 reset（设置了 ResetRunID）
	for _, id := range []string{okA, okB} {
		run, err := mgr.Get(id)
		if err != nil {
			t.Fatal(err)
		}
		if run.ResetRunID == "" {
			t.Fatalf("successful run %s: expected ResetRunID set (real reset)", id)
		}
	}
}

func TestStartBatchOperation_BatchReset_PerRunDistinctTruncation(t *testing.T) {
	// Edge Case: 同批两个 history 长度不同的 run，A 有 1 个 WFT close、B 有 3 个，ResetType=last。
	// 断言: 各自截到自己的 last 边界 —— 新 run 事件数随源 history 长度不同而不同，不共用一个 eventID。
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	server := assembleServer(mgr, newTestTaskQueue(), newTestTaskQueue())

	runA := batchResetRunWithWFTs(t, ctx, server, mgr, "wf-batch-distinct-a", 1) // 1 个 WFT close
	runB := batchResetRunWithWFTs(t, ctx, server, mgr, "wf-batch-distinct-b", 3) // 3 个 WFT close

	op, err := driveBatch(server, batches.StartRequest{
		Namespace: "default",
		Type:      batch.TypeEnums.Reset,
		Filter:    execution.RunListFilter{RunIDs: []string{runA, runB}, IncludeTerminal: true},
		Reason:    "distinct truncation test",
		ResetType: execution.ResetTypeEnums.LastWorkflowTask,
	})
	if err != nil {
		t.Fatal(err)
	}
	if op.SucceededCount != 2 {
		t.Fatalf("expected succeeded_count=2, got %d (errors=%+v)", op.SucceededCount, op.Errors)
	}

	ra, _ := mgr.Get(runA)
	rb, _ := mgr.Get(runB)
	if ra.ResetRunID == "" || rb.ResetRunID == "" {
		t.Fatalf("expected both runs reset: A.ResetRunID=%q B.ResetRunID=%q", ra.ResetRunID, rb.ResetRunID)
	}
	if ra.ResetRunID == rb.ResetRunID {
		t.Fatalf("expected distinct new runs, both got %s", ra.ResetRunID)
	}

	aEvents, err := mgr.GetEvents(ra.ResetRunID)
	if err != nil {
		t.Fatal(err)
	}
	bEvents, err := mgr.GetEvents(rb.ResetRunID)
	if err != nil {
		t.Fatal(err)
	}
	// B 的 history 更长，last WFT close 的 nodeID 更靠后，截断后的新 run 应比 A 含更多事件。
	// 若两个 run 误用了同一截断点（共用 eventID），新 run 事件数会相等 —— 此处断言不相等以钉死"各自解析"。
	if len(aEvents) == len(bEvents) {
		t.Fatalf("expected per-run distinct truncation: A new run events=%d, B new run events=%d (must differ)", len(aEvents), len(bEvents))
	}
	if len(aEvents) >= len(bEvents) {
		t.Fatalf("expected B (longer history) to truncate later → more events than A: A=%d B=%d", len(aEvents), len(bEvents))
	}
}

func TestStartBatchOperation_BatchReset_AllFail_StatusFailed(t *testing.T) {
	// Error Case: 一批 3 个 run 全部 reset 失败（都无 WFT close）。
	// 断言: Status=Failed、SucceededCount=0、FailedCount=全部、Errors 覆盖全部 run。
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	server := assembleServer(mgr, newTestTaskQueue(), newTestTaskQueue())

	runIDs := make([]string, 3)
	for i := range runIDs {
		runIDs[i] = batchResetRunWithWFTs(t, ctx, server, mgr, fmt.Sprintf("wf-batch-allfail-%d", i), 0) // 无 WFT close → 全失败
	}

	op, err := driveBatch(server, batches.StartRequest{
		Namespace: "default",
		Type:      batch.TypeEnums.Reset,
		Filter:    execution.RunListFilter{RunIDs: runIDs, IncludeTerminal: true},
		Reason:    "all fail test",
		ResetType: execution.ResetTypeEnums.LastWorkflowTask,
	})
	if err != nil {
		t.Fatal(err)
	}
	if op.TotalCount != 3 {
		t.Fatalf("expected total_count=3, got %d", op.TotalCount)
	}
	if op.SucceededCount != 0 {
		t.Fatalf("expected succeeded_count=0, got %d", op.SucceededCount)
	}
	if op.FailedCount != 3 {
		t.Fatalf("expected failed_count=3, got %d", op.FailedCount)
	}
	if op.Status != batch.StatusEnums.Failed {
		t.Fatalf("expected status=failed (all failed), got %s", op.Status)
	}
	if len(op.Errors) != 3 {
		t.Fatalf("expected 3 error entries, got %d: %+v", len(op.Errors), op.Errors)
	}
}

func TestStartBatchOperation_BatchReset_EmptyMatch_NoPanic(t *testing.T) {
	// Edge Case: filter 匹配 0 个 run（不存在的 workflow_id）。
	// 断言: 不 panic、TotalCount=0、SucceededCount=0、FailedCount=0、Status=Completed（无失败即非 Failed）。
	mgr := memory.NewExecutionManager()
	server := assembleServer(mgr, newTestTaskQueue(), newTestTaskQueue())

	if _, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{ID: "wf-batch-empty", Program: programtest.Noop()}); err != nil {
		t.Fatal(err)
	}

	op, err := driveBatch(server, batches.StartRequest{
		Namespace: "default",
		Type:      batch.TypeEnums.Reset,
		Filter:    execution.RunListFilter{WorkflowID: "wf-batch-empty-nomatch", IncludeTerminal: true},
		Reason:    "empty match test",
		ResetType: execution.ResetTypeEnums.LastWorkflowTask,
	})
	if err != nil {
		t.Fatal(err)
	}
	if op.TotalCount != 0 {
		t.Fatalf("expected total_count=0, got %d", op.TotalCount)
	}
	if op.SucceededCount != 0 || op.FailedCount != 0 {
		t.Fatalf("expected 0 processed, got success=%d failure=%d", op.SucceededCount, op.FailedCount)
	}
	if op.Status != batch.StatusEnums.Completed {
		t.Fatalf("expected status=completed (no failures), got %s", op.Status)
	}
	if len(op.Errors) != 0 {
		t.Fatalf("expected no errors, got %+v", op.Errors)
	}
}
