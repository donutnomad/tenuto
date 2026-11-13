package engine

import (
	"context"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

// TerminateWorkflow + UpsertSearchAttributes 原子性回归测试。
// 验证：事件落库 + run.Status/SearchAttributes 更新在同一事务提交后一致，
// 且重复调用（幂等门控）不会重复落事件。

// Happy Path: Terminate 后 run.Status==Terminated 且历史有 WorkflowTerminated 事件。
func TestTerminateWorkflow_HappyPath_StatusAndEventAtomic(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	server := assembleServer(mgr, newTestTaskQueue(), newTestTaskQueue())
	_ = ctx

	run := &execution.Run{
		ID:                    "run-terminate-happy",
		WorkflowID:            "wf-terminate",
		WorkflowType:          "wf-terminate",
		WorkflowConfigVersion: "v1",
		Status:                history.StatusRunning,
		CreatedAt:             time.Now(),
	}
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(run)) }); err != nil {
		t.Fatal(err)
	}

	if err := server.TerminateWorkflow(context.Background(), "", "", run.ID, "test-reason"); err != nil {
		t.Fatalf("TerminateWorkflow failed: %v", err)
	}

	// run.Status 必须是 Terminated
	updated, err := mgr.Get(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if updated.Status != history.StatusTerminated {
		t.Fatalf("expected run.Status=Terminated, got %s", updated.Status)
	}

	// 历史必须有 WorkflowTerminated 事件
	events, err := mgr.GetEvents(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, ev := range events {
		if ev.EventType() == history.EventWorkflowTerminated {
			found = true
			if ev.WorkflowTerminated.Reason != "test-reason" {
				t.Fatalf("unexpected reason in event: %s", ev.WorkflowTerminated.Reason)
			}
			break
		}
	}
	if !found {
		t.Fatalf("WorkflowTerminated event not found in history; events=%v", events)
	}
}

// Idempotent: 重复 Terminate 同一 run，第二次必须返回 error（已终态）而不是幂等 nil。
// 注：TerminateWorkflow 对已终态 run 返回 error（"workflow X is Terminated"），
// 符合"已终态不可再终止"语义；事件也不应被重复插入。
func TestTerminateWorkflow_AlreadyTerminated_ReturnsError(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	server := assembleServer(mgr, newTestTaskQueue(), newTestTaskQueue())
	_ = ctx

	run := &execution.Run{
		ID:                    "run-terminate-idempotent",
		WorkflowID:            "wf-terminate",
		WorkflowType:          "wf-terminate",
		WorkflowConfigVersion: "v1",
		Status:                history.StatusRunning,
		CreatedAt:             time.Now(),
	}
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(run)) }); err != nil {
		t.Fatal(err)
	}

	if err := server.TerminateWorkflow(context.Background(), "", "", run.ID, "first"); err != nil {
		t.Fatalf("first TerminateWorkflow failed: %v", err)
	}

	// 第二次调用：run 已是 Terminated，应返回 error
	err := server.TerminateWorkflow(context.Background(), "", "", run.ID, "second")
	if err == nil {
		t.Fatal("expected error on second Terminate of already-terminated run, got nil")
	}

	// 历史中 WorkflowTerminated 事件应只有 1 条（幂等门控防重复）
	events, err2 := mgr.GetEvents(run.ID)
	if err2 != nil {
		t.Fatal(err2)
	}
	count := 0
	for _, ev := range events {
		if ev.EventType() == history.EventWorkflowTerminated {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("expected exactly 1 WorkflowTerminated event, got %d", count)
	}
}

// Crash-recovery（白盒，直接驱动 RunInTx 复刻 Terminate 事务体）：
// 历史已有 WorkflowTerminated 事件，但 run.Status 仍停在 Running
// （模拟上次落事件成功、UpdateRun 前崩溃的不一致状态）。
//
// 注：无法通过公开的 TerminateWorkflow API 触达此路径——其入口 currentRunStatus
// → reconcileRunStatusFromHistory 会在进入事务前就把 stale status 自愈为 Terminated。
// 因此这里直接驱动 RunInTx 事务体，断言 AppendIdempotent 返回 ins=false 时，
// 事务内仍把 run.Status 修为 Terminated（验证修复："!ins 路径也更新 status"）。
func TestTerminateWorkflow_TxBody_EventExists_StillUpdatesStatus(t *testing.T) {
	mgr := memory.NewExecutionManager()

	run := &execution.Run{
		ID:                    "run-terminate-txbody",
		WorkflowID:            "wf-terminate",
		WorkflowType:          "wf-terminate",
		WorkflowConfigVersion: "v1",
		Status:                history.StatusRunning,
		CreatedAt:             time.Now(),
	}
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(run)) }); err != nil {
		t.Fatal(err)
	}
	// 预置不一致：WorkflowTerminated 事件已在历史，但 run.Status 仍 Running。
	if _, _, err := mgr.AppendIdempotent(run.ID, run.ID, run.ID, history.NewWorkflowTerminatedEvent("prior-crash")); err != nil {
		t.Fatal(err)
	}

	// 复刻 server.TerminateWorkflow 的事务体（与生产代码一致：不论 ins 与否都更新 status）。
	var inserted bool
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error {
		_, ins, e := tx.AppendIdempotent(run.ID, run.ID, run.ID, history.NewWorkflowTerminatedEvent("retry"))
		if e != nil {
			return e
		}
		inserted = ins
		current, e := tx.GetRunForUpdate(run.ID)
		if e != nil {
			return e
		}
		if e := current.MarkTerminal(history.StatusTerminated, nil); e != nil {
			return e
		}
		return tx.UpdateRun(current)
	}); err != nil {
		t.Fatal(err)
	}

	// 事件已存在，本次必然 ins=false（走幂等分支）。
	if inserted {
		t.Fatalf("expected AppendIdempotent ins=false (event already present), got inserted=true")
	}

	// 即便 !ins，事务内 UpdateRun 也必须把 run.Status 修为 Terminated。
	healed, err := mgr.Get(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if healed.Status != history.StatusTerminated {
		t.Fatalf("expected run.Status updated to Terminated on !ins path, got %s", healed.Status)
	}

	// 事件不得被重复插入（幂等门控）。
	events, err := mgr.GetEvents(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, ev := range events {
		if ev.EventType() == history.EventWorkflowTerminated {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("expected exactly 1 WorkflowTerminated event, got %d", count)
	}
}

// Happy Path: UpsertSearchAttributes 后 run.SearchAttributes 已更新且历史有 SearchAttributesUpserted 事件。
func TestUpsertSearchAttributes_HappyPath_Atomic(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	server := assembleServer(mgr, newTestTaskQueue(), newTestTaskQueue())

	run := &execution.Run{
		ID:                    "run-upsert-happy",
		WorkflowID:            "wf-upsert",
		WorkflowType:          "wf-upsert",
		WorkflowConfigVersion: "v1",
		Status:                history.StatusRunning,
		CreatedAt:             time.Now(),
	}
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(run)) }); err != nil {
		t.Fatal(err)
	}

	if err := server.UpsertSearchAttributes(ctx, "", "", run.ID, map[string]string{
		"order_id": "ord-123",
	}); err != nil {
		t.Fatalf("UpsertSearchAttributes failed: %v", err)
	}

	// run.SearchAttributes 必须已更新
	updated, err := mgr.Get(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if updated.SearchAttributes["order_id"] != "ord-123" {
		t.Fatalf("expected order_id=ord-123, got %s", updated.SearchAttributes["order_id"])
	}

	// 历史必须有 SearchAttributesUpserted 事件
	events, err := mgr.GetEvents(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, ev := range events {
		if ev.EventType() == history.EventSearchAttributesUpserted {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("SearchAttributesUpserted event not found in history; events=%v", events)
	}
}

// Happy Path: UpsertSearchAttributes 多次调用，属性累加（合并而非覆盖全量）。
func TestUpsertSearchAttributes_MultipleUpserts_Merge(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	server := assembleServer(mgr, newTestTaskQueue(), newTestTaskQueue())

	run := &execution.Run{
		ID:                    "run-upsert-merge",
		WorkflowID:            "wf-upsert",
		WorkflowType:          "wf-upsert",
		WorkflowConfigVersion: "v1",
		Status:                history.StatusRunning,
		CreatedAt:             time.Now(),
	}
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(run)) }); err != nil {
		t.Fatal(err)
	}

	if err := server.UpsertSearchAttributes(ctx, "", "", run.ID, map[string]string{"k1": "v1"}); err != nil {
		t.Fatalf("first upsert failed: %v", err)
	}
	if err := server.UpsertSearchAttributes(ctx, "", "", run.ID, map[string]string{"k2": "v2"}); err != nil {
		t.Fatalf("second upsert failed: %v", err)
	}

	updated, err := mgr.Get(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if updated.SearchAttributes["k1"] != "v1" {
		t.Fatalf("expected k1=v1, got %s", updated.SearchAttributes["k1"])
	}
	if updated.SearchAttributes["k2"] != "v2" {
		t.Fatalf("expected k2=v2, got %s", updated.SearchAttributes["k2"])
	}

	// 两次 upsert 应各产生 1 条 SearchAttributesUpserted 事件
	events, err := mgr.GetEvents(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, ev := range events {
		if ev.EventType() == history.EventSearchAttributesUpserted {
			count++
		}
	}
	if count != 2 {
		t.Fatalf("expected 2 SearchAttributesUpserted events, got %d", count)
	}
}
