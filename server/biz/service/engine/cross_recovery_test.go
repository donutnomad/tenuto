package engine

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/program/programtest"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

// seedRunWithPendingState 建一个 running run，在单个 WFT 内同时 schedule 一个 retryable
// activity（经 RunInTx 写 ActivityState）+ 一个 durable timer（经 RunInTx 写 TimerState），
// 再直接种一个 pending（started）ChildWorkflowState（其子 Run 已存在），让该 run 同时持有
// activity / timer / child 三类 pending state。返回 parentRunID。
//
// 三类 schedule 都走真实 RunInTx 写状态路径（activity/timer 经 HandleWorkflowTaskCompleted），
// child 用与 TestRecoverChildReadsStateNotHistory 一致的"只写 state 不写父 history"种法，
// 确保 recovery 的数据源只能是 state 表而非 history 推导。
func seedRunWithPendingState(t *testing.T, srv *Server, mgr execution.ExecutionManager) string {
	t.Helper()
	const runID = "run-cross-recovery"
	seedRunningWorkflowWithActivity(t, srv, mgr, runID)
	wftID := currentWFTScheduledID(t, srv, runID)
	if err := srv.HandleWorkflowTaskCompleted(srv.WorkflowTaskContext(context.Background(), runID, wftID), runID, wftID, []history.Command{
		{
			Type:                history.CmdScheduleActivity,
			ActivityID:          "settle",
			ActivityType:        "Settle",
			StartToCloseTimeout: time.Second,
			RetryPolicy:         &protocol.ParsedRetryPolicy{MaxAttempts: 3, InitialInterval: time.Second, BackoffCoefficient: 2},
		},
		{
			Type:    history.CmdScheduleTimer,
			TimerID: "sleep-1",
			Delay:   time.Hour, // deadline 在未来，避免 recovery 立即 fire
		},
	}, ""); err != nil {
		t.Fatal(err)
	}

	// 种一个 pending（started）child：子 Run 已存在 → recovery 走"continue"路径（不重建），
	// 数据源是 ChildWorkflowState 而非父 history。
	childRunID := runID + "-child"
	childRef, err := srv.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{ID: "cross-recovery-child", Program: programtest.Noop()})
	if err != nil {
		t.Fatal(err)
	}
	child := &execution.Run{ID: childRunID, WorkflowID: childRef.WorkflowID, WorkflowType: childRef.WorkflowID, WorkflowConfigVersion: childRef.Version, Status: history.StatusRunning, CreatedAt: time.Now()}
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(child)) }); err != nil {
		t.Fatal(err)
	}
	if err := mgr.UpsertChildWorkflowState(&execution.ChildWorkflowState{
		RunID: runID, ScheduledEventID: 999, ChildID: "child_1", ChildRunID: childRunID,
		WorkflowID: childRef.WorkflowID, WorkflowConfigVersion: childRef.Version,
		Status: execution.ChildWorkflowStatusEnums.Started, Input: map[string]any{"x": 1},
		CreatedAt: time.Now(), UpdatedAt: time.Now(),
	}); err != nil {
		t.Fatal(err)
	}
	return runID
}

// Integration: 跨类崩溃恢复——一个 run 同时有 pending activity + pending timer + pending child，
// "重启"后 recovery 直读各 state 表重建，全程不依赖 GetEvents 推导 pending 状态（state 表是唯一权威）。
//
// 本测试是 spec 核心目标"崩溃恢复直读 DB 不扫历史"的端到端证明：
//   - 各类 state 在 state 表中（schedule 路径经 RunInTx 已写 / child 直接种）；
//   - srv.Stop() 清空内存句柄，模拟进程重启；
//   - recovery 从 ListActivityStates/ListTimerStates/ListChildWorkflowStates 读回重建；
//   - 断言读回的 pending 项数量/状态正确 + recovery 期间 GetEvents 增量有界（不随 pending 数增长）。
func TestCrossRecoveryRebuildsAllStatesFromDB(t *testing.T) {
	mgr := &getEventsCountingManager{ExecutionManager: memory.NewExecutionManager()} // 同一持久化 mgr 跨"重启"复用
	srv := assembleServer(mgr, newTestTaskQueue(), newTestTaskQueue())
	runID := seedRunWithPendingState(t, srv, mgr)

	// 重启前：state 表里应有 pending activity + pending timer + pending child。
	actBefore, _ := mgr.ListActivityStates(runID)
	timBefore, _ := mgr.ListTimerStates(runID)
	childBefore, _ := mgr.ListChildWorkflowStates(runID)
	if len(actBefore) == 0 || len(timBefore) == 0 || len(childBefore) == 0 {
		t.Fatalf("setup wrong: activity=%d timer=%d child=%d (all must be >0 before restart)", len(actBefore), len(timBefore), len(childBefore))
	}

	// 1) 模拟进程重启：Stop 清空内存 timer 句柄，state 表（mgr）保留。
	srv.Stop()

	// 2) "重启"：同一持久化 mgr，全新 Server（内存全空）。recovery 直读各 state 表重建。
	srv2 := assembleServer(mgr, newTestTaskQueue(), newTestTaskQueue())
	t.Cleanup(srv2.Stop)
	callsBeforeRecovery := mgr.getEventsCalls.Load()

	if _, err := srv2.recoverActivityRetries(context.Background(), runID, true); err != nil {
		t.Fatalf("recoverActivityRetries: %v", err)
	}
	if err := srv2.recoverDurableTimers(context.Background(), runID, true); err != nil {
		t.Fatalf("recoverDurableTimers: %v", err)
	}
	if err := srv2.recoverChildWorkflows(context.Background(), runID); err != nil {
		t.Fatalf("recoverChildWorkflows: %v", err)
	}

	// 3) recovery 后：三类 state 仍可从 DB 读回（数据源是 state 表，不是 history 推导）。
	actAfter, _ := mgr.ListActivityStates(runID)
	timAfter, _ := mgr.ListTimerStates(runID)
	childAfter, _ := mgr.ListChildWorkflowStates(runID)
	if len(actAfter) != len(actBefore) || len(timAfter) != len(timBefore) || len(childAfter) != len(childBefore) {
		t.Fatalf("recovery lost state: activity %d->%d timer %d->%d child %d->%d",
			len(actBefore), len(actAfter), len(timBefore), len(timAfter), len(childBefore), len(childAfter))
	}

	// 4) 状态正确性：activity 仍 scheduled、timer 未 fire、child 仍 started（非终态保留）。
	if actAfter[0].IsTerminal() || !actAfter[0].IsScheduled() {
		t.Fatalf("recovered activity state wrong: %+v", actAfter[0])
	}
	if timAfter[0].IsTerminal() {
		t.Fatalf("recovered timer state wrong (should be pending): %+v", timAfter[0])
	}
	if childAfter[0].IsTerminal() || childAfter[0].Status != execution.ChildWorkflowStatusEnums.Started {
		t.Fatalf("recovered child state wrong: %+v", childAfter[0])
	}

	// 5) 关键断言：recovery 期间对 GetEvents 的调用只能来自合法的 run 级判定（child 已存在子 Run
	//    的"continue"路径不读父 history；activity/timer recovery 完全不读 history）。
	//    这里三类 recovery 全程不应触发任何全量扫历史推导 pending 状态——增量必须有界（<=2），
	//    绝不能随 pending 项数线性增长；本断言锁住"没有全量扫历史"。
	recoveryCalls := mgr.getEventsCalls.Load() - callsBeforeRecovery
	if recoveryCalls > 2 {
		t.Fatalf("recovery called GetEvents %d times (>2): suspected history-scan derivation of pending state", recoveryCalls)
	}
}

// getEventsCountingManager 统计经存储接口读取完整 History 的次数。
type getEventsCountingManager struct {
	execution.ExecutionManager
	getEventsCalls atomic.Int64
}

func (m *getEventsCountingManager) GetEvents(runID string) ([]history.Event, error) {
	m.getEventsCalls.Add(1)
	return m.ExecutionManager.GetEvents(runID)
}
