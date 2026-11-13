package engine

import (
	"context"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/program/programtest"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

// ============================================
// 场景 D：reset 一个正在 running、且有 in-flight activity（已 started 未完成）的 run
// ============================================
// 测试策略（并发正确性钉死，回归保护）：
// - Happy Path: reset 成功 → 新 run（running + reset marker）、老 run 被 terminate
// - Error Case（并发竞态）：老 run 的 in-flight activity 在 terminate 之后才上报回来，
//   带老 runID 的回调必须被丢弃，不污染新 run 的 activity state
//
// 核实结论支撑：
//   1. HandleActivityCompleted/Failed/Started 入口 workflowAlreadyTerminated(runID) 拒绝
//      （server.go:1278 / 1385 / 1166）；即便绕过，事务内 st.IsTerminal()/st.Stamp 二次拦截
//      （server.go:1295 / 1405 / 1185-1196）。
//   2. ActivityState 以 runID 为隔离维度（map[runID]map[scheduledEventID]）；老 runID 的回调
//      到不了新 runID 的 state bucket。
// ============================================

// seedRunningRunWithInflightActivity 构造一个 running run：
//   - 写一个 WorkflowTaskCompleted（reset 截断点 = 该 WFT close）
//   - schedule 一个带 retry 的 activity 并把它的 ActivityState 标记为 Started（in-flight）
//
// 返回 runID、scheduledEventID、started 时的 stamp。
func seedRunningRunWithInflightActivity(t *testing.T, srv *Server, mgr execution.ExecutionManager, runID string) (string, int64, int64) {
	t.Helper()
	// scheduleActivityWithRetry 内部会：建 running run、写 open WFT、HandleWorkflowTaskCompleted
	//（写 WorkflowTaskCompleted 边界 + ActivityScheduled）、返回 scheduledEventID。
	_, schedID := scheduleActivityWithRetry(t, srv, mgr, runID, &protocol.ParsedRetryPolicy{
		MaxAttempts: 3, InitialInterval: time.Second, BackoffCoefficient: 2,
	})

	// 让 activity 进入 in-flight（started 未完成）。attempt=1, stamp=1（schedule 时初值）。
	if err := srv.HandleActivityStarted(runID, schedID, 1, 1, time.Second, 0); err != nil {
		t.Fatalf("HandleActivityStarted: %v", err)
	}
	st, ok, err := mgr.GetActivityState(runID, schedID)
	if err != nil || !ok {
		t.Fatalf("expected activity state after start: ok=%v err=%v", ok, err)
	}
	if !st.IsStarted() {
		t.Fatalf("setup: expected in-flight (started) activity, got status=%s", st.Status)
	}
	return runID, schedID, st.Stamp
}

// TestResetRunningRun_InflightActivityCallbackDroppedAfterTerminate 钉死场景 D：
// reset 一个 running + in-flight activity 的 run 后，老 in-flight activity 回报回来被丢弃，
// 不污染新 run。
func TestResetRunningRun_InflightActivityCallbackDroppedAfterTerminate(t *testing.T) {
	// Happy Path + Error Case：reset 成功产生新 run（running + reset marker）、老 run terminate，
	// 老 in-flight activity 回报（HandleActivityCompleted，带老 runID+stamp）被丢弃，新 run 不受影响。
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	server := assembleServer(mgr, newTestTaskQueue(), newTestTaskQueue())
	defer server.Stop()

	if _, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{ID: "wf-running-reset", Program: programtest.Noop()}); err != nil {
		t.Fatal(err)
	}

	oldRunID, schedID, inflightStamp := seedRunningRunWithInflightActivity(t, server, mgr, "run-running-reset-inflight")

	// 前置确认：老 run 当前是 Running。
	oldRun, err := mgr.Get(oldRunID)
	if err != nil {
		t.Fatal(err)
	}
	status, err := server.GetRunStatus(oldRun.ID)
	if err != nil {
		t.Fatal(err)
	}
	if status != history.StatusRunning {
		t.Fatalf("setup: expected old run Running, got %s", status)
	}

	// 执行 reset（running run → 内部先 TerminateWorkflow，再 fork 新 run）。
	newRunID, err := server.ResetWorkflow(ctx, execution.ResetWorkflowRequest{
		RunID:     oldRunID,
		Reason:    "reset-running",
		ResetType: execution.ResetTypeEnums.LastWorkflowTask,
	})
	if err != nil {
		t.Fatalf("ResetWorkflow on running run: %v", err)
	}

	// 断言 1：产生了一个新的、不同的 run id。
	if newRunID == "" || newRunID == oldRunID {
		t.Fatalf("expected a distinct new run id, got %q (old=%q)", newRunID, oldRunID)
	}

	// 断言 2：新 run 是 Running，且带 reset marker（WorkflowTaskFailed{Cause:reset_workflow}）。
	newRun, err := mgr.Get(newRunID)
	if err != nil {
		t.Fatal(err)
	}
	if newRun.Status != history.StatusRunning {
		t.Fatalf("new run status = %s, want %s", newRun.Status, history.StatusRunning)
	}
	newEvents, err := mgr.GetEvents(newRunID)
	if err != nil {
		t.Fatal(err)
	}
	resetMarkers := 0
	for _, e := range newEvents {
		if e.EventType() == history.EventWorkflowTaskFailed && e.WorkflowTaskFailed != nil && e.WorkflowTaskFailed.Cause == "reset_workflow" {
			resetMarkers++
		}
	}
	if resetMarkers != 1 {
		t.Fatalf("expected exactly 1 reset_workflow marker in new run, got %d", resetMarkers)
	}

	// 断言 3：老 run 被 terminate。
	oldRunAfter, err := mgr.Get(oldRunID)
	if err != nil {
		t.Fatal(err)
	}
	oldStatus, err := server.GetRunStatus(oldRunAfter.ID)
	if err != nil {
		t.Fatal(err)
	}
	if oldStatus != history.StatusTerminated {
		t.Fatalf("old run status = %s, want %s", oldStatus, history.StatusTerminated)
	}
	// 老 run 的 ResetRunID 指向新 run。
	if oldRunAfter.ResetRunID != newRunID {
		t.Fatalf("old run ResetRunID = %q, want %q", oldRunAfter.ResetRunID, newRunID)
	}

	// 记录新 run 的 activity state 数量（reset 后新 run 尚未重放调度 activity，应为 0，
	// 是干净的；这是"老回调不能污染新 run"的对照基线）。
	newStatesBefore, err := mgr.ListActivityStates(newRunID)
	if err != nil {
		t.Fatal(err)
	}

	// 记录老 run 的 in-flight activity state，验证它在 terminate 后仍是 started（未被改成 completed）。
	oldStBefore, ok, err := mgr.GetActivityState(oldRunID, schedID)
	if err != nil || !ok {
		t.Fatalf("expected old activity state to still exist: ok=%v err=%v", ok, err)
	}

	// ====== 核心：老 in-flight activity 现在（terminate 之后）才上报回来 ======
	// 带的是老 runID + schedID + stamp（reset 前的 in-flight 任务），attempt=1。
	// 期望：因老 run 已 Terminated（workflowAlreadyTerminated 入口拒绝），整个回报被丢弃，
	//      不写任何事件、不改任何 state。HandleActivityCompleted 对终态 run 返回 nil（非错误）。
	if err := server.HandleActivityCompleted(ctx, oldRunID, schedID, 1, inflightStamp, map[string]any{"late": "result"}); err != nil {
		t.Fatalf("late activity completion on terminated run should be silently dropped (nil), got err: %v", err)
	}

	// 断言 4：老回报没有污染新 run —— 新 run 的 activity state 不受老回调影响（数量不变）。
	newStatesAfter, err := mgr.ListActivityStates(newRunID)
	if err != nil {
		t.Fatal(err)
	}
	if len(newStatesAfter) != len(newStatesBefore) {
		t.Fatalf("late old-run callback polluted new run activity states: before=%d after=%d",
			len(newStatesBefore), len(newStatesAfter))
	}
	// 新 run 不应出现这个 schedID 的 activity state（它属于老 run 的隔离维度）。
	if _, found, _ := mgr.GetActivityState(newRunID, schedID); found {
		t.Fatalf("late old-run callback leaked activity state (schedID=%d) into new run", schedID)
	}

	// 断言 5：老 run 的 activity state 也没被这个迟到回报改成 completed
	//（终态 run 入口拒绝 → 空提交，state 不变）。
	oldStAfter, ok, err := mgr.GetActivityState(oldRunID, schedID)
	if err != nil || !ok {
		t.Fatalf("old activity state vanished: ok=%v err=%v", ok, err)
	}
	if oldStAfter.Status != oldStBefore.Status {
		t.Fatalf("late callback mutated old activity state: before=%s after=%s",
			oldStBefore.Status, oldStAfter.Status)
	}

	// 断言 6：迟到回报没有往新 run 写 ActivityCompleted 事件。
	newEventsAfter, err := mgr.GetEvents(newRunID)
	if err != nil {
		t.Fatal(err)
	}
	if got := countEngineEvents(newEventsAfter, history.EventActivityCompleted); got != 0 {
		t.Fatalf("late old-run callback wrote ActivityCompleted into new run: got %d", got)
	}
}

// TestResetRunningRun_StaleStampCallbackDropped 补充钉死 stamp 隔离维度：
// 即便绕过"老 run 已 terminate"这一层（构造一个 running 的目标 run 接收老 stamp 回报），
// stamp 不符也会被丢弃。此处用同一个新 run 的 schedID 但喂错 stamp，验证 stamp 校验生效。
func TestResetRunningRun_StaleStampCallbackDropped(t *testing.T) {
	// Error Case：stamp 不符的 activity 回报被丢弃（reset/retry 后旧 generation 失效）。
	srv, mgr := newStateTestServer(t)
	runID, schedID := scheduleRetryableActivity2(t, srv, mgr)

	// in-flight：attempt=1 started，stamp=1。
	if err := srv.HandleActivityStarted(runID, schedID, 1, 1, time.Second, 0); err != nil {
		t.Fatal(err)
	}
	before, _, _ := mgr.GetActivityState(runID, schedID)

	// 用一个不匹配的旧 stamp（999）回报 completed → 必须被 stamp 校验丢弃。
	if err := srv.HandleActivityCompleted(context.Background(), runID, schedID, 1, 999, map[string]any{"x": 1}); err != nil {
		t.Fatalf("stale-stamp completion should be silently dropped, got err: %v", err)
	}
	after, _, _ := mgr.GetActivityState(runID, schedID)
	if after.Status != before.Status {
		t.Fatalf("stale-stamp callback mutated state: before=%s after=%s", before.Status, after.Status)
	}
	if countEngineEvents(mustEvents(t, mgr, runID), history.EventActivityCompleted) != 0 {
		t.Fatal("stale-stamp callback wrote ActivityCompleted event")
	}
}

func mustEvents(t *testing.T, mgr execution.ExecutionManager, runID string) []history.Event {
	t.Helper()
	events, err := mgr.GetEvents(runID)
	if err != nil {
		t.Fatal(err)
	}
	return events
}
