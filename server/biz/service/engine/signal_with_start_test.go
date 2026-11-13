package engine

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
)

// ============================================
// SignalWithStart 测试（对标 UpdateWithStart，signal 是 fire-and-forget 严格子集）
// ============================================
// 测试策略:
// - Happy Path: workflow 不存在 → start + signal（恰 1 WorkflowStarted + 1 WorkflowSignaled）
// - Happy Path: workflow 已存在 → UseExisting 复用 current run（不重复 start）
// - Edge Case: 并发多个 signal-with-start 到同一新 workflow → keyed lock 串行，只 1 start、N signal
// - Integration: signal 投递后 workflow body 能通过 WaitSignal 收到
// - Edge Case: current run 已结束 → 启动新 run（见 start_identity_test.go）
// - Edge Case: signal 写入失败 → start 一并回滚
// - Error Case: 空 signalName / 空 workflowType → 拒绝
// ============================================

// countStartedAndSignaled 统计该 run 历史里 WorkflowStarted / WorkflowSignaled 事件条数。
func countStartedAndSignaled(t *testing.T, srv *Server, runID string) (started, signaled int) {
	t.Helper()
	events, err := srv.GetEvents(runID)
	if err != nil {
		t.Fatal(err)
	}
	for _, ev := range events {
		if ev.WorkflowStarted != nil {
			started++
		}
	}
	return started, countSignaledEvents(events)
}

// TestSignalWithStart_NewWorkflow_StartsAndSignals
// Happy Path: workflow 不存在 → SignalWithStart 先 start（产 WorkflowStarted）再 signal（产 WorkflowSignaled），
// 历史恰好一条 WorkflowStarted + 一条 WorkflowSignaled，run 进入 Running，返回非空 runID + 非零 eventID。
func TestSignalWithStart_NewWorkflow_StartsAndSignals(t *testing.T) {
	env := newUpdateWithStartTestEnv(t, "wf-sws-new")
	ctx := context.Background()

	runID, eventID, err := env.runtime.SignalWithStart(ctx, "", env.workflowID, env.workflowID,
		map[string]any{"seed": 1}, "notify", "", map[string]any{"msg": "hello"})

	if err != nil {
		t.Fatal(err)
	}
	if runID == "" {
		t.Fatal("expected non-empty runID from start")
	}
	if eventID == 0 {
		t.Fatal("expected non-zero signal eventID")
	}

	started, signaled := countStartedAndSignaled(t, env.server, runID)
	if started != 1 {
		t.Fatalf("expected exactly 1 WorkflowStarted, got %d", started)
	}
	if signaled != 1 {
		t.Fatalf("expected exactly 1 WorkflowSignaled, got %d", signaled)
	}

	cur, err := env.server.mgr.GetCurrentRunID(protocol.DefaultNamespace, env.workflowID)
	if err != nil {
		t.Fatal(err)
	}
	if cur != runID {
		t.Fatalf("current run %q != started run %q", cur, runID)
	}
	status, err := env.server.GetRunStatus(runID)
	if err != nil {
		t.Fatal(err)
	}
	if status != history.StatusRunning {
		t.Fatalf("expected Running, got %q", status)
	}
}

// TestSignalWithStart_ExistingWorkflow_UseExisting
// Happy Path: workflow 已在跑 → UseExisting：复用 current run，只追加 signal，不重复 start
// （历史里仍只一条 WorkflowStarted，runID 不变）。
func TestSignalWithStart_ExistingWorkflow_UseExisting(t *testing.T) {
	env := newUpdateWithStartTestEnv(t, "wf-sws-existing")
	ctx := context.Background()

	existingRunID, err := startedRunID(env.server.StartWorkflow(ctx, "", env.workflowID, env.workflowID, "", map[string]any{"seed": 0}))
	if err != nil {
		t.Fatal(err)
	}
	startedBefore, signaledBefore := countStartedAndSignaled(t, env.server, existingRunID)
	if startedBefore != 1 || signaledBefore != 0 {
		t.Fatalf("precondition: expected 1 started / 0 signaled, got %d / %d", startedBefore, signaledBefore)
	}

	runID, eventID, err := env.runtime.SignalWithStart(ctx, "", env.workflowID, env.workflowID,
		map[string]any{"seed": 99}, "notify", "", map[string]any{"msg": "again"})

	if err != nil {
		t.Fatal(err)
	}
	if runID != existingRunID {
		t.Fatalf("UseExisting must reuse current run: got %q want %q", runID, existingRunID)
	}
	if eventID == 0 {
		t.Fatal("expected non-zero signal eventID")
	}

	started, signaled := countStartedAndSignaled(t, env.server, runID)
	if started != 1 {
		t.Fatalf("UseExisting must not re-start: expected 1 WorkflowStarted, got %d", started)
	}
	if signaled != 1 {
		t.Fatalf("expected 1 WorkflowSignaled, got %d", signaled)
	}
}

// TestSignalWithStart_Concurrent_SerializedNoDoubleStart
// Edge Case: 并发 N 个 SignalWithStart 到同一个新 workflow → keyed lock 串行：
// 只有一条 WorkflowStarted（首个 start 建 run，其余 UseExisting），恰好 N 条 WorkflowSignaled（无丢失）。
func TestSignalWithStart_Concurrent_SerializedNoDoubleStart(t *testing.T) {
	env := newUpdateWithStartTestEnv(t, "wf-sws-atomic")
	ctx := context.Background()

	const n = 8
	var wg sync.WaitGroup
	runIDs := make([]string, n)
	eventIDs := make([]int64, n)
	for i := range n {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			runID, eventID, err := env.runtime.SignalWithStart(ctx, "", env.workflowID, env.workflowID,
				map[string]any{"seed": i}, "notify", "", map[string]any{"i": i})

			if err != nil {
				t.Errorf("SignalWithStart[%d] failed: %v", i, err)
				return
			}
			runIDs[i] = runID
			eventIDs[i] = eventID
		}(i)
	}
	wg.Wait()

	cur, err := env.server.mgr.GetCurrentRunID(protocol.DefaultNamespace, env.workflowID)
	if err != nil {
		t.Fatal(err)
	}
	for i, rid := range runIDs {
		if rid != cur {
			t.Fatalf("run[%d]=%q must equal current %q (atomic start+UseExisting)", i, rid, cur)
		}
	}

	// 只一条 WorkflowStarted（start 与 signal 同锁，无并发重复 start），恰好 n 条 signaled（none lost）。
	started, signaled := countStartedAndSignaled(t, env.server, cur)
	if started != 1 {
		t.Fatalf("atomicity broken: expected exactly 1 WorkflowStarted, got %d", started)
	}
	if signaled != n {
		t.Fatalf("expected %d WorkflowSignaled (one per call), got %d", n, signaled)
	}

	// signal 事件 ID 无重复（每条 signal 独立 append）。
	seen := make(map[int64]bool, n)
	for i, id := range eventIDs {
		if id == 0 {
			t.Fatalf("eventID[%d] is zero", i)
		}
		if seen[id] {
			t.Fatalf("duplicate signal eventID %d", id)
		}
		seen[id] = true
	}
}

// TestSignalWithStart_StartAndSignalRollBackAtomically
// Start 与 Signal 在同一个事务里：Signal 写入失败时 Run 与 current 指针一并回滚，
// 重试重新启动并恰好记一条 Signal。
func TestSignalWithStart_StartAndSignalRollBackAtomically(t *testing.T) {
	env := newUpdateWithStartTestEnv(t, "wf-sws-atomic")
	ctx := context.Background()
	mgr := env.mgr

	mgr.failOn(history.EventWorkflowSignaled)
	if _, _, err := env.runtime.SignalWithStart(ctx, "", env.workflowID, env.workflowID,
		map[string]any{"seed": 1}, "notify", "", map[string]any{"msg": "first"}); err == nil {
		t.Fatal("expected signal failure error, got nil")
	}
	mgr.failOn("")
	if _, err := env.server.mgr.GetCurrentRunID(protocol.DefaultNamespace, env.workflowID); !errors.Is(err, execution.ErrNoCurrentRun) {
		t.Fatalf("failed signal-with-start left a current run: %v", err)
	}

	runID, eventID, err := env.runtime.SignalWithStart(ctx, "", env.workflowID, env.workflowID,
		map[string]any{"seed": 2}, "notify", "", map[string]any{"msg": "retry"})
	if err != nil || eventID == 0 {
		t.Fatalf("retry: eventID=%d err=%v", eventID, err)
	}
	if started, signaled := countStartedAndSignaled(t, env.server, runID); started != 1 || signaled != 1 {
		t.Fatalf("retry started=%d signaled=%d, want 1/1", started, signaled)
	}
}

// TestSignalWithStart_EmptyName_Rejected
// Error Case: 空 signalName → 返回 error，零副作用（不 start）。
func TestSignalWithStart_EmptyName_Rejected(t *testing.T) {
	env := newUpdateWithStartTestEnv(t, "wf-sws-emptyname")
	ctx := context.Background()

	_, _, err := env.runtime.SignalWithStart(ctx, "", env.workflowID, env.workflowID,
		map[string]any{"seed": 1}, "", "", map[string]any{"msg": "x"})

	if err == nil {
		t.Fatal("expected error for empty signal name")
	}
	if _, e := env.server.mgr.GetCurrentRunID(protocol.DefaultNamespace, env.workflowID); e == nil {
		t.Fatal("empty signal name must not start workflow (no current run expected)")
	}
}

// TestSignalWithStart_EmptyWorkflowType_Rejected
// Error Case: 空 workflowType → 返回 error。
func TestSignalWithStart_EmptyWorkflowType_Rejected(t *testing.T) {
	env := newUpdateWithStartTestEnv(t, "wf-sws-emptywfid")
	ctx := context.Background()

	_, _, err := env.runtime.SignalWithStart(ctx, "", "", "",
		map[string]any{"seed": 1}, "notify", "", map[string]any{"msg": "x"})

	if err == nil {
		t.Fatal("expected error for empty workflow type")
	}
}

// TestSignalWithStart_DeadlockRegression
// Edge Case (-race): 并发 N 个 SignalWithStart（各自新 workflow），断言无死锁（watchdog 超时即失败）
// ——验证 start + signal 在同一 keyed lock 内串行、signal 无 long-poll Wait 段（比 update 更简单，死锁面更小）。
func TestSignalWithStart_DeadlockRegression(t *testing.T) {
	const n = 20
	envs := make([]*updateWithStartTestEnv, n)
	for i := range n {
		envs[i] = newUpdateWithStartTestEnv(t, "wf-sws-dl-"+string(rune('a'+i%26))+string(rune('0'+i/26)))
	}

	ctx := context.Background()
	watchdog := time.AfterFunc(30*time.Second, func() {
		panic("TestSignalWithStart_DeadlockRegression: deadlock detected (watchdog fired)")
	})
	defer watchdog.Stop()

	var wg sync.WaitGroup
	for i := range n {
		env := envs[i]
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, _, err := env.runtime.SignalWithStart(ctx, "", env.workflowID, env.workflowID,
				map[string]any{"seed": 1}, "notify", "", map[string]any{"i": i})

			if err != nil {
				t.Errorf("SignalWithStart failed: %v", err)
			}
		}(i)
	}
	wg.Wait()
}
