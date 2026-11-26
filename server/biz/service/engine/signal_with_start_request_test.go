package engine

// SignalWithStart 按 request ID 去重（对齐 Temporal 的 dedupSignalWithStartRequest）：第一次请求已提交、应答丢失，
// SDK 用同一个 request ID 重发时返回第一次的 Run 与 Signal 事件 ID，即使那个 Run 已经结束。

import (
	"context"
	"testing"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/program/programtest"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

func TestSignalWithStart_RetryAfterRunClosedReturnsFirstRun(t *testing.T) {
	env := newUpdateWithStartTestEnv(t, "wf-sws-retry-closed")
	ctx := context.Background()
	const requestID = "req-1"

	first, firstEventID, err := env.runtime.SignalWithStart(ctx, "", env.workflowID, env.workflowID,
		map[string]any{"order": "o-1"}, "notify", requestID, map[string]any{"msg": "hello"})
	if err != nil {
		t.Fatal(err)
	}
	events, err := env.server.GetEvents(first)
	if err != nil {
		t.Fatal(err)
	}
	if got := events[0].WorkflowStarted.RequestID; got != requestID {
		t.Fatalf("WorkflowStarted.RequestID = %q, want %q", got, requestID)
	}
	// 应答丢失；Workflow 很短，重发到达之前已经跑完。
	if err := env.server.completeWorkflow(first, map[string]any{"ok": true}); err != nil {
		t.Fatal(err)
	}
	second, secondEventID, err := env.runtime.SignalWithStart(ctx, "", env.workflowID, env.workflowID,
		map[string]any{"order": "o-1"}, "notify", requestID, map[string]any{"msg": "hello"})
	if err != nil {
		t.Fatal(err)
	}
	if second != first || secondEventID != firstEventID {
		t.Fatalf("retry returned (%s, %d), want first (%s, %d)", second, secondEventID, first, firstEventID)
	}
	if current, err := env.server.mgr.GetCurrentRunID(protocol.DefaultNamespace, env.workflowID); err != nil || current != first {
		t.Fatalf("current run = %q (%v), want %q: the retry started a second run", current, err, first)
	}
	if _, signaled := countStartedAndSignaled(t, env.server, first); signaled != 1 {
		t.Fatalf("WorkflowSignaled count = %d, want 1", signaled)
	}

	// 另一个 request ID 照常启动新 Run。
	third, _, err := env.runtime.SignalWithStart(ctx, "", env.workflowID, env.workflowID,
		map[string]any{"order": "o-2"}, "notify", "req-2", map[string]any{"msg": "hello"})
	if err != nil {
		t.Fatal(err)
	}
	if third == first {
		t.Fatal("a new request ID must start a new run after the previous one closed")
	}
}

// Signal 落在已在运行的 Run 上，之后那个 Run 结束：重发返回那个 Run 与事件 ID。
func TestSignalWithStart_RetryAfterSignaledExistingRunClosed(t *testing.T) {
	env := newUpdateWithStartTestEnv(t, "wf-sws-retry-existing")
	ctx := context.Background()

	existing, err := startedRunID(env.server.StartWorkflow(ctx, "", env.workflowID, env.workflowID, "", nil))
	if err != nil {
		t.Fatal(err)
	}
	runID, eventID, err := env.runtime.SignalWithStart(ctx, "", env.workflowID, env.workflowID, nil, "notify", "req-1", nil)
	if err != nil || runID != existing {
		t.Fatalf("SignalWithStart = (%s, %v), want existing run %s", runID, err, existing)
	}
	if err := env.server.completeWorkflow(existing, nil); err != nil {
		t.Fatal(err)
	}
	retryRunID, retryEventID, err := env.runtime.SignalWithStart(ctx, "", env.workflowID, env.workflowID, nil, "notify", "req-1", nil)
	if err != nil {
		t.Fatal(err)
	}
	if retryRunID != existing || retryEventID != eventID {
		t.Fatalf("retry returned (%s, %d), want (%s, %d)", retryRunID, retryEventID, existing, eventID)
	}
}

// SignalWithStart 启动的 Run 失败后由重试 Run 接替：重发返回链首的 Run 与第一次的事件 ID，不在重试 Run 上再记 Signal。
func TestSignalWithStart_RetryAfterChainContinuedReturnsChainHead(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	server, _ := newFakeClockServer(mgr, newTestTaskQueue(), newTestTaskQueue())
	defer server.Stop()
	ref, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{
		Program: programtest.Noop(), Type: "wf-sws-retry-chain",
		RetryPolicy: &program.RetryPolicy{MaxAttempts: 3, InitialInterval: "1h", BackoffCoefficient: 2.0},
	})
	if err != nil {
		t.Fatal(err)
	}
	first, eventID, err := server.SignalWithStart(ctx, "", ref.WorkflowType, ref.WorkflowType, nil, "notify", "req-1", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := server.failWorkflow(first, protocol.Failure{Message: "boom"}, ""); err != nil {
		t.Fatal(err)
	}
	retryRun := onlyRetryRun(t, mgr, ref.WorkflowType, first)

	runID, retryEventID, err := server.SignalWithStart(ctx, "", ref.WorkflowType, ref.WorkflowType, nil, "notify", "req-1", nil)
	if err != nil {
		t.Fatal(err)
	}
	if runID != first || retryEventID != eventID {
		t.Fatalf("retry returned (%s, %d), want chain head (%s, %d)", runID, retryEventID, first, eventID)
	}
	events, err := mgr.GetEvents(retryRun)
	if err != nil {
		t.Fatal(err)
	}
	if got := countEngineEvents(events, history.EventWorkflowSignaled); got != 0 {
		t.Fatalf("retry run got %d WorkflowSignaled, want 0", got)
	}
}
