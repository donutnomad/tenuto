package engine

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/program/programtest"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
)

// 重发应答丢失的启动请求（同一个 request ID）得到第一次启动的 Run：运行中与已结束都一样，不报冲突、不启动第二个 Run。
func TestStartWorkflowWithRequestIDIsIdempotent(t *testing.T) {
	ctx := context.Background()
	server := newTestServer()
	defer server.Stop()
	registerScheduleWorkflow(t, server, "wf-start-request")

	first, err := server.StartWorkflow(ctx, "", "wf-start-request", "order-1", "req-1", nil)
	if err != nil {
		t.Fatal(err)
	}
	again, err := server.StartWorkflow(ctx, "", "wf-start-request", "order-1", "req-1", nil)
	if err != nil || again.ID != first.ID {
		t.Fatalf("retried start = %v, %v; want run %s", again, err, first.ID)
	}
	if _, err := server.StartWorkflow(ctx, "", "wf-start-request", "order-1", "req-2", nil); !errors.Is(err, execution.ErrCurrentRunConflict) {
		t.Fatalf("start with another request id: err = %v, want ErrCurrentRunConflict", err)
	}

	// Run 很快结束之后到达的重发同样得到它，不在同一个 Workflow ID 上启动第二个 Run。
	if err := server.completeWorkflow(first.ID, nil); err != nil {
		t.Fatal(err)
	}
	afterClose, err := server.StartWorkflow(ctx, "", "wf-start-request", "order-1", "req-1", nil)
	if err != nil || afterClose.ID != first.ID {
		t.Fatalf("retried start after close = %v, %v; want run %s", afterClose, err, first.ID)
	}
	next, err := server.StartWorkflow(ctx, "", "wf-start-request", "order-1", "req-3", nil)
	if err != nil || next.ID == first.ID {
		t.Fatalf("new request after close = %v, %v; want a new run", next, err)
	}

	if _, err := server.StartWorkflow(ctx, "", "wf-start-request", "order-2", strings.Repeat("x", execution.MaxRequestIDLength+1), nil); !errors.Is(err, protocol.ErrInvalidRequest) {
		t.Fatalf("oversized request id: err = %v, want ErrInvalidRequest", err)
	}
}

// 重发的启动请求在接续链推进之后（失败重试、continue-as-new）得到这条链的 current Run，不报冲突、不启动第二次执行：
// 派生 Run 的 WorkflowStarted 继承链首的 RequestID。
func TestStartWorkflowWithRequestIDFollowsDerivedRuns(t *testing.T) {
	ctx := context.Background()
	server := newTestServer()
	defer server.Stop()
	ref, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{
		Type: "wf-start-request-chain", Program: programtest.Noop(),
		RetryPolicy: &program.RetryPolicy{MaxAttempts: 3, InitialInterval: "1h"},
	})
	if err != nil {
		t.Fatal(err)
	}

	first, err := server.StartWorkflow(ctx, "", ref.WorkflowType, "chain-retry", "req-retry", nil)
	if err != nil {
		t.Fatal(err)
	}
	wftID := openWorkflowTaskForTest(t, server.mgr, first.ID)
	if err := server.HandleWorkflowTaskCompleted(server.WorkflowTaskContext(ctx, first.ID, wftID), first.ID, wftID,
		[]history.Command{{Type: history.CmdFailWorkflow, Error: "boom"}}, ""); err != nil {
		t.Fatal(err)
	}
	retryRunID := retryRunIDForTest(t, server.mgr, first.ID)
	again, err := server.StartWorkflow(ctx, "", ref.WorkflowType, "chain-retry", "req-retry", nil)
	if err != nil || again.ID != retryRunID {
		t.Fatalf("retried start after a workflow retry = %v, %v; want retry run %s", again, err, retryRunID)
	}

	first, err = server.StartWorkflow(ctx, "", ref.WorkflowType, "chain-can", "req-can", nil)
	if err != nil {
		t.Fatal(err)
	}
	nextRunID := continueAsNewForTest(t, server, server.mgr, first.ID, nil)
	again, err = server.StartWorkflow(ctx, "", ref.WorkflowType, "chain-can", "req-can", nil)
	if err != nil || again.ID != nextRunID {
		t.Fatalf("retried start after continue-as-new = %v, %v; want continued run %s", again, err, nextRunID)
	}
	// 链结束之后的重发同样得到链的最后一个 Run。
	if err := server.completeWorkflow(nextRunID, nil); err != nil {
		t.Fatal(err)
	}
	again, err = server.StartWorkflow(ctx, "", ref.WorkflowType, "chain-can", "req-can", nil)
	if err != nil || again.ID != nextRunID {
		t.Fatalf("retried start after the chain closed = %v, %v; want run %s", again, err, nextRunID)
	}
}

// 同一个 request ID 的 Signal 在一个 Run 上只记一条；SignalWithStart 的重发不再启动或多记 Signal。
func TestSignalWithRequestIDIsIdempotent(t *testing.T) {
	ctx := context.Background()
	server := newTestServer()
	defer server.Stop()
	registerScheduleWorkflow(t, server, "wf-signal-request")

	run, err := server.StartWorkflow(ctx, "", "wf-signal-request", "sig-1", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	firstID, err := server.SignalWorkflow(ctx, "", "sig-1", "", "approve", "sig-req-1", map[string]any{"n": int64(1)})
	if err != nil {
		t.Fatal(err)
	}
	againID, err := server.SignalWorkflow(ctx, "", "sig-1", "", "approve", "sig-req-1", map[string]any{"n": int64(1)})
	if err != nil || againID != firstID {
		t.Fatalf("retried signal = event %d, %v; want event %d", againID, err, firstID)
	}
	if _, err := server.SignalWorkflow(ctx, "", "sig-1", "", "approve", "sig-req-2", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := server.SignalWorkflow(ctx, "", "sig-1", "", "approve", "", nil); err != nil {
		t.Fatal(err)
	}
	if got := countSignals(t, server, run.ID); got != 3 {
		t.Fatalf("WorkflowSignaled events = %d, want 3 (one per request id plus one without)", got)
	}

	runID, eventID, err := server.SignalWithStart(ctx, "", "wf-signal-request", "sig-2", nil, "approve", "sws-1", nil)
	if err != nil {
		t.Fatal(err)
	}
	againRunID, againEventID, err := server.SignalWithStart(ctx, "", "wf-signal-request", "sig-2", nil, "approve", "sws-1", nil)
	if err != nil || againRunID != runID || againEventID != eventID {
		t.Fatalf("retried signal-with-start = run %s event %d, %v; want run %s event %d", againRunID, againEventID, err, runID, eventID)
	}
	if got := countSignals(t, server, runID); got != 1 {
		t.Fatalf("WorkflowSignaled events after retried signal-with-start = %d, want 1", got)
	}
}

func countSignals(t *testing.T, server *Server, runID string) int {
	t.Helper()
	events, err := server.GetEvents(runID)
	if err != nil {
		t.Fatal(err)
	}
	return countEngineEvents(events, history.EventWorkflowSignaled)
}
