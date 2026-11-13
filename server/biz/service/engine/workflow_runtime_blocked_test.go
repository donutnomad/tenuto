package engine

import (
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
)

// ============================================
// get-run 响应附带 blocked 信息块测试
// ============================================
// 测试策略（响应层派生，不改 Run 结构体；events 用 resolveReadRunID 解析后的 runID 取）:
// - A1 Happy Path: Block 的 run → 响应 blocked.blocked=true + cause + streak
// - A2 Happy Path: 正常 run → blocked.blocked=false
// ============================================

// seedBlockedHistory 在 run 上追加「WFT Failed 尾部无 open」的历史，令其处于 Block 态。
func seedBlockedHistory(t *testing.T, mgr execution.ExecutionManager, runID string, streak int) {
	t.Helper()
	base := time.Now().Add(-time.Minute)
	for i := 0; i < streak; i++ {
		ts := base.Add(time.Duration(i) * time.Second)
		schedID, err := mgr.Append(runID, runID, runID, history.Event{Timestamp: ts, WorkflowTaskScheduled: &history.WorkflowTaskScheduledEvent{Attempt: 1}})
		if err != nil {
			t.Fatal(err)
		}
		// Failed 的 ScheduledEventID 指向本轮 Scheduled，令其 close（否则 open WFT 会误判未卡住）。
		if _, err := mgr.Append(runID, runID, runID, history.Event{Timestamp: ts.Add(500 * time.Millisecond), WorkflowTaskFailed: &history.WorkflowTaskFailedEvent{ScheduledEventID: schedID, Cause: "non_deterministic_error", Reason: "marker mismatch"}}); err != nil {
			t.Fatal(err)
		}
	}
}

// A1 Happy Path: Block 的 run → GetRunWithBlocked 响应 blocked.blocked=true + cause/streak。
func TestGetRun_BlockedRun_ResponseHasBlockedInfo(t *testing.T) {
	svc, mgr := newCurrentReadRuntime(t)
	seedRun(t, mgr, "run-blocked", "wf-blocked", history.StatusRunning, true)
	seedBlockedHistory(t, mgr, "run-blocked", 2)

	resp, err := svc.View().GetRunWithBlocked("", "", "run-blocked")
	if err != nil {
		t.Fatal(err)
	}
	if resp.Run == nil || resp.Run.ID != "run-blocked" {
		t.Fatalf("expected run-blocked, got %+v", resp.Run)
	}
	if !resp.Blocked.Blocked {
		t.Fatalf("expected blocked.blocked=true for blocked run")
	}
	if resp.Blocked.Cause != "non_deterministic_error" {
		t.Fatalf("expected cause non_deterministic_error, got %q", resp.Blocked.Cause)
	}
	if resp.Blocked.ConsecutiveFailures != 2 {
		t.Fatalf("expected ConsecutiveFailures=2, got %d", resp.Blocked.ConsecutiveFailures)
	}
}

// A2 Happy Path: 正常 run（无 Failed 尾）→ blocked.blocked=false。
func TestGetRun_HealthyRun_BlockedFalse(t *testing.T) {
	svc, mgr := newCurrentReadRuntime(t)
	seedRun(t, mgr, "run-healthy", "wf-healthy", history.StatusRunning, true)
	if _, err := mgr.Append("run-healthy", "run-healthy", "run-healthy", history.NewWorkflowTaskScheduledEvent(0)); err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.Append("run-healthy", "run-healthy", "run-healthy", history.NewWorkflowTaskCompletedEvent(0, "")); err != nil {
		t.Fatal(err)
	}

	resp, err := svc.View().GetRunWithBlocked("", "", "run-healthy")
	if err != nil {
		t.Fatal(err)
	}
	if resp.Blocked.Blocked {
		t.Fatalf("expected blocked.blocked=false for healthy run")
	}
}
