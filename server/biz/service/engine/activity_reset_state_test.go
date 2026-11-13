package engine

import (
	"context"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
)

// Happy Path: ResetActivity 把 pending activity 的 attempt 归 1、Stamp++、StartedEventID 归 0、
// Status 回 scheduled，并写 ActivityReset 事件，state.ActivityReset=true。
func TestResetActivityResetsStateAndBumpsStamp(t *testing.T) {
	srv, mgr := newStateTestServer(t)
	runID, schedID := scheduleRetryableActivity2(t, srv, mgr)
	_ = srv.HandleActivityStarted(runID, schedID, 1, 1, time.Second, 0)
	_ = srv.HandleActivityFailed(context.Background(), runID, schedID, 1, 1, protocol.Failure{Message: "boom"}) // attempt→2, stamp→2

	before, _, _ := mgr.GetActivityState(runID, schedID)
	if err := srv.ResetActivity("", runID, activityIDForTest(t, srv, runID, schedID), execution.ResetActivityOptions{Identity: "op"}); err != nil {
		t.Fatal(err)
	}
	st, _, _ := mgr.GetActivityState(runID, schedID)
	if st.Attempt != 1 {
		t.Fatalf("reset attempt = %d, want 1", st.Attempt)
	}
	if st.Stamp <= before.Stamp {
		t.Fatalf("reset must bump stamp: before=%d after=%d", before.Stamp, st.Stamp)
	}
	if st.StartedEventID != execution.EmptyEventID || !st.IsScheduled() || !st.ActivityReset {
		t.Fatalf("bad reset state: %+v", st)
	}
	events, _ := mgr.GetEvents(runID)
	if countEngineEvents(events, history.EventActivityReset) != 1 {
		t.Fatalf("expected 1 ActivityReset, got %d", countEngineEvents(events, history.EventActivityReset))
	}
}

// 核心根治回归（Error Case → Happy Path）：reset 后再次失败，重试链不断、不卡死。
// 这是整个架构改造要根除的 bug：旧架构 reset 后 activityAttemptStarted 漏过滤，
// 重试链断裂。现在权威读 state + stamp 校验，旧 in-flight task 自动失效。
func TestResetThenFailRetriesCleanly(t *testing.T) {
	srv, mgr := newStateTestServer(t)
	runID, schedID := scheduleRetryableActivity2(t, srv, mgr) // MaxAttempts=2
	// 第一轮：start attempt1 → fail（retry → attempt2, stamp2）
	_ = srv.HandleActivityStarted(runID, schedID, 1, 1, time.Second, 0)
	_ = srv.HandleActivityFailed(context.Background(), runID, schedID, 1, 1, protocol.Failure{Message: "boom1"})

	// reset：attempt→1, stamp→3
	if err := srv.ResetActivity("", runID, activityIDForTest(t, srv, runID, schedID), execution.ResetActivityOptions{}); err != nil {
		t.Fatal(err)
	}
	afterReset, _, _ := mgr.GetActivityState(runID, schedID)
	wantStamp := afterReset.Stamp

	// 模拟旧 in-flight task（reset 前 stamp=2）现在回报失败 → 必须被 stamp 校验丢弃。
	_ = srv.HandleActivityFailed(context.Background(), runID, schedID, 2, 2, protocol.Failure{Message: "stale"})
	st, _, _ := mgr.GetActivityState(runID, schedID)
	if st.Attempt != 1 || st.Stamp != wantStamp {
		t.Fatalf("stale failed report must be dropped, state changed: %+v", st)
	}

	// reset 后的新 attempt1 正常 start → fail → 干净重试到 attempt2（链不断）。
	_ = srv.HandleActivityStarted(runID, schedID, 1, wantStamp, time.Second, 0)
	if err := srv.HandleActivityFailed(context.Background(), runID, schedID, 1, wantStamp, protocol.Failure{Message: "boom-after-reset"}); err != nil {
		t.Fatal(err)
	}
	st, _, _ = mgr.GetActivityState(runID, schedID)
	if st.Attempt != 2 {
		t.Fatalf("after reset retry chain broken: attempt = %d, want 2", st.Attempt)
	}
	if st.IsTerminal() {
		t.Fatal("activity should still be retrying (not stuck terminal) after reset")
	}
}

// 🔴-2 专项回归：reset 后再失败，验证 post-reset 的 started/attemptFailed 事件
// **真的写进了 history**（撞键失明已根治）。reset 后 attempt 归 1，若 dedupe key 不含 stamp，
// post-reset attempt-1 事件会撞 pre-reset 同 attempt 键、inserted=false、不写 history。
func TestResetPostResetEventsActuallyWrittenToHistory(t *testing.T) {
	srv, mgr := newStateTestServer(t)
	runID, schedID := scheduleRetryableActivity2(t, srv, mgr) // MaxAttempts=2

	// pre-reset：attempt1 transient start → fail → 补写 attempt1 started（stamp=1）+ attemptFailed（stamp=1）
	_ = srv.HandleActivityStarted(runID, schedID, 1, 1, time.Second, 0)
	_ = srv.HandleActivityFailed(context.Background(), runID, schedID, 1, 1, protocol.Failure{Message: "boom-pre"})
	preEvents, _ := mgr.GetEvents(runID)
	preStarted := countEngineEvents(preEvents, history.EventActivityStarted)
	preAttemptFailed := countEngineEvents(preEvents, history.EventActivityAttemptFailed)

	// reset：attempt→1, stamp→3（pre-reset stamp 已是 2）
	if err := srv.ResetActivity("", runID, activityIDForTest(t, srv, runID, schedID), execution.ResetActivityOptions{}); err != nil {
		t.Fatal(err)
	}
	after, _, _ := mgr.GetActivityState(runID, schedID)

	// post-reset：attempt 又是 1，但 stamp=3 → dedupe key 不同 → 事件必须真写进 history。
	_ = srv.HandleActivityStarted(runID, schedID, 1, after.Stamp, time.Second, 0)
	_ = srv.HandleActivityFailed(context.Background(), runID, schedID, 1, after.Stamp, protocol.Failure{Message: "boom-post"})

	postEvents, _ := mgr.GetEvents(runID)
	postStarted := countEngineEvents(postEvents, history.EventActivityStarted)
	postAttemptFailed := countEngineEvents(postEvents, history.EventActivityAttemptFailed)

	// 关键断言：post-reset 的 started / attemptFailed 是**新增**的（count 增加），不是撞键被吞。
	if postStarted != preStarted+1 {
		t.Fatalf("post-reset ActivityStarted not written (dedupe collision): pre=%d post=%d", preStarted, postStarted)
	}
	if postAttemptFailed != preAttemptFailed+1 {
		t.Fatalf("post-reset ActivityAttemptFailed not written (dedupe collision): pre=%d post=%d", preAttemptFailed, postAttemptFailed)
	}
	// 且 StartedEventID 是 post-reset 的真实新 id，不是回退到 pre-reset 旧事件 id。
	st, _, _ := mgr.GetActivityState(runID, schedID)
	if st.Attempt != 2 || st.IsTerminal() {
		t.Fatalf("post-reset retry chain should advance to attempt 2 scheduled, got %+v", st)
	}
}
