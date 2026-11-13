package engine

import (
	"context"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	historypkg "github.com/donutnomad/tenuto/protocol/history"

	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

// newStateTestServer 用内存执行存储构造测试 Server。
func newStateTestServer(t *testing.T) (*Server, *memory.ExecutionManager) {
	return newServerForTest(t)
}

// seedRunningWorkflowWithActivity 建一个 running run 并写入一个 open WFT，返回 runID。
func seedRunningWorkflowWithActivity(t *testing.T, srv *Server, mgr execution.ExecutionManager, runID string) {
	t.Helper()
	run := &execution.Run{ID: runID, WorkflowID: "wf", WorkflowType: "wf", Status: historypkg.StatusRunning, CreatedAt: time.Now()}
	createStartedRunForTest(t, mgr, run)
	appendOpenWorkflowTaskForTest(t, mgr, runID)
}

// currentWFTScheduledID 读 execution state 里当前 open WFT 的 scheduled id。
func currentWFTScheduledID(t *testing.T, srv *Server, runID string) int64 {
	t.Helper()
	state, err := srv.mgr.GetWorkflowExecutionState(runID)
	if err != nil {
		t.Fatal(err)
	}
	id := state.OpenWorkflowTaskEventID
	if id == 0 {
		t.Fatalf("no open workflow task for run %s", runID)
	}
	return id
}

// scheduleRetryableActivity schedules 一个带 retry（MaxAttempts=3）的 activity，
// 返回 runID + scheduledEventID（即 ActivityScheduled 事件 ID）。
func scheduleRetryableActivity(t *testing.T, srv *Server, mgr execution.ExecutionManager) (string, int64) {
	t.Helper()
	return scheduleActivityWithRetry(t, srv, mgr, "run-retryable", &protocol.ParsedRetryPolicy{
		MaxAttempts: 3, InitialInterval: time.Second, BackoffCoefficient: 2,
	})
}

// scheduleRetryableActivity2 schedules 一个带 retry（MaxAttempts=2）的 activity。
func scheduleRetryableActivity2(t *testing.T, srv *Server, mgr execution.ExecutionManager) (string, int64) {
	t.Helper()
	return scheduleActivityWithRetry(t, srv, mgr, "run-retryable-2", &protocol.ParsedRetryPolicy{
		MaxAttempts: 2, InitialInterval: time.Second, BackoffCoefficient: 2,
	})
}

// scheduleNonRetryableActivity schedules 一个无 retry（MaxAttempts=1）的 activity。
func scheduleNonRetryableActivity(t *testing.T, srv *Server, mgr execution.ExecutionManager) (string, int64) {
	t.Helper()
	return scheduleActivityWithRetry(t, srv, mgr, "run-non-retryable", &protocol.ParsedRetryPolicy{
		MaxAttempts: 1, InitialInterval: time.Second, BackoffCoefficient: 2,
	})
}

func scheduleActivityWithRetry(t *testing.T, srv *Server, mgr execution.ExecutionManager, runID string, retry *protocol.ParsedRetryPolicy) (string, int64) {
	t.Helper()
	seedRunningWorkflowWithActivity(t, srv, mgr, runID)
	wftID := currentWFTScheduledID(t, srv, runID)
	if err := srv.HandleWorkflowTaskCompleted(srv.WorkflowTaskContext(context.Background(), runID, wftID), runID, wftID, []historypkg.Command{{
		Type:                historypkg.CmdScheduleActivity,
		ActivityID:          "settle",
		ActivityType:        "Settle",
		StartToCloseTimeout: time.Second,
		RetryPolicy:         retry,
	}}, ""); err != nil {
		t.Fatal(err)
	}
	states, _ := mgr.ListActivityStates(runID)
	if len(states) != 1 {
		t.Fatalf("want 1 activity state after schedule, got %d", len(states))
	}
	return runID, states[0].ScheduledEventID
}

// Happy Path: schedule 一个带 retry 的 activity 后，ActivityState 应为
// Attempt=1 / Stamp=1 / StartedEventID=EmptyEventID / Status=scheduled，且 history 有 ActivityScheduled。
func TestScheduleActivityCreatesState(t *testing.T) {
	srv, mgr := newStateTestServer(t)
	runID := "run-schedule-state"
	seedRunningWorkflowWithActivity(t, srv, mgr, runID)
	_ = srv.HandleWorkflowTaskCompleted(srv.WorkflowTaskContext(context.Background(), runID, currentWFTScheduledID(t, srv, runID)), runID, currentWFTScheduledID(t, srv, runID), []historypkg.Command{{
		Type: historypkg.CmdScheduleActivity, ActivityID: "settle", ActivityType: "Settle",
		StartToCloseTimeout: time.Second,
		// Command.RetryPolicy 是 *config.ParsedRetryPolicy（engine/command.go），非 RetryPolicyConfig。
		RetryPolicy: &protocol.ParsedRetryPolicy{MaxAttempts: 3, InitialInterval: time.Second, BackoffCoefficient: 2},
	}}, "")
	states, _ := mgr.ListActivityStates(runID)
	if len(states) != 1 {
		t.Fatalf("want 1 activity state, got %d", len(states))
	}
	st := states[0]
	if st.Attempt != 1 || st.Stamp != 1 || st.StartedEventID != execution.EmptyEventID || !st.IsScheduled() {
		t.Fatalf("bad initial state: %+v", st)
	}
	history, _ := mgr.GetEvents(runID)
	if countEngineEvents(history, historypkg.EventActivityScheduled) != 1 {
		t.Fatalf("expected 1 ActivityScheduled event, got %d", countEngineEvents(history, historypkg.EventActivityScheduled))
	}
}

// Happy Path: 带 retry 的 activity start 走 transient——
// ActivityState.StartedEventID=TransientEventID(-124)、Status=started，
// 且 history 中 NO ActivityStarted 事件（transient 不写 history）。
func TestStartActivityTransientForRetryable(t *testing.T) {
	srv, mgr := newStateTestServer(t)
	runID, schedID := scheduleRetryableActivity(t, srv, mgr)
	err := srv.HandleActivityStarted(runID, schedID, 1, 1, time.Second, 0)
	if err != nil {
		t.Fatal(err)
	}
	st, _, _ := mgr.GetActivityState(runID, schedID)
	if st.StartedEventID != execution.TransientEventID || !st.IsStarted() {
		t.Fatalf("expected transient started, got startedEventID=%d status=%s", st.StartedEventID, st.Status)
	}
	events, _ := mgr.GetEvents(runID)
	if countEngineEvents(events, historypkg.EventActivityStarted) != 0 {
		t.Fatalf("transient attempt must NOT write ActivityStarted to history, found %d", countEngineEvents(events, historypkg.EventActivityStarted))
	}
}

// Edge Case: 无 retry 的 activity start 正常写 history——StartedEventID>0、history 有 1 个 ActivityStarted。
func TestStartActivityWritesHistoryWhenNoRetry(t *testing.T) {
	srv, mgr := newStateTestServer(t)
	runID, schedID := scheduleNonRetryableActivity(t, srv, mgr)
	if err := srv.HandleActivityStarted(runID, schedID, 1, 1, time.Second, 0); err != nil {
		t.Fatal(err)
	}
	st, _, _ := mgr.GetActivityState(runID, schedID)
	if st.StartedEventID <= 0 {
		t.Fatalf("non-retryable start must write real history id, got %d", st.StartedEventID)
	}
	events, _ := mgr.GetEvents(runID)
	if countEngineEvents(events, historypkg.EventActivityStarted) != 1 {
		t.Fatalf("expected 1 ActivityStarted, got %d", countEngineEvents(events, historypkg.EventActivityStarted))
	}
}

// Error Case: stamp 不符的 start 被丢弃（stale task）——state 不变。
func TestStartActivityRejectsStaleStamp(t *testing.T) {
	srv, mgr := newStateTestServer(t)
	runID, schedID := scheduleRetryableActivity(t, srv, mgr)
	// state.Stamp=1，但 task 携带 stamp=99（旧 generation）
	if err := srv.HandleActivityStarted(runID, schedID, 1, 99, time.Second, 0); err != nil {
		t.Fatal(err)
	}
	st, _, _ := mgr.GetActivityState(runID, schedID)
	if st.StartedEventID != execution.EmptyEventID || !st.IsScheduled() {
		t.Fatalf("stale-stamp start must be dropped, state mutated: %+v", st)
	}
}

// Happy Path: complete 成功 → state.Status=completed，history 有 ActivityCompleted。
func TestCompleteActivityWritesState(t *testing.T) {
	srv, mgr := newStateTestServer(t)
	runID, schedID := scheduleRetryableActivity(t, srv, mgr)
	_ = srv.HandleActivityStarted(runID, schedID, 1, 1, time.Second, 0)
	if err := srv.HandleActivityCompleted(context.Background(), runID, schedID, 1, 1, map[string]any{"ok": true}); err != nil {
		t.Fatal(err)
	}
	st, _, _ := mgr.GetActivityState(runID, schedID)
	if st.Status != execution.ActivityStatusEnums.Completed {
		t.Fatalf("status = %s", st.Status)
	}
}

// Error Case: complete 携带旧 stamp → 拒绝（state 不变为终态）。
func TestCompleteActivityRejectsStaleStamp(t *testing.T) {
	srv, mgr := newStateTestServer(t)
	runID, schedID := scheduleRetryableActivity(t, srv, mgr)
	_ = srv.HandleActivityStarted(runID, schedID, 1, 1, time.Second, 0)
	if err := srv.HandleActivityCompleted(context.Background(), runID, schedID, 1, 99, nil); err != nil {
		t.Fatal(err)
	}
	st, _, _ := mgr.GetActivityState(runID, schedID)
	if st.IsTerminal() {
		t.Fatal("stale-stamp complete must be rejected")
	}
}

// Happy Path: retry——attempt 失败重试，state.Attempt++ 且 Stamp++（tracker G），
// StartedEventID 回 EmptyEventID，Status 回 scheduled；history 有 ActivityAttemptFailed。
func TestFailActivityRetriesIncrementsAttemptAndStamp(t *testing.T) {
	srv, mgr := newStateTestServer(t)
	runID, schedID := scheduleRetryableActivity(t, srv, mgr) // MaxAttempts=3
	_ = srv.HandleActivityStarted(runID, schedID, 1, 1, time.Second, 0)
	if err := srv.HandleActivityFailed(context.Background(), runID, schedID, 1, 1, protocol.Failure{Message: "boom"}); err != nil {
		t.Fatal(err)
	}
	st, _, _ := mgr.GetActivityState(runID, schedID)
	if st.Attempt != 2 {
		t.Fatalf("attempt = %d, want 2", st.Attempt)
	}
	if st.Stamp != 2 {
		t.Fatalf("stamp = %d, want 2 (retry increments stamp)", st.Stamp)
	}
	if st.StartedEventID != execution.EmptyEventID || !st.IsScheduled() {
		t.Fatalf("after retry expect scheduled/empty, got %+v", st)
	}
	events, _ := mgr.GetEvents(runID)
	if countEngineEvents(events, historypkg.EventActivityAttemptFailed) != 1 {
		t.Fatalf("expected 1 ActivityAttemptFailed, got %d", countEngineEvents(events, historypkg.EventActivityAttemptFailed))
	}
}

// Happy Path（tracker F）: MaxAttempts=2，每个 attempt transient start → fail，
// 每次失败都补写对应 attempt 的 ActivityStarted（UI 重试链不断），耗尽时终态 ActivityFailed。
// transient 判定 = RetryPolicy != nil && MaxAttempts != 1（MaxAttempts=1 等价无重试，正常写 history）。
func TestFailActivityExhaustedBackfillsTransientStarted(t *testing.T) {
	srv, mgr := newStateTestServer(t)
	runID, schedID := scheduleRetryableActivity2(t, srv, mgr) // MaxAttempts=2
	// attempt 1：transient start → fail（重试 → attempt2, stamp2），补写 attempt1 started
	_ = srv.HandleActivityStarted(runID, schedID, 1, 1, time.Second, 0)
	_ = srv.HandleActivityFailed(context.Background(), runID, schedID, 1, 1, protocol.Failure{Message: "boom1"})
	// attempt 2：transient start → fail（耗尽，终态），补写 attempt2 started + ActivityFailed
	stAfter1, _, _ := mgr.GetActivityState(runID, schedID) // attempt2, stamp2
	_ = srv.HandleActivityStarted(runID, schedID, stAfter1.Attempt, stAfter1.Stamp, time.Second, 0)
	_ = srv.HandleActivityFailed(context.Background(), runID, schedID, stAfter1.Attempt, stAfter1.Stamp, protocol.Failure{Message: "boom2"})

	st, _, _ := mgr.GetActivityState(runID, schedID)
	if st.Status != execution.ActivityStatusEnums.Failed {
		t.Fatalf("status = %s, want failed", st.Status)
	}
	events, _ := mgr.GetEvents(runID)
	// tracker F：每个 attempt 失败都补写其 started → 2 个 ActivityStarted（🔴-2 stamp-aware dedupe 保证两条都写入）
	if countEngineEvents(events, historypkg.EventActivityStarted) != 2 {
		t.Fatalf("expected 2 backfilled ActivityStarted (one per attempt), got %d", countEngineEvents(events, historypkg.EventActivityStarted))
	}
	if countEngineEvents(events, historypkg.EventActivityFailed) != 1 {
		t.Fatalf("expected 1 ActivityFailed, got %d", countEngineEvents(events, historypkg.EventActivityFailed))
	}
}
