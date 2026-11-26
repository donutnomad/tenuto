package engine

import (
	"context"
	"testing"
	"time"

	historypkg "github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/clock/clocktest"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

// scheduleDurableTimer 通过 HandleWorkflowTaskCompleted 提交一个 CmdScheduleTimer，
// 返回 runID + scheduledEventID（即 TimerScheduled 事件 ID = TimerState 生命周期 key）。
func scheduleDurableTimer(t *testing.T, srv *Server, mgr execution.ExecutionManager, runID string, delay time.Duration) (string, int64) {
	t.Helper()
	seedRunningWorkflowWithActivity(t, srv, mgr, runID)
	wftID := currentWFTScheduledID(t, srv, runID)
	if err := srv.HandleWorkflowTaskCompleted(srv.WorkflowTaskContext(context.Background(), runID, wftID), runID, wftID, []historypkg.Command{{
		Type:    historypkg.CmdScheduleTimer,
		TimerID: "sleep-1",
		Delay:   delay,
	}}, ""); err != nil {
		t.Fatal(err)
	}
	states, _ := mgr.ListTimerStates(runID)
	if len(states) != 1 {
		t.Fatalf("want 1 timer state after schedule, got %d", len(states))
	}
	return runID, states[0].ScheduledEventID
}

// Happy Path: schedule 一个 durable timer 后，TimerState 应为 Status=scheduled、
// TimerID/Deadline 正确，且 history 有 1 个 TimerScheduled 事件，生命周期 key = 该事件 ID。
func TestScheduleTimerCreatesState(t *testing.T) {
	srv, mgr := newServerForTest(t)
	runID, schedID := scheduleDurableTimer(t, srv, mgr, "run-timer-schedule", time.Hour)

	st, ok, _ := mgr.GetTimerState(runID, schedID)
	if !ok {
		t.Fatal("timer state not created")
	}
	if st.TimerID != "sleep-1" || !st.IsScheduled() {
		t.Fatalf("bad timer state: %+v", st)
	}
	if st.Deadline.IsZero() {
		t.Fatal("deadline must be set for crash-recovery remaining calc")
	}
	history, _ := mgr.GetEvents(runID)
	if countEngineEvents(history, historypkg.EventTimerScheduled) != 1 {
		t.Fatalf("expected 1 TimerScheduled, got %d", countEngineEvents(history, historypkg.EventTimerScheduled))
	}
	// 生命周期 key = TimerScheduled 事件 ID
	for i := range history {
		if history[i].EventType() == historypkg.EventTimerScheduled && history[i].ID != schedID {
			t.Fatalf("timer state key %d must equal TimerScheduled event ID %d", schedID, history[i].ID)
		}
	}
}

// Happy Path: timer fire 后 TimerState=fired，history 有 1 个 TimerFired 事件。
func TestTimerFiredMarksStateFired(t *testing.T) {
	srv, mgr := newServerForTest(t)
	runID, schedID := scheduleDurableTimer(t, srv, mgr, "run-timer-fire", time.Hour)

	srv.handleTimerFired(context.Background(), runID, schedID)

	st, _, _ := mgr.GetTimerState(runID, schedID)
	if st.Status != execution.TimerStatusEnums.Fired || !st.IsTerminal() {
		t.Fatalf("expected fired terminal, got %+v", st)
	}
	history, _ := mgr.GetEvents(runID)
	if countEngineEvents(history, historypkg.EventTimerFired) != 1 {
		t.Fatalf("expected 1 TimerFired, got %d", countEngineEvents(history, historypkg.EventTimerFired))
	}
}

// Edge Case（幂等）: 重复 fire 被 Status 终态拦截——不写第二个 TimerFired（替代 timerAlreadyClosed 历史扫描）。
func TestTimerFiredIdempotentOnRepeat(t *testing.T) {
	srv, mgr := newServerForTest(t)
	runID, schedID := scheduleDurableTimer(t, srv, mgr, "run-timer-dup", time.Hour)

	srv.handleTimerFired(context.Background(), runID, schedID)
	srv.handleTimerFired(context.Background(), runID, schedID) // 重复（旧句柄/重投）

	history, _ := mgr.GetEvents(runID)
	if got := countEngineEvents(history, historypkg.EventTimerFired); got != 1 {
		t.Fatalf("repeat fire must be idempotent, got %d TimerFired", got)
	}
}

// Happy Path（崩溃恢复）: 写 TimerState(scheduled) 后"重启"（Stop 清空内存 timer，复用同 mgr），
// recoverDurableTimers 从 ListTimerStates 读回重建内存句柄；deadline 已过的 timer 立即 fire（不扫历史）。
func TestRecoverDurableTimersFromState(t *testing.T) {
	srv, mgr, clk := newFakeServerForTest(t)
	runID, schedID := scheduleDurableTimer(t, srv, mgr, "run-timer-recover", time.Hour)

	// "重启"：Stop 清空内存 timer（mgr 内存保留，模拟进程重启后内存丢失但 DB 在）；重启前 deadline 已过。
	srv.Stop()
	clk.Advance(time.Hour + time.Minute)

	if err := srv.recoverDurableTimers(context.Background(), runID, true, testRecoveryHorizon); err != nil {
		t.Fatal(err)
	}
	// 已过期 timer 重建后立即 fire（startDurableTimer 的 remaining<=0 路径 → go handleTimerFired）。
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if s, _, _ := mgr.GetTimerState(runID, schedID); s != nil && s.Status == execution.TimerStatusEnums.Fired {
			return // 成功：recovery 从 state 重建并 fire，不依赖历史
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("recovered timer did not fire from TimerState")
}

// Edge Case: 已 fired 的 TimerState 在 recovery 时被跳过（不重建、不重复 fire）。
func TestRecoverDurableTimersSkipsFired(t *testing.T) {
	srv, mgr, clk := newFakeServerForTest(t)
	runID, schedID := scheduleDurableTimer(t, srv, mgr, "run-timer-recover-fired", time.Hour)
	srv.handleTimerFired(context.Background(), runID, schedID)
	if st, _, _ := mgr.GetTimerState(runID, schedID); !st.IsTerminal() {
		t.Fatalf("timer must be fired before restart, got %+v", st)
	}
	srv.Stop()

	historyBefore, _ := mgr.GetEvents(runID)
	firedBefore := countEngineEvents(historyBefore, historypkg.EventTimerFired)

	if err := srv.recoverDurableTimers(context.Background(), runID, true, testRecoveryHorizon); err != nil {
		t.Fatal(err)
	}
	clk.Advance(2 * time.Hour)
	historyAfter, _ := mgr.GetEvents(runID)
	if got := countEngineEvents(historyAfter, historypkg.EventTimerFired) - firedBefore; got != 0 {
		t.Fatalf("fired timer must not be recovered/re-fired, got %d extra TimerFired", got)
	}
}

// Edge Case: 未来 deadline 的 timer 经 recovery 重建后不立即 fire，按持久化的 deadline 到期。
func TestRecoverDurableTimersFutureDeadlineNotFiredImmediately(t *testing.T) {
	srv, mgr, clk := newFakeServerForTest(t)
	// schedule 一个 1 小时后到期的 timer（未来 deadline）。
	runID, schedID := scheduleDurableTimer(t, srv, mgr, "run-timer-future", time.Hour)
	srv.Stop() // "重启"：清空内存 timer，TimerState（scheduled, deadline=+1h）保留

	if err := srv.recoverDurableTimers(context.Background(), runID, true, testRecoveryHorizon); err != nil {
		t.Fatal(err)
	}
	clk.Advance(time.Hour - time.Millisecond)
	st, _, _ := mgr.GetTimerState(runID, schedID)
	if st.Status != execution.TimerStatusEnums.Scheduled {
		t.Fatalf("future-deadline timer must stay scheduled after recovery, got %q", st.Status)
	}
	history, _ := mgr.GetEvents(runID)
	if got := countEngineEvents(history, historypkg.EventTimerFired); got != 0 {
		t.Fatalf("future-deadline timer must NOT fire before its deadline, got %d TimerFired", got)
	}
	clk.Advance(time.Millisecond)
	if st, _, _ := mgr.GetTimerState(runID, schedID); st.Status != execution.TimerStatusEnums.Fired {
		t.Fatalf("recovered timer status at its deadline = %q, want fired", st.Status)
	}
}

// Happy Path: durable timer 在 delay 到期时落 TimerFired 并调度下一个 WFT，之前不触发。
func TestDurableTimerFiresAtDeadline(t *testing.T) {
	srv, mgr, clk := newFakeServerForTest(t)
	runID, schedID := scheduleDurableTimer(t, srv, mgr, "run-timer-deadline", 10*time.Minute)
	st, _, _ := mgr.GetTimerState(runID, schedID)
	if !st.Deadline.Equal(clk.Now().Add(10 * time.Minute)) {
		t.Fatalf("persisted deadline = %v, want %v", st.Deadline, clk.Now().Add(10*time.Minute))
	}

	clk.Advance(10*time.Minute - time.Millisecond)
	if history, _ := mgr.GetEvents(runID); countEngineEvents(history, historypkg.EventTimerFired) != 0 {
		t.Fatal("timer fired before its deadline")
	}
	clk.Advance(time.Millisecond)
	history, _ := mgr.GetEvents(runID)
	if countEngineEvents(history, historypkg.EventTimerFired) != 1 {
		t.Fatalf("TimerFired = %d, want 1", countEngineEvents(history, historypkg.EventTimerFired))
	}
	if state, _ := mgr.GetWorkflowExecutionState(runID); state.OpenWorkflowTaskEventID == 0 {
		t.Fatal("timer fire must schedule the next workflow task")
	}
}

func newFakeServerForTest(tb testing.TB) (*Server, execution.ExecutionManager, *clocktest.Fake) {
	mgr := memory.NewExecutionManager()
	srv, clk := newFakeClockServer(mgr, newTestTaskQueue(), newTestTaskQueue())
	tb.Cleanup(srv.Stop)
	return srv, mgr, clk
}

// Run 结束时仍未触发的 timer 随 Run 关闭（run_closed，不写事件）：完成与终止两条关闭路径；之后到期、恢复扫描
// 与残留的内存句柄都不再触发它。
func TestRunCloseClosesPendingTimers(t *testing.T) {
	for _, tc := range []struct {
		name  string
		close func(t *testing.T, srv *Server, mgr execution.ExecutionManager, runID string)
	}{
		{name: "complete", close: func(t *testing.T, srv *Server, mgr execution.ExecutionManager, runID string) {
			wftID := appendOpenWorkflowTaskForTest(t, mgr, runID)
			if err := srv.HandleWorkflowTaskCompleted(srv.WorkflowTaskContext(context.Background(), runID, wftID), runID, wftID,
				[]historypkg.Command{{Type: historypkg.CmdCompleteWorkflow}}, ""); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "terminate", close: func(t *testing.T, srv *Server, _ execution.ExecutionManager, runID string) {
			if err := srv.TerminateWorkflow(context.Background(), "", "", runID, "stop"); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv, mgr, clk := newFakeServerForTest(t)
			runID, schedID := scheduleDurableTimer(t, srv, mgr, "run-close-timer-"+tc.name, time.Hour)
			tc.close(t, srv, mgr, runID)
			if status, err := srv.GetRunStatus(runID); err != nil || !historypkg.IsTerminalStatus(status) {
				t.Fatalf("run status = %s (err %v), want closed", status, err)
			}
			states, err := mgr.ListTimerStates(runID)
			if err != nil {
				t.Fatal(err)
			}
			for _, st := range states {
				if st.IsScheduled() {
					t.Fatalf("closed run still has a scheduled timer: %+v", st)
				}
			}
			if st, _, _ := mgr.GetTimerState(runID, schedID); st.Status != execution.TimerStatusEnums.RunClosed {
				t.Fatalf("timer status = %s, want run_closed", st.Status)
			}
			before, _ := mgr.GetEvents(runID)

			clk.Advance(2 * time.Hour)
			if err := srv.recoverDurableTimers(context.Background(), runID, true, testRecoveryHorizon); err != nil {
				t.Fatal(err)
			}
			srv.handleTimerFired(context.Background(), runID, schedID) // 另一个副本上残留的内存句柄

			after, _ := mgr.GetEvents(runID)
			if len(after) != len(before) || countEngineEvents(after, historypkg.EventTimerFired) != 0 {
				t.Fatalf("history changed after close: %d -> %d events, %d TimerFired", len(before), len(after), countEngineEvents(after, historypkg.EventTimerFired))
			}
			if countEngineEvents(after, historypkg.EventTimerCancelled) != 0 {
				t.Fatal("closing the run must not write TimerCancelled")
			}
			if st, _, _ := mgr.GetTimerState(runID, schedID); st.Status != execution.TimerStatusEnums.RunClosed {
				t.Fatalf("timer status after deadline = %s, want run_closed", st.Status)
			}
		})
	}
}
