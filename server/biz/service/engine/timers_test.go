package engine

import (
	"sync"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/server/biz/domain/clock/clocktest"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

// timerCount 统计 srv 上某类待到期 timer 的数量。
func timerCount(srv *Server, kind timerKind) int {
	srv.timers.mu.Lock()
	defer srv.timers.mu.Unlock()
	count := 0
	for _, run := range srv.timers.runs {
		for ref := range run {
			if ref.Kind == kind {
				count++
			}
		}
	}
	return count
}

func timerArmed(srv *Server, ref timerRef) bool {
	_, armed := srv.timers.Deadline(ref)
	return armed
}

// activityTimersArmed 报告该 Activity 是否还有任一待到期 timer。
func activityTimersArmed(srv *Server, runID string, scheduledEventID int64) bool {
	for _, kind := range append([]timerKind{timerActivityScheduleToClose}, activityAttemptTimerKinds...) {
		if timerArmed(srv, activityTimerRef(kind, runID, scheduledEventID)) {
			return true
		}
	}
	return false
}

func activityAttemptForTest(t *testing.T, srv *Server, runID string, scheduledEventID int64) int {
	t.Helper()
	st, ok, err := srv.GetActivityState(runID, scheduledEventID)
	if err != nil || !ok {
		t.Fatalf("activity state (run=%s scheduledEventID=%d): ok=%v err=%v", runID, scheduledEventID, ok, err)
	}
	return st.Attempt
}

// activityIDForTest 读取 Activity 的 ActivityID，供按 ActivityID 寻址的操作（ResetActivity）使用。
func activityIDForTest(t *testing.T, srv *Server, runID string, scheduledEventID int64) string {
	t.Helper()
	st, ok, err := srv.GetActivityState(runID, scheduledEventID)
	if err != nil || !ok {
		t.Fatalf("activity state (run=%s scheduledEventID=%d): ok=%v err=%v", runID, scheduledEventID, ok, err)
	}
	return st.ActivityID
}

type firedTimers struct {
	mu   sync.Mutex
	refs []timerRef
}

func (f *firedTimers) record(ref timerRef) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.refs = append(f.refs, ref)
}

func (f *firedTimers) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.refs)
}

// 同键重复 Arm 时旧 timer 被替换：周期补偿每轮都会重排，同一个 timer 只到期一次，且按新的截止时间。
func TestTimerSetRearmReplacesPreviousTimer(t *testing.T) {
	clk := clocktest.NewFake(testClockStart)
	var fired firedTimers
	timers := newTimerSet(clk, fired.record)
	ref := timerRef{Kind: timerDurable, RunID: "run-1", ScheduledEventID: 7}

	timers.Arm(ref, clk.Now().Add(time.Minute))
	timers.Arm(ref, clk.Now().Add(2*time.Minute))

	clk.Advance(90 * time.Second)
	if got := fired.count(); got != 0 {
		t.Fatalf("superseded timer fired: %d", got)
	}
	clk.Advance(time.Minute)
	if got := fired.count(); got != 1 {
		t.Fatalf("fired = %d, want 1", got)
	}
	if _, armed := timers.Deadline(ref); armed {
		t.Fatal("fired timer must leave the set")
	}
}

func TestTimerSetStopPreventsFire(t *testing.T) {
	clk := clocktest.NewFake(testClockStart)
	var fired firedTimers
	timers := newTimerSet(clk, fired.record)
	kept := timerRef{Kind: timerWorkflowRun, RunID: "run-kept"}
	timers.Arm(kept, clk.Now().Add(time.Minute))
	timers.Arm(timerRef{Kind: timerWorkflowRun, RunID: "run-1"}, clk.Now().Add(time.Minute))
	timers.Arm(timerRef{Kind: timerDurable, RunID: "run-1", ScheduledEventID: 3}, clk.Now().Add(time.Minute))
	timers.Arm(timerRef{Kind: timerDurable, RunID: "run-2", ScheduledEventID: 4}, clk.Now().Add(time.Minute))

	timers.StopRun("run-1")
	timers.Stop(timerRef{Kind: timerDurable, RunID: "run-2", ScheduledEventID: 4})

	clk.Advance(2 * time.Minute)
	if len(fired.refs) != 1 || fired.refs[0] != kept {
		t.Fatalf("fired = %+v, want only %+v", fired.refs, kept)
	}
	if clk.Pending() != 0 {
		t.Fatalf("stopped timers still pending: %d", clk.Pending())
	}
}

// 截止时间已过的 Arm 立即到期；到期回调在锁外执行，可以重新 Arm 同一个键。
func TestTimerSetFireRunsOutsideLock(t *testing.T) {
	clk := clocktest.NewFake(testClockStart)
	ref := timerRef{Kind: timerActivityHeartbeat, RunID: "run-1", ScheduledEventID: 9}
	var timers *timerSet
	fires := make(chan int, 2)
	count := 0
	timers = newTimerSet(clk, func(fired timerRef) {
		count++
		if count == 1 {
			timers.Arm(fired, clk.Now().Add(time.Minute))
		}
		fires <- count
	})

	timers.Arm(ref, clk.Now().Add(-time.Second))
	if got := <-fires; got != 1 {
		t.Fatalf("fires = %d, want 1", got)
	}
	if _, armed := timers.Deadline(ref); !armed {
		t.Fatal("the overdue timer must be re-armed by its callback")
	}
	clk.Advance(time.Minute)
	if got := <-fires; got != 2 {
		t.Fatalf("fires = %d, want 2", got)
	}
}

// 上一次 attempt 残留的 LastHeartbeatAt 不能让重试中的 Activity 丢失 heartbeat 定时器。
func TestArmActivityTimeoutsIgnoresPreviousAttemptHeartbeat(t *testing.T) {
	srv := assembleServer(memory.NewExecutionManager(), newTestTaskQueue(), newTestTaskQueue())
	t.Cleanup(srv.Stop)

	now := time.Now()
	previousHeartbeat := now.Add(-time.Hour)
	srv.armActivityTimeouts(&execution.ActivityState{
		RunID:              "run-retry-heartbeat",
		ScheduledEventID:   7,
		ScheduledTime:      now.Add(-2 * time.Hour),
		StartedTime:        &now,
		LastHeartbeatAt:    &previousHeartbeat,
		Status:             execution.ActivityStatusEnums.Started,
		Attempt:            2,
		HeartbeatTimeoutMs: time.Minute.Milliseconds(),
	}, false)
	deadline, armed := srv.timers.Deadline(activityTimerRef(timerActivityHeartbeat, "run-retry-heartbeat", 7))
	if !armed || !deadline.Equal(now.Add(time.Minute)) {
		t.Fatalf("heartbeat timer must be armed from the current attempt's start time, got %v armed=%v", deadline, armed)
	}
}
