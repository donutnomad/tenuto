package engine

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	historypkg "github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/clock/clocktest"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

const overdueTestGrace = time.Minute

// newOverdueServer 用假时钟装配 Server；副本共用这只时钟，持久化的截止时间都相对 clk.Now()。
func newOverdueServer(t *testing.T) (*Server, execution.ExecutionManager, *clocktest.Fake) {
	t.Helper()
	mgr := memory.NewExecutionManager()
	srv, clk := newFakeClockServer(mgr, newTestTaskQueue(), newTestTaskQueue())
	t.Cleanup(srv.Stop)
	return srv, mgr, clk
}

// loseReplica 模拟副本永久下线：内存定时器随 Stop 消失，共享存储保留，由新 Server 接管扫描。
func loseReplica(t *testing.T, srv *Server, mgr execution.ExecutionManager, clk *clocktest.Fake) *Server {
	t.Helper()
	srv.Stop()
	survivor := assembleServerWithClock(mgr, newTestTaskQueue(), newTestTaskQueue(), clk)
	t.Cleanup(survivor.Stop)
	return survivor
}

func scanOverdue(t *testing.T, srv *Server) OverdueTimerResult {
	t.Helper()
	result, err := srv.FireOverdueTimers(context.Background(), overdueTestGrace)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func historyLen(t *testing.T, mgr execution.ExecutionManager, runID string) int {
	t.Helper()
	history, err := mgr.GetEvents(runID)
	if err != nil {
		t.Fatal(err)
	}
	return len(history)
}

func completeRun(t *testing.T, srv *Server, mgr execution.ExecutionManager, runID string) {
	t.Helper()
	if err := srv.HandleWorkflowTaskCompleted(srv.WorkflowTaskContext(context.Background(), runID, currentWFTScheduledID(t, srv, runID)), runID, currentWFTScheduledID(t, srv, runID), []historypkg.Command{{Type: historypkg.CmdCompleteWorkflow}}, ""); err != nil {
		t.Fatal(err)
	}
	run, err := mgr.Get(runID)
	if err != nil {
		t.Fatal(err)
	}
	if run.Status != historypkg.StatusCompleted {
		t.Fatalf("run must complete after takeover, got %s", run.Status)
	}
}

// advanceTo 把假时钟推进到 at；持久化的截止时间不变，由时间流逝让它过期。
func advanceTo(clk *clocktest.Fake, at time.Time) {
	clk.Advance(at.Sub(clk.Now()))
}

// advanceTimerOverdue 推进假时钟，使 durable timer 的持久化截止时间已过去 overdue。
func advanceTimerOverdue(t *testing.T, mgr execution.ExecutionManager, clk *clocktest.Fake, runID string, schedID int64, overdue time.Duration) {
	t.Helper()
	st, ok, err := mgr.GetTimerState(runID, schedID)
	if err != nil || !ok {
		t.Fatalf("timer state missing: ok=%v err=%v", ok, err)
	}
	advanceTo(clk, st.Deadline.Add(overdue))
}

// Integration: 副本丢失后，存活副本只在超过宽限期时接管 durable timer，重复扫描无副作用，Run 能完成。
func TestFireOverdueTimersTakesOverLostDurableTimer(t *testing.T) {
	srv, mgr, clk := newOverdueServer(t)
	runID, schedID := scheduleDurableTimer(t, srv, mgr, "run-overdue-timer", time.Hour)
	survivor := loseReplica(t, srv, mgr, clk)

	if got := scanOverdue(t, survivor); got != (OverdueTimerResult{}) {
		t.Fatalf("future timer must not fire: %+v", got)
	}

	advanceTimerOverdue(t, mgr, clk, runID, schedID, 10*time.Second)
	if got := scanOverdue(t, survivor); got != (OverdueTimerResult{}) {
		t.Fatalf("timer within grace must be left to its holder: %+v", got)
	}

	advanceTimerOverdue(t, mgr, clk, runID, schedID, 2*overdueTestGrace)
	if got := scanOverdue(t, survivor); got.DurableTimers != 1 {
		t.Fatalf("overdue timer must fire once: %+v", got)
	}
	history, _ := mgr.GetEvents(runID)
	if got := countEngineEvents(history, historypkg.EventTimerFired); got != 1 {
		t.Fatalf("expected 1 TimerFired, got %d", got)
	}
	before := len(history)

	if got := scanOverdue(t, survivor); got != (OverdueTimerResult{}) {
		t.Fatalf("repeat scan must be a no-op: %+v", got)
	}
	if got := historyLen(t, mgr, runID); got != before {
		t.Fatalf("repeat scan changed history: %d -> %d", before, got)
	}
	completeRun(t, survivor, mgr, runID)
}

// Integration: 持有 Activity 超时定时器的副本下线后，超时按持久化时间接管，Run 能继续。
func TestFireOverdueTimersTakesOverLostActivityTimeout(t *testing.T) {
	srv, mgr, clk := newOverdueServer(t)
	const runID = "run-overdue-activity"
	seedRunningWorkflowWithActivity(t, srv, mgr, runID)
	if err := srv.HandleWorkflowTaskCompleted(srv.WorkflowTaskContext(context.Background(), runID, currentWFTScheduledID(t, srv, runID)), runID, currentWFTScheduledID(t, srv, runID), []historypkg.Command{{
		Type: historypkg.CmdScheduleActivity, ActivityID: "step", ActivityType: "slow", StartToCloseTimeout: time.Hour,
		RetryPolicy: retryPolicy(1, time.Second),
	}}, ""); err != nil {
		t.Fatal(err)
	}
	states, _ := mgr.ListActivityStates(runID)
	if len(states) != 1 {
		t.Fatalf("want 1 activity state, got %d", len(states))
	}
	st := states[0]
	if err := srv.HandleActivityStarted(runID, st.ScheduledEventID, st.Attempt, st.Stamp, time.Hour, 0); err != nil {
		t.Fatal(err)
	}
	survivor := loseReplica(t, srv, mgr, clk)

	started, _, _ := mgr.GetActivityState(runID, st.ScheduledEventID)
	deadline := started.StartedTime.Add(time.Hour)

	advanceTo(clk, deadline.Add(10*time.Second))
	if got := scanOverdue(t, survivor); got != (OverdueTimerResult{}) {
		t.Fatalf("timeout within grace must be left to its holder: %+v", got)
	}

	advanceTo(clk, deadline.Add(2*overdueTestGrace))
	if got := scanOverdue(t, survivor); got.ActivityTimeouts != 1 {
		t.Fatalf("overdue activity must time out once: %+v", got)
	}
	current, _, _ := mgr.GetActivityState(runID, st.ScheduledEventID)
	if !current.IsTerminal() {
		t.Fatalf("expected terminal activity, got %s", current.Status)
	}
	history, _ := mgr.GetEvents(runID)
	if got := countEngineEvents(history, historypkg.EventActivityTimedOut); got != 1 {
		t.Fatalf("expected 1 ActivityTimedOut, got %d", got)
	}
	before := len(history)

	if got := scanOverdue(t, survivor); got != (OverdueTimerResult{}) {
		t.Fatalf("repeat scan must be a no-op: %+v", got)
	}
	if got := historyLen(t, mgr, runID); got != before {
		t.Fatalf("repeat scan changed history: %d -> %d", before, got)
	}
	completeRun(t, survivor, mgr, runID)
}

// Integration: 带 RetryPolicy 的 Activity 超过 ScheduleToClose 后，扫描只让它失败一次，
// 正在运行的 attempt 被关闭、不产生新 attempt，Run 按 Workflow 逻辑继续推进。
func TestFireOverdueTimersScheduleToCloseEndsRetries(t *testing.T) {
	srv, mgr, clk := newOverdueServer(t)
	const runID = "run-overdue-s2c-retry"
	seedRunningWorkflowWithActivity(t, srv, mgr, runID)
	if err := srv.HandleWorkflowTaskCompleted(srv.WorkflowTaskContext(context.Background(), runID, currentWFTScheduledID(t, srv, runID)), runID, currentWFTScheduledID(t, srv, runID), []historypkg.Command{{
		Type: historypkg.CmdScheduleActivity, ActivityID: "step", ActivityType: "slow", ScheduleToCloseTimeout: time.Hour,
		RetryPolicy: retryPolicy(0, time.Minute),
	}}, ""); err != nil {
		t.Fatal(err)
	}
	states, _ := mgr.ListActivityStates(runID)
	if len(states) != 1 {
		t.Fatalf("want 1 activity state, got %d", len(states))
	}
	st := states[0]
	if err := srv.HandleActivityStarted(runID, st.ScheduledEventID, 1, st.Stamp, 0, 0); err != nil {
		t.Fatal(err)
	}
	if err := srv.HandleActivityFailed(context.Background(), runID, st.ScheduledEventID, 1, st.Stamp, protocol.Failure{Message: "boom"}); err != nil {
		t.Fatal(err)
	}
	retried, _, _ := mgr.GetActivityState(runID, st.ScheduledEventID)
	if retried.Attempt != 2 {
		t.Fatalf("attempt = %d, want 2", retried.Attempt)
	}
	if err := srv.HandleActivityStarted(runID, st.ScheduledEventID, 2, retried.Stamp, 0, 0); err != nil {
		t.Fatal(err)
	}
	survivor := loseReplica(t, srv, mgr, clk)

	current, _, _ := mgr.GetActivityState(runID, st.ScheduledEventID)
	advanceTo(clk, current.ScheduledTime.Add(time.Hour+2*overdueTestGrace))
	if got := scanOverdue(t, survivor); got.ActivityTimeouts != 1 {
		t.Fatalf("overdue activity must time out once: %+v", got)
	}

	closed, _, _ := mgr.GetActivityState(runID, st.ScheduledEventID)
	if !closed.IsTerminal() || closed.Attempt != 2 || closed.Stamp != current.Stamp {
		t.Fatalf("closed state = %+v, want terminal attempt 2 with unchanged stamp", closed)
	}
	history, _ := mgr.GetEvents(runID)
	if got := countEngineEvents(history, historypkg.EventActivityTimedOut); got != 1 {
		t.Fatalf("expected 1 ActivityTimedOut, got %d", got)
	}
	if got := countEngineEvents(history, historypkg.EventActivityFailed); got != 1 {
		t.Fatalf("expected 1 ActivityFailed, got %d", got)
	}
	if got := countEngineEvents(history, historypkg.EventActivityAttemptFailed); got != 1 {
		t.Fatalf("expected only the worker failure as ActivityAttemptFailed, got %d", got)
	}
	before := len(history)

	if got := scanOverdue(t, survivor); got != (OverdueTimerResult{}) {
		t.Fatalf("repeat scan must be a no-op: %+v", got)
	}
	if got := historyLen(t, mgr, runID); got != before {
		t.Fatalf("repeat scan changed history: %d -> %d", before, got)
	}
	completeRun(t, survivor, mgr, runID)
}

// Integration: Run 的 execution / run 超时没有 reconciler 重建路径，由扫描接管。
func TestFireOverdueTimersTakesOverLostRunDeadlines(t *testing.T) {
	for name, setDeadline := range map[string]func(*execution.Run, time.Time){
		"execution timeout": func(run *execution.Run, at time.Time) { run.DeadlineAt = &at },
		"run timeout":       func(run *execution.Run, at time.Time) { run.RunDeadlineAt = &at },
	} {
		t.Run(name, func(t *testing.T) {
			srv, mgr, clk := newOverdueServer(t)
			run := &execution.Run{ID: "run-overdue-deadline", WorkflowID: "wf", WorkflowType: "wf", Status: historypkg.StatusRunning, CreatedAt: clk.Now()}
			setDeadline(run, clk.Now().Add(-10*time.Second))
			if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(run)) }); err != nil {
				t.Fatal(err)
			}
			survivor := loseReplica(t, srv, mgr, clk)

			if got := scanOverdue(t, survivor); got != (OverdueTimerResult{}) {
				t.Fatalf("deadline within grace must be left to its holder: %+v", got)
			}

			clk.Advance(2 * overdueTestGrace)
			if got := scanOverdue(t, survivor); got.RunTimeouts != 1 {
				t.Fatalf("overdue deadline must fire once: %+v", got)
			}
			current, _ := mgr.Get(run.ID)
			if current.Status != historypkg.StatusFailed {
				t.Fatalf("expected failed run, got %s", current.Status)
			}
			if got := scanOverdue(t, survivor); got != (OverdueTimerResult{}) {
				t.Fatalf("repeat scan must be a no-op: %+v", got)
			}
		})
	}
}

// Integration: WFT 失败退避丢失后 Run 没有 open WFT，扫描在退避到期后重新调度。
func TestFireOverdueTimersReschedulesFailedWorkflowTask(t *testing.T) {
	srv, mgr, clk := newOverdueServer(t)
	srv.wftBackoffBase = time.Hour // 让原副本的退避定时器在测试期间不触发
	srv.wftBackoffCap = time.Hour
	const runID = "run-overdue-wft"
	seedRunningWorkflowWithActivity(t, srv, mgr, runID)
	if err := srv.HandleWorkflowTaskFailed(srv.WorkflowTaskContext(context.Background(), runID, currentWFTScheduledID(t, srv, runID)), runID, currentWFTScheduledID(t, srv, runID), historypkg.WorkflowTaskFailedCauseNonDeterministic, "boom"); err != nil {
		t.Fatal(err)
	}
	survivor := loseReplica(t, srv, mgr, clk)

	survivor.wftBackoffBase = time.Hour
	survivor.wftBackoffCap = time.Hour
	if got := scanOverdue(t, survivor); got != (OverdueTimerResult{}) {
		t.Fatalf("workflow task within backoff must not reschedule: %+v", got)
	}

	survivor.wftBackoffBase = time.Millisecond
	survivor.wftBackoffCap = time.Millisecond
	if got := scanOverdue(t, survivor); got != (OverdueTimerResult{}) {
		t.Fatalf("workflow task within grace must not reschedule: %+v", got)
	}

	clk.Advance(5 * time.Millisecond)
	result, err := survivor.FireOverdueTimers(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if result.WorkflowTasks != 1 {
		t.Fatalf("overdue workflow task must reschedule once: %+v", result)
	}
	before := historyLen(t, mgr, runID)
	result, err = survivor.FireOverdueTimers(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if result != (OverdueTimerResult{}) {
		t.Fatalf("open workflow task must stop rescheduling: %+v", result)
	}
	if got := historyLen(t, mgr, runID); got != before {
		t.Fatalf("repeat scan changed history: %d -> %d", before, got)
	}
	completeRun(t, survivor, mgr, runID)
}

// Edge Case: ConfigError 暂停的 Run 重调度不会产生 WFT，扫描不计入接管。
func TestFireOverdueTimersSkipsConfigErrorRun(t *testing.T) {
	srv, mgr, clk := newOverdueServer(t)
	srv.wftBackoffBase = time.Hour
	srv.wftBackoffCap = time.Hour
	const runID = "run-overdue-config-error"
	seedRunningWorkflowWithActivity(t, srv, mgr, runID)
	if err := srv.HandleWorkflowTaskFailed(srv.WorkflowTaskContext(context.Background(), runID, currentWFTScheduledID(t, srv, runID)), runID, currentWFTScheduledID(t, srv, runID), historypkg.WorkflowTaskFailedCauseNonDeterministic, "boom"); err != nil {
		t.Fatal(err)
	}
	survivor := loseReplica(t, srv, mgr, clk)
	survivor.wftBackoffBase = time.Millisecond
	survivor.wftBackoffCap = time.Millisecond
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error {
		current, err := tx.GetRunForUpdate(runID)
		if err != nil {
			return err
		}
		return execution.PauseRun(tx, current, "contract mismatch")
	}); err != nil {
		t.Fatal(err)
	}

	clk.Advance(5 * time.Millisecond)
	result, err := survivor.FireOverdueTimers(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if result != (OverdueTimerResult{}) {
		t.Fatalf("config-error run must not count as takeover: %+v", result)
	}
}

// Edge Case: 全量补偿不挂未来的定时器，周期补偿在 lookahead 内挂上；过期定时器留给扫描按宽限期接管。
func TestDispatchPendingTasksLeavesOverdueTimersToScan(t *testing.T) {
	srv, mgr, clk := newOverdueServer(t)
	runID, schedID := scheduleDurableTimer(t, srv, mgr, "run-dispatch-overdue", time.Hour)
	survivor := loseReplica(t, srv, mgr, clk)
	ref := timerRef{Kind: timerDurable, RunID: runID, ScheduledEventID: schedID}

	if _, err := survivor.DispatchPendingTasks(context.Background()); err != nil {
		t.Fatal(err)
	}
	if timerArmed(survivor, ref) {
		t.Fatal("full reconciliation must leave a future timer to the periodic reconciler")
	}
	if _, _, err := survivor.Reconcile(context.Background(), overdueTestGrace, 2*time.Hour); err != nil {
		t.Fatal(err)
	}
	if !timerArmed(survivor, ref) {
		t.Fatal("future timer within lookahead must be armed on the surviving replica")
	}

	survivor.stopDurableTimer(runID, schedID)
	advanceTimerOverdue(t, mgr, clk, runID, schedID, 10*time.Second)
	if _, err := survivor.DispatchPendingTasks(context.Background()); err != nil {
		t.Fatal(err)
	}
	// 错装的过期定时器在假时钟上立即于新 goroutine 触发，不经 Advance；留出调度时间再看 History。
	time.Sleep(50 * time.Millisecond)
	history, _ := mgr.GetEvents(runID)
	if got := countEngineEvents(history, historypkg.EventTimerFired); got != 0 {
		t.Fatalf("dispatch must not fire a timer within grace, got %d TimerFired", got)
	}
}

// Edge Case: 多副本同时扫描同一到期定时器，靠事务内非终态校验收敛为一次触发。
func TestFireOverdueTimersConvergesAcrossReplicas(t *testing.T) {
	srv, mgr, clk := newOverdueServer(t)
	runID, schedID := scheduleDurableTimer(t, srv, mgr, "run-overdue-concurrent", time.Hour)
	first := loseReplica(t, srv, mgr, clk)
	second := assembleServerWithClock(mgr, newTestTaskQueue(), newTestTaskQueue(), clk)
	t.Cleanup(second.Stop)
	advanceTimerOverdue(t, mgr, clk, runID, schedID, 2*overdueTestGrace)

	var wg sync.WaitGroup
	for _, replica := range []*Server{first, second} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := replica.FireOverdueTimers(context.Background(), overdueTestGrace); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()

	history, _ := mgr.GetEvents(runID)
	if got := countEngineEvents(history, historypkg.EventTimerFired); got != 1 {
		t.Fatalf("expected 1 TimerFired across replicas, got %d", got)
	}
	completeRun(t, first, mgr, runID)
}

// Edge Case: 过期的 Run 超过一页，触发让 Run 离开 Running；一轮扫描仍覆盖每一个 Run。
func TestFireOverdueTimersCoversEveryRunAcrossPages(t *testing.T) {
	srv, mgr, clk := newOverdueServer(t)
	const total = 2*runScanPageSize + 50
	expired := clk.Now().Add(-2 * overdueTestGrace)
	for i := range total {
		run := &execution.Run{
			ID: fmt.Sprintf("run-overdue-page-%04d", i), WorkflowID: fmt.Sprintf("wf-overdue-page-%04d", i), WorkflowType: fmt.Sprintf("wf-overdue-page-%04d", i),
			Status: historypkg.StatusRunning, CreatedAt: clk.Now(), RunDeadlineAt: &expired,
		}
		if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(run)) }); err != nil {
			t.Fatal(err)
		}
	}

	if got := scanOverdue(t, srv); got.RunTimeouts != total {
		t.Fatalf("one scan must fire every overdue run: fired %d of %d", got.RunTimeouts, total)
	}
	running, err := runsByStatus(mgr, historypkg.StatusRunning)
	if err != nil {
		t.Fatal(err)
	}
	if len(running) != 0 {
		t.Fatalf("%d runs were skipped and are still running", len(running))
	}
}

// Integration: 周期补偿的单次扫描同时完成到期接管；重复扫描不改动 History。
func TestReconcileFiresOverdueTimersInOnePass(t *testing.T) {
	srv, mgr, clk := newOverdueServer(t)
	runID, schedID := scheduleDurableTimer(t, srv, mgr, "run-reconcile-timer", time.Hour)
	survivor := loseReplica(t, srv, mgr, clk)
	advanceTimerOverdue(t, mgr, clk, runID, schedID, 2*overdueTestGrace)

	_, overdue, err := survivor.Reconcile(context.Background(), overdueTestGrace, 0)
	if err != nil {
		t.Fatal(err)
	}
	if overdue.DurableTimers != 1 {
		t.Fatalf("overdue timer must fire once: %+v", overdue)
	}
	before := historyLen(t, mgr, runID)
	if _, overdue, err = survivor.Reconcile(context.Background(), overdueTestGrace, 0); err != nil || overdue != (OverdueTimerResult{}) {
		t.Fatalf("repeat reconcile: overdue=%+v err=%v", overdue, err)
	}
	if got := historyLen(t, mgr, runID); got != before {
		t.Fatalf("repeat reconcile changed history: %d -> %d", before, got)
	}
	completeRun(t, survivor, mgr, runID)
}
