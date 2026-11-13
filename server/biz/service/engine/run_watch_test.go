package engine

import (
	"context"
	"log/slog"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/clock/clocktest"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

// EntryCount 返回有等待者的 Run 数，测试用它验证等待者都已退出。
func (w *runWatch) EntryCount() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.runs)
}

// fakeWatchStates 是 runWatch 读取的 execution state，记录每次读取的 Run。
type fakeWatchStates struct {
	mu    sync.Mutex
	next  map[string]int64
	reads [][]string
}

func (f *fakeWatchStates) set(runID string, next int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.next[runID] = next
}

func (f *fakeWatchStates) list(runIDs []string) (map[string]*execution.WorkflowExecutionState, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reads = append(f.reads, slices.Sorted(slices.Values(runIDs)))
	result := make(map[string]*execution.WorkflowExecutionState)
	for _, runID := range runIDs {
		if next, ok := f.next[runID]; ok {
			result[runID] = &execution.WorkflowExecutionState{RunID: runID, NextEventID: next}
		}
	}
	return result, nil
}

func (f *fakeWatchStates) readCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.reads)
}

func newTestRunWatch() (*runWatch, *fakeWatchStates, *clocktest.Fake) {
	clk := clocktest.NewFake(testClockStart)
	states := &fakeWatchStates{next: make(map[string]int64)}
	return newRunWatch(clk, states.list, slog.New(slog.DiscardHandler)), states, clk
}

// startWait 在后台调用 Wait，等它登记之后返回结果 channel。
func startWait(t *testing.T, w *runWatch, runID string, seen int64, waiters int) <-chan bool {
	t.Helper()
	woken := make(chan bool, 1)
	go func() { woken <- w.Wait(context.Background(), runID, seen, time.Hour) }()
	waitFor(t, "waiter registered", func() bool {
		w.mu.Lock()
		defer w.mu.Unlock()
		return w.runs[runID] != nil && w.runs[runID].waiters == waiters
	})
	return woken
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

func receiveWithin[T any](t *testing.T, ch <-chan T, what string) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
		var zero T
		return zero
	}
}

// 没有等待者时不排定读取：时钟推进多久都不读库。
func TestRunWatchIdleReadsNothing(t *testing.T) {
	w, states, clk := newTestRunWatch()
	clk.Advance(time.Minute)
	if n := states.readCount(); n != 0 || clk.Pending() != 0 {
		t.Fatalf("idle watch read %d times, %d timers pending", n, clk.Pending())
	}
	// 等待者退出之后同样停止读取。
	states.set("run-1", 2)
	woken := startWait(t, w, "run-1", 1, 1)
	clk.Advance(runWatchPollInterval)
	if !receiveWithin(t, woken, "wake") {
		t.Fatal("Wait returned false after the run changed")
	}
	waitFor(t, "waiter released", func() bool { return w.EntryCount() == 0 })
	reads := states.readCount()
	clk.Advance(time.Minute)
	if states.readCount() != reads || clk.Pending() != 0 {
		t.Fatalf("watch kept reading after its last waiter left: %d -> %d reads, %d timers", reads, states.readCount(), clk.Pending())
	}
}

// 同一轮读取覆盖全部被等待的 Run，一个 Run 的多个等待者只读一次；没有变化的 Run 不唤醒。
func TestRunWatchBatchesRunsAndWakesOnlyChangedRuns(t *testing.T) {
	w, states, clk := newTestRunWatch()
	states.set("run-a", 5)
	states.set("run-b", 7)
	a1 := startWait(t, w, "run-a", 5, 1)
	a2 := startWait(t, w, "run-a", 5, 2)
	b := startWait(t, w, "run-b", 7, 1)

	clk.Advance(runWatchPollInterval)
	if got := states.reads; len(got) != 1 || !slices.Equal(got[0], []string{"run-a", "run-b"}) {
		t.Fatalf("reads = %v, want one read of both runs", got)
	}
	states.set("run-a", 6)
	clk.Advance(runWatchPollInterval)
	if !receiveWithin(t, a1, "first waiter") || !receiveWithin(t, a2, "second waiter") {
		t.Fatal("waiters of the changed run were not woken")
	}
	select {
	case <-b:
		t.Fatal("the waiter of an unchanged run was woken")
	default:
	}
	states.set("run-b", 8)
	waitFor(t, "run-a released", func() bool { return w.EntryCount() == 1 })
	clk.Advance(runWatchPollInterval)
	if !receiveWithin(t, b, "run-b waiter") {
		t.Fatal("run-b waiter not woken")
	}
}

// 调用方读到的值已落后于已观察到的变化时立即返回；Notify 不经读库唤醒；Run 被删除时唤醒。
func TestRunWatchStaleSeenNotifyAndDeletedRun(t *testing.T) {
	w, states, clk := newTestRunWatch()
	states.set("run-1", 3)
	holder := startWait(t, w, "run-1", 3, 1)
	newer := make(chan bool, 1)
	go func() { newer <- w.Wait(context.Background(), "run-1", 4, time.Hour) }() // 比已知值新：唤醒持有旧值的等待者
	if !receiveWithin(t, holder, "older waiter") {
		t.Fatal("older waiter not woken by a newer observation")
	}
	waitFor(t, "newer waiter alone", func() bool {
		w.mu.Lock()
		defer w.mu.Unlock()
		run := w.runs["run-1"]
		return run != nil && run.seen == 4 && run.waiters == 1
	})
	if !w.Wait(context.Background(), "run-1", 3, time.Hour) {
		t.Fatal("a stale observation must return immediately")
	}
	w.Notify("run-1")
	if !receiveWithin(t, newer, "notified waiter") {
		t.Fatal("Notify did not wake the waiter")
	}
	if states.readCount() != 0 {
		t.Fatalf("wakes above must not read the store, reads=%d", states.readCount())
	}

	deleted := startWait(t, w, "run-gone", 9, 1)
	clk.Advance(runWatchPollInterval)
	if !receiveWithin(t, deleted, "deleted run waiter") {
		t.Fatal("waiter of a deleted run was not woken")
	}
}

// timeout 与 ctx 结束时返回 false 并释放等待者。
func TestRunWatchTimeoutAndCancel(t *testing.T) {
	w, states, clk := newTestRunWatch()
	states.set("run-1", 1)
	timedOut := make(chan bool, 1)
	go func() { timedOut <- w.Wait(context.Background(), "run-1", 1, time.Second) }()
	waitFor(t, "waiter registered", func() bool { return w.EntryCount() == 1 })
	if advanceUntilReturned(t, clk, time.Second, timedOut) {
		t.Fatal("a timed-out wait returned true")
	}

	ctx, cancel := context.WithCancel(context.Background())
	canceled := make(chan bool, 1)
	go func() { canceled <- w.Wait(ctx, "run-1", 1, 0) }()
	waitFor(t, "waiter registered", func() bool { return w.EntryCount() == 1 })
	cancel()
	if receiveWithin(t, canceled, "canceled wait") {
		t.Fatal("a canceled wait returned true")
	}
	waitFor(t, "waiters released", func() bool { return w.EntryCount() == 0 })
}

// twoReplicaEnv 是共用一个存储的两个引擎实例，模拟同一个数据库上的两个 Server 副本。
type twoReplicaEnv struct {
	a, b *Server
	clk  *clocktest.Fake
	run  *execution.Run
}

func newTwoReplicaEnv(t *testing.T, runID string) *twoReplicaEnv {
	t.Helper()
	mgr := memory.NewExecutionManager()
	clk := clocktest.NewFake(testClockStart)
	a := assembleServerWithClock(mgr, newTestTaskQueue(), newTestTaskQueue(), clk)
	b := assembleServerWithClock(mgr, newTestTaskQueue(), newTestTaskQueue(), clk)
	run := &execution.Run{ID: runID, WorkflowID: "wf-" + runID, WorkflowType: "wf-" + runID, Namespace: protocol.DefaultNamespace, Status: history.StatusRunning}
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(run)) }); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { a.Stop(); b.Stop() })
	return &twoReplicaEnv{a: a, b: b, clk: clk, run: run}
}

// completePendingUpdate 在 server 上以一次 Workflow Task 完成 run 上已接受、未完成的 Update。
func completePendingUpdate(t *testing.T, server *Server, runID string, output map[string]any) {
	t.Helper()
	events, err := server.GetEvents(runID)
	if err != nil {
		t.Fatal(err)
	}
	var updateID string
	var scheduledEventID int64
	for _, ev := range events {
		if ev.WorkflowTaskScheduled != nil {
			scheduledEventID = ev.ID
		}
		if ev.WorkflowUpdateAccepted != nil {
			updateID = ev.WorkflowUpdateAccepted.UpdateID
		}
	}
	if updateID == "" || scheduledEventID == 0 {
		t.Fatalf("no accepted update with a scheduled workflow task in %d events", len(events))
	}
	ctx := context.Background()
	if err := server.HandleWorkflowTaskCompleted(server.WorkflowTaskContext(ctx, runID, scheduledEventID), runID, scheduledEventID, []history.Command{{
		Type: history.CmdCompleteWorkflowUpdate, Name: "set_priority", UpdateID: updateID,
		Output: output, UpdateOutcome: updateSuccessOutcome(t, output),
	}}, ""); err != nil {
		t.Fatal(err)
	}
}

// 副本 A 上等待 Update 结果，副本 B 完成它：A 在一个读取间隔内拿到结果，不等 20s 软超时。
func TestUpdateWaiterWokenByCompletionOnAnotherReplica(t *testing.T) {
	env := newTwoReplicaEnv(t, "run-cross-replica-update")
	type updateReturn struct {
		result protocol.WorkflowUpdateResult
		err    error
	}
	returned := make(chan updateReturn, 1)
	go func() {
		result, err := env.a.UpdateWorkflow(context.Background(), "", env.run.WorkflowID, env.run.ID, "set_priority", "up-cross", protocol.UpdateWaitStageEnums.Completed, updateArgs(map[string]any{"priority": "high"}))
		returned <- updateReturn{result, err}
	}()
	waitFor(t, "replica A waiting", func() bool { return env.a.runWatch.EntryCount() == 1 })

	completePendingUpdate(t, env.b, env.run.ID, map[string]any{"by": "replica-b"})
	env.clk.Advance(runWatchPollInterval)

	got := receiveWithin(t, returned, "update result on replica A")
	if got.err != nil {
		t.Fatal(got.err)
	}
	if got.result.Stage != protocol.UpdateReachedStageEnums.Completed || updateOutput(got.result)["by"] != "replica-b" {
		t.Fatalf("result = %+v", got.result)
	}
	if elapsed := env.clk.Now().Sub(testClockStart); elapsed > runWatchPollInterval {
		t.Fatalf("result arrived after %v of server time, want <= %v", elapsed, runWatchPollInterval)
	}
	waitFor(t, "waiter released", func() bool { return env.a.runWatch.EntryCount() == 0 })
}

// 副本 A 上等待 Update 结果，副本 B 终止 Run：A 在一个读取间隔内从 History 读到 Update 的失败终态。
func TestUpdateWaiterWokenByRunCloseOnAnotherReplica(t *testing.T) {
	env := newTwoReplicaEnv(t, "run-cross-replica-update-close")
	returned := make(chan protocol.WorkflowUpdateResult, 1)
	go func() {
		result, err := env.a.UpdateWorkflow(context.Background(), "", env.run.WorkflowID, env.run.ID, "set_priority", "up-close", protocol.UpdateWaitStageEnums.Completed, updateArgs(map[string]any{"priority": "high"}))
		if err != nil {
			t.Error(err)
		}
		returned <- result
	}()
	waitFor(t, "replica A waiting", func() bool { return env.a.runWatch.EntryCount() == 1 })

	if err := env.b.TerminateWorkflow(context.Background(), "", "", env.run.ID, "stop"); err != nil {
		t.Fatal(err)
	}
	env.clk.Advance(runWatchPollInterval)

	result := receiveWithin(t, returned, "update result on replica A")
	if result.Stage != protocol.UpdateReachedStageEnums.Completed || result.Outcome == nil || result.Outcome.Failure == nil {
		t.Fatalf("result = %+v, want the failed terminal outcome recorded at close", result)
	}
}

// WaitRunClosed：Run 由另一个副本结束时一个读取间隔内返回，由本副本结束时不推进时钟即返回；
// 期限到期时返回 ctx 的错误并释放等待者。
func TestWaitRunClosedAcrossReplicas(t *testing.T) {
	env := newTwoReplicaEnv(t, "run-wait-closed")
	type closedReturn struct {
		run     *execution.Run
		closing history.Event
		err     error
	}
	wait := func(server *Server, ctx context.Context) <-chan closedReturn {
		returned := make(chan closedReturn, 1)
		go func() {
			run, closing, err := server.WaitRunClosed(ctx, "", "", env.run.ID)
			returned <- closedReturn{run, closing, err}
		}()
		return returned
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	expired := receiveWithin(t, wait(env.a, ctx), "expired wait")
	cancel()
	if expired.err == nil || ctx.Err() == nil {
		t.Fatalf("expired wait = %+v", expired)
	}

	onA := wait(env.a, context.Background())
	onB := wait(env.b, context.Background())
	waitFor(t, "both replicas waiting", func() bool { return env.a.runWatch.EntryCount() == 1 && env.b.runWatch.EntryCount() == 1 })
	if err := env.b.TerminateWorkflow(context.Background(), "", "", env.run.ID, "done"); err != nil {
		t.Fatal(err)
	}
	local := receiveWithin(t, onB, "local close")
	if local.err != nil || local.closing.WorkflowTerminated == nil || local.run.Status != history.StatusTerminated {
		t.Fatalf("local wait = %+v", local)
	}
	select {
	case got := <-onA:
		t.Fatalf("replica A returned before its poll: %+v", got)
	default:
	}
	env.clk.Advance(runWatchPollInterval)
	remote := receiveWithin(t, onA, "remote close")
	if remote.err != nil || remote.closing.WorkflowTerminated == nil || remote.closing.WorkflowTerminated.Reason != "done" || remote.run.ID != env.run.ID {
		t.Fatalf("remote wait = %+v", remote)
	}

	// 已结束的 Run 立即返回。
	again, _, err := env.a.WaitRunClosed(context.Background(), "", "", env.run.ID)
	if err != nil || again.Status != history.StatusTerminated {
		t.Fatalf("closed run = %+v, %v", again, err)
	}
}
