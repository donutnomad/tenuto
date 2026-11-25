package engine

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/program/programtest"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/clock/clocktest"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

// ackRecordingQueue 是记录 Ack 的 Workflow Task 队列；队列空时 Dequeue 等到 ctx 结束。
type ackRecordingQueue struct {
	testTaskQueue
	ackMu sync.Mutex
	acked []protocol.TaskLease
}

func (q *ackRecordingQueue) Dequeue(ctx context.Context) (protocol.TaskLease, error) {
	q.mu.Lock()
	if len(q.tasks) == 0 {
		q.mu.Unlock()
		<-ctx.Done()
		return protocol.TaskLease{}, ctx.Err()
	}
	lease := q.tasks[0]
	q.tasks = q.tasks[1:]
	q.mu.Unlock()
	return lease, nil
}

func (q *ackRecordingQueue) DequeueForWorker(ctx context.Context, _ string) (protocol.TaskLease, error) {
	return q.Dequeue(ctx)
}

func (q *ackRecordingQueue) Ack(_ context.Context, lease protocol.TaskLease) error {
	q.ackMu.Lock()
	defer q.ackMu.Unlock()
	q.acked = append(q.acked, lease)
	return nil
}

func (q *ackRecordingQueue) ackedLeases() []protocol.TaskLease {
	q.ackMu.Lock()
	defer q.ackMu.Unlock()
	return append([]protocol.TaskLease(nil), q.acked...)
}

const testWorkflowTaskTimeout = 30 * time.Second

// startPolledWorkflowTask 启动一个 Run 并以声明了 testWorkflowTaskTimeout 的 Worker 领取它的首个 WFT。
func startPolledWorkflowTask(t *testing.T, server *Server, workflowID string) (string, protocol.TaskLease) {
	t.Helper()
	if _, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{Type: workflowID, Program: programtest.Noop()}); err != nil {
		t.Fatal(err)
	}
	runID, err := startedRunID(server.StartWorkflow(context.Background(), "", workflowID, workflowID, "", nil))
	if err != nil {
		t.Fatal(err)
	}
	return runID, pollWorkflowTaskAs(t, server, "worker-a")
}

func pollWorkflowTaskAs(t *testing.T, server *Server, identity string) protocol.TaskLease {
	t.Helper()
	ctx, cancel := context.WithTimeout(protocol.WithWorkerInfo(context.Background(), protocol.WorkerInfo{
		Identity: identity, BuildID: "build-1", WorkflowTaskTimeout: testWorkflowTaskTimeout,
		WorkflowCapabilities: program.CurrentWLangCapabilities(),
	}), 5*time.Second)
	defer cancel()
	lease, err := server.PollWorkflowTask(capableWorkerContext(ctx))
	if err != nil {
		t.Fatal(err)
	}
	return lease
}

func countWorkflowTaskTimeouts(t *testing.T, mgr execution.EventStore, runID string) int {
	t.Helper()
	events, err := mgr.GetEvents(runID)
	if err != nil {
		t.Fatal(err)
	}
	return countEngineEvents(events, history.EventWorkflowTaskTimedOut)
}

// Worker 领取 WFT 后崩溃：Server 在领取时按 Worker 声明的超时计时，到期记 WorkflowTaskTimedOut、
// 结束 sticky 会话，并把新的 WFT 投进普通队列让任何 Worker 领取。
func TestServerTimesOutStartedWorkflowTaskWhenWorkerVanishes(t *testing.T) {
	mgr := memory.NewExecutionManager()
	workflowTasks := &ackRecordingQueue{}
	clk := clocktest.NewFake(testClockStart)
	server := assembleServerWithClock(mgr, workflowTasks, newTestTaskQueue(), clk)
	t.Cleanup(server.Stop)

	runID, lease := startPolledWorkflowTask(t, server, "wft-timeout-vanish")
	firstWFT := lease.Task.ScheduledEventID
	events, _ := mgr.GetEvents(runID)
	started := events[len(events)-1].WorkflowTaskStarted
	if started == nil || started.TimeoutMs != testWorkflowTaskTimeout.Milliseconds() {
		t.Fatalf("WorkflowTaskStarted must record the worker's timeout: %+v", started)
	}
	state, _ := mgr.GetWorkflowExecutionState(runID)
	if _, assigned, err := mgr.AssignStickyExecution(runID, state.ExecutionGeneration, state.StickyGeneration, "worker-a", "build-1", clk.Now().Add(time.Hour)); err != nil || !assigned {
		t.Fatalf("assign sticky: assigned=%v err=%v", assigned, err)
	}

	clk.Advance(testWorkflowTaskTimeout - time.Millisecond)
	if got := countWorkflowTaskTimeouts(t, mgr, runID); got != 0 {
		t.Fatalf("workflow task timed out before its deadline: %d", got)
	}

	clk.Advance(time.Millisecond)
	if got := countWorkflowTaskTimeouts(t, mgr, runID); got != 1 {
		t.Fatalf("WorkflowTaskTimedOut = %d, want 1", got)
	}
	state, err := mgr.GetWorkflowExecutionState(runID)
	if err != nil {
		t.Fatal(err)
	}
	if state.WorkflowTaskOpen(firstWFT) || state.OpenWorkflowTaskEventID == 0 {
		t.Fatalf("after timeout: first WFT open=%v, open WFT=%d; want a new open WFT", state.WorkflowTaskOpen(firstWFT), state.OpenWorkflowTaskEventID)
	}
	if state.StickyWorkerIdentity != "" || state.StickyExpiresAt != nil {
		t.Fatalf("sticky routing survived the timeout: %q expires=%v", state.StickyWorkerIdentity, state.StickyExpiresAt)
	}
	queued := workflowTasks.snapshot()
	if len(queued) != 1 || queued[0].Task.ScheduledEventID != state.OpenWorkflowTaskEventID ||
		queued[0].Task.WorkflowTaskRoute != protocol.WorkflowTaskRouteNormal {
		t.Fatalf("retried workflow task must go to the normal queue: %+v", queued)
	}
	if timerArmed(server, workflowTaskTimeoutRef(runID, firstWFT)) {
		t.Fatal("the timed-out workflow task keeps its timer")
	}

	// 另一个 Worker 领取重试的 WFT 并完成，Run 照常推进。
	retry := pollWorkflowTaskAs(t, server, "worker-b")
	ctx := protocol.WithWorkflowTaskToken(context.Background(), retry.Task.TaskToken)
	if err := server.HandleWorkflowTaskCompleted(ctx, runID, retry.Task.ScheduledEventID, []history.Command{{Type: history.CmdCompleteWorkflow}}, "worker-b"); err != nil {
		t.Fatal(err)
	}
	if status, _ := server.GetRunStatus(runID); status != history.StatusCompleted {
		t.Fatalf("status = %s, want completed", status)
	}
	if timerArmed(server, workflowTaskTimeoutRef(runID, retry.Task.ScheduledEventID)) {
		t.Fatal("a completed workflow task keeps its timer")
	}
}

// Worker 自己上报的超时与 Server 计时走同一条路径：先到的一方关闭 WFT，另一方不再写入，也不多调度 WFT。
func TestWorkflowTaskTimeoutReportedByWorkerAndServerClosesOnce(t *testing.T) {
	for _, workerFirst := range []bool{true, false} {
		name := "server first"
		if workerFirst {
			name = "worker first"
		}
		t.Run(name, func(t *testing.T) {
			mgr := memory.NewExecutionManager()
			clk := clocktest.NewFake(testClockStart)
			server := assembleServerWithClock(mgr, &ackRecordingQueue{}, newTestTaskQueue(), clk)
			t.Cleanup(server.Stop)
			runID, lease := startPolledWorkflowTask(t, server, "wft-timeout-race")
			report := func() {
				ctx := protocol.WithWorkflowTaskToken(context.Background(), lease.Task.TaskToken)
				_ = server.HandleWorkflowTaskTimedOut(ctx, runID, lease.Task.ScheduledEventID, "context deadline exceeded")
			}
			if workerFirst {
				report()
				clk.Advance(testWorkflowTaskTimeout)
			} else {
				clk.Advance(testWorkflowTaskTimeout)
				report()
			}
			events, err := mgr.GetEvents(runID)
			if err != nil {
				t.Fatal(err)
			}
			if timedOut, scheduled := countEngineEvents(events, history.EventWorkflowTaskTimedOut), countEngineEvents(events, history.EventWorkflowTaskScheduled); timedOut != 1 || scheduled != 2 {
				t.Fatalf("WorkflowTaskTimedOut=%d WorkflowTaskScheduled=%d, want 1/2", timedOut, scheduled)
			}
		})
	}
}

// Server 重启后按 execution state 记录的截止时间重挂 WFT 超时，到期照常超时。
func TestWorkflowTaskTimeoutSurvivesServerRestart(t *testing.T) {
	clk := clocktest.NewFake(testClockStart)
	// 两个 Server 共用同一组仓储，相当于重启后连回同一个数据库。
	deps := assembleServerDeps(memory.NewExecutionManager(), &ackRecordingQueue{}, QueueActivityRouter{Queue: newTestTaskQueue()})
	deps.Clock = clk
	mgr := deps.Executions
	crashed := NewServer(deps)
	runID, lease := startPolledWorkflowTask(t, crashed, "wft-timeout-restart")
	deadline := clk.Now().Add(testWorkflowTaskTimeout)
	crashed.Stop()

	clk.Advance(10 * time.Second)
	deps.WorkflowQueue = &ackRecordingQueue{}
	restarted := NewServer(deps)
	t.Cleanup(restarted.Stop)
	if err := restarted.RecoverRunningWorkflows(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got, armed := restarted.timers.Deadline(workflowTaskTimeoutRef(runID, lease.Task.ScheduledEventID)); !armed || !got.Equal(deadline) {
		t.Fatalf("recovered timeout: deadline=%v armed=%v, want %v", got, armed, deadline)
	}
	clk.Advance(testWorkflowTaskTimeout - 10*time.Second)
	if got := countWorkflowTaskTimeouts(t, mgr, runID); got != 1 {
		t.Fatalf("WorkflowTaskTimedOut after restart = %d, want 1", got)
	}
}

// 持有 timer 的副本下线后，另一个副本的到期扫描在宽限期之后接管 WFT 超时。
func TestWorkflowTaskTimeoutTakenOverByOverdueScan(t *testing.T) {
	const grace = 30 * time.Second
	mgr := memory.NewExecutionManager()
	clk := clocktest.NewFake(testClockStart)
	holder := assembleServerWithClock(mgr, &ackRecordingQueue{}, newTestTaskQueue(), clk)
	other := assembleServerWithClock(mgr, &ackRecordingQueue{}, newTestTaskQueue(), clk)
	t.Cleanup(other.Stop)
	runID, lease := startPolledWorkflowTask(t, holder, "wft-timeout-takeover")
	holder.Stop()

	clk.Advance(testWorkflowTaskTimeout + grace)
	result, err := other.FireOverdueTimers(context.Background(), grace)
	if err != nil {
		t.Fatal(err)
	}
	if result.WorkflowTaskTimeouts != 0 || countWorkflowTaskTimeouts(t, mgr, runID) != 0 {
		t.Fatalf("overdue scan took over within the grace period: %+v", result)
	}

	clk.Advance(time.Millisecond)
	result, err = other.FireOverdueTimers(context.Background(), grace)
	if err != nil {
		t.Fatal(err)
	}
	if result.WorkflowTaskTimeouts != 1 || countWorkflowTaskTimeouts(t, mgr, runID) != 1 {
		t.Fatalf("overdue scan result %+v, timeouts=%d; want 1/1", result, countWorkflowTaskTimeouts(t, mgr, runID))
	}
	// 第二轮扫描与迟到的 timer 都不再写入。
	if _, err := other.FireOverdueTimers(context.Background(), grace); err != nil {
		t.Fatal(err)
	}
	other.handleWorkflowTaskTimeoutFired(context.Background(), runID, lease.Task.ScheduledEventID)
	if got := countWorkflowTaskTimeouts(t, mgr, runID); got != 1 {
		t.Fatalf("WorkflowTaskTimedOut = %d after repeated takeovers, want 1", got)
	}
}

// 同一个 open WFT 的另一份投递正在执行时，被激活 fence 拒绝的重复投递由 Server 确认丢弃，不交给 Worker。
func TestDuplicateWorkflowTaskRejectedByActivationFenceIsAcked(t *testing.T) {
	mgr := memory.NewExecutionManager()
	workflowTasks := &ackRecordingQueue{}
	clk := clocktest.NewFake(testClockStart)
	server := assembleServerWithClock(mgr, workflowTasks, newTestTaskQueue(), clk)
	t.Cleanup(server.Stop)
	runID, lease := startPolledWorkflowTask(t, server, "wft-duplicate-fence")

	duplicate := lease.Task
	duplicate.ActivationID = ""
	duplicate.TaskToken = "another-delivery"
	if err := workflowTasks.Enqueue(context.Background(), duplicate); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(protocol.WithWorkerInfo(context.Background(), protocol.WorkerInfo{Identity: "worker-b", WorkflowCapabilities: program.CurrentWLangCapabilities()}), 200*time.Millisecond)
	defer cancel()
	if got, err := server.PollWorkflowTask(capableWorkerContext(ctx)); err == nil {
		t.Fatalf("the duplicate delivery was handed to a worker: %+v", got.Task)
	}
	acked := workflowTasks.ackedLeases()
	if len(acked) != 1 || acked[0].Task.RunID != runID || acked[0].Task.ScheduledEventID != lease.Task.ScheduledEventID {
		t.Fatalf("acked = %+v, want the duplicate delivery", acked)
	}
	state, _ := mgr.GetWorkflowExecutionState(runID)
	if state.CurrentActivationID != lease.Task.ActivationID {
		t.Fatalf("the duplicate replaced the running activation: %q", state.CurrentActivationID)
	}
}
