package integration

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/app/assembly"
	"github.com/donutnomad/tenuto/server/biz/service/engine"
	"github.com/donutnomad/tenuto/server/infra/persistence"
	"github.com/donutnomad/tenuto/server/infra/taskqueue/taskq"
)

// ============================================
// 真实 TaskQ + MySQL：投递丢失与陈旧投递（2026-11-12 tenuto-cluster failover）
// ============================================
// - Error Case: sticky 会话在 TaskQ 已把投递交给目标 Worker（routing 期限内，RoutingFallback=false）之后被清除，
//   这份投递仍按普通投递开始 open WFT。
// - Error Case: open WFT 在 TaskQ 里没有投递，派发记录是 dispatched：reconciler 补投。
// - Error Case: 已关闭 WFT 的陈旧投递由 Server 确认一次后从 taskq_tasks 删除，不再重投。
// ============================================

type lostDeliveryFixture struct {
	h      *taskQHarness
	store  *persistence.Store
	queue  *taskq.Queue
	server *engine.Server
	runID  string
	wftID  int64
}

// newLostDeliveryFixture 启动一个 Run，取走并确认它第一个 WFT 在 TaskQ 里的投递：WFT 仍 open，派发记录是
// dispatched，队列里没有它的投递，与故障注入里丢失投递之后的状态相同。
func newLostDeliveryFixture(t *testing.T, workflowID string) *lostDeliveryFixture {
	t.Helper()
	ctx := context.Background()
	h := newTaskQHarness(t, map[string]int{"workflow-tasks": 1, "activity-tasks": 1})
	t.Cleanup(h.close)
	store := newMySQLWorkflowStore(t)
	queue := taskq.NewQueue(h.frontend(t, "workflow-tasks"))
	server := assembly.NewServer(assembly.Options{Stores: assembly.Persistent(store), WorkflowQueue: queue, ActivityRouter: h.activityRouter(t, nil)})
	t.Cleanup(server.Stop)
	registerStickyWorkflow(t, server, workflowID)
	runID, err := startedRunID(server.StartWorkflow(ctx, "", workflowID, workflowID, "", nil))
	if err != nil {
		t.Fatal(err)
	}
	pollCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	lease, err := queue.Dequeue(pollCtx)
	if err != nil {
		t.Fatal(err)
	}
	if err := queue.Ack(ctx, lease); err != nil {
		t.Fatal(err)
	}
	state, err := store.Executions.GetWorkflowExecutionState(runID)
	if err != nil {
		t.Fatal(err)
	}
	if state.OpenWorkflowTaskEventID != lease.Task.ScheduledEventID {
		t.Fatalf("open workflow task = %d, consumed delivery = %+v", state.OpenWorkflowTaskEventID, lease.Task)
	}
	return &lostDeliveryFixture{h: h, store: store, queue: queue, server: server, runID: runID, wftID: lease.Task.ScheduledEventID}
}

func (f *lostDeliveryFixture) poll(t *testing.T, identity string, timeout time.Duration) (protocol.TaskLease, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(protocol.WithWorkerInfo(context.Background(), protocol.WorkerInfo{
		Identity: identity, BuildID: "workflow-v1", WorkflowCapabilities: program.CurrentWLangCapabilities(),
	}), timeout)
	defer cancel()
	lease, err := f.server.PollWorkflowTask(ctx)
	if errors.Is(err, protocol.ErrTaskPollTimeout) {
		// 没有投递：Server 把长轮询到期报告为 ErrTaskPollTimeout。
		err = context.DeadlineExceeded
	}
	return lease, err
}

func (f *lostDeliveryFixture) workflowTaskStarted(t *testing.T) int {
	t.Helper()
	events, err := f.server.GetEvents(f.runID)
	if err != nil {
		t.Fatal(err)
	}
	return countEvent(events, history.EventWorkflowTaskStarted)
}

func TestRealTaskQStickyDeliveryStartsOpenWorkflowTaskAfterStickyClear(t *testing.T) {
	f := newLostDeliveryFixture(t, "taskq-sticky-cleared-in-flight")
	state, err := f.store.Executions.GetWorkflowExecutionState(f.runID)
	if err != nil {
		t.Fatal(err)
	}
	expires := time.Now().Add(time.Hour)
	generation, assigned, err := f.store.Executions.AssignStickyExecution(f.runID, state.ExecutionGeneration, state.StickyGeneration, "owner-worker", "workflow-v1", expires)
	if err != nil || !assigned {
		t.Fatalf("assign sticky: assigned=%v err=%v", assigned, err)
	}
	if err := f.queue.EnqueueSticky(context.Background(), "owner-worker", protocol.Task{
		Namespace: protocol.DefaultNamespace, Type: protocol.TaskTypeWorkflow, RunID: f.runID, ScheduledEventID: f.wftID,
		ExecutionGeneration: state.ExecutionGeneration, StickyGeneration: generation, StickyScheduleToStartDeadline: expires,
	}); err != nil {
		t.Fatal(err)
	}
	if _, cleared, err := f.store.Executions.ClearStickyExecution(f.runID, state.ExecutionGeneration, generation); err != nil || !cleared {
		t.Fatalf("clear sticky: cleared=%v err=%v", cleared, err)
	}

	lease, err := f.poll(t, "owner-worker", 5*time.Second)
	if err != nil {
		t.Fatalf("the only delivery of the open workflow task was dropped: %v", err)
	}
	if lease.Task.ScheduledEventID != f.wftID || lease.Task.WorkflowTaskRoute != protocol.WorkflowTaskRouteNormal {
		t.Fatalf("lease = %+v", lease.Task)
	}
	if started := f.workflowTaskStarted(t); started != 1 {
		t.Fatalf("WorkflowTaskStarted events = %d, want 1", started)
	}
}

func TestRealTaskQReconcileRedeliversLostWorkflowTask(t *testing.T) {
	f := newLostDeliveryFixture(t, "taskq-lost-delivery")
	if _, err := f.poll(t, "worker-a", 500*time.Millisecond); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("poll before reconcile: err = %v, want no delivery", err)
	}
	_, overdue, err := f.server.Reconcile(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if overdue.WorkflowTasks != 1 {
		t.Fatalf("overdue = %+v, want one workflow task redelivered", overdue)
	}
	lease, err := f.poll(t, "worker-a", 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if lease.Task.ScheduledEventID != f.wftID {
		t.Fatalf("lease = %+v", lease.Task)
	}
	if started := f.workflowTaskStarted(t); started != 1 {
		t.Fatalf("WorkflowTaskStarted events = %d, want 1", started)
	}
}

func TestRealTaskQStaleDeliveryIsAcknowledgedOnce(t *testing.T) {
	f := newLostDeliveryFixture(t, "taskq-stale-delivery")
	// WFT 1000 从未调度（等同早已关闭）：一份 sticky 路由已过期的陈旧投递，与故障注入里堆积的那些相同。
	if err := f.queue.EnqueueSticky(context.Background(), "gone-worker", protocol.Task{
		Namespace: protocol.DefaultNamespace, Type: protocol.TaskTypeWorkflow, RunID: f.runID, ScheduledEventID: 1000,
		StickyGeneration: 1, StickyScheduleToStartDeadline: time.Now().Add(-time.Second),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.poll(t, "worker-a", time.Second); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("poll returned the stale delivery: err = %v", err)
	}
	waitForPersistedTaskCount(t, f.h, "workflow-tasks", 0, 10*time.Second)
	if started := f.workflowTaskStarted(t); started != 0 {
		t.Fatalf("WorkflowTaskStarted events = %d, want 0", started)
	}
}
