package engine

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/program/programtest"
	"github.com/donutnomad/tenuto/protocol"
	historypkg "github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/dispatch"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

// ============================================
// 投递丢失：派发记录已是 dispatched，队列里却没有可领取的投递
// ============================================
// 测试策略（2026-11-12 tenuto-cluster failover 里 WFT 28 永久停住）:
// - Error Case: sticky 会话在投递交到目标 Worker 之后、领取之前被清除（TaskQ 负责 sticky 期限时引擎只清 sticky），
//   这份投递仍是 open WFT 唯一的投递，按普通投递开始执行，不被丢弃。
// - Error Case: 队列丢失了 open WFT 的投递，reconciler 在等待时长后补投。
// - Error Case: 队列丢失了 Scheduled Activity attempt 的投递，reconciler 在等待时长后补投。
// - Error Case: 准备 lease 之后、领取事务之前 sticky 会话被清除：这份投递放回队列按当前代数重新准备，
//   不等可见性超时。
// - Edge Case: 已 started 的 WFT 不补投（由 start-to-close 超时接管）；等待时长按派发次数翻倍。
// ============================================

func newLostDeliveryServer(t *testing.T) (*Server, *memory.ExecutionManager, *memory.TaskQueue, *memory.TaskQueue) {
	t.Helper()
	mgr := memory.NewExecutionManager()
	workflowTasks := memory.NewTaskQueue(16)
	activityTasks := memory.NewTaskQueue(16)
	srv := assembleServer(mgr, workflowTasks, activityTasks)
	t.Cleanup(srv.Stop)
	return srv, mgr, workflowTasks, activityTasks
}

func startLostDeliveryRun(t *testing.T, srv *Server, id string) string {
	t.Helper()
	return startReconcileTestRun(t, srv, &program.WorkflowConfig{Type: id, Program: programtest.Noop()})
}

// takeQueued 取出队列里现有的一份任务；队列为空时 ok 为 false。
func takeQueued(queue *memory.TaskQueue) (protocol.TaskLease, bool) {
	// 已取消的 ctx 不领任务；短超时让空队列很快返回。
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	lease, err := queue.Dequeue(ctx)
	return lease, err == nil
}

func stickyWorkerContext(identity string) context.Context {
	return protocol.WithWorkerInfo(context.Background(), protocol.WorkerInfo{
		Identity: identity, BuildID: "build-1", WorkflowCapabilities: program.CurrentWLangCapabilities(),
	})
}

// 故障注入里的丢失路径：WFT 投给 sticky Worker，TaskQ 在 sticky 期限前把它交给目标 Worker；Server 准备这份
// lease 之前，sticky 期限扫描（队列负责回落时只清 sticky、不重投）清除了会话。修复前准备时按代数不匹配 Ack
// 丢弃，WFT 没有任何投递，派发记录停在 dispatched，Run 永久停住。
func TestStickyDeliveryOfOpenWorkflowTaskSurvivesStickyClear(t *testing.T) {
	srv, mgr, workflowTasks, _ := newLostDeliveryServer(t)
	runID := startLostDeliveryRun(t, srv, "wf-sticky-cleared-in-flight")
	if _, ok := takeQueued(workflowTasks); !ok {
		t.Fatal("start did not dispatch the first workflow task")
	}
	state, err := mgr.GetWorkflowExecutionState(runID)
	if err != nil {
		t.Fatal(err)
	}
	stickyGeneration, assigned, err := mgr.AssignStickyExecution(runID, state.ExecutionGeneration, state.StickyGeneration, "worker-a", "build-1", time.Now().Add(time.Hour))
	if err != nil || !assigned {
		t.Fatalf("assign sticky: assigned=%v err=%v", assigned, err)
	}
	if err := workflowTasks.EnqueueSticky(context.Background(), "worker-a", protocol.Task{
		Namespace: protocol.DefaultNamespace, Type: protocol.TaskTypeWorkflow, RunID: runID,
		ScheduledEventID: state.OpenWorkflowTaskEventID, ExecutionGeneration: state.ExecutionGeneration, StickyGeneration: stickyGeneration,
	}); err != nil {
		t.Fatal(err)
	}
	if _, cleared, err := mgr.ClearStickyExecution(runID, state.ExecutionGeneration, stickyGeneration); err != nil || !cleared {
		t.Fatalf("clear sticky: cleared=%v err=%v", cleared, err)
	}

	ctx, cancel := context.WithTimeout(stickyWorkerContext("worker-a"), 2*time.Second)
	defer cancel()
	lease, err := srv.PollWorkflowTask(ctx)
	if err != nil {
		t.Fatalf("the only delivery of the open workflow task was dropped: %v", err)
	}
	if lease.Task.ScheduledEventID != state.OpenWorkflowTaskEventID || lease.Task.WorkflowTaskRoute != protocol.WorkflowTaskRouteNormal {
		t.Fatalf("lease = %+v, want the open workflow task as a normal delivery", lease.Task)
	}
	events, err := mgr.GetEvents(runID)
	if err != nil {
		t.Fatal(err)
	}
	if countEngineEvents(events, historypkg.EventWorkflowTaskStarted) != 1 {
		t.Fatalf("expected the delivery to start the workflow task, history %+v", events)
	}
}

// 队列丢失 open WFT 的投递（任何原因）：派发记录挡住 recoverWorkflowTasks 的补投，修复前 Run 永久停住。
func TestReconcileRedeliversLostWorkflowTask(t *testing.T) {
	ctx := context.Background()
	srv, mgr, workflowTasks, _ := newLostDeliveryServer(t)
	runID := startLostDeliveryRun(t, srv, "wf-lost-delivery")
	if _, ok := takeQueued(workflowTasks); !ok {
		t.Fatal("start did not dispatch the first workflow task")
	}
	wftID := openWorkflowTaskForTest(t, mgr, runID)

	// 等待时长未到：投递可能只是还在队列里等 Worker，不补投。
	if dispatched, overdue, err := srv.Reconcile(ctx, time.Hour, 0); err != nil || dispatched.WorkflowTasks != 0 || overdue.WorkflowTasks != 0 {
		t.Fatalf("reconcile before the wait: dispatched=%+v overdue=%+v err=%v", dispatched, overdue, err)
	}
	if _, ok := takeQueued(workflowTasks); ok {
		t.Fatal("redelivered before the wait elapsed")
	}

	_, overdue, err := srv.Reconcile(ctx, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if overdue.WorkflowTasks != 1 {
		t.Fatalf("overdue = %+v, want one workflow task redelivered", overdue)
	}
	lease, ok := takeQueued(workflowTasks)
	if !ok || lease.Task.RunID != runID || lease.Task.ScheduledEventID != wftID {
		t.Fatalf("redelivered lease = %+v ok=%v", lease, ok)
	}
	record, _, err := srv.dispatches.GetTaskDispatch(lease.Task)
	if err != nil || record.Status != dispatch.StatusDispatched || record.Attempts != 2 {
		t.Fatalf("dispatch record after redelivery = %+v err=%v", record, err)
	}

	// 补投的这份开始执行后，WFT 已 started：不再补投，由 start-to-close 超时接管。
	if err := workflowTasks.Enqueue(ctx, lease.Task); err != nil {
		t.Fatal(err)
	}
	if _, err := srv.PollWorkflowTask(capableWorkerContext(ctx)); err != nil {
		t.Fatal(err)
	}
	if _, overdue, err := srv.Reconcile(ctx, 0, 0); err != nil || overdue.WorkflowTasks != 0 {
		t.Fatalf("reconcile after start: overdue=%+v err=%v", overdue, err)
	}
	if _, ok := takeQueued(workflowTasks); ok {
		t.Fatal("a started workflow task was redelivered")
	}
}

// Scheduled Activity attempt 的投递在队列里丢失：派发记录挡住 recoverActivityRetries 的补投，修复前只能等超时。
func TestReconcileRedeliversLostActivityTask(t *testing.T) {
	ctx := context.Background()
	srv, mgr, _, activityTasks := newLostDeliveryServer(t)
	runID := startLostDeliveryRun(t, srv, "wf-lost-activity")
	seedScheduledActivityState(t, mgr, runID, 50, "#a1", nil)
	activity, found, err := mgr.GetActivityState(runID, 50)
	if err != nil || !found {
		t.Fatalf("activity state: found=%v err=%v", found, err)
	}
	task := srv.activityTaskFromState(activity)
	if dispatched, err := srv.dispatchActivityTask(ctx, task); err != nil || !dispatched {
		t.Fatalf("dispatch activity: dispatched=%v err=%v", dispatched, err)
	}
	if _, ok := takeQueued(activityTasks); !ok {
		t.Fatal("activity was not enqueued")
	}

	_, overdue, err := srv.Reconcile(ctx, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if overdue.ActivityTasks != 1 {
		t.Fatalf("overdue = %+v, want one activity task redelivered", overdue)
	}
	lease, ok := takeQueued(activityTasks)
	if !ok || lease.Task.RunID != runID || lease.Task.ScheduledEventID != 50 || lease.Task.Attempt != 1 {
		t.Fatalf("redelivered activity lease = %+v ok=%v", lease, ok)
	}
}

// clearStickyAfterReadManager 在 armed 之后第一次读 execution state 时，把读到的 sticky 会话清除：
// lease 按旧代数准备，领取事务看到新代数。
type clearStickyAfterReadManager struct {
	*memory.ExecutionManager
	armed atomic.Bool
	once  sync.Once
}

func (m *clearStickyAfterReadManager) GetWorkflowExecutionState(runID string) (*execution.WorkflowExecutionState, error) {
	state, err := m.ExecutionManager.GetWorkflowExecutionState(runID)
	if err == nil && m.armed.Load() {
		m.once.Do(func() {
			_, _, _ = m.ExecutionManager.ClearStickyExecution(runID, state.ExecutionGeneration, state.StickyGeneration)
		})
	}
	return state, err
}

// 修复前领取事务的激活 fence 拒绝这份投递后返回错误，lease 既不确认也不放回，要等队列的可见性超时
// （TaskQ 默认 60s）才再出现；故障注入里 replica-1 记了 75 次 `workflow activation fence rejected task`。
func TestStickyClearedBetweenPrepareAndStartRequeuesDelivery(t *testing.T) {
	mgr := &clearStickyAfterReadManager{ExecutionManager: memory.NewExecutionManager()}
	workflowTasks := memory.NewTaskQueue(16)
	srv := assembleServer(mgr, workflowTasks, memory.NewTaskQueue(16))
	t.Cleanup(srv.Stop)
	runID := startLostDeliveryRun(t, srv, "wf-sticky-cleared-before-start")
	if _, ok := takeQueued(workflowTasks); !ok {
		t.Fatal("start did not dispatch the first workflow task")
	}
	state, err := mgr.GetWorkflowExecutionState(runID)
	if err != nil {
		t.Fatal(err)
	}
	stickyGeneration, assigned, err := mgr.AssignStickyExecution(runID, state.ExecutionGeneration, state.StickyGeneration, "worker-a", "build-1", time.Now().Add(time.Hour))
	if err != nil || !assigned {
		t.Fatalf("assign sticky: assigned=%v err=%v", assigned, err)
	}
	if err := workflowTasks.EnqueueSticky(context.Background(), "worker-a", protocol.Task{
		Namespace: protocol.DefaultNamespace, Type: protocol.TaskTypeWorkflow, RunID: runID,
		ScheduledEventID: state.OpenWorkflowTaskEventID, ExecutionGeneration: state.ExecutionGeneration, StickyGeneration: stickyGeneration,
	}); err != nil {
		t.Fatal(err)
	}
	mgr.armed.Store(true)

	ctx, cancel := context.WithTimeout(stickyWorkerContext("worker-a"), 2*time.Second)
	defer cancel()
	lease, err := srv.PollWorkflowTask(ctx)
	if err != nil {
		t.Fatalf("poll: %v", err)
	}
	if lease.Task.ScheduledEventID != state.OpenWorkflowTaskEventID || lease.Task.StickyGeneration != stickyGeneration+1 {
		t.Fatalf("lease = %+v, want the open workflow task re-prepared at sticky generation %d", lease.Task, stickyGeneration+1)
	}
	events, err := mgr.GetEvents(runID)
	if err != nil {
		t.Fatal(err)
	}
	if countEngineEvents(events, historypkg.EventWorkflowTaskStarted) != 1 {
		t.Fatalf("expected one WorkflowTaskStarted, history %+v", events)
	}
}

// cancelOnDequeueQueue 在交出 lease 的同时让 poll 的 ctx 到期，Ack/Nack 与 TaskQ frontend 一样拒绝已结束的 ctx。
type cancelOnDequeueQueue struct {
	*memory.TaskQueue
	cancel context.CancelFunc
}

func (q *cancelOnDequeueQueue) DequeueForWorker(ctx context.Context, workerIdentity string) (protocol.TaskLease, error) {
	lease, err := q.TaskQueue.DequeueForWorker(ctx, workerIdentity)
	if err == nil {
		q.cancel()
	}
	return lease, err
}

func (q *cancelOnDequeueQueue) Ack(ctx context.Context, lease protocol.TaskLease) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return q.TaskQueue.Ack(ctx, lease)
}

func (q *cancelOnDequeueQueue) Nack(ctx context.Context, lease protocol.TaskLease) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return q.TaskQueue.Nack(ctx, lease)
}

// poll 的期限在 lease 交出时到期：需要放回队列的投递仍立即放回。修复前 Nack 带着已结束的 ctx 失败，这份投递
// 既不确认也不放回，要等可见性超时（TaskQ 默认 60s），无故障压测里由 reconciler 的兜底约 56 秒后补投。
func TestPollDeadlineAfterDequeueStillRequeuesDelivery(t *testing.T) {
	mgr := &clearStickyAfterReadManager{ExecutionManager: memory.NewExecutionManager()}
	ctx, cancel := context.WithCancel(stickyWorkerContext("worker-a"))
	defer cancel()
	workflowTasks := &cancelOnDequeueQueue{TaskQueue: memory.NewTaskQueue(16), cancel: cancel}
	srv := assembleServer(mgr, workflowTasks, memory.NewTaskQueue(16))
	t.Cleanup(srv.Stop)
	runID := startLostDeliveryRun(t, srv, "wf-poll-deadline-after-dequeue")
	if _, ok := takeQueued(workflowTasks.TaskQueue); !ok {
		t.Fatal("start did not dispatch the first workflow task")
	}
	state, err := mgr.GetWorkflowExecutionState(runID)
	if err != nil {
		t.Fatal(err)
	}
	stickyGeneration, assigned, err := mgr.AssignStickyExecution(runID, state.ExecutionGeneration, state.StickyGeneration, "worker-a", "build-1", time.Now().Add(time.Hour))
	if err != nil || !assigned {
		t.Fatalf("assign sticky: assigned=%v err=%v", assigned, err)
	}
	if err := workflowTasks.EnqueueSticky(context.Background(), "worker-a", protocol.Task{
		Namespace: protocol.DefaultNamespace, Type: protocol.TaskTypeWorkflow, RunID: runID,
		ScheduledEventID: state.OpenWorkflowTaskEventID, ExecutionGeneration: state.ExecutionGeneration, StickyGeneration: stickyGeneration,
	}); err != nil {
		t.Fatal(err)
	}
	mgr.armed.Store(true)

	if _, err := srv.PollWorkflowTask(ctx); err == nil {
		t.Fatal("poll returned a lease after its context ended")
	}
	takeCtx, takeCancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer takeCancel()
	lease, err := workflowTasks.TaskQueue.DequeueForWorker(takeCtx, "worker-a")
	if err != nil || lease.Task.RunID != runID || lease.Task.ScheduledEventID != state.OpenWorkflowTaskEventID {
		t.Fatalf("requeued delivery = %+v (err %v), want the open workflow task back in the queue", lease.Task, err)
	}
}
