package engine

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

// 准备 Workflow Task 时的确定性失败（Run 采用的 Program 版本读不到）只处理一次：Run 以 ConfigError 暂停，这份投递被确认，
// 队列里没有可以重投的任务。
func TestWorkflowTaskPrepareDeterministicFailurePausesRunOnce(t *testing.T) {
	mgr := memory.NewExecutionManager()
	queue := &redeliveringTaskQueue{}
	server := assembleServer(mgr, queue, newTestTaskQueue())
	defer server.Stop()
	run := &execution.Run{ID: "run-missing-program", WorkflowID: "wf-missing-program", WorkflowType: "wf-missing-program",
		Status: history.StatusRunning, WorkflowConfigVersion: "missing-version"}
	createStartedRunForTest(t, mgr, run)
	queue.enqueueWorkflowTask(run.ID, appendOpenWorkflowTaskForTest(t, mgr, run.ID))

	if _, err := server.PollWorkflowTask(capableWorkerContext(context.Background())); !errors.Is(err, protocol.ErrTaskPollTimeout) {
		t.Fatalf("first poll: err = %v, want the queue to be drained", err)
	}
	// 租约到期后的重投：确认过的投递不会回到队列。
	queue.expireLeases()
	if _, err := server.PollWorkflowTask(capableWorkerContext(context.Background())); !errors.Is(err, protocol.ErrTaskPollTimeout) {
		t.Fatalf("second poll: err = %v, want no redelivery", err)
	}

	if dequeues, acks, nacks := queue.counts(); dequeues != 1 || acks != 1 || nacks != 0 {
		t.Fatalf("dequeues/acks/nacks = %d/%d/%d, want 1/1/0", dequeues, acks, nacks)
	}
	paused, err := mgr.Get(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(paused.ConfigError, "workflow task cannot be prepared") || !strings.Contains(paused.ConfigError, "workflow config not found") {
		t.Fatalf("ConfigError = %q, want the prepare failure", paused.ConfigError)
	}
	if status, _ := server.GetRunStatus(run.ID); status != history.StatusRunning {
		t.Fatalf("run status = %s, want running (paused)", status)
	}
}

// 准备 Workflow Task 时的暂时性失败（存储读取出错）不暂停 Run、不确认投递：租约到期后任务重投，下一次准备成功。
func TestWorkflowTaskPrepareTransientFailureIsRetried(t *testing.T) {
	mgr := &flakyExecutionStateManager{ExecutionManager: memory.NewExecutionManager(), failures: 1}
	queue := &redeliveringTaskQueue{}
	server := assembleServer(mgr, queue, newTestTaskQueue())
	defer server.Stop()
	run := &execution.Run{ID: "run-flaky-storage", WorkflowID: "wf-flaky-storage", WorkflowType: "wf-flaky-storage", Status: history.StatusRunning}
	registerRunWorkflowForTest(t, server, run)
	createStartedRunForTest(t, mgr, run)
	queue.enqueueWorkflowTask(run.ID, appendOpenWorkflowTaskForTest(t, mgr, run.ID))

	if _, err := server.PollWorkflowTask(capableWorkerContext(context.Background())); !errors.Is(err, errStorageUnavailable) {
		t.Fatalf("first poll: err = %v, want the storage error", err)
	}
	if dequeues, acks, nacks := queue.counts(); dequeues != 1 || acks != 0 || nacks != 0 {
		t.Fatalf("after the transient failure dequeues/acks/nacks = %d/%d/%d, want 1/0/0", dequeues, acks, nacks)
	}
	queue.expireLeases()
	lease, err := server.PollWorkflowTask(capableWorkerContext(context.Background()))
	if err != nil {
		t.Fatalf("redelivered poll: %v", err)
	}
	if lease.Task.RunID != run.ID {
		t.Fatalf("redelivered lease run = %q, want %q", lease.Task.RunID, run.ID)
	}
	if dequeues, _, _ := queue.counts(); dequeues != 2 {
		t.Fatalf("dequeues = %d, want 2", dequeues)
	}
	current, err := mgr.Get(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current.ConfigError != "" {
		t.Fatalf("ConfigError = %q, want none after a transient failure", current.ConfigError)
	}
}

var errStorageUnavailable = errors.New("storage unavailable")

// flakyExecutionStateManager 让最先 failures 次 execution state 读取失败。
type flakyExecutionStateManager struct {
	execution.ExecutionManager
	failures int32
}

func (m *flakyExecutionStateManager) GetWorkflowExecutionState(runID string) (*execution.WorkflowExecutionState, error) {
	if atomic.AddInt32(&m.failures, -1) >= 0 {
		return nil, errStorageUnavailable
	}
	return m.ExecutionManager.GetWorkflowExecutionState(runID)
}

// redeliveringTaskQueue 按真实队列的租约语义工作：Nack 立即重投，没有确认的租约在 expireLeases（租约到期）时重投，
// 确认过的投递不再出现；队列为空时立即返回 ErrTaskPollTimeout。
type redeliveringTaskQueue struct {
	mu                    sync.Mutex
	ready                 []protocol.Task
	leased                map[string]protocol.Task
	next                  int
	dequeues, acks, nacks int
}

func (q *redeliveringTaskQueue) enqueueWorkflowTask(runID string, scheduledEventID int64) {
	_ = q.Enqueue(context.Background(), protocol.Task{Type: protocol.TaskTypeWorkflow, Namespace: protocol.DefaultNamespace, RunID: runID, ScheduledEventID: scheduledEventID})
}

func (q *redeliveringTaskQueue) Enqueue(_ context.Context, task protocol.Task) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.ready = append(q.ready, task)
	return nil
}

func (q *redeliveringTaskQueue) EnqueueSticky(ctx context.Context, _ string, task protocol.Task) error {
	return q.Enqueue(ctx, task)
}

func (q *redeliveringTaskQueue) Dequeue(context.Context) (protocol.TaskLease, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.ready) == 0 {
		return protocol.TaskLease{}, protocol.ErrTaskPollTimeout
	}
	task := q.ready[0]
	q.ready = q.ready[1:]
	q.next++
	token := strconv.Itoa(q.next)
	if q.leased == nil {
		q.leased = make(map[string]protocol.Task)
	}
	q.leased[token] = task
	q.dequeues++
	return protocol.TaskLease{Task: task, Token: token}, nil
}

func (q *redeliveringTaskQueue) DequeueForWorker(ctx context.Context, _ string) (protocol.TaskLease, error) {
	return q.Dequeue(ctx)
}

func (q *redeliveringTaskQueue) Ack(_ context.Context, lease protocol.TaskLease) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	delete(q.leased, lease.Token)
	q.acks++
	return nil
}

func (q *redeliveringTaskQueue) Nack(_ context.Context, lease protocol.TaskLease) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if task, ok := q.leased[lease.Token]; ok {
		delete(q.leased, lease.Token)
		q.ready = append(q.ready, task)
	}
	q.nacks++
	return nil
}

func (*redeliveringTaskQueue) HandlesStickyRoutingDeadline() bool { return true }
func (*redeliveringTaskQueue) Durable() bool                      { return true }

// expireLeases 让全部未确认的租约到期，任务回到队列。
func (q *redeliveringTaskQueue) expireLeases() {
	q.mu.Lock()
	defer q.mu.Unlock()
	for token, task := range q.leased {
		delete(q.leased, token)
		q.ready = append(q.ready, task)
	}
}

func (q *redeliveringTaskQueue) counts() (dequeues, acks, nacks int) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.dequeues, q.acks, q.nacks
}
