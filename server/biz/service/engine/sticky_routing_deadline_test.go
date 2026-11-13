package engine

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	dispatchpkg "github.com/donutnomad/tenuto/server/biz/domain/dispatch"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

func TestAtomicStickyRoutingDeadlineClearsAssignmentWithoutRedispatch(t *testing.T) {
	run := &execution.Run{ID: "run-deadline", WorkflowID: "workflow-deadline", WorkflowType: "workflow-deadline", Status: history.StatusRunning}
	expiredSticky := time.Now().Add(-time.Second)
	mgr := newStickyDeadlineTestManager(run, &execution.WorkflowExecutionState{
		RunID:                   run.ID,
		Status:                  history.StatusRunning,
		ExecutionGeneration:     1,
		StickyGeneration:        3,
		StickyWorkerIdentity:    "owner-worker",
		StickyWorkerBuildID:     "workflow-v1",
		StickyExpiresAt:         &expiredSticky,
		OpenWorkflowTaskEventID: 42,
	})
	queue := &atomicDeadlineTestQueue{}
	server := assembleServer(mgr, queue, newTestTaskQueue())
	task := protocol.Task{
		Namespace:        protocol.DefaultNamespace,
		Type:             protocol.TaskTypeWorkflow,
		RunID:            run.ID,
		ScheduledEventID: 42,
	}
	dispatchLease, acquired, err := server.dispatches.AcquireTaskDispatch(task, time.Minute)
	if err != nil || !acquired {
		t.Fatalf("acquire dispatch: acquired=%v err=%v", acquired, err)
	}
	if err := server.dispatches.MarkTaskDispatchDispatched(task, dispatchLease.LeaseID); err != nil {
		t.Fatal(err)
	}

	if err := server.FallbackExpiredStickyWorkflowTasks(context.Background()); err != nil {
		t.Fatal(err)
	}
	state, err := mgr.GetWorkflowExecutionState(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if state.StickyWorkerIdentity != "" || state.StickyGeneration != 4 {
		t.Fatalf("sticky state = %+v", state)
	}
	if queue.normalEnqueues != 0 || queue.stickyEnqueues != 0 {
		t.Fatalf("queue enqueues: normal=%d sticky=%d", queue.normalEnqueues, queue.stickyEnqueues)
	}
	dispatch, ok, err := server.dispatches.(*memory.DispatchRepository).GetTaskDispatch(task)
	if err != nil || !ok {
		t.Fatalf("get dispatch: found=%v err=%v", ok, err)
	}
	if dispatch.Status != dispatchpkg.StatusDispatched || dispatch.Attempts != 1 {
		t.Fatalf("dispatch after fallback = %+v", dispatch)
	}
}

type atomicDeadlineTestQueue struct {
	normalEnqueues int
	stickyEnqueues int
}

func (*atomicDeadlineTestQueue) HandlesStickyRoutingDeadline() bool { return true }
func (*atomicDeadlineTestQueue) Durable() bool                      { return false }

func (q *atomicDeadlineTestQueue) Enqueue(context.Context, protocol.Task) error {
	q.normalEnqueues++
	return nil
}

func (*atomicDeadlineTestQueue) Dequeue(context.Context) (protocol.TaskLease, error) {
	return protocol.TaskLease{}, context.Canceled
}

func (*atomicDeadlineTestQueue) Ack(context.Context, protocol.TaskLease) error  { return nil }
func (*atomicDeadlineTestQueue) Nack(context.Context, protocol.TaskLease) error { return nil }

func (q *atomicDeadlineTestQueue) EnqueueSticky(context.Context, string, protocol.Task) error {
	q.stickyEnqueues++
	return nil
}

func (*atomicDeadlineTestQueue) DequeueForWorker(context.Context, string) (protocol.TaskLease, error) {
	return protocol.TaskLease{}, context.Canceled
}

type stickyDeadlineTestManager struct {
	execution.ExecutionManager
	stateMu sync.Mutex
	state   *execution.WorkflowExecutionState
}

func newStickyDeadlineTestManager(run *execution.Run, state *execution.WorkflowExecutionState) *stickyDeadlineTestManager {
	mgr := &stickyDeadlineTestManager{ExecutionManager: memory.NewExecutionManager(), state: execution.CloneWorkflowExecutionState(state)}
	_ = mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(run)) })
	return mgr
}

func (m *stickyDeadlineTestManager) GetWorkflowExecutionState(string) (*execution.WorkflowExecutionState, error) {
	m.stateMu.Lock()
	defer m.stateMu.Unlock()
	return execution.CloneWorkflowExecutionState(m.state), nil
}

func (m *stickyDeadlineTestManager) ListWorkflowExecutionStates([]string) (map[string]*execution.WorkflowExecutionState, error) {
	m.stateMu.Lock()
	defer m.stateMu.Unlock()
	return map[string]*execution.WorkflowExecutionState{m.state.RunID: execution.CloneWorkflowExecutionState(m.state)}, nil
}

// ListExpiredStickyExecutions 与 gormstore 的 WHERE 语义一致：只返回 sticky 已过期且有 open workflow task 的行。
func (m *stickyDeadlineTestManager) ListExpiredStickyExecutions(now time.Time, _ int) ([]*execution.WorkflowExecutionState, error) {
	m.stateMu.Lock()
	defer m.stateMu.Unlock()
	if m.state.StickyWorkerIdentity == "" || m.state.StickyValidAt(now) || m.state.OpenWorkflowTaskEventID == 0 {
		return nil, nil
	}
	return []*execution.WorkflowExecutionState{execution.CloneWorkflowExecutionState(m.state)}, nil
}

func (m *stickyDeadlineTestManager) AssignStickyExecution(_ string, executionGeneration, expectedStickyGeneration int64, workerIdentity, workerBuildID string, expiresAt time.Time) (int64, bool, error) {
	m.stateMu.Lock()
	defer m.stateMu.Unlock()
	if m.state.ExecutionGeneration != executionGeneration || m.state.StickyGeneration != expectedStickyGeneration {
		return m.state.StickyGeneration, false, nil
	}
	m.state.StickyWorkerIdentity = workerIdentity
	m.state.StickyWorkerBuildID = workerBuildID
	m.state.StickyExpiresAt = &expiresAt
	return m.state.StickyGeneration, true, nil
}

func (m *stickyDeadlineTestManager) ClearStickyExecution(_ string, executionGeneration, expectedStickyGeneration int64) (int64, bool, error) {
	m.stateMu.Lock()
	defer m.stateMu.Unlock()
	if m.state.ExecutionGeneration != executionGeneration || m.state.StickyGeneration != expectedStickyGeneration {
		return m.state.StickyGeneration, false, nil
	}
	m.state.StickyGeneration++
	m.state.StickyWorkerIdentity = ""
	m.state.StickyWorkerBuildID = ""
	m.state.StickyExpiresAt = nil
	return m.state.StickyGeneration, true, nil
}

// pollDeadlineRecordingQueue 记录每次 DequeueForWorker 收到的期限，并立即以长轮询到期结束。
type pollDeadlineRecordingQueue struct {
	atomicDeadlineTestQueue
	handlesDeadline bool
	mu              sync.Mutex
	deadlines       []time.Time
}

func (q *pollDeadlineRecordingQueue) HandlesStickyRoutingDeadline() bool { return q.handlesDeadline }

func (q *pollDeadlineRecordingQueue) DequeueForWorker(ctx context.Context, _ string) (protocol.TaskLease, error) {
	deadline, _ := ctx.Deadline()
	q.mu.Lock()
	q.deadlines = append(q.deadlines, deadline)
	q.mu.Unlock()
	return protocol.TaskLease{}, protocol.ErrTaskPollTimeout
}

// 队列自己处理 sticky 期限（TaskQ）时，Workflow Task 长轮询按调用方的期限进行，不再截成每秒一轮；
// 期限由引擎处理的队列仍每秒重新开始，让期限扫描按节拍重新派发过期投递。
func TestWorkflowTaskPollIsNotRestartedEverySecondWhenQueueHandlesStickyDeadline(t *testing.T) {
	for _, handles := range []bool{true, false} {
		queue := &pollDeadlineRecordingQueue{handlesDeadline: handles}
		server := assembleServer(memory.NewExecutionManager(), queue, newTestTaskQueue())
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		callerDeadline, _ := ctx.Deadline()
		_, err := server.dequeueWorkflowTask(ctx, "worker-a")
		cancel()
		server.Stop()
		if handles {
			if err != protocol.ErrTaskPollTimeout {
				t.Fatalf("handles=%v: err = %v, want the queue's poll timeout", handles, err)
			}
			if len(queue.deadlines) != 1 || !queue.deadlines[0].Equal(callerDeadline) {
				t.Fatalf("handles=%v: poll deadlines = %v, want the caller's %v", handles, queue.deadlines, callerDeadline)
			}
			continue
		}
		if len(queue.deadlines) == 0 || queue.deadlines[0].Sub(time.Now()) > stickyFallbackScanInterval {
			t.Fatalf("handles=%v: poll deadlines = %v, want at most %v from now", handles, queue.deadlines, stickyFallbackScanInterval)
		}
	}
}
