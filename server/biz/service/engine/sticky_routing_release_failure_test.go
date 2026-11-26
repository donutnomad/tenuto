package engine

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/program/programtest"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/clock/clocktest"
	"github.com/donutnomad/tenuto/server/biz/domain/dispatch"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

var errReleaseDispatchFailed = errors.New("release dispatch failed")

// failingReleaseDispatchStore 在 ReleaseTaskDispatch 上注入失败，其余行为委托给真实的
// memory.DispatchRepository，以便保留去重门控的真实语义。
type failingReleaseDispatchStore struct {
	*memory.DispatchRepository
	releaseCalls  int
	dispatchCalls int
}

func (s *failingReleaseDispatchStore) ReleaseTaskDispatch(protocol.Task, string) error {
	s.releaseCalls++
	return errReleaseDispatchFailed
}

func (s *failingReleaseDispatchStore) AcquireTaskDispatch(task protocol.Task, ttl time.Duration) (dispatch.Lease, bool, error) {
	s.dispatchCalls++
	return s.DispatchRepository.AcquireTaskDispatch(task, ttl)
}

// nonDeadlineStickyQueue 不实现 StickyRoutingDeadlineQueue，使 fallback 走到
// Release + 重投分支（atomicDeadlineTestQueue 会在此之前 continue 掉）。
type nonDeadlineStickyQueue struct {
	normalEnqueues int
	stickyEnqueues int
}

func (q *nonDeadlineStickyQueue) Enqueue(context.Context, protocol.Task) error {
	q.normalEnqueues++
	return nil
}

func (*nonDeadlineStickyQueue) HandlesStickyRoutingDeadline() bool { return false }
func (*nonDeadlineStickyQueue) Durable() bool                      { return false }

func (*nonDeadlineStickyQueue) Dequeue(context.Context) (protocol.TaskLease, error) {
	return protocol.TaskLease{}, context.Canceled
}

func (*nonDeadlineStickyQueue) Ack(context.Context, protocol.TaskLease) error  { return nil }
func (*nonDeadlineStickyQueue) Nack(context.Context, protocol.TaskLease) error { return nil }

func (q *nonDeadlineStickyQueue) EnqueueSticky(context.Context, string, protocol.Task) error {
	q.stickyEnqueues++
	return nil
}

func (*nonDeadlineStickyQueue) DequeueForWorker(context.Context, string) (protocol.TaskLease, error) {
	return protocol.TaskLease{}, context.Canceled
}

// TestStickyFallbackAbortsWhenReleaseFails 锁定回归：ReleaseTaskDispatch 失败时，
// fallback 必须返回错误中止本轮，而不是静默继续走一次注定被去重门控挡掉的 dispatch。
//
// Release 先于 ClearStickyExecution 执行：失败时 sticky 保持原状，这个 Run 留在下一轮扫描的
// 候选里重试。旧租约还在时继续 dispatch 会让 AcquireTaskDispatch 返回 (false, nil)，task 就此孤立。
func TestStickyFallbackAbortsWhenReleaseFails(t *testing.T) {
	run := &execution.Run{ID: "run-release-fail", WorkflowID: "workflow-release-fail", WorkflowType: "workflow-release-fail", Status: history.StatusRunning}
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
	queue := &nonDeadlineStickyQueue{}
	server := assembleServer(mgr, queue, newTestTaskQueue())

	dispatches := &failingReleaseDispatchStore{DispatchRepository: memory.NewDispatchRepository()}
	server.dispatches = dispatches

	// 让该 task 处于 Dispatched 状态，模拟"旧租约仍在"。
	task := protocol.Task{
		Namespace:        protocol.DefaultNamespace,
		Type:             protocol.TaskTypeWorkflow,
		RunID:            run.ID,
		ScheduledEventID: 42,
	}
	lease, acquired, err := dispatches.DispatchRepository.AcquireTaskDispatch(task, time.Minute)
	if err != nil || !acquired {
		t.Fatalf("seed acquire: acquired=%v err=%v", acquired, err)
	}
	if err := dispatches.DispatchRepository.MarkTaskDispatchDispatched(task, lease.LeaseID); err != nil {
		t.Fatal(err)
	}
	dispatches.dispatchCalls = 0

	err = server.FallbackExpiredStickyWorkflowTasks(context.Background())

	if !errors.Is(err, errReleaseDispatchFailed) {
		t.Fatalf("fallback err = %v, want %v; Release 失败必须向上传播以便 poll 循环重试", err, errReleaseDispatchFailed)
	}
	if dispatches.releaseCalls != 1 {
		t.Fatalf("ReleaseTaskDispatch calls = %d, want 1", dispatches.releaseCalls)
	}
	// 关键断言：中止发生在 dispatch 之前，没有走那次注定无效的重投。
	if dispatches.dispatchCalls != 0 {
		t.Fatalf("AcquireTaskDispatch calls after failed release = %d, want 0 (不应继续 dispatch)", dispatches.dispatchCalls)
	}
	if queue.normalEnqueues != 0 || queue.stickyEnqueues != 0 {
		t.Fatalf("queue enqueues: normal=%d sticky=%d, want 0/0", queue.normalEnqueues, queue.stickyEnqueues)
	}
	// sticky 保持原状，下一轮扫描还能选中这个 Run。
	state, err := mgr.GetWorkflowExecutionState(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if state.StickyWorkerIdentity != "owner-worker" || state.StickyGeneration != 3 {
		t.Fatalf("sticky after failed release = %q gen %d, want owner-worker gen 3", state.StickyWorkerIdentity, state.StickyGeneration)
	}
}

// 已被 Worker 领取的 Workflow Task 在 sticky 过期后保持原状：重投会签发新的激活 ID，
// 新任务过不了激活 fence，原 Worker 的提交又被清除 sticky 后的代数 fence 拒绝。
// 领取它的 Worker 若已下线，由 Server 侧的 start-to-close 超时重新调度（见 server_workflow_task_timeout_test.go）。
func TestStickyFallbackLeavesStartedWorkflowTaskAlone(t *testing.T) {
	run := &execution.Run{ID: "run-started-sticky", WorkflowID: "workflow-started-sticky", WorkflowType: "workflow-started-sticky", Status: history.StatusRunning}
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
		CurrentActivationID:     "activation-1",
		WorkflowTask:            execution.WorkflowTaskProjection{StartedScheduledEventID: 42},
	})
	queue := &nonDeadlineStickyQueue{}
	server := assembleServer(mgr, queue, newTestTaskQueue())

	if err := server.FallbackExpiredStickyWorkflowTasks(context.Background()); err != nil {
		t.Fatal(err)
	}
	if queue.normalEnqueues != 0 || queue.stickyEnqueues != 0 {
		t.Fatalf("queue enqueues: normal=%d sticky=%d, want 0/0", queue.normalEnqueues, queue.stickyEnqueues)
	}
	state, err := mgr.GetWorkflowExecutionState(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if state.StickyWorkerIdentity != "owner-worker" || state.StickyGeneration != 3 {
		t.Fatalf("sticky = %q gen %d, want owner-worker gen 3", state.StickyWorkerIdentity, state.StickyGeneration)
	}
}

// sticky 绑定之后调度的 WFT 投给 sticky Worker，从调度时刻起计 sticky schedule-to-start：期限之前不回落；假时钟
// 推进到期限后，回落扫描清除 sticky 并把未领取的 WFT 重投普通队列。
func TestStickyFallbackRedispatchesAfterStickyExpires(t *testing.T) {
	const stickyTTL = execution.StickyScheduleToStartTimeout
	mgr := memory.NewExecutionManager()
	queue := &nonDeadlineStickyQueue{}
	clk := clocktest.NewFake(testClockStart)
	server := assembleServerWithClock(mgr, queue, newTestTaskQueue(), clk)
	t.Cleanup(server.Stop)
	if _, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{Type: "sticky-expiry", Program: programtest.Noop()}); err != nil {
		t.Fatal(err)
	}
	runID, err := startedRunID(server.StartWorkflow(context.Background(), "", "sticky-expiry", "sticky-expiry", "", nil))
	if err != nil {
		t.Fatal(err)
	}
	state, err := mgr.GetWorkflowExecutionState(runID)
	if err != nil {
		t.Fatal(err)
	}
	if _, assigned, err := mgr.AssignStickyExecution(runID, state.ExecutionGeneration, state.StickyGeneration, "owner-worker", "workflow-v1"); err != nil || !assigned {
		t.Fatalf("assign sticky: assigned=%v err=%v", assigned, err)
	}
	// 首个 WFT 在绑定之前调度，走普通队列；完成它之后，下一个 WFT 在一段长于 sticky 期限的间隔后才调度，
	// 仍投给 sticky Worker（绑定不随时间失效）。
	if _, _, err := mgr.AppendWorkflowTaskClosed(runID, runID, runID, history.NewWorkflowTaskCompletedEvent(state.OpenWorkflowTaskEventID, "owner-worker")); err != nil {
		t.Fatal(err)
	}
	clk.Advance(time.Minute)
	if err := server.scheduleWorkflowTask(context.Background(), runID); err != nil {
		t.Fatal(err)
	}
	if queue.stickyEnqueues != 1 {
		t.Fatalf("sticky enqueues = %d, want the workflow task scheduled a minute after completion routed sticky", queue.stickyEnqueues)
	}
	enqueuedAtStart := queue.normalEnqueues
	if state, _ := mgr.GetWorkflowExecutionState(runID); state.StickyExpiresAt == nil || !state.StickyExpiresAt.Equal(clk.Now().Add(stickyTTL)) {
		t.Fatalf("sticky schedule-to-start deadline = %v, want %v", state.StickyExpiresAt, clk.Now().Add(stickyTTL))
	}

	clk.Advance(stickyTTL - time.Millisecond)
	if err := server.FallbackExpiredStickyWorkflowTasks(context.Background()); err != nil {
		t.Fatal(err)
	}
	if state, _ := mgr.GetWorkflowExecutionState(runID); state.StickyWorkerIdentity != "owner-worker" || queue.normalEnqueues != enqueuedAtStart {
		t.Fatalf("sticky fell back before it expired: identity=%q enqueues=%d", state.StickyWorkerIdentity, queue.normalEnqueues)
	}

	clk.Advance(time.Millisecond)
	if err := server.FallbackExpiredStickyWorkflowTasks(context.Background()); err != nil {
		t.Fatal(err)
	}
	if state, _ := mgr.GetWorkflowExecutionState(runID); state.StickyWorkerIdentity != "" {
		t.Fatalf("sticky not cleared after it expired: %q", state.StickyWorkerIdentity)
	}
	if queue.normalEnqueues != enqueuedAtStart+1 || queue.stickyEnqueues != 1 {
		t.Fatalf("enqueues after fallback: normal=%d sticky=%d, want %d/1", queue.normalEnqueues, queue.stickyEnqueues, enqueuedAtStart+1)
	}
}
