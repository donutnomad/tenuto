package worker_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/internal/worker"
	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/sdk/activity"
)

// ============================================
// Worker 优雅停止
// ============================================
// 测试策略:
// - Happy Path: 宽限期内结束的 Activity 照常上报结果；察觉停止返回 ErrWorkerStopping 的 Activity 交还 attempt
// - Edge Case: 没有在途任务时立即返回；重复、并发调用；宽限期内结束的 Workflow Task 照常确认
// - Error Case: 宽限期到期后 Activity 的 context 被取消、交还而不是失败；Workflow Task 被放弃、不上报超时
// ============================================

// stopTestQueue 是只出不进的任务队列：测试往 leases 里放任务，poll 在 ctx 结束时返回。
type stopTestQueue struct {
	leases chan protocol.TaskLease
	mu     sync.Mutex
	acks   int
	nacks  int
}

func newStopTestQueue() *stopTestQueue {
	return &stopTestQueue{leases: make(chan protocol.TaskLease, 4)}
}

func (q *stopTestQueue) poll(ctx context.Context) (protocol.TaskLease, error) {
	select {
	case lease := <-q.leases:
		return lease, nil
	case <-ctx.Done():
		return protocol.TaskLease{}, ctx.Err()
	}
}

func (q *stopTestQueue) PollActivityTask(ctx context.Context, _ string) (protocol.TaskLease, error) {
	return q.poll(ctx)
}

func (q *stopTestQueue) AckActivityTask(context.Context, protocol.TaskLease) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.acks++
	return nil
}

func (q *stopTestQueue) NackActivityTask(context.Context, protocol.TaskLease) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.nacks++
	return nil
}

func (q *stopTestQueue) counts() (acks, nacks int) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.acks, q.nacks
}

// stopTestRuntime 记录 Activity 的上报。
type stopTestRuntime struct {
	mu        sync.Mutex
	completed int
	failed    int
	released  int
}

func (r *stopTestRuntime) BeginActivityTask(context.Context, protocol.Task) (protocol.ActivityTaskBeginResult, error) {
	return protocol.ActivityTaskBeginResult{Execute: true}, nil
}

func (r *stopTestRuntime) RecordActivityHeartbeat(context.Context, protocol.Task, map[string]any) (protocol.ActivityHeartbeatResult, error) {
	return protocol.ActivityHeartbeatResult{}, nil
}

func (r *stopTestRuntime) CompleteActivityTask(ctx context.Context, _ protocol.Task, _ map[string]any, _ *protocol.Failure) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.completed++
	return ctx.Err()
}

func (r *stopTestRuntime) FailActivityTask(ctx context.Context, _ protocol.Task, _ protocol.Failure) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.failed++
	return ctx.Err()
}

func (r *stopTestRuntime) ReleaseActivityTask(ctx context.Context, _ protocol.Task) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.released++
	return ctx.Err()
}

func (r *stopTestRuntime) CheckActivityTaskCancellation(context.Context, protocol.Task) (protocol.ActivityCancellationResult, error) {
	return protocol.ActivityCancellationResult{}, nil
}

func (r *stopTestRuntime) counts() (completed, failed, released int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.completed, r.failed, r.released
}

func activityLease() protocol.TaskLease {
	return protocol.TaskLease{Task: protocol.Task{
		Type: protocol.TaskTypeActivity, RunID: "run-stop", ActivityID: "step", ScheduledEventID: 5, Attempt: 1, ActivityType: "long",
	}}
}

// startActivityWorker 启动只消费 Activity 的 Worker，并在 fn 开始执行后返回。
func startActivityWorker(t *testing.T, graceful time.Duration, fn protocol.ActivityFunc) (*worker.Worker, *stopTestQueue, *stopTestRuntime) {
	t.Helper()
	started := make(chan struct{})
	registry := activity.NewRegistry()
	if err := registry.Register("long", func(ctx context.Context, input map[string]any) (map[string]any, error) {
		close(started)
		return fn(ctx, input)
	}); err != nil {
		t.Fatal(err)
	}
	queue, runtime := newStopTestQueue(), &stopTestRuntime{}
	w := worker.NewActivityWorkerWithQueue(queue, runtime, registry, worker.WorkerOptions{GracefulStopTimeout: graceful})
	w.Start(context.Background())
	queue.leases <- activityLease()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("activity did not start")
	}
	return w, queue, runtime
}

func TestStopWithNothingRunningReturnsPromptly(t *testing.T) {
	queue := newStopTestQueue()
	w := worker.NewActivityWorkerWithQueue(queue, &stopTestRuntime{}, activity.NewRegistry(), worker.WorkerOptions{})
	w.Start(context.Background())
	startedAt := time.Now()
	w.Stop()
	if elapsed := time.Since(startedAt); elapsed > time.Second {
		t.Fatalf("Stop with idle pollers took %s; the long polls must be cancelled at once", elapsed)
	}
}

func TestStopBeforeStartReturns(t *testing.T) {
	w := worker.NewActivityWorkerWithQueue(newStopTestQueue(), &stopTestRuntime{}, activity.NewRegistry(), worker.WorkerOptions{})
	w.Stop()
	w.Stop()
}

func TestStopLetsInFlightActivityFinishWithinGracePeriod(t *testing.T) {
	w, queue, runtime := startActivityWorker(t, 5*time.Second, func(ctx context.Context, _ map[string]any) (map[string]any, error) {
		<-activity.GetWorkerStopChannel(ctx)
		if ctx.Err() != nil {
			return nil, errors.New("activity context was cancelled at the start of the drain")
		}
		return map[string]any{"done": true}, nil
	})
	w.Stop()
	if completed, failed, released := runtime.counts(); completed != 1 || failed != 0 || released != 0 {
		t.Fatalf("completed=%d failed=%d released=%d, want the result reported", completed, failed, released)
	}
	if acks, nacks := queue.counts(); acks != 1 || nacks != 0 {
		t.Fatalf("acks=%d nacks=%d", acks, nacks)
	}
}

func TestStopReleasesActivityThatReturnsErrWorkerStopping(t *testing.T) {
	w, _, runtime := startActivityWorker(t, 5*time.Second, func(ctx context.Context, _ map[string]any) (map[string]any, error) {
		<-activity.GetWorkerStopChannel(ctx)
		return nil, activity.ErrWorkerStopping
	})
	w.Stop()
	if completed, failed, released := runtime.counts(); completed != 0 || failed != 0 || released != 1 {
		t.Fatalf("completed=%d failed=%d released=%d, want the attempt released", completed, failed, released)
	}
}

func TestStopTimeoutCancelsActivityAndReleasesIt(t *testing.T) {
	w, _, runtime := startActivityWorker(t, 20*time.Millisecond, func(ctx context.Context, _ map[string]any) (map[string]any, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	})
	w.Stop()
	if completed, failed, released := runtime.counts(); completed != 0 || failed != 0 || released != 1 {
		t.Fatalf("completed=%d failed=%d released=%d, want the attempt released, not failed", completed, failed, released)
	}
}

// ErrWorkerStopping 只在 Worker 停止时表示交还；平常返回它仍是失败。
func TestErrWorkerStoppingOutsideStopIsAFailure(t *testing.T) {
	w, _, runtime := startActivityWorker(t, 5*time.Second, func(context.Context, map[string]any) (map[string]any, error) {
		return nil, activity.ErrWorkerStopping
	})
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, failed, _ := runtime.counts(); failed == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("failure was not reported")
		}
		time.Sleep(time.Millisecond)
	}
	w.Stop()
	if completed, failed, released := runtime.counts(); completed != 0 || failed != 1 || released != 0 {
		t.Fatalf("completed=%d failed=%d released=%d", completed, failed, released)
	}
}

func TestConcurrentStopWaitsForDrain(t *testing.T) {
	finish := make(chan struct{})
	finished := make(chan struct{})
	w, _, runtime := startActivityWorker(t, 5*time.Second, func(context.Context, map[string]any) (map[string]any, error) {
		<-finish
		close(finished)
		return map[string]any{}, nil
	})
	var callers sync.WaitGroup
	returned := make(chan struct{}, 3)
	for range 3 {
		callers.Add(1)
		go func() {
			defer callers.Done()
			w.Stop()
			returned <- struct{}{}
		}()
	}
	select {
	case <-returned:
		t.Fatal("Stop returned while an activity was still running")
	case <-time.After(20 * time.Millisecond):
	}
	close(finish)
	callers.Wait()
	<-finished
	if completed, _, _ := runtime.counts(); completed != 1 {
		t.Fatalf("completed=%d", completed)
	}
}

// stopTestWorkflowAPI 的 Workflow Task 在读取 Run 状态时阻塞，直到 release 关闭或 ctx 结束。
type stopTestWorkflowAPI struct {
	*stopTestQueue
	reading  chan struct{}
	release  chan struct{}
	timedOut int
	finished int
}

func (a *stopTestWorkflowAPI) PollWorkflowTask(ctx context.Context) (protocol.TaskLease, error) {
	return a.poll(ctx)
}

func (a *stopTestWorkflowAPI) AckWorkflowTask(ctx context.Context, lease protocol.TaskLease) error {
	return a.AckActivityTask(ctx, lease)
}

func (a *stopTestWorkflowAPI) NackWorkflowTask(ctx context.Context, lease protocol.TaskLease) error {
	return a.NackActivityTask(ctx, lease)
}

func (a *stopTestWorkflowAPI) GetRunStatusContext(ctx context.Context, _ string) (history.Status, error) {
	close(a.reading)
	select {
	case <-a.release:
		return history.StatusCompleted, nil
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

func (a *stopTestWorkflowAPI) GetRunStatus(string) (history.Status, error) {
	return history.StatusRunning, nil
}

func (a *stopTestWorkflowAPI) GetEvents(string) ([]history.Event, error) { return nil, nil }

func (a *stopTestWorkflowAPI) GetWorkflowConfig(context.Context, string, string) (*program.WorkflowConfig, error) {
	return nil, errors.New("not used")
}

func (a *stopTestWorkflowAPI) HandleWorkflowTaskCompleted(context.Context, string, int64, []history.Command, string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.finished++
	return nil
}

func (a *stopTestWorkflowAPI) HandleWorkflowTaskTimedOut(context.Context, string, int64, string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.timedOut++
	return nil
}

func (a *stopTestWorkflowAPI) HandleWorkflowTaskFailed(context.Context, string, int64, string, string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.finished++
	return nil
}

func startWorkflowWorker(t *testing.T, graceful time.Duration) (*worker.Worker, *stopTestWorkflowAPI) {
	t.Helper()
	api := &stopTestWorkflowAPI{stopTestQueue: newStopTestQueue(), reading: make(chan struct{}), release: make(chan struct{})}
	w := worker.NewWorkflowWorker(api, worker.WorkerOptions{GracefulStopTimeout: graceful})
	w.Start(context.Background())
	api.leases <- protocol.TaskLease{Task: protocol.Task{Type: protocol.TaskTypeWorkflow, RunID: "run-stop", ScheduledEventID: 2}}
	select {
	case <-api.reading:
	case <-time.After(5 * time.Second):
		t.Fatal("workflow task did not start")
	}
	return w, api
}

func TestStopLetsInFlightWorkflowTaskFinish(t *testing.T) {
	w, api := startWorkflowWorker(t, 5*time.Second)
	stopped := make(chan struct{})
	go func() {
		w.Stop()
		close(stopped)
	}()
	select {
	case <-stopped:
		t.Fatal("Stop returned while a workflow task was in flight")
	case <-time.After(20 * time.Millisecond):
	}
	close(api.release)
	<-stopped
	if acks, nacks := api.counts(); acks != 1 || nacks != 0 || api.timedOut != 0 {
		t.Fatalf("acks=%d nacks=%d timedOut=%d", acks, nacks, api.timedOut)
	}
}

func TestStopTimeoutAbandonsWorkflowTaskWithoutReporting(t *testing.T) {
	w, api := startWorkflowWorker(t, 20*time.Millisecond)
	w.Stop()
	if acks, nacks := api.counts(); acks != 1 || nacks != 0 || api.timedOut != 0 || api.finished != 0 {
		t.Fatalf("acks=%d nacks=%d timedOut=%d finished=%d, want the task abandoned without a report", acks, nacks, api.timedOut, api.finished)
	}
}

// lateWorkflowPoll 的 poll 在停止拉取之后才交出任务：模拟长轮询与 Stop 同时到达，Server 已把任务记为 started。
type lateWorkflowPoll struct {
	*stopTestWorkflowAPI
	polling chan struct{}
}

func (a lateWorkflowPoll) PollWorkflowTask(ctx context.Context) (protocol.TaskLease, error) {
	close(a.polling)
	<-ctx.Done()
	return <-a.leases, nil
}

func TestStopProcessesWorkflowTaskPolledAfterStopBegan(t *testing.T) {
	api := lateWorkflowPoll{
		stopTestWorkflowAPI: &stopTestWorkflowAPI{stopTestQueue: newStopTestQueue(), reading: make(chan struct{}), release: make(chan struct{})},
		polling:             make(chan struct{}),
	}
	api.leases <- protocol.TaskLease{Task: protocol.Task{Type: protocol.TaskTypeWorkflow, RunID: "run-late", ScheduledEventID: 2}}
	w := worker.NewWorkflowWorker(api, worker.WorkerOptions{GracefulStopTimeout: 5 * time.Second})
	w.Start(context.Background())
	<-api.polling
	stopped := make(chan struct{})
	go func() {
		w.Stop()
		close(stopped)
	}()
	select {
	case <-api.reading:
	case <-time.After(5 * time.Second):
		t.Fatal("停止之后领到的 Workflow Task 没有被处理")
	}
	close(api.release)
	<-stopped
	if acks, nacks := api.counts(); acks != 1 || nacks != 0 || api.timedOut != 0 {
		t.Fatalf("acks=%d nacks=%d timedOut=%d", acks, nacks, api.timedOut)
	}
}
