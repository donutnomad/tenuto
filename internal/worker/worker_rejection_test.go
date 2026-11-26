package worker_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/internal/worker"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/sdk/activity"
)

// ============================================
// Server 以不可重试的 4xx 拒绝 Worker 的上报
// ============================================
// - Begin 被拒绝：按连续拒绝次数退避后才退回投递，不在 Worker 与队列之间无退避地循环
// - 结果被拒绝：改报一次不可重试的 ActivityResultRejected 失败
// - Workflow Task 上报被拒绝：不确认也不退回，投递留到可见性超时
// ============================================

// redeliveringQueue 在 Nack 时把投递放回队列，记录每次 Nack。
type redeliveringQueue struct {
	*stopTestQueue
}

func (q redeliveringQueue) NackActivityTask(ctx context.Context, lease protocol.TaskLease) error {
	_ = q.stopTestQueue.NackActivityTask(ctx, lease)
	q.leases <- lease
	return nil
}

// rejectingRuntime 按配置以 InvalidRequest 拒绝 Begin 或 Complete，记录失败上报。
type rejectingRuntime struct {
	stopTestRuntime
	rejectBegin, rejectComplete bool
	mu                          sync.Mutex
	begins                      int
	failures                    []protocol.Failure
}

func (r *rejectingRuntime) BeginActivityTask(context.Context, protocol.Task) (protocol.ActivityTaskBeginResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.begins++
	if r.rejectBegin {
		return protocol.ActivityTaskBeginResult{}, protocol.InvalidRequestf("activity targets worker %q; caller is %q", "other", "")
	}
	return protocol.ActivityTaskBeginResult{Execute: true}, nil
}

func (r *rejectingRuntime) CompleteActivityTask(context.Context, protocol.Task, map[string]any, *protocol.Failure) error {
	if r.rejectComplete {
		return protocol.InvalidRequestf("invalid activity result")
	}
	return nil
}

func (r *rejectingRuntime) FailActivityTask(_ context.Context, _ protocol.Task, failure protocol.Failure) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.failures = append(r.failures, failure)
	return nil
}

func startRejectingActivityWorker(t *testing.T, runtime *rejectingRuntime) (*worker.Worker, *stopTestQueue) {
	t.Helper()
	registry := activity.NewRegistry()
	if err := registry.Register("long", func(context.Context, map[string]any) (map[string]any, error) {
		return map[string]any{"ok": true}, nil
	}); err != nil {
		t.Fatal(err)
	}
	queue := newStopTestQueue()
	w := worker.NewActivityWorkerWithQueue(redeliveringQueue{queue}, runtime, registry, worker.WorkerOptions{GracefulStopTimeout: 100 * time.Millisecond})
	w.Start(context.Background())
	t.Cleanup(w.Stop)
	queue.leases <- activityLease()
	return w, queue
}

func TestRejectedBeginBacksOffBeforeReturningTheTask(t *testing.T) {
	runtime := &rejectingRuntime{rejectBegin: true}
	_, queue := startRejectingActivityWorker(t, runtime)
	time.Sleep(1500 * time.Millisecond)
	runtime.mu.Lock()
	begins := runtime.begins
	runtime.mu.Unlock()
	_, nacks := queue.counts()
	// 第一次拒绝后等 1s 再退回，第二次拒绝后等 2s：1.5s 内只有两次 Begin、一次 Nack。
	if begins != 2 || nacks != 1 {
		t.Fatalf("in 1.5s: begins=%d nacks=%d, want 2 begins and 1 nack", begins, nacks)
	}
}

func TestRejectedResultIsReportedAsNonRetryableFailure(t *testing.T) {
	runtime := &rejectingRuntime{rejectComplete: true}
	startRejectingActivityWorker(t, runtime)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		runtime.mu.Lock()
		failures := append([]protocol.Failure(nil), runtime.failures...)
		runtime.mu.Unlock()
		if len(failures) > 0 {
			if len(failures) != 1 || failures[0].Type != protocol.FailureTypeActivityResultRejected || !failures[0].NonRetryable {
				t.Fatalf("failures = %+v, want one non-retryable %s", failures, protocol.FailureTypeActivityResultRejected)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("the rejected result was not reported as a failure")
}

// rejectingWorkflowAPI 以 InvalidRequest 拒绝 Workflow Task 读取 Run 状态之后的上报。
type rejectingWorkflowAPI struct {
	*stopTestWorkflowAPI
}

func (a rejectingWorkflowAPI) GetRunStatusContext(context.Context, string) (history.Status, error) {
	return "", protocol.InvalidRequestf("workflow task token is stale")
}

func TestRejectedWorkflowTaskReportLeavesTheDelivery(t *testing.T) {
	api := rejectingWorkflowAPI{&stopTestWorkflowAPI{stopTestQueue: newStopTestQueue(), reading: make(chan struct{}), release: make(chan struct{})}}
	w := worker.NewWorkflowWorker(api, worker.WorkerOptions{GracefulStopTimeout: 100 * time.Millisecond})
	w.Start(context.Background())
	api.leases <- protocol.TaskLease{Task: protocol.Task{Type: protocol.TaskTypeWorkflow, RunID: "run-rejected", ScheduledEventID: 2}}
	time.Sleep(300 * time.Millisecond)
	w.Stop()
	if acks, nacks := api.counts(); acks != 0 || nacks != 0 {
		t.Fatalf("acks=%d nacks=%d, want the rejected delivery left to the visibility timeout", acks, nacks)
	}
}
