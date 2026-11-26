package executor

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/sdk/activity"
)

// heartbeatRecordingRuntime 记录每次心跳 RPC 的 details 与结果上报的 context 期限。cancelOn > 0 时第 cancelOn 次
// 心跳的应答带回取消请求；failHeartbeats 时心跳 RPC 一律失败（Server 不可达）。
type heartbeatRecordingRuntime struct {
	recordingActivityRuntime
	mu             sync.Mutex
	heartbeats     []map[string]any
	cancelOn       int
	failHeartbeats bool
	reportBudget   time.Duration
	events         []string
}

func (r *heartbeatRecordingRuntime) RecordActivityHeartbeat(_ context.Context, _ protocol.Task, details map[string]any) (protocol.ActivityHeartbeatResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.failHeartbeats {
		return protocol.ActivityHeartbeatResult{}, errors.New("connection refused")
	}
	r.heartbeats = append(r.heartbeats, details)
	r.events = append(r.events, "heartbeat")
	if r.cancelOn > 0 && len(r.heartbeats) >= r.cancelOn {
		return protocol.ActivityHeartbeatResult{CancelRequested: true, Reason: protocol.ActivityCancelRequestedReason}, nil
	}
	return protocol.ActivityHeartbeatResult{}, nil
}

func (r *heartbeatRecordingRuntime) CompleteActivityTask(ctx context.Context, task protocol.Task, output map[string]any, failure *protocol.Failure) error {
	r.mu.Lock()
	if deadline, ok := ctx.Deadline(); ok {
		r.reportBudget = time.Until(deadline)
	}
	r.events = append(r.events, "complete")
	r.mu.Unlock()
	return r.recordingActivityRuntime.CompleteActivityTask(ctx, task, output, failure)
}

func (r *heartbeatRecordingRuntime) FailActivityTask(ctx context.Context, task protocol.Task, failure protocol.Failure) error {
	r.mu.Lock()
	r.events = append(r.events, "fail")
	r.mu.Unlock()
	return r.recordingActivityRuntime.FailActivityTask(ctx, task, failure)
}

func (r *heartbeatRecordingRuntime) snapshot() ([]map[string]any, []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]map[string]any(nil), r.heartbeats...), append([]string(nil), r.events...)
}

func runHeartbeatActivity(t *testing.T, runtime ActivityRuntime, task protocol.Task, fn func(ctx context.Context) (map[string]any, error)) (string, error) {
	t.Helper()
	registry := activity.NewRegistry()
	if err := registry.Register("work", func(ctx context.Context, _ map[string]any) (map[string]any, error) { return fn(ctx) }); err != nil {
		t.Fatal(err)
	}
	task.RunID, task.ActivityID, task.ActivityType = "run-1", "s1", "work"
	outcome, executionErr, reportErr := RunActivityTask(context.Background(), task, runtime, registry, nil)
	if reportErr != nil {
		t.Fatalf("report error: %v", reportErr)
	}
	return outcome, executionErr
}

func TestHeartbeatThrottle(t *testing.T) {
	cases := []struct {
		timeout, want time.Duration
	}{
		{0, 30 * time.Second},
		{10 * time.Second, 8 * time.Second},
		{5 * time.Minute, time.Minute},
	}
	for _, tc := range cases {
		if got := heartbeatThrottle(tc.timeout); got != tc.want {
			t.Fatalf("heartbeatThrottle(%v) = %v, want %v", tc.timeout, got, tc.want)
		}
	}
}

// 一个节流窗口内的心跳合并成一次 RPC：第一次立即发出，窗口内其余的只保留最新 details，窗口结束时发出。
func TestRunActivityTaskThrottlesHeartbeats(t *testing.T) {
	runtime := &heartbeatRecordingRuntime{}
	outcome, _ := runHeartbeatActivity(t, runtime, protocol.Task{HeartbeatTimeout: 250 * time.Millisecond}, func(ctx context.Context) (map[string]any, error) {
		for i := 1; i <= 100; i++ {
			activity.RecordHeartbeat(ctx, map[string]any{"progress": i})
		}
		time.Sleep(300 * time.Millisecond) // 窗口（200ms）结束，最新 details 发出
		return map[string]any{"ok": true}, nil
	})
	heartbeats, _ := runtime.snapshot()
	if outcome != "success" || len(heartbeats) != 2 {
		t.Fatalf("outcome = %s heartbeats = %v, want success with 2 RPCs", outcome, heartbeats)
	}
	if heartbeats[0]["progress"] != 1 || heartbeats[1]["progress"] != 100 {
		t.Fatalf("heartbeat details = %v, want the first and the latest", heartbeats)
	}
}

// 以失败结束时，窗口内尚未发出的最新 details 在失败上报之前发出；成功结束时不再发出。
func TestRunActivityTaskFlushesPendingHeartbeatBeforeFailure(t *testing.T) {
	runtime := &heartbeatRecordingRuntime{}
	outcome, _ := runHeartbeatActivity(t, runtime, protocol.Task{HeartbeatTimeout: time.Minute}, func(ctx context.Context) (map[string]any, error) {
		activity.RecordHeartbeat(ctx, map[string]any{"progress": 1})
		activity.RecordHeartbeat(ctx, map[string]any{"progress": 2})
		return nil, errors.New("disk full")
	})
	heartbeats, events := runtime.snapshot()
	if outcome != "failed" || len(heartbeats) != 2 || heartbeats[1]["progress"] != 2 {
		t.Fatalf("outcome = %s heartbeats = %v, want failed with the latest details flushed", outcome, heartbeats)
	}
	if events[len(events)-1] != "fail" || events[len(events)-2] != "heartbeat" {
		t.Fatalf("events = %v, want the flushed heartbeat before the failure", events)
	}

	runtime = &heartbeatRecordingRuntime{}
	runHeartbeatActivity(t, runtime, protocol.Task{HeartbeatTimeout: time.Minute}, func(ctx context.Context) (map[string]any, error) {
		activity.RecordHeartbeat(ctx, map[string]any{"progress": 1})
		activity.RecordHeartbeat(ctx, map[string]any{"progress": 2})
		return map[string]any{"ok": true}, nil
	})
	if heartbeats, _ := runtime.snapshot(); len(heartbeats) != 1 {
		t.Fatalf("heartbeats after success = %v, want only the first", heartbeats)
	}
}

// 取消请求由窗口结束时发出的心跳带回：逐条心跳的 Activity 在一个窗口内看到取消。
func TestRunActivityTaskDeliversCancellationThroughThrottledHeartbeats(t *testing.T) {
	runtime := &heartbeatRecordingRuntime{cancelOn: 2}
	outcome, _ := runHeartbeatActivity(t, runtime, protocol.Task{HeartbeatTimeout: 100 * time.Millisecond}, func(ctx context.Context) (map[string]any, error) {
		deadline := time.After(2 * time.Second)
		for {
			activity.RecordHeartbeat(ctx, nil)
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-deadline:
				return nil, errors.New("cancellation not delivered")
			case <-time.After(5 * time.Millisecond):
			}
		}
	})
	if outcome != "cancelled" {
		t.Fatalf("outcome = %s, want cancelled", outcome)
	}
}

// Server 不可达超过 HeartbeatTimeout 时 Server 已判这次 attempt 超时：Activity context 被取消，结果不上报。
func TestRunActivityTaskAbandonsAttemptWhenHeartbeatsFailPastTheTimeout(t *testing.T) {
	runtime := &heartbeatRecordingRuntime{failHeartbeats: true}
	outcome, _ := runHeartbeatActivity(t, runtime, protocol.Task{HeartbeatTimeout: 100 * time.Millisecond}, func(ctx context.Context) (map[string]any, error) {
		deadline := time.After(2 * time.Second)
		for {
			activity.RecordHeartbeat(ctx, nil)
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-deadline:
				return nil, errors.New("attempt not abandoned")
			case <-time.After(5 * time.Millisecond):
			}
		}
	})
	_, events := runtime.snapshot()
	if outcome != "cancelled" || len(events) != 0 {
		t.Fatalf("outcome = %s events = %v, want cancelled with nothing reported", outcome, events)
	}
}

// 结果上报的期限是 min(attempt 剩余时间, 60s)，不短于 5s：Server 重启超过 5s 时结果仍能送达。
func TestRunActivityTaskReportsWithinTheAttemptDeadline(t *testing.T) {
	runtime := &heartbeatRecordingRuntime{}
	runHeartbeatActivity(t, runtime, protocol.Task{StartToCloseTimeout: 10 * time.Minute}, func(context.Context) (map[string]any, error) {
		return map[string]any{"ok": true}, nil
	})
	if runtime.reportBudget < 55*time.Second || runtime.reportBudget > time.Minute {
		t.Fatalf("report budget = %v, want about 60s", runtime.reportBudget)
	}
}

func TestReportTimeout(t *testing.T) {
	now := time.Now()
	cases := []struct {
		name     string
		deadline time.Time
		want     time.Duration
	}{
		{"no deadline", time.Time{}, time.Minute},
		{"far deadline", now.Add(time.Hour), time.Minute},
		{"near deadline", now.Add(20 * time.Second), 20 * time.Second},
		{"passed deadline", now.Add(-time.Second), 5 * time.Second},
	}
	for _, tc := range cases {
		if got := reportTimeout(tc.deadline, now); got != tc.want {
			t.Fatalf("%s: reportTimeout = %v, want %v", tc.name, got, tc.want)
		}
	}
}
