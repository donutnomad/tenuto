package executor

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/sdk/activity"
)

// rejectingHeartbeatRuntime 的心跳得到 4xx（例如反向代理的 404、413）。
type rejectingHeartbeatRuntime struct {
	startedActivityRuntime
}

func (r *rejectingHeartbeatRuntime) RecordActivityHeartbeat(context.Context, protocol.Task, map[string]any) (protocol.ActivityHeartbeatResult, error) {
	return protocol.ActivityHeartbeatResult{}, protocol.InvalidRequestf("404 page not found")
}

// runNotFoundRuntime 的心跳得到 Server 明确的 run_not_found 答复。
type runNotFoundRuntime struct {
	recordingActivityRuntime
}

func (r *runNotFoundRuntime) RecordActivityHeartbeat(context.Context, protocol.Task, map[string]any) (protocol.ActivityHeartbeatResult, error) {
	return protocol.ActivityHeartbeatResult{CancelRequested: true, Reason: "run_not_found"}, nil
}

// waitCtx 注册一个只在 ctx 结束时返回的 Activity（带 ctx 的外部调用遇到对端挂起），hb 为 true 时先心跳一次。
func waitCtx(t *testing.T, hb bool) *activity.Registry {
	t.Helper()
	registry := activity.NewRegistry()
	if err := registry.Register("hang", func(ctx context.Context, _ map[string]any) (map[string]any, error) {
		if hb {
			activity.RecordHeartbeat(ctx, map[string]any{"step": 1})
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(10 * time.Second):
			return nil, errors.New("ctx never ended")
		}
	}); err != nil {
		t.Fatal(err)
	}
	return registry
}

// Activity 的 ctx 截止在 StartToClose 与 ScheduleToClose 中先到的一个；截止后 Activity 按 ctx 返回时 Worker 不上报，
// 由 Server 的超时计时决定这次 attempt（对标 Temporal Go SDK），槽位随 Activity 返回释放。
func TestRunActivityTaskEndsAtAttemptDeadline(t *testing.T) {
	for name, task := range map[string]protocol.Task{
		"schedule to close only":         {ScheduleToCloseTimeout: 200 * time.Millisecond},
		"schedule to close before start": {ScheduleToCloseTimeout: 200 * time.Millisecond, StartToCloseTimeout: time.Hour},
		"start to close before schedule": {StartToCloseTimeout: 200 * time.Millisecond, ScheduleToCloseTimeout: time.Hour},
		"start to close only":            {StartToCloseTimeout: 200 * time.Millisecond},
	} {
		t.Run(name, func(t *testing.T) {
			task.RunID, task.ActivityID, task.ActivityType = "run-1", "a1", "hang"
			task.ScheduledTime = time.Now()
			runtime := &startedActivityRuntime{startedTime: time.Now()}
			begin := time.Now()
			outcome, executionErr, reportErr := RunActivityTask(context.Background(), task, runtime, waitCtx(t, false), nil)
			if elapsed := time.Since(begin); elapsed > 3*time.Second {
				t.Fatalf("activity ran %v past its deadline", elapsed)
			}
			if outcome != "timed_out" || !errors.Is(executionErr, context.DeadlineExceeded) || reportErr != nil {
				t.Fatalf("outcome=%q executionErr=%v reportErr=%v", outcome, executionErr, reportErr)
			}
			if len(runtime.failures) != 0 || len(runtime.completed) != 0 {
				t.Fatalf("reported failures=%v completed=%v, want nothing reported", runtime.failures, runtime.completed)
			}
		})
	}
}

// 心跳得到 4xx 只是一次失败的心跳：Activity 继续执行，下次心跳再试，到截止时间才结束（交给 Server 判定超时）。
func TestRunActivityTaskKeepsRunningWhenHeartbeatGets4xx(t *testing.T) {
	runtime := &rejectingHeartbeatRuntime{startedActivityRuntime{startedTime: time.Now()}}
	begin := time.Now()
	outcome, _, reportErr := RunActivityTask(context.Background(),
		protocol.Task{RunID: "run-1", ActivityID: "a1", ActivityType: "hang", StartToCloseTimeout: 300 * time.Millisecond},
		runtime, waitCtx(t, true), nil)
	if elapsed := time.Since(begin); elapsed < 250*time.Millisecond {
		t.Fatalf("activity ended after %v: a 4xx heartbeat abandoned the attempt", elapsed)
	}
	if outcome != "timed_out" || reportErr != nil {
		t.Fatalf("outcome=%q reportErr=%v, want the attempt to run until its deadline", outcome, reportErr)
	}
}

// Server 明确答复 Run 不存在：结果没有接收方，Activity 的 ctx 立即取消、不上报。
func TestRunActivityTaskAbandonsAttemptWhenRunNotFound(t *testing.T) {
	runtime := &runNotFoundRuntime{}
	begin := time.Now()
	outcome, _, reportErr := RunActivityTask(context.Background(),
		protocol.Task{RunID: "run-1", ActivityID: "a1", ActivityType: "hang", StartToCloseTimeout: time.Hour},
		runtime, waitCtx(t, true), nil)
	if elapsed := time.Since(begin); elapsed > 3*time.Second {
		t.Fatalf("activity ran %v after the server said the run is gone", elapsed)
	}
	if outcome != "cancelled" || reportErr != nil || len(runtime.failures) != 0 || len(runtime.completed) != 0 {
		t.Fatalf("outcome=%q reportErr=%v failures=%v completed=%v, want an abandoned attempt", outcome, reportErr, runtime.failures, runtime.completed)
	}
}
