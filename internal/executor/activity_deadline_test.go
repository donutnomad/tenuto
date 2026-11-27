package executor

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/sdk/activity"
)

// rejectingHeartbeatRuntime 以不可重试的 4xx 拒绝心跳（Server 不再认这次 attempt）。
type rejectingHeartbeatRuntime struct {
	recordingActivityRuntime
}

func (r *rejectingHeartbeatRuntime) RecordActivityHeartbeat(context.Context, protocol.Task, map[string]any) (protocol.ActivityHeartbeatResult, error) {
	return protocol.ActivityHeartbeatResult{}, protocol.InvalidRequestf("run not found")
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

// 心跳被 Server 以 4xx 拒绝（Run 不存在等）：结果没有接收方，Activity 的 ctx 立即取消、不上报。
func TestRunActivityTaskAbandonsAttemptRejectedByHeartbeat(t *testing.T) {
	runtime := &rejectingHeartbeatRuntime{}
	begin := time.Now()
	outcome, _, reportErr := RunActivityTask(context.Background(),
		protocol.Task{RunID: "run-1", ActivityID: "a1", ActivityType: "hang", StartToCloseTimeout: time.Hour},
		runtime, waitCtx(t, true), nil)
	if elapsed := time.Since(begin); elapsed > 3*time.Second {
		t.Fatalf("activity ran %v after the heartbeat was rejected", elapsed)
	}
	if outcome != "cancelled" || reportErr != nil || len(runtime.failures) != 0 || len(runtime.completed) != 0 {
		t.Fatalf("outcome=%q reportErr=%v failures=%v completed=%v, want an abandoned attempt", outcome, reportErr, runtime.failures, runtime.completed)
	}
}
