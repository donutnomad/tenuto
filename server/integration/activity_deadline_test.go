package integration

import (
	"testing"

	historypkg "github.com/donutnomad/tenuto/protocol/history"
)

// Hang 只在 ctx 结束时返回（带 ctx 的外部调用遇到对端挂起）。超时之后 Workflow 再调度一个 Echo。
const hangThenEchoSrc = `package flows

import (
	"time"

	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/activities"
)

func W(ctx workflow.Context, input map[string]any) (map[string]any, error) {
	var out map[string]any
	err := workflow.ExecuteActivity(ctx, activities.Hang, input, workflow.ActivityOptions{OPTS}).Get(ctx, &out)
	if workflow.TimeoutTypeOf(err) == "" {
		return nil, err
	}
	if err := workflow.ExecuteActivity(ctx, activities.Echo, map[string]any{"next": true}, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &out); err != nil {
		return nil, err
	}
	return out, nil
}
`

// Worker 只有 1 个 Activity 槽位。Activity 的 ctx 在 ScheduleToClose 到期时结束，Server 判定超时之后槽位已释放，
// Workflow 的下一个 Activity 照常执行。
func TestActivityContextEndsAtScheduleToClose(t *testing.T) {
	for _, tc := range []struct{ name, opts string }{
		{"schedule_to_close_only", `ScheduleToCloseTimeout: 500 * time.Millisecond`},
		{"schedule_to_close_before_start_to_close", `ScheduleToCloseTimeout: 500 * time.Millisecond, StartToCloseTimeout: time.Hour`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newGoWorkerHarness(t, 1)
			h.register("hang-"+tc.name, replaceAll(hangThenEchoSrc, "OPTS", tc.opts))
			runID := h.start("hang-"+tc.name, "", map[string]any{})
			out := h.output(runID)
			if out["next"] != true {
				t.Fatalf("output = %v, want the echo of the next activity", out)
			}
			if h.count("HangCtxDone").Load() == 0 {
				t.Errorf("the hang activity's ctx never ended")
			}
			events := h.events(runID)
			if countEvent(events, historypkg.EventActivityTimedOut) != 1 {
				t.Errorf("history %s, want one ActivityTimedOut", eventTypes(events))
			}
		})
	}
}

// 只设 StartToClose（2 次尝试）、Activity 按 ctx 返回：Worker 不上报 ctx 截止，Workflow 稳定收到 StartToClose 超时错误。
const startToCloseCtxSrc = `package flows

import (
	"time"

	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/activities"
)

func W(ctx workflow.Context, input map[string]any) (map[string]any, error) {
	var out map[string]any
	err := workflow.ExecuteActivity(ctx, activities.Hang, input, workflow.ActivityOptions{StartToCloseTimeout: 300 * time.Millisecond, RetryPolicy: workflow.RetryPolicy{MaxAttempts: 2, InitialInterval: 50 * time.Millisecond}}).Get(ctx, &out)
	res := map[string]any{"msg": "", "startToClose": workflow.TimeoutTypeOf(err) == workflow.TimeoutTypeStartToClose}
	if err != nil {
		res["msg"] = err.Error()
	}
	return res, nil
}
`

func TestStartToCloseReachesWorkflowAsTimeoutWhenActivityHonoursContext(t *testing.T) {
	h := newGoWorkerHarness(t, 2)
	h.register("ctx-timeout", startToCloseCtxSrc)
	for i := 0; i < 3; i++ {
		runID := h.start("ctx-timeout", "", map[string]any{})
		out := h.output(runID)
		events := h.events(runID)
		if out["startToClose"] != true {
			t.Errorf("run %d: TimeoutTypeOf(err) != StartToClose, workflow saw %q; history %s", i, out["msg"], eventTypes(events))
		}
		// 每次 attempt 都由 Server 判定超时。
		if got := countEvent(events, historypkg.EventActivityTimedOut); got != 2 {
			t.Errorf("run %d: ActivityTimedOut = %d, want 2; history %s", i, got, eventTypes(events))
		}
	}
}
