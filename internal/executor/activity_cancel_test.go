package executor

import (
	"context"
	"errors"
	"testing"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/sdk/activity"
	"github.com/donutnomad/tenuto/sdk/apperror"
)

// cancellingActivityRuntime 的心跳响应带回 reason 这个取消原因。
type cancellingActivityRuntime struct {
	recordingActivityRuntime
	reason string
}

func (r *cancellingActivityRuntime) RecordActivityHeartbeat(context.Context, protocol.Task, map[string]any) (protocol.ActivityHeartbeatResult, error) {
	return protocol.ActivityHeartbeatResult{CancelRequested: true, Reason: r.reason}, nil
}

// Workflow 请求取消 Activity 之后，Activity 以取消错误或 context.Canceled 结束时上报取消确认（CanceledError 失败，
// 带 details）；返回其他错误时照常上报失败；成功仍上报完成。心跳说明结果无人接收时什么也不报。
func TestRunActivityTaskReportsCancellation(t *testing.T) {
	cases := []struct {
		name        string
		reason      string
		result      func(ctx context.Context) (map[string]any, error)
		wantOutcome string
		wantType    string // 上报失败的 Type；空表示没有失败上报
		wantDetails int
		wantDone    int
	}{
		{"canceled error with details", protocol.ActivityCancelRequestedReason, func(ctx context.Context) (map[string]any, error) {
			return nil, apperror.NewCanceled("half done")
		}, "cancelled", protocol.CanceledFailureType, 1, 0},
		{"context canceled", protocol.ActivityCancelRequestedReason, func(ctx context.Context) (map[string]any, error) {
			return nil, ctx.Err()
		}, "cancelled", protocol.CanceledFailureType, 0, 0},
		{"other error", protocol.ActivityCancelRequestedReason, func(ctx context.Context) (map[string]any, error) {
			return nil, errors.New("disk full")
		}, "failed", "", 0, 0},
		{"completion", protocol.ActivityCancelRequestedReason, func(ctx context.Context) (map[string]any, error) {
			return map[string]any{"ok": true}, nil
		}, "success", "", 0, 1},
		{"result abandoned", "run_closed", func(ctx context.Context) (map[string]any, error) {
			return nil, ctx.Err()
		}, "cancelled", "", 0, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			registry := activity.NewRegistry()
			if err := registry.Register("work", func(ctx context.Context, _ map[string]any) (map[string]any, error) {
				activity.RecordHeartbeat(ctx, nil)
				return tc.result(ctx)
			}); err != nil {
				t.Fatal(err)
			}
			runtime := &cancellingActivityRuntime{reason: tc.reason}
			outcome, _, reportErr := RunActivityTask(context.Background(), protocol.Task{RunID: "run-1", ActivityID: "s1", ActivityType: "work"}, runtime, registry, nil)
			if outcome != tc.wantOutcome || reportErr != nil {
				t.Fatalf("outcome = %q reportErr = %v, want %q", outcome, reportErr, tc.wantOutcome)
			}
			if len(runtime.completed) != tc.wantDone {
				t.Fatalf("completions = %d, want %d", len(runtime.completed), tc.wantDone)
			}
			switch {
			case tc.wantType != "":
				if len(runtime.failures) != 1 || runtime.failures[0].Type != tc.wantType || len(runtime.failures[0].Details) != tc.wantDetails {
					t.Fatalf("failures = %+v, want one %s with %d details", runtime.failures, tc.wantType, tc.wantDetails)
				}
			case tc.wantOutcome == "failed":
				if len(runtime.failures) != 1 || runtime.failures[0].Type == protocol.CanceledFailureType {
					t.Fatalf("failures = %+v, want one ordinary failure", runtime.failures)
				}
			default:
				if len(runtime.failures) != 0 {
					t.Fatalf("failures = %+v, want none", runtime.failures)
				}
			}
		})
	}
}
