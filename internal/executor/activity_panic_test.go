package executor

import (
	"context"
	"strings"
	"testing"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/sdk/activity"
)

// recordingActivityRuntime 记录 Activity 的上报结果。
type recordingActivityRuntime struct {
	failures  []protocol.Failure
	completed []map[string]any
}

func (r *recordingActivityRuntime) BeginActivityTask(context.Context, protocol.Task) (protocol.ActivityTaskBeginResult, error) {
	return protocol.ActivityTaskBeginResult{Execute: true}, nil
}

func (r *recordingActivityRuntime) RecordActivityHeartbeat(context.Context, protocol.Task, map[string]any) (protocol.ActivityHeartbeatResult, error) {
	return protocol.ActivityHeartbeatResult{}, nil
}

func (r *recordingActivityRuntime) CompleteActivityTask(_ context.Context, _ protocol.Task, output map[string]any, _ *protocol.Failure) error {
	r.completed = append(r.completed, output)
	return nil
}

func (r *recordingActivityRuntime) FailActivityTask(_ context.Context, _ protocol.Task, failure protocol.Failure) error {
	r.failures = append(r.failures, failure)
	return nil
}

// Error Case：Activity panic 被收成 PanicError 失败并走正常的失败上报，随后的任务照常执行。
func TestRunActivityTaskRecoversPanic(t *testing.T) {
	registry := activity.NewRegistry()
	if err := registry.Register("boom", func(context.Context, map[string]any) (map[string]any, error) {
		var orders map[string]int
		orders["o-1"] = 1 // nil map 写入
		return nil, nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register("ok", func(context.Context, map[string]any) (map[string]any, error) {
		return map[string]any{"done": true}, nil
	}); err != nil {
		t.Fatal(err)
	}
	runtime := &recordingActivityRuntime{}

	outcome, executionErr, reportErr := RunActivityTask(context.Background(), protocol.Task{RunID: "run-1", ActivityID: "s1", ActivityType: "boom"}, runtime, registry, nil)
	if outcome != "failed" || executionErr == nil || reportErr != nil {
		t.Fatalf("outcome=%q executionErr=%v reportErr=%v, want failed with an execution error", outcome, executionErr, reportErr)
	}
	if len(runtime.failures) != 1 {
		t.Fatalf("failures = %+v, want one reported failure", runtime.failures)
	}
	failure := runtime.failures[0]
	if failure.Type != "PanicError" || !strings.Contains(failure.Message, "assignment to entry in nil map") || failure.NonRetryable {
		t.Fatalf("failure = %+v, want retryable PanicError carrying the panic value", failure)
	}
	var stack string
	if len(failure.Details) != 1 || failure.Details[0].Decode(&stack) != nil || !strings.Contains(stack, "TestRunActivityTaskRecoversPanic") {
		t.Fatalf("failure details must carry the stack, got %q", stack)
	}

	outcome, executionErr, reportErr = RunActivityTask(context.Background(), protocol.Task{RunID: "run-1", ActivityID: "s2", ActivityType: "ok"}, runtime, registry, nil)
	if outcome != "success" || executionErr != nil || reportErr != nil || len(runtime.completed) != 1 {
		t.Fatalf("next task: outcome=%q executionErr=%v reportErr=%v completed=%v", outcome, executionErr, reportErr, runtime.completed)
	}
}

func (*recordingActivityRuntime) ReleaseActivityTask(context.Context, protocol.Task) error {
	return nil
}
