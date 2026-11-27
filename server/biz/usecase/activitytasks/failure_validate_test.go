package activitytasks_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/biz/usecase/activitytasks"
)

// failureBackend 是最小后端：只记录收到的失败载荷。
type failureBackend struct {
	failures []protocol.Failure
}

func (b *failureBackend) GetWorkflowExecutionState(string) (*execution.WorkflowExecutionState, error) {
	return &execution.WorkflowExecutionState{Status: history.StatusRunning}, nil
}
func (b *failureBackend) TaskRun(task protocol.Task) (*execution.Run, error) {
	return &execution.Run{ID: task.RunID, Namespace: task.Namespace}, nil
}
func (b *failureBackend) GetActivityState(string, int64) (*execution.ActivityState, bool, error) {
	return nil, false, nil
}
func (b *failureBackend) StartActivityAttempt(protocol.Task, *execution.Run, time.Duration, time.Duration) (time.Time, bool, error) {
	return time.Time{}, true, nil
}
func (b *failureBackend) CompleteActivityAttempt(context.Context, *execution.Run, int64, int, int64, map[string]any) error {
	return nil
}
func (b *failureBackend) FailActivityAttempt(_ context.Context, _ *execution.Run, _ int64, _ int, _ int64, failure protocol.Failure) error {
	b.failures = append(b.failures, failure)
	return nil
}
func (b *failureBackend) HandleActivityHeartbeat(string, int64, int, int64, map[string]any) error {
	return nil
}

func runtimeTask() protocol.Task {
	return protocol.Task{RunID: "run-1", ScheduledEventID: 7, Attempt: 1, Stamp: 1}
}

// 入口复用既有 payload 上限：超限的 details 在服务层明确失败，不落到后端。
func TestFailActivityTaskRejectsInvalidDetails(t *testing.T) {
	backend := &failureBackend{}
	service := activitytasks.NewUseCase(backend)

	payload, err := protocol.NewJSONPayload("x")
	if err != nil {
		t.Fatalf("NewJSONPayload: %v", err)
	}
	details := make(protocol.Payloads, 1025) // 超过 maxUpdatePayloadCount=1024
	for i := range details {
		details[i] = payload
	}
	failure := protocol.Failure{Message: "too many details", Type: "T", Details: details}

	err = service.FailActivityTask(context.Background(), runtimeTask(), failure)
	if err == nil || !strings.Contains(err.Error(), "invalid activity failure") {
		t.Fatalf("超限失败应明确报错, got %v", err)
	}
	if len(backend.failures) != 0 {
		t.Fatalf("超限失败不应落到后端: %+v", backend.failures)
	}
}

// 结构化失败原样传给后端。
func TestFailActivityTaskPassesStructuredFailure(t *testing.T) {
	backend := &failureBackend{}
	service := activitytasks.NewUseCase(backend)

	payload, err := protocol.NewJSONPayload("card-1")
	if err != nil {
		t.Fatalf("NewJSONPayload: %v", err)
	}
	failure := protocol.Failure{Message: "m", Type: "T", Details: protocol.Payloads{payload}}
	if err := service.FailActivityTask(context.Background(), runtimeTask(), failure); err != nil {
		t.Fatalf("FailActivityTask: %v", err)
	}
	if len(backend.failures) != 1 || backend.failures[0].Type != "T" || len(backend.failures[0].Details) != 1 {
		t.Fatalf("结构化失败未透传: %+v", backend.failures)
	}
}

func (*failureBackend) ReleaseActivityAttempt(context.Context, *execution.Run, int64, int, int64) error {
	return nil
}

func (*failureBackend) RefuseIncompatibleActivityClaim(context.Context, protocol.Task, *execution.Run) (bool, error) {
	return false, nil
}

func (b *failureBackend) PayloadSizeLimit() int { return 0 }
