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

// ============================================
// 缺陷 6：activitytasks.UseCase 异步完成单入口
// ============================================
// failure == nil = 成功；failure != nil = 失败（空 Message 仍失败）。
// 入口复用 protocol.Failure.Validate：超限载荷在服务层明确失败、不落到后端。
// ============================================

// completionBackend 记录完成与失败两条路径的调用。
type completionBackend struct {
	completed int
	failures  []protocol.Failure
}

func (b *completionBackend) GetWorkflowExecutionState(string) (*execution.WorkflowExecutionState, error) {
	return &execution.WorkflowExecutionState{Status: history.StatusRunning}, nil
}
func (b *completionBackend) TaskRun(task protocol.Task) (*execution.Run, error) {
	return &execution.Run{ID: task.RunID, Namespace: task.Namespace}, nil
}
func (b *completionBackend) GetActivityState(string, int64) (*execution.ActivityState, bool, error) {
	return nil, false, nil
}
func (b *completionBackend) StartActivityAttempt(protocol.Task, *execution.Run, time.Duration, time.Duration) (time.Time, bool, error) {
	return time.Time{}, true, nil
}
func (b *completionBackend) CompleteActivityAttempt(context.Context, *execution.Run, int64, int, int64, map[string]any) error {
	b.completed++
	return nil
}
func (b *completionBackend) FailActivityAttempt(_ context.Context, _ *execution.Run, _ int64, _ int, _ int64, failure protocol.Failure) error {
	b.failures = append(b.failures, failure)
	return nil
}
func (b *completionBackend) HandleActivityHeartbeat(string, int64, int, int64, map[string]any) error {
	return nil
}

// Happy Path: failure 为 nil 上报成功，且不触发失败上报。
func TestCompleteActivityTaskNilFailureCompletes(t *testing.T) {
	backend := &completionBackend{}
	service := activitytasks.NewUseCase(backend)

	if err := service.CompleteActivityTask(context.Background(), runtimeTask(), map[string]any{"paid": true}, nil); err != nil {
		t.Fatalf("CompleteActivityTask: %v", err)
	}
	if backend.completed != 1 {
		t.Fatalf("completed calls = %d, want 1", backend.completed)
	}
	if len(backend.failures) != 0 {
		t.Fatalf("nil failure must not report a failure: %+v", backend.failures)
	}
}

// Error Case: 空 Message 的失败仍是失败，结构化字段原样传给后端。
func TestCompleteActivityTaskKeepsEmptyMessageFailure(t *testing.T) {
	backend := &completionBackend{}
	service := activitytasks.NewUseCase(backend)

	failure := &protocol.Failure{Type: "PaymentDeclined", NonRetryable: true}
	if err := service.CompleteActivityTask(context.Background(), runtimeTask(), nil, failure); err != nil {
		t.Fatalf("CompleteActivityTask: %v", err)
	}
	if backend.completed != 0 {
		t.Fatalf("empty-message failure must not complete, completed calls = %d", backend.completed)
	}
	if len(backend.failures) != 1 {
		t.Fatalf("failure calls = %d, want 1 (%+v)", len(backend.failures), backend.failures)
	}
	got := backend.failures[0]
	if got.Type != "PaymentDeclined" || !got.NonRetryable || got.Message != "" {
		t.Fatalf("structured failure mangled: %+v", got)
	}
}

// Error Case: 入口校验复用 protocol.Failure.Validate——超限 details 在服务层失败，不落到后端。
func TestCompleteActivityTaskRejectsInvalidDetails(t *testing.T) {
	backend := &completionBackend{}
	service := activitytasks.NewUseCase(backend)

	payload, err := protocol.NewJSONPayload("x")
	if err != nil {
		t.Fatalf("NewJSONPayload: %v", err)
	}
	details := make(protocol.Payloads, 1025) // 超过 maxUpdatePayloadCount=1024
	for i := range details {
		details[i] = payload
	}
	err = service.CompleteActivityTask(context.Background(), runtimeTask(), nil, &protocol.Failure{Type: "T", Details: details})
	if err == nil || !strings.Contains(err.Error(), "invalid activity failure") {
		t.Fatalf("超限失败应明确报错, got %v", err)
	}
	if backend.completed != 0 || len(backend.failures) != 0 {
		t.Fatalf("校验失败不应落到后端: %+v", backend)
	}
}

func (*completionBackend) ReleaseActivityAttempt(context.Context, *execution.Run, int64, int, int64) error {
	return nil
}

func (*completionBackend) RefuseIncompatibleActivityClaim(context.Context, protocol.Task, *execution.Run) (bool, error) {
	return false, nil
}
