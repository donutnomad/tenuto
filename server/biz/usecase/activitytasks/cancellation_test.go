package activitytasks_test

import (
	"context"
	"testing"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/biz/usecase/activitytasks"
)

// closedRunBackend 的 Run 处于给定状态，Activity 仍是与 task 匹配的未关闭 attempt。
type closedRunBackend struct {
	failureBackend
	status history.Status
}

func (b *closedRunBackend) GetWorkflowExecutionState(string) (*execution.WorkflowExecutionState, error) {
	return &execution.WorkflowExecutionState{Status: b.status}, nil
}

func (b *closedRunBackend) GetActivityState(string, int64) (*execution.ActivityState, bool, error) {
	task := runtimeTask()
	return &execution.ActivityState{Attempt: task.Attempt, Stamp: task.Stamp, Status: execution.ActivityStatusEnums.Started}, true, nil
}

// Run 进入任一终态后，仍在执行的 Activity 都收到取消。
func TestCheckActivityTaskCancellationCoversEveryTerminalStatus(t *testing.T) {
	cases := map[history.Status]string{
		history.StatusCompleted:      "run_closed",
		history.StatusFailed:         "run_closed",
		history.StatusContinuedAsNew: "run_closed",
		history.StatusCancelled:      "run_cancelled",
		history.StatusTerminated:     "run_terminated",
	}
	for status, reason := range cases {
		t.Run(string(status), func(t *testing.T) {
			service := activitytasks.NewUseCase(&closedRunBackend{status: status})
			got, err := service.CheckActivityTaskCancellation(context.Background(), runtimeTask())
			if err != nil {
				t.Fatal(err)
			}
			if want := (protocol.ActivityCancellationResult{CancelRequested: true, Reason: reason}); got != want {
				t.Fatalf("got %+v, want %+v", got, want)
			}
		})
	}

	service := activitytasks.NewUseCase(&closedRunBackend{status: history.StatusRunning})
	got, err := service.CheckActivityTaskCancellation(context.Background(), runtimeTask())
	if err != nil || got.CancelRequested {
		t.Fatalf("a running run with a live attempt must keep executing, got %+v err=%v", got, err)
	}
}

func (*closedRunBackend) ReleaseActivityAttempt(context.Context, *execution.Run, int64, int, int64) error {
	return nil
}

func (*closedRunBackend) RefuseIncompatibleActivityClaim(context.Context, protocol.Task, *execution.Run) (bool, error) {
	return false, nil
}

// cancelRequestedBackend 的 Run 已收到取消请求但尚未关闭。
type cancelRequestedBackend struct {
	closedRunBackend
}

func (b *cancelRequestedBackend) GetWorkflowExecutionState(string) (*execution.WorkflowExecutionState, error) {
	return &execution.WorkflowExecutionState{Status: history.StatusRunning, CancelRequested: true}, nil
}

// Workflow 收到取消请求后仍在运行，可能调度清理 Activity：取消请求本身不阻止 attempt 开始，
// 也不让执行中的 attempt 停下。
func TestWorkflowCancellationRequestDoesNotStopActivities(t *testing.T) {
	service := activitytasks.NewUseCase(&cancelRequestedBackend{})
	got, err := service.BeginActivityTask(context.Background(), runtimeTask())
	if err != nil {
		t.Fatal(err)
	}
	if !got.Execute {
		t.Fatalf("begin after workflow cancellation request = %+v, want Execute", got)
	}
	cancel, err := service.CheckActivityTaskCancellation(context.Background(), runtimeTask())
	if err != nil || cancel.CancelRequested {
		t.Fatalf("cancellation check after workflow cancellation request = %+v err=%v, want no cancel", cancel, err)
	}
}
