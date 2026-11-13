package engine

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/compiler/sdkgen/sdkgentest"
	"github.com/donutnomad/tenuto/internal/worker"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/sdk/activity"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
)

func TestWorkerRecordsWorkflowTaskTimedOutAndAcksLease(t *testing.T) {
	api := newTimeoutWorkerAPI(t, 20000)
	worker := assembleWorker(api, activity.NewRegistry(), worker.WorkerOptions{
		WorkflowConcurrency: 1,
		ActivityConcurrency: 1,
		WorkflowTaskTimeout: 1 * time.Millisecond,
	})

	worker.HandleWorkflowTask(context.Background(), protocol.TaskLease{
		Task:  protocol.Task{Type: protocol.TaskTypeWorkflow, RunID: api.runID},
		Token: "lease-token",
	})

	if api.workflowTaskTimedOut != 1 {
		t.Fatalf("expected one workflow task timeout event, got %d", api.workflowTaskTimedOut)
	}
	if api.workflowAcks != 1 {
		t.Fatalf("expected workflow task ack, got %d", api.workflowAcks)
	}
	if api.workflowNacks != 0 {
		t.Fatalf("expected no workflow task nack, got %d", api.workflowNacks)
	}
	if api.workflowCompletions != 0 {
		t.Fatalf("expected no workflow task completion after timeout, got %d", api.workflowCompletions)
	}
}

type timeoutWorkerAPI struct {
	runID                 string
	wf                    *program.WorkflowConfig
	events                []history.Event
	workflowTaskTimedOut  int
	workflowTimeoutCtxErr error
	workflowAcks          int
	workflowNacks         int
	workflowCompletions   int
}

// newTimeoutWorkerAPI 构造一个顺序执行 activityCount 个 Activity 的 Workflow 与全部完成的历史：
// 重放它所需的时间远超测试给的 Workflow Task 超时。
func newTimeoutWorkerAPI(t *testing.T, activityCount int) *timeoutWorkerAPI {
	wf := sdkgentest.Loop(t, &program.WorkflowConfig{ID: "wf-timeout"}, "noop", activityCount)
	events := []history.Event{history.NewWorkflowStartedEvent(map[string]any{"request_id": "r-1"}, "", "", "", "", 0)}
	for i := 0; i < activityCount; i++ {
		stepID := fmt.Sprintf("#a%d", i+1)
		eventID := int64(i + 1)
		events = append(events,
			history.Event{ID: eventID, Timestamp: time.Now(), ActivityScheduled: &history.ActivityScheduledEvent{ActivityID: stepID}},
			history.Event{Timestamp: time.Now(), ActivityCompleted: &history.ActivityCompletedEvent{ScheduledEventID: eventID, Output: map[string]any{"ok": true}}},
		)
	}
	return &timeoutWorkerAPI{runID: "run-timeout", wf: wf, events: events}
}

func (a *timeoutWorkerAPI) PollWorkflowTask(ctx context.Context) (protocol.TaskLease, error) {
	return protocol.TaskLease{}, nil
}

func (a *timeoutWorkerAPI) PollActivityTask(ctx context.Context, taskQueue string) (protocol.TaskLease, error) {
	return protocol.TaskLease{}, nil
}

func (a *timeoutWorkerAPI) AckWorkflowTask(ctx context.Context, lease protocol.TaskLease) error {
	a.workflowAcks++
	return nil
}

func (a *timeoutWorkerAPI) NackWorkflowTask(ctx context.Context, lease protocol.TaskLease) error {
	a.workflowNacks++
	return nil
}

func (a *timeoutWorkerAPI) AckActivityTask(ctx context.Context, lease protocol.TaskLease) error {
	return nil
}

func (a *timeoutWorkerAPI) NackActivityTask(ctx context.Context, lease protocol.TaskLease) error {
	return nil
}

func (a *timeoutWorkerAPI) GetWorkflowConfig(_ context.Context, runID, _ string) (*program.WorkflowConfig, error) {
	return a.wf, nil
}

func (a *timeoutWorkerAPI) GetEvents(runID string) ([]history.Event, error) {
	return a.events, nil
}

func (a *timeoutWorkerAPI) GetRunStatus(runID string) (history.Status, error) {
	return history.StatusRunning, nil
}

func (a *timeoutWorkerAPI) HandleWorkflowTaskCompleted(ctx context.Context, runID string, scheduledEventID int64, commands []history.Command, workerIdentity string) error {
	a.workflowCompletions++
	return nil
}

func (a *timeoutWorkerAPI) HandleWorkflowTaskTimedOut(ctx context.Context, runID string, scheduledEventID int64, errMsg string) error {
	a.workflowTaskTimedOut++
	a.workflowTimeoutCtxErr = ctx.Err()
	return nil
}

func (a *timeoutWorkerAPI) HandleWorkflowTaskFailed(ctx context.Context, runID string, scheduledEventID int64, cause, reason string) error {
	return nil
}

func (a *timeoutWorkerAPI) GetActivityState(runID string, scheduledEventID int64) (*execution.ActivityState, bool, error) {
	return nil, false, nil
}

func (a *timeoutWorkerAPI) GetWorkflowExecutionState(runID string) (*execution.WorkflowExecutionState, error) {
	return workerAPIExecutionState(a, runID)
}

func (a *timeoutWorkerAPI) TaskRun(task protocol.Task) (*execution.Run, error) {
	return &execution.Run{ID: task.RunID, Namespace: task.Namespace}, nil
}

func (a *timeoutWorkerAPI) StartActivityAttempt(task protocol.Task, _ *execution.Run, startToCloseTimeout, heartbeatTimeout time.Duration) (time.Time, bool, error) {
	return time.Time{}, true, nil
}

func (a *timeoutWorkerAPI) CompleteActivityAttempt(ctx context.Context, _ *execution.Run, scheduledEventID int64, attempt int, stamp int64, output map[string]any) error {
	return nil
}

func (a *timeoutWorkerAPI) FailActivityAttempt(ctx context.Context, _ *execution.Run, scheduledEventID int64, attempt int, stamp int64, failure protocol.Failure) error {
	return nil
}

func (a *timeoutWorkerAPI) HandleActivityHeartbeat(runID string, scheduledEventID int64, attempt int, stamp int64, details map[string]any) error {
	return nil
}

func (a *timeoutWorkerAPI) CancelWorkflow(runID, reason string) error {
	return nil
}

func (a *timeoutWorkerAPI) TerminateWorkflow(runID, reason string) error {
	return nil
}

func (*timeoutWorkerAPI) GetWorkflowActivation(context.Context, history.WorkflowActivationRequest) (history.WorkflowActivation, error) {
	return history.WorkflowActivation{}, errors.New("incremental activation is not used by this fake")
}

func (*timeoutWorkerAPI) CompleteWorkflowActivation(context.Context, history.WorkflowTaskCompletionRequest) (history.WorkflowTaskCompletionAck, error) {
	return history.WorkflowTaskCompletionAck{}, errors.New("incremental activation is not used by this fake")
}

func (*timeoutWorkerAPI) ReleaseActivityAttempt(context.Context, *execution.Run, int64, int, int64) error {
	return nil
}

func (*timeoutWorkerAPI) RefuseIncompatibleActivityClaim(context.Context, protocol.Task, *execution.Run) (bool, error) {
	return false, nil
}
