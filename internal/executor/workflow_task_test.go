package executor

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/program/programtest"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
)

type workflowTaskBackendStub struct {
	events         []history.Event
	config         *program.WorkflowConfig
	configErr      error
	configReads    int
	completed      []history.Command
	completedCalls int
	completedErr   error
	failedCause    string
	timeoutMessage string
	timeoutCtx     context.Context
	status         history.Status
}

func (s *workflowTaskBackendStub) GetRunStatus(string) (history.Status, error) {
	if s.status == "" {
		return history.StatusRunning, nil
	}
	return s.status, nil
}

func (s *workflowTaskBackendStub) GetEvents(string) ([]history.Event, error) { return s.events, nil }
func (s *workflowTaskBackendStub) GetWorkflowConfig(_ context.Context, _, _ string) (*program.WorkflowConfig, error) {
	s.configReads++
	return s.config, s.configErr
}
func (s *workflowTaskBackendStub) HandleWorkflowTaskCompleted(_ context.Context, _ string, _ int64, commands []history.Command, _ string) error {
	s.completedCalls++
	s.completed = commands
	return s.completedErr
}
func (s *workflowTaskBackendStub) HandleWorkflowTaskTimedOut(ctx context.Context, _ string, _ int64, message string) error {
	s.timeoutCtx = ctx
	s.timeoutMessage = message
	return nil
}
func (s *workflowTaskBackendStub) HandleWorkflowTaskFailed(_ context.Context, _ string, _ int64, cause, _ string) error {
	s.failedCause = cause
	return nil
}

func TestRunWorkflowTaskSkipsClosedTask(t *testing.T) {
	backend := &workflowTaskBackendStub{
		events: []history.Event{history.NewWorkflowTaskCompletedEvent(12, "worker")},
	}
	result := RunWorkflowTask(context.Background(), context.Background(), protocol.Task{RunID: "run-1", ScheduledEventID: 12}, backend,
		WorkflowTaskOptions{Capabilities: program.CurrentWLangCapabilities(), Execute: func(context.Context, *program.WorkflowConfig, []history.Event) ([]history.Command, error) {
			t.Fatal("closed task executed")
			return nil, nil
		}})
	if result.Outcome != "skipped" || result.ReportErr != nil || backend.configReads != 0 {
		t.Fatalf("closed task result = %+v, config reads = %d", result, backend.configReads)
	}
}

func TestRunWorkflowTaskReportsCommandsAndExecutionFailure(t *testing.T) {
	task := protocol.Task{RunID: "run-1", ScheduledEventID: 12, WorkerIdentity: "worker-1"}
	backend := &workflowTaskBackendStub{config: programtest.Workflow("wf")}
	want := []history.Command{{Type: history.CmdCompleteWorkflow}}
	result := RunWorkflowTask(context.Background(), context.Background(), task, backend, WorkflowTaskOptions{
		Capabilities: program.CurrentWLangCapabilities(),
		Execute: func(context.Context, *program.WorkflowConfig, []history.Event) ([]history.Command, error) {
			return want, nil
		},
	})
	if result.Outcome != "" || result.ReportErr != nil || backend.completedCalls != 1 || len(backend.completed) != 1 || backend.completed[0].Type != history.CmdCompleteWorkflow {
		t.Fatalf("completed task result = %+v, commands = %+v", result, backend.completed)
	}

	backend = &workflowTaskBackendStub{config: programtest.Workflow("wf")}
	wantErr := errors.New("history diverged")
	result = RunWorkflowTask(context.Background(), context.Background(), task, backend, WorkflowTaskOptions{
		Capabilities: program.CurrentWLangCapabilities(),
		Execute: func(context.Context, *program.WorkflowConfig, []history.Event) ([]history.Command, error) {
			return nil, wantErr
		},
		BlockedCause: func(err error) string {
			return map[bool]string{true: "non_deterministic_error"}[errors.Is(err, wantErr)]
		},
	})
	if result.Outcome != "failed" || !errors.Is(result.Error, wantErr) || result.ReportErr != nil || backend.failedCause != "non_deterministic_error" || backend.completedCalls != 0 {
		t.Fatalf("blocked task result = %+v, cause = %q", result, backend.failedCause)
	}
}

func TestRunWorkflowTaskReportsTimeoutWithLiveRuntimeContext(t *testing.T) {
	taskCtx, cancel := context.WithDeadline(context.Background(), time.Unix(0, 0))
	defer cancel()
	type contextKey struct{}
	runtimeCtx := context.WithValue(context.Background(), contextKey{}, "task-token")
	backend := &workflowTaskBackendStub{}
	result := RunWorkflowTask(taskCtx, runtimeCtx, protocol.Task{RunID: "run-1", ScheduledEventID: 12}, backend,
		WorkflowTaskOptions{Capabilities: program.CurrentWLangCapabilities(), Execute: func(context.Context, *program.WorkflowConfig, []history.Event) ([]history.Command, error) {
			t.Fatal("timed out task executed")
			return nil, nil
		}})
	if result.Outcome != "timeout" || result.ReportErr != nil || backend.timeoutMessage != context.DeadlineExceeded.Error() {
		t.Fatalf("timeout result = %+v, message = %q", result, backend.timeoutMessage)
	}
	if backend.timeoutCtx == nil || backend.timeoutCtx.Err() != nil || backend.timeoutCtx.Value(contextKey{}) != "task-token" {
		t.Fatalf("timeout runtime context = %v", backend.timeoutCtx)
	}
}

// 取消（Worker 停止的宽限期已过）而不是到期：放弃任务，不上报超时，由 Server 的 Workflow Task 超时重新调度。
func TestRunWorkflowTaskAbandonsCancelledTask(t *testing.T) {
	taskCtx, cancel := context.WithCancel(context.Background())
	cancel()
	backend := &workflowTaskBackendStub{}
	result := RunWorkflowTask(taskCtx, context.Background(), protocol.Task{RunID: "run-1", ScheduledEventID: 12}, backend,
		WorkflowTaskOptions{Capabilities: program.CurrentWLangCapabilities(), Execute: func(context.Context, *program.WorkflowConfig, []history.Event) ([]history.Command, error) {
			t.Fatal("cancelled task executed")
			return nil, nil
		}})
	if result.Outcome != "abandoned" || result.ReportErr != nil || backend.timeoutCtx != nil || backend.completedCalls != 0 {
		t.Fatalf("result = %+v, timeout reported = %v, completed = %d", result, backend.timeoutCtx != nil, backend.completedCalls)
	}
}
