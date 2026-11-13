package executor

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/program/programtest"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
)

func TestIncrementalFailureReportsTimeoutWithLiveRuntimeContext(t *testing.T) {
	taskCtx, cancel := context.WithDeadline(context.Background(), time.Unix(0, 0))
	defer cancel()
	type contextKey struct{}
	runtimeCtx := context.WithValue(taskCtx, contextKey{}, "task-token")
	backend := &workflowTaskBackendStub{}
	result := reportIncrementalTaskResult(taskCtx, runtimeCtx,
		protocol.Task{RunID: "run-1", ScheduledEventID: 12}, backend,
		IncrementalTaskResult{Failure: IncrementalTaskReadFailure, Err: context.DeadlineExceeded}, WorkflowTaskOptions{})
	if result.Outcome != "timeout" || result.ReportErr != nil || backend.timeoutMessage != context.DeadlineExceeded.Error() {
		t.Fatalf("result=%+v timeout message=%q", result, backend.timeoutMessage)
	}
	if backend.timeoutCtx == nil || backend.timeoutCtx.Err() != nil || backend.timeoutCtx.Value(contextKey{}) != "task-token" {
		t.Fatalf("timeout context=%v", backend.timeoutCtx)
	}
}

func TestIncrementalFailureReportsBlockedExecution(t *testing.T) {
	backend := &workflowTaskBackendStub{}
	want := errors.New("non-deterministic command")
	result := reportIncrementalTaskResult(context.Background(), context.Background(), protocol.Task{RunID: "run-1", ScheduledEventID: 12}, backend,
		IncrementalTaskResult{Failure: IncrementalTaskExecutionFailure, Err: want}, WorkflowTaskOptions{
			BlocksOnError: func(err error) bool { return errors.Is(err, want) }, BlockedCause: "non_deterministic_error",
		})
	if result.Outcome != "failed" || !errors.Is(result.Error, want) || result.ReportErr != nil || backend.failedCause != "non_deterministic_error" {
		t.Fatalf("result=%+v cause=%q", result, backend.failedCause)
	}
}

func TestSelectIncrementalWorkflow(t *testing.T) {
	workflow := programtest.Workflow("wf")
	if selected, err := SelectIncrementalWorkflow(protocol.Task{ExecutorABIVersion: "abi"}, workflow, program.CurrentWLangCapabilities(), "abi"); err != nil || !selected {
		t.Fatalf("matching ABI: selected=%t err=%v", selected, err)
	}
	if selected, err := SelectIncrementalWorkflow(protocol.Task{ExecutorABIVersion: "old"}, workflow, program.CurrentWLangCapabilities(), "abi"); err != nil || selected {
		t.Fatalf("other ABI: selected=%t err=%v", selected, err)
	}
	var unsupported *program.UnsupportedWLangCapabilityError
	if _, err := SelectIncrementalWorkflow(protocol.Task{ExecutorABIVersion: "abi"}, workflow, nil, "abi"); !errors.As(err, &unsupported) {
		t.Fatalf("missing capability: err=%v", err)
	}
}

// session 校验失败写 WorkflowTaskFailed（Server 退避重调度），不产生 CmdFailWorkflow。
func TestIncrementalFailureRetriesInvalidSession(t *testing.T) {
	backend := &workflowTaskBackendStub{}
	want := fmt.Errorf("%w: delta is empty", ErrExecutionSessionInvalid)
	result := reportIncrementalTaskResult(context.Background(), context.Background(), protocol.Task{RunID: "run-1", ScheduledEventID: 12}, backend,
		IncrementalTaskResult{Failure: IncrementalTaskSessionFailure, Err: want}, WorkflowTaskOptions{})
	if result.Outcome != "error" || !errors.Is(result.Error, want) || result.ReportErr != nil {
		t.Fatalf("result=%+v", result)
	}
	if backend.failedCause != history.WorkflowTaskFailedCauseSessionInvalid || backend.completedCalls != 0 {
		t.Fatalf("cause=%q completed calls=%d", backend.failedCause, backend.completedCalls)
	}
}
