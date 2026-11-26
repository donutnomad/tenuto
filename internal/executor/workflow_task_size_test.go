package executor

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/program/programtest"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
)

// threeLargeCommands 是 3 条各带 1.5 MiB 输入的调度命令：每条都在单个载荷上限之内，合计超过
// protocol.MaxWorkflowTaskCommandsSize。
func threeLargeCommands() []history.Command {
	blob := strings.Repeat("x", 1536*1024)
	commands := make([]history.Command, 3)
	for i := range commands {
		commands[i] = history.Command{Type: history.CmdScheduleActivity, ActivityID: fmt.Sprintf("a%d", i), ActivityType: "t",
			StartToCloseTimeout: time.Minute, Input: map[string]any{"blob": blob}}
	}
	return commands
}

// 命令总量超过上限：Worker 不提交，直接以 payload_size_limit_exceeded 让 Workflow Task 失败。
func TestRunWorkflowTaskFailsCommandsOverTotalLimit(t *testing.T) {
	backend := &workflowTaskBackendStub{config: programtest.Workflow("wf")}
	result := RunWorkflowTask(context.Background(), context.Background(), protocol.Task{RunID: "run-1", ScheduledEventID: 12}, backend, WorkflowTaskOptions{
		Capabilities: program.CurrentWLangCapabilities(),
		Execute: func(context.Context, *program.WorkflowConfig, []history.Event) ([]history.Command, error) {
			return threeLargeCommands(), nil
		},
	})
	if backend.completedCalls != 0 || backend.failedCause != history.WorkflowTaskFailedCausePayloadSizeLimitExceeded ||
		result.Outcome != "failed" || !errors.Is(result.Error, protocol.ErrPayloadTooLarge) || result.ReportErr != nil {
		t.Fatalf("result=%+v completed=%d cause=%q", result, backend.completedCalls, backend.failedCause)
	}
}

// Server 以 413 拒绝提交：同样改报 payload_size_limit_exceeded，不等 Workflow Task 超时。
func TestRunWorkflowTaskFailsWhenServerRejectsBodySize(t *testing.T) {
	backend := &workflowTaskBackendStub{config: programtest.Workflow("wf"), completedErr: fmt.Errorf("complete: %w", protocol.ErrPayloadTooLarge)}
	result := RunWorkflowTask(context.Background(), context.Background(), protocol.Task{RunID: "run-1", ScheduledEventID: 12}, backend, WorkflowTaskOptions{
		Capabilities: program.CurrentWLangCapabilities(),
		Execute: func(context.Context, *program.WorkflowConfig, []history.Event) ([]history.Command, error) {
			return []history.Command{{Type: history.CmdCompleteWorkflow}}, nil
		},
	})
	if backend.completedCalls != 1 || backend.failedCause != history.WorkflowTaskFailedCausePayloadSizeLimitExceeded || result.ReportErr != nil {
		t.Fatalf("result=%+v completed=%d cause=%q", result, backend.completedCalls, backend.failedCause)
	}
}

// 增量路径：总量超限时不调用 Complete，丢弃 session，并以 payload_size_limit_exceeded 让 Workflow Task 失败。
func TestIncrementalTaskFailsCommandsOverTotalLimit(t *testing.T) {
	session := &incrementalTestSession{commands: threeLargeCommands()}
	opts := incrementalTestOptions(t, session)
	completes := 0
	opts.Complete = func(context.Context, protocol.Task, history.ActivationResult) (history.WorkflowTaskCompletionAck, error) {
		completes++
		return history.WorkflowTaskCompletionAck{}, nil
	}
	task := protocol.Task{RunID: "run-1", ScheduledEventID: 12}
	incremental := RunIncrementalTask(context.Background(), context.Background(), task, opts)
	if completes != 0 || incremental.Failure != IncrementalTaskCompletionFailure || !session.closed {
		t.Fatalf("incremental=%+v completes=%d closed=%v", incremental, completes, session.closed)
	}
	backend := &workflowTaskBackendStub{}
	result := reportIncrementalTaskResult(context.Background(), context.Background(), task, backend, incremental, WorkflowTaskOptions{})
	if backend.failedCause != history.WorkflowTaskFailedCausePayloadSizeLimitExceeded || result.ReportErr != nil {
		t.Fatalf("result=%+v cause=%q", result, backend.failedCause)
	}
}
