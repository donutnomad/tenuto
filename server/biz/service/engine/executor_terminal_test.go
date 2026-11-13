package engine

import (
	"testing"

	"github.com/donutnomad/tenuto/internal/executor/interpreter"
	"github.com/donutnomad/tenuto/program/programtest"
	"github.com/donutnomad/tenuto/protocol/history"
)

func TestWorkflowExecutorReturnsEmptyCommandsOnTerminalEvent(t *testing.T) {
	tests := []struct {
		name          string
		terminalEvent history.Event
	}{
		{
			"Happy Path: WorkflowCompleted 终态返回空 commands",
			history.Event{WorkflowCompleted: &history.WorkflowCompletedEvent{Output: map[string]any{"done": true}}},
		},
		{
			"Happy Path: WorkflowFailed 终态返回空 commands",
			history.Event{WorkflowFailed: &history.WorkflowFailedEvent{Error: "something went wrong"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			events := []history.Event{
				{WorkflowStarted: &history.WorkflowStartedEvent{Input: map[string]any{}}},
				tt.terminalEvent,
			}

			commands, err := interpreter.NewWorkflowExecutor(programtest.Workflow("terminal"), events).Execute()
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(commands) != 0 {
				t.Fatalf("expected empty commands for terminal state, got %d: %+v", len(commands), commands)
			}
		})
	}
}
