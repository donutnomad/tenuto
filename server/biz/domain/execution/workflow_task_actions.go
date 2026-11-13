package execution

import (
	"fmt"

	"github.com/donutnomad/tenuto/protocol/history"
)

// WorkflowTaskAction pairs a command with the event produced by its commit.
// FailWorkflow has no command event because retry handling records its failure.
type WorkflowTaskAction struct {
	Index   int
	Command history.Command
	Event   WorkflowTaskCommandEventResult
	// EventIndex 是命令事件在 CommandEvents 里的位置；FailWorkflow 为 -1。
	EventIndex int
}

func WorkflowTaskActions(commands []history.Command, result WorkflowTaskCompletionResult) ([]WorkflowTaskAction, error) {
	actions := make([]WorkflowTaskAction, 0, len(commands))
	eventIndex := 0
	for index, command := range commands {
		if command.Type == history.CmdFailWorkflow {
			actions = append(actions, WorkflowTaskAction{Index: index, Command: command, EventIndex: -1})
			continue
		}
		if eventIndex >= len(result.CommandEvents) {
			return nil, fmt.Errorf("workflow task command result missing for %s", command.Type)
		}
		actions = append(actions, WorkflowTaskAction{
			Index: index, Command: command, Event: result.CommandEvents[eventIndex], EventIndex: eventIndex,
		})
		eventIndex++
	}
	return actions, nil
}
