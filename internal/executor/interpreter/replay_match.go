package interpreter

import (
	"github.com/donutnomad/tenuto/protocol/history"
)

func (e *WorkflowExecutor) canInReplay() bool {
	for _, command := range e.replayCommands {
		if command.Type == history.CmdContinueAsNew {
			return true
		}
	}
	return false
}
