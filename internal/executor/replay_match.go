package executor

import (
	"bytes"
	"fmt"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
)

// MatchReplayWithHistory checks the replayed decision sequence against the
// command events committed by earlier Workflow Tasks.
func MatchReplayWithHistory(commands []history.Command, history []history.Event) error {
	commandIndex, historyIndex := 0, 0
	for commandIndex < len(commands) || historyIndex < len(history) {
		if historyIndex < len(history) && skipMatchForEvent(history[historyIndex]) {
			historyIndex++
			continue
		}
		if commandIndex < len(commands) && skipMatchForCommand(commands[commandIndex]) {
			commandIndex++
			continue
		}
		if commandIndex >= len(commands) {
			return NewNonDeterministicError(fmt.Sprintf(
				"nondeterministic workflow: missing replay command for %s (di=%d, hi=%d)",
				describeEvent(history[historyIndex]), commandIndex, historyIndex))
		}
		if historyIndex >= len(history) {
			return NewNonDeterministicError(fmt.Sprintf(
				"nondeterministic workflow: extra replay command %s (di=%d, hi=%d)",
				describeCommand(commands[commandIndex]), commandIndex, historyIndex))
		}
		if !commandMatchesEvent(commands[commandIndex], history[historyIndex]) {
			return NewNonDeterministicError(fmt.Sprintf(
				"nondeterministic workflow: history event is %s, replay command is %s (di=%d, hi=%d)",
				describeEvent(history[historyIndex]), describeCommand(commands[commandIndex]), commandIndex, historyIndex))
		}
		commandIndex++
		historyIndex++
	}
	return nil
}

const VersionMarkerName = "GetVersion"

func skipMatchForCommand(command history.Command) bool {
	return command.Type == history.CmdRecordMarker && command.MarkerName == VersionMarkerName
}

func skipMatchForEvent(event history.Event) bool {
	switch {
	case event.MarkerRecorded != nil && event.MarkerRecorded.MarkerName == VersionMarkerName:
		return true
	case history.IsChangeVersionUpsert(event):
		return true
	case event.WorkflowCompleted != nil, event.WorkflowFailed != nil,
		event.WorkflowCancelled != nil, event.WorkflowContinuedAsNew != nil:
		return true
	}
	return false
}

func commandMatchesEvent(command history.Command, event history.Event) bool {
	switch command.Type {
	case history.CmdScheduleActivity:
		return event.ActivityScheduled != nil &&
			event.ActivityScheduled.ActivityID == command.ActivityID &&
			event.ActivityScheduled.ActivityType == command.ActivityType
	case history.CmdRequestCancelActivity:
		return event.ActivityCancelRequested != nil && event.ActivityCancelRequested.ActivityID == command.ActivityID
	case history.CmdScheduleTimer:
		return event.TimerScheduled != nil && event.TimerScheduled.TimerID == command.TimerID
	case history.CmdCancelTimer:
		return event.TimerCancelled != nil && event.TimerCancelled.TimerID == command.TimerID
	case history.CmdRequestCancelChildWorkflow:
		return event.ChildWorkflowCancelRequested != nil && event.ChildWorkflowCancelRequested.ChildID == command.ChildID
	case history.CmdStartChildWorkflow:
		return event.ChildWorkflowStarted != nil &&
			event.ChildWorkflowStarted.ChildID == command.ChildID &&
			event.ChildWorkflowStarted.Name == command.WorkflowType
	case history.CmdRecordMarker:
		return event.MarkerRecorded != nil && event.MarkerRecorded.MarkerName == command.MarkerName
	case history.CmdCompleteWorkflowUpdate:
		if event.WorkflowUpdateCompleted == nil ||
			event.WorkflowUpdateCompleted.UpdateID != command.UpdateID ||
			event.WorkflowUpdateCompleted.Name != command.Name {
			return false
		}
		if command.UpdateOutcome == nil {
			return event.WorkflowUpdateCompleted.Outcome == nil
		}
		return updateOutcomesEqual(command.UpdateOutcome, event.WorkflowUpdateCompleted.Outcome)
	}
	return false
}

func updateOutcomesEqual(left, right *protocol.UpdateOutcome) bool {
	if left == nil || right == nil {
		return left == right
	}
	if left.Validate() != nil || right.Validate() != nil {
		return false
	}
	if left.Success != nil || right.Success != nil {
		return left.Success != nil && right.Success != nil && payloadsEqual(*left.Success, *right.Success)
	}
	if left.Failure.Message != right.Failure.Message || left.Failure.Type != right.Failure.Type || len(left.Failure.Details) != len(right.Failure.Details) {
		return false
	}
	for i := range left.Failure.Details {
		if !payloadsEqual(left.Failure.Details[i], right.Failure.Details[i]) {
			return false
		}
	}
	return true
}

func payloadsEqual(left, right protocol.Payload) bool {
	canonicalLeft, err := protocol.CanonicalPayload(left)
	if err != nil {
		return false
	}
	canonicalRight, err := protocol.CanonicalPayload(right)
	if err != nil {
		return false
	}
	return canonicalLeft.Encoding == canonicalRight.Encoding && bytes.Equal(canonicalLeft.Data, canonicalRight.Data)
}

func describeCommand(command history.Command) string {
	switch command.Type {
	case history.CmdScheduleActivity:
		return fmt.Sprintf("ScheduleActivity(id=%s, type=%s)", command.ActivityID, command.ActivityType)
	case history.CmdRequestCancelActivity:
		return fmt.Sprintf("RequestCancelActivity(id=%s)", command.ActivityID)
	case history.CmdScheduleTimer:
		return fmt.Sprintf("ScheduleTimer(id=%s)", command.TimerID)
	case history.CmdCancelTimer:
		return fmt.Sprintf("CancelTimer(id=%s)", command.TimerID)
	case history.CmdRequestCancelChildWorkflow:
		return fmt.Sprintf("RequestCancelChildWorkflow(child=%s)", command.ChildID)
	case history.CmdStartChildWorkflow:
		return fmt.Sprintf("StartChildWorkflow(child=%s, workflow=%s)", command.ChildID, command.WorkflowType)
	case history.CmdRecordMarker:
		return fmt.Sprintf("RecordMarker(name=%s, id=%s)", command.MarkerName, command.MarkerID)
	case history.CmdCompleteWorkflowUpdate:
		return fmt.Sprintf("CompleteWorkflowUpdate(name=%s, id=%s)", command.Name, command.UpdateID)
	}
	return string(command.Type)
}

func describeEvent(event history.Event) string {
	switch {
	case event.ActivityScheduled != nil:
		return fmt.Sprintf("ActivityScheduled(id=%s, type=%s)", event.ActivityScheduled.ActivityID, event.ActivityScheduled.ActivityType)
	case event.ActivityCancelRequested != nil:
		return fmt.Sprintf("ActivityCancelRequested(id=%s)", event.ActivityCancelRequested.ActivityID)
	case event.TimerScheduled != nil:
		return fmt.Sprintf("TimerScheduled(id=%s)", event.TimerScheduled.TimerID)
	case event.TimerCancelled != nil:
		return fmt.Sprintf("TimerCancelled(id=%s)", event.TimerCancelled.TimerID)
	case event.ChildWorkflowCancelRequested != nil:
		return fmt.Sprintf("ChildWorkflowCancelRequested(child=%s)", event.ChildWorkflowCancelRequested.ChildID)
	case event.ChildWorkflowStarted != nil:
		return fmt.Sprintf("ChildWorkflowStarted(child=%s, workflow=%s)", event.ChildWorkflowStarted.ChildID, event.ChildWorkflowStarted.Name)
	case event.MarkerRecorded != nil:
		return fmt.Sprintf("MarkerRecorded(name=%s, id=%s)", event.MarkerRecorded.MarkerName, event.MarkerRecorded.MarkerID)
	case event.WorkflowUpdateCompleted != nil:
		return fmt.Sprintf("WorkflowUpdateCompleted(name=%s, id=%s)", event.WorkflowUpdateCompleted.Name, event.WorkflowUpdateCompleted.UpdateID)
	}
	return string(event.EventType())
}
