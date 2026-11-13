package execution

import (
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
)

// UpdateRecordsFromEvents projects accepted and completed updates from History.
// An update accepted but not completed when the Run closed is failed by the
// close: its outcome is protocol.WorkflowClosedUpdateOutcome and its
// CompletedEventID is the terminal event. The result follows from the terminal
// event that the closing transaction commits, so every reader sees the same
// final state without a separate write.
func UpdateRecordsFromEvents(events []history.Event) []history.WorkflowUpdateRecord {
	var records []history.WorkflowUpdateRecord
	index := make(map[string]int)
	var closedEventID int64
	for _, event := range events {
		if history.IsWorkflowTerminalEventType(event.EventType()) {
			closedEventID = event.ID
		}
		switch {
		case event.WorkflowUpdateAccepted != nil:
			updateID := event.WorkflowUpdateAccepted.UpdateID
			index[updateID] = len(records)
			records = append(records, history.WorkflowUpdateRecord{
				UpdateID: updateID, Name: event.WorkflowUpdateAccepted.Name,
				Args:            event.WorkflowUpdateAccepted.Args,
				AcceptedEventID: event.ID,
			})
		case event.WorkflowUpdateCompleted != nil:
			updateID := event.WorkflowUpdateCompleted.UpdateID
			if idx, ok := index[updateID]; ok {
				records[idx].Outcome = event.WorkflowUpdateCompleted.Outcome
				records[idx].CompletedEventID = event.ID
			} else {
				index[updateID] = len(records)
				records = append(records, history.WorkflowUpdateRecord{
					UpdateID: updateID, Name: event.WorkflowUpdateCompleted.Name,
					Outcome:          event.WorkflowUpdateCompleted.Outcome,
					CompletedEventID: event.ID,
				})
			}
		}
	}
	if closedEventID != 0 {
		for i := range records {
			if records[i].CompletedEventID == 0 {
				records[i].Outcome = protocol.WorkflowClosedUpdateOutcome()
				records[i].CompletedEventID = closedEventID
			}
		}
	}
	return records
}

// CompleteUpdateResult 把 handler 的执行结果定型为对外结果。Outcome 是唯一权威来源：
// Failure 非 nil 即失败（message 可以为空）；否则成功。
func CompleteUpdateResult(result protocol.WorkflowUpdateResult, outcome *history.UpdateOutcome) protocol.WorkflowUpdateResult {
	result.Stage = protocol.UpdateReachedStageEnums.Completed
	result.Outcome = outcome
	return result
}

// RecordedUpdateResult 返回 runID 的 History 里已有的 Update 记录对应的结果：已完成给出结果，
// 否则停在 Accepted。
func RecordedUpdateResult(runID, updateID string, existing history.WorkflowUpdateRecord) protocol.WorkflowUpdateResult {
	result := protocol.WorkflowUpdateResult{
		UpdateID: updateID, RunID: runID, AcceptedEventID: existing.AcceptedEventID,
		Stage: protocol.UpdateReachedStageEnums.Accepted,
	}
	if existing.CompletedEventID != 0 {
		return CompleteUpdateResult(result, existing.Outcome)
	}
	return result
}

// AcceptOrReuseUpdate makes the idempotency decision inside the Run write
// boundary. The caller schedules a Workflow Task only when scheduled is true.
// An update already recorded in History returns its recorded result, also
// after the Run closed; only a new update requires an active Run.
func AcceptOrReuseUpdate(tx ExecutionTx, current *Run, treeID, branchID, name, updateID string, args history.Payloads, existing history.WorkflowUpdateRecord, hasExisting bool) (result protocol.WorkflowUpdateResult, scheduled bool, err error) {
	if hasExisting {
		return RecordedUpdateResult(current.ID, updateID, existing), false, nil
	}
	acceptedID, err := AcceptUpdate(tx, current, treeID, branchID, name, updateID, args)
	if err != nil {
		return protocol.WorkflowUpdateResult{}, false, err
	}
	return protocol.WorkflowUpdateResult{
		UpdateID: updateID, RunID: current.ID, AcceptedEventID: acceptedID,
		Stage: protocol.UpdateReachedStageEnums.Accepted,
	}, true, nil
}
