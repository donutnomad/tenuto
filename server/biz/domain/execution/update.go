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

// UpdateDedupeKeys 返回一个 Update 的 WorkflowUpdateAccepted 与 WorkflowUpdateCompleted 的去重键（history.DedupeKey）。
// 两类事件都按去重键写入，按这两个键读到的就是这个 Update 在 History 里的全部事件。
func UpdateDedupeKeys(updateID string) []string {
	var keys []string
	for _, event := range []history.Event{
		history.NewWorkflowUpdateAcceptedEvent("", updateID, nil),
		{WorkflowUpdateCompleted: &history.WorkflowUpdateCompletedEvent{UpdateID: updateID}},
	} {
		if key, ok := history.DedupeKey(event); ok {
			keys = append(keys, key)
		}
	}
	return keys
}

// UpdateRecordFromEvents 从一个 Update 自己的事件（UpdateDedupeKeys 读到的）组装它的记录，与 UpdateRecordsFromEvents
// 在完整 History 上的结果相同。closedEventID 是 Run 的终态事件 ID（未结束为 0）：已接受、未完成的 Update 以
// AcceptedUpdateCompletedWorkflow 失败结束在这个事件上。
func UpdateRecordFromEvents(updateID string, events []history.Event, closedEventID int64) (history.WorkflowUpdateRecord, bool) {
	for _, record := range UpdateRecordsFromEvents(events) {
		if record.UpdateID != updateID {
			continue
		}
		if record.CompletedEventID == 0 && closedEventID != 0 {
			record.Outcome = protocol.WorkflowClosedUpdateOutcome()
			record.CompletedEventID = closedEventID
		}
		return record, true
	}
	return history.WorkflowUpdateRecord{}, false
}

// UpdateCompletedIn 在一段新增事件里找 updateID 的 WorkflowUpdateCompleted，返回它的结果。
func UpdateCompletedIn(updateID string, events []history.Event) (*history.UpdateOutcome, bool) {
	for _, event := range events {
		if completed := event.WorkflowUpdateCompleted; completed != nil && completed.UpdateID == updateID {
			return completed.Outcome, true
		}
	}
	return nil, false
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
