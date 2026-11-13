package executionrepo

import (
	"fmt"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
)

// currentMarkerValue 返回 is_current=true 时写入 current_marker 列的拼接串，唯一约束以此判重。
func currentMarkerValue(namespace, workflowID string) string {
	return protocol.NormalizeNamespace(namespace) + "/" + workflowID
}

// currentMarkerFor 把 IsCurrent 映射为 current_marker：true→拼接串指针，false→nil（NULL，不参与唯一约束）。
func currentMarkerFor(isCurrent bool, namespace, workflowID string) *string {
	if !isCurrent {
		return nil
	}
	v := currentMarkerValue(namespace, workflowID)
	return &v
}

func runToRow(run *execution.Run) (*RunPO, error) {
	input, err := marshalMap(run.Input)
	if err != nil {
		return nil, err
	}
	output, err := marshalMap(run.Output)
	if err != nil {
		return nil, err
	}
	searchAttributes, err := marshalStringMap(run.SearchAttributes)
	if err != nil {
		return nil, err
	}
	row := new(RunPO).ToPO(run)
	row.Namespace = protocol.NormalizeNamespace(run.Namespace)
	// attempt 从 1 起计，与 tenuto_runs.attempt 的 DEFAULT 1 一致；GORM 插入时写出零值，不会用到列默认值。
	row.Attempt = max(run.Attempt, 1)
	row.InputJSON = input
	row.OutputJSON = output
	row.SearchAttributesJSON = searchAttributes
	row.CurrentMarker = currentMarkerFor(run.IsCurrent, run.Namespace, run.WorkflowID)
	return row, nil
}

func rowToRun(row *RunPO) (*execution.Run, error) {
	input, err := unmarshalMap(row.InputJSON)
	if err != nil {
		return nil, err
	}
	output, err := unmarshalMap(row.OutputJSON)
	if err != nil {
		return nil, err
	}
	searchAttributes, err := unmarshalStringMap(row.SearchAttributesJSON)
	if err != nil {
		return nil, err
	}
	return &execution.Run{
		ID:                            row.ID,
		Namespace:                     protocol.NormalizeNamespace(row.Namespace),
		WorkflowID:                    row.WorkflowID,
		WorkflowType:                  row.WorkflowType,
		WorkflowConfigVersion:         row.WorkflowConfigVersion,
		OriginalWorkflowConfigVersion: row.OriginalConfigVersion,
		Policy:                        row.Policy,
		PolicySource:                  row.PolicySource,
		TargetWorkerIdentity:          row.TargetWorkerIdentity,
		UpgradeError:                  row.UpgradeError,
		ConfigError:                   row.ConfigError,
		ParentRunID:                   row.ParentRunID,
		ParentChildID:                 row.ParentChildID,
		ParentScheduledEventID:        row.ParentScheduledEventID,
		FirstRunID:                    row.FirstRunID,
		ContinuedRunID:                row.ContinuedRunID,
		ResetRunID:                    row.ResetRunID,
		BranchToken:                   row.BranchTokenJSON,
		Status:                        history.Status(row.Status),
		Input:                         input,
		Output:                        output,
		SearchAttributes:              searchAttributes,
		CreatedAt:                     row.CreatedAt,
		DeadlineAt:                    row.DeadlineAt,
		RunDeadlineAt:                 row.RunDeadlineAt,
		Attempt:                       row.Attempt,
		Initiator:                     row.Initiator,
		IsCurrent:                     row.IsCurrent,
	}, nil
}

// eventToRow 把已经打上时间、归一化并带上 ID 的事件与它的 JSON 编码装成一行。列的取值全部来自事件本身。
func eventToRow(runID, treeID, branchID string, event history.Event, eventJSON []byte, dedupeKey *string) *EventPO {
	return &EventPO{
		RunID:          runID,
		TreeID:         treeID,
		BranchID:       branchID,
		NodeID:         event.ID,
		Type:           string(event.EventType()),
		DedupeKey:      dedupeKey,
		Timestamp:      event.Timestamp,
		EventSizeBytes: int64(len(eventJSON)),
		EventJSON:      eventJSON,
	}
}

func rowToEvent(row *EventPO) (*history.Event, error) {
	event, err := history.DecodeEventJSON(row.EventJSON)
	if err != nil {
		return nil, fmt.Errorf("event row id=%d type=%s: %w", row.ID, row.Type, err)
	}
	return &event, nil
}
