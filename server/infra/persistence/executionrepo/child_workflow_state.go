package executionrepo

import (
	"github.com/donutnomad/gsql"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	infradb "github.com/donutnomad/tenuto/server/infra/db"
)

func (s *Repository) GetChildWorkflowState(runID string, scheduledEventID int64) (*execution.ChildWorkflowState, bool, error) {
	row, err := gsql.From[ChildWorkflowStatePO](ChildWorkflowStateSchema).
		Where(
			ChildWorkflowStateSchema.RunID.Eq(runID),
			ChildWorkflowStateSchema.ScheduledEventID.Eq(scheduledEventID),
		).Take(s.db)
	if err != nil {
		return nil, false, err
	}
	if row == nil {
		return nil, false, nil
	}
	st, err := childWorkflowStateRowToState(row)
	if err != nil {
		return nil, false, err
	}
	return st, true, nil
}

func (s *Repository) UpsertChildWorkflowState(state *execution.ChildWorkflowState) error {
	return s.upsertChildWorkflowState(s.db, state)
}

// upsertChildWorkflowState 接受任意数据库句柄（含事务），供 gormExecutionTx 复用。
func (s *Repository) upsertChildWorkflowState(db infradb.DB, state *execution.ChildWorkflowState) error {
	row, err := childWorkflowStateToRow(state)
	if err != nil {
		return err
	}
	changed := state.ExportPatch()
	patch := new(ChildWorkflowStatePO).ToPatch(state)
	if changed.Input.IsPresent() {
		patch["input_json"] = row.InputJSON
	}
	if changed.Output.IsPresent() {
		patch["output_json"] = row.OutputJSON
	}
	err = insertOrPatch(db, row, patch, func() error {
		return gsql.From[ChildWorkflowStatePO](ChildWorkflowStateSchema).
			Where(ChildWorkflowStateSchema.RunID.Eq(state.RunID), ChildWorkflowStateSchema.ScheduledEventID.Eq(state.ScheduledEventID)).
			Update(db, patch).Error
	})
	if err != nil {
		return err
	}
	state.ClearPatch()
	return nil
}

func (s *Repository) ListChildWorkflowStates(runID string) ([]*execution.ChildWorkflowState, error) {
	rows, err := gsql.From[ChildWorkflowStatePO](ChildWorkflowStateSchema).
		Where(ChildWorkflowStateSchema.RunID.Eq(runID)).
		Order(ChildWorkflowStateSchema.ScheduledEventID).Find(s.db)
	if err != nil {
		return nil, err
	}
	out := make([]*execution.ChildWorkflowState, 0, len(rows))
	for i := range rows {
		st, err := childWorkflowStateRowToState(rows[i])
		if err != nil {
			return nil, err
		}
		out = append(out, st)
	}
	return out, nil
}

func childWorkflowStateToRow(st *execution.ChildWorkflowState) (*ChildWorkflowStatePO, error) {
	input, err := marshalMap(st.Input)
	if err != nil {
		return nil, err
	}
	output, err := marshalMap(st.Output)
	if err != nil {
		return nil, err
	}
	row := new(ChildWorkflowStatePO).ToPO(st)
	row.InputJSON = input
	row.OutputJSON = output
	return row, nil
}

func childWorkflowStateRowToState(row *ChildWorkflowStatePO) (*execution.ChildWorkflowState, error) {
	input, err := unmarshalMap(row.InputJSON)
	if err != nil {
		return nil, err
	}
	output, err := unmarshalMap(row.OutputJSON)
	if err != nil {
		return nil, err
	}
	return &execution.ChildWorkflowState{
		RunID:                 row.RunID,
		ScheduledEventID:      row.ScheduledEventID,
		ChildID:               row.ChildID,
		ChildRunID:            row.ChildRunID,
		WorkflowType:          row.WorkflowType,
		WorkflowConfigVersion: row.WorkflowConfigVersion,
		Status:                execution.ChildWorkflowStatus(row.Status),
		ParentClosePolicy:     protocol.ParentClosePolicy(row.ParentClosePolicy),
		CancelRequested:       row.CancelRequested,
		Input:                 input,
		Output:                output,
		Error:                 row.Error,
		Reason:                row.Reason,
		CreatedAt:             row.CreatedAt,
		UpdatedAt:             row.UpdatedAt,
	}, nil
}
