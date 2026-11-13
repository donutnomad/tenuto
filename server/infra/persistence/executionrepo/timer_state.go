package executionrepo

import (
	"fmt"

	"github.com/donutnomad/gsql"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	infradb "github.com/donutnomad/tenuto/server/infra/db"
)

func (s *Repository) GetTimerState(runID string, scheduledEventID int64) (*execution.TimerState, bool, error) {
	row, err := gsql.From[TimerStatePO](TimerStateSchema).
		Where(
			TimerStateSchema.RunID.Eq(runID),
			TimerStateSchema.ScheduledEventID.Eq(scheduledEventID),
		).Take(s.db)
	if err != nil {
		return nil, false, err
	}
	if row == nil {
		return nil, false, nil
	}
	state, err := timerStateRowToState(row)
	if err != nil {
		return nil, false, err
	}
	return state, true, nil
}

func (s *Repository) UpsertTimerState(state *execution.TimerState) error {
	return s.upsertTimerState(s.db, state)
}

// upsertTimerState 接受任意数据库句柄（含事务），供 RunInTx 复用。
func (s *Repository) upsertTimerState(db infradb.DB, state *execution.TimerState) error {
	row, patch, err := timerStatePatch(state)
	if err != nil {
		return err
	}
	if err := insertOrPatch(db, row, patch, func() error { return updateTimerStateColumns(db, state, patch) }); err != nil {
		return err
	}
	state.ClearPatch()
	return nil
}

// timerStatePatch 返回整行与经 setter 改过的列；trace_context_json 由 ToPatch 之外补上编码后的值。
func timerStatePatch(state *execution.TimerState) (*TimerStatePO, map[string]any, error) {
	row, err := timerStateToRow(state)
	if err != nil {
		return nil, nil, err
	}
	patch := new(TimerStatePO).ToPatch(state)
	if state.ExportPatch().TraceContext.IsPresent() {
		patch["trace_context_json"] = row.TraceContextJSON
	}
	return row, patch, nil
}

func updateTimerStateColumns(db infradb.DB, state *execution.TimerState, patch map[string]any) error {
	return gsql.From[TimerStatePO](TimerStateSchema).
		Where(TimerStateSchema.RunID.Eq(state.RunID), TimerStateSchema.ScheduledEventID.Eq(state.ScheduledEventID)).
		Update(db, patch).Error
}

// updateTimerState 只对已存在的行写 patch，空 patch 不写库。
func updateTimerState(db infradb.DB, state *execution.TimerState) error {
	_, patch, err := timerStatePatch(state)
	if err != nil {
		return err
	}
	if len(patch) > 0 {
		if err := updateTimerStateColumns(db, state, patch); err != nil {
			return err
		}
	}
	state.ClearPatch()
	return nil
}

func (s *Repository) ListTimerStates(runID string) ([]*execution.TimerState, error) {
	rows, err := gsql.From[TimerStatePO](TimerStateSchema).
		Where(TimerStateSchema.RunID.Eq(runID)).
		Order(TimerStateSchema.ScheduledEventID).Find(s.db)
	if err != nil {
		return nil, err
	}
	out := make([]*execution.TimerState, 0, len(rows))
	for i := range rows {
		state, err := timerStateRowToState(rows[i])
		if err != nil {
			return nil, err
		}
		out = append(out, state)
	}
	return out, nil
}

func timerStateToRow(st *execution.TimerState) (*TimerStatePO, error) {
	traceContext, err := traceContextColumn(st.TraceContext)
	if err != nil {
		return nil, fmt.Errorf("timer trace context: %w", err)
	}
	row := new(TimerStatePO).ToPO(st)
	row.TraceContextJSON = traceContext
	return row, nil
}

func timerStateRowToState(row *TimerStatePO) (*execution.TimerState, error) {
	traceContext, err := traceContextFromColumn(row.TraceContextJSON)
	if err != nil {
		return nil, fmt.Errorf("timer trace context: %w", err)
	}
	return &execution.TimerState{
		RunID:            row.RunID,
		ScheduledEventID: row.ScheduledEventID,
		TimerID:          row.TimerID,
		Deadline:         row.Deadline,
		Status:           execution.TimerStatus(row.Status),
		Kind:             execution.TimerKind(row.Kind),
		TraceContext:     traceContext,
		CreatedAt:        row.CreatedAt,
		UpdatedAt:        row.UpdatedAt,
	}, nil
}
