package executionrepo

import (
	"encoding/json"

	"github.com/donutnomad/gsql"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	infradb "github.com/donutnomad/tenuto/server/infra/db"
	"gorm.io/gorm/clause"
)

func (s *Repository) GetActivityState(runID string, scheduledEventID int64) (*execution.ActivityState, bool, error) {
	row, err := gsql.From[ActivityStatePO](ActivityStateSchema).
		Where(
			ActivityStateSchema.RunID.Eq(runID),
			ActivityStateSchema.ScheduledEventID.Eq(scheduledEventID),
		).Take(s.db)
	if err != nil {
		return nil, false, err
	}
	if row == nil {
		return nil, false, nil
	}
	st, err := activityStateRowToState(row)
	if err != nil {
		return nil, false, err
	}
	return st, true, nil
}

func (s *Repository) UpsertActivityState(state *execution.ActivityState) error {
	return s.upsertActivityState(s.db, state)
}

// upsertActivityState 接受任意数据库句柄（含事务），供 gormExecutionTx 复用。
func (s *Repository) upsertActivityState(db infradb.DB, state *execution.ActivityState) error {
	row, patch, err := activityStatePatch(state)
	if err != nil {
		return err
	}
	if err := insertOrPatch(db, row, patch, func() error { return updateActivityStateColumns(db, state, patch) }); err != nil {
		return err
	}
	state.ClearPatch()
	return nil
}

// updateActivityState 只对已存在的行写 patch，空 patch 不写库。
func (s *Repository) updateActivityState(db infradb.DB, state *execution.ActivityState) error {
	_, patch, err := activityStatePatch(state)
	if err != nil {
		return err
	}
	if len(patch) > 0 {
		if err := updateActivityStateColumns(db, state, patch); err != nil {
			return err
		}
	}
	state.ClearPatch()
	return nil
}

func updateActivityStateColumns(db infradb.DB, state *execution.ActivityState, patch map[string]any) error {
	return gsql.From[ActivityStatePO](ActivityStateSchema).
		Where(ActivityStateSchema.RunID.Eq(state.RunID), ActivityStateSchema.ScheduledEventID.Eq(state.ScheduledEventID)).
		Update(db, patch).Error
}

// activityStatePatch 返回整行与经 setter 改过的列；JSON 列由 ToPatch 之外补上编码后的值。
func activityStatePatch(state *execution.ActivityState) (*ActivityStatePO, map[string]any, error) {
	row, err := activityStateToRow(state)
	if err != nil {
		return nil, nil, err
	}
	changed := state.ExportPatch()
	patch := new(ActivityStatePO).ToPatch(state)
	if changed.LastHeartbeat.IsPresent() {
		patch["last_heartbeat_json"] = row.LastHeartbeatJSON
	}
	if changed.RetryPolicy.IsPresent() {
		patch["retry_policy_json"] = row.RetryPolicyJSON
	}
	if changed.Input.IsPresent() {
		patch["input_json"] = row.InputJSON
	}
	if changed.TraceContext.IsPresent() {
		patch["trace_context_json"] = row.TraceContextJSON
	}
	// timeout_at 由状态、各时间点与各超时字段合成，其中任何一个改过就按整份 state 重算这一列。
	if changed.Status.IsPresent() || changed.ScheduledTime.IsPresent() || changed.StartedTime.IsPresent() ||
		changed.NextAttemptAt.IsPresent() || changed.LastHeartbeatAt.IsPresent() ||
		changed.ScheduleToStartTimeoutMs.IsPresent() || changed.StartToCloseTimeoutMs.IsPresent() ||
		changed.ScheduleToCloseTimeoutMs.IsPresent() || changed.HeartbeatTimeoutMs.IsPresent() {
		patch["timeout_at"] = row.TimeoutAt
	}
	return row, patch, nil
}

// insertOrPatch 是 Activity / Timer / Child Workflow state 的 upsert：行不存在时整行插入；
// 行已存在时由 update 只写 patch 里的列，空 patch 不写库。调用方持有的可能是旧快照，
// 整行覆盖会把其它写入方已经提交的字段改回去。三张表的主键都是 (run_id, scheduled_event_id)。
func insertOrPatch(db infradb.DB, row any, patch map[string]any, update func() error) error {
	created := db.Clauses(onDuplicateKeyNoop("run_id")).Create(row)
	if created.Error != nil || created.RowsAffected > 0 || len(patch) == 0 {
		return created.Error
	}
	return update()
}

// onDuplicateKeyNoop 让 INSERT 撞上主键或唯一键时不改任何列：MySQL 上生成
// ON DUPLICATE KEY UPDATE column = column，撞键时 RowsAffected 为 0。column 是表里的任一列，赋值给自己不改变行。
// 不用 INSERT IGNORE（gsql.InsertIgnore）：它会把截断、NOT NULL 等写入错误一并降成警告。
func onDuplicateKeyNoop(column string) clause.OnConflict {
	col := clause.Column{Name: column}
	return clause.OnConflict{DoUpdates: clause.Set{{Column: col, Value: col}}}
}

func (s *Repository) ListActivityStates(runID string) ([]*execution.ActivityState, error) {
	rows, err := gsql.From[ActivityStatePO](ActivityStateSchema).
		Where(ActivityStateSchema.RunID.Eq(runID)).
		Order(ActivityStateSchema.ScheduledEventID).Find(s.db)
	if err != nil {
		return nil, err
	}
	out := make([]*execution.ActivityState, 0, len(rows))
	for i := range rows {
		st, err := activityStateRowToState(rows[i])
		if err != nil {
			return nil, err
		}
		out = append(out, st)
	}
	return out, nil
}

func activityStateToRow(st *execution.ActivityState) (*ActivityStatePO, error) {
	heartbeat, err := marshalMap(st.LastHeartbeat)
	if err != nil {
		return nil, err
	}
	input, err := marshalMap(st.Input)
	if err != nil {
		return nil, err
	}
	traceContext, err := traceContextColumn(st.TraceContext)
	if err != nil {
		return nil, err
	}
	var retryJSON []byte
	if st.RetryPolicy != nil {
		retryJSON, err = json.Marshal(st.RetryPolicy)
		if err != nil {
			return nil, err
		}
	}
	row := new(ActivityStatePO).ToPO(st)
	row.LastHeartbeatJSON = heartbeat
	row.RetryPolicyJSON = retryJSON
	row.InputJSON = input
	row.TraceContextJSON = traceContext
	return row, nil
}

func activityStateRowToState(row *ActivityStatePO) (*execution.ActivityState, error) {
	heartbeat, err := unmarshalMap(row.LastHeartbeatJSON)
	if err != nil {
		return nil, err
	}
	input, err := unmarshalMap(row.InputJSON)
	if err != nil {
		return nil, err
	}
	traceContext, err := traceContextFromColumn(row.TraceContextJSON)
	if err != nil {
		return nil, err
	}
	var retryPolicy *history.RetryPolicy
	if len(row.RetryPolicyJSON) > 0 {
		retryPolicy = &history.RetryPolicy{}
		if err := json.Unmarshal(row.RetryPolicyJSON, retryPolicy); err != nil {
			return nil, err
		}
	}
	return &execution.ActivityState{
		RunID:                    row.RunID,
		ScheduledEventID:         row.ScheduledEventID,
		ActivityID:               row.ActivityID,
		ActivityType:             row.ActivityType,
		TaskQueue:                row.TaskQueue,
		TargetWorkerIdentity:     row.TargetWorkerIdentity,
		Attempt:                  row.Attempt,
		Stamp:                    row.Stamp,
		StartedEventID:           row.StartedEventID,
		ActivityReset:            row.ActivityReset,
		CancelRequested:          row.CancelRequested,
		WorkerIdentity:           row.WorkerIdentity,
		WorkerBuildID:            row.WorkerBuildID,
		LastHeartbeat:            heartbeat,
		LastHeartbeatAt:          row.LastHeartbeatAt,
		RetryLastFailure:         row.RetryLastFailure,
		NextAttemptAt:            row.NextAttemptAt,
		Status:                   execution.ActivityStatus(row.Status),
		ScheduledTime:            row.ScheduledTime,
		StartedTime:              row.StartedTime,
		ScheduleToStartTimeoutMs: row.ScheduleToStartTimeoutMs,
		StartToCloseTimeoutMs:    row.StartToCloseTimeoutMs,
		ScheduleToCloseTimeoutMs: row.ScheduleToCloseTimeoutMs,
		HeartbeatTimeoutMs:       row.HeartbeatTimeoutMs,
		RetryPolicy:              retryPolicy,
		Input:                    input,
		TraceContext:             traceContext,
		CreatedAt:                row.CreatedAt,
		UpdatedAt:                row.UpdatedAt,
	}, nil
}
