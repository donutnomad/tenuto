package schedulerepo

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/donutnomad/gsql"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/server/biz/domain/schedule"
	infradb "github.com/donutnomad/tenuto/server/infra/db"
	"github.com/samber/mo"
	"gorm.io/gorm"
)

type Repository struct {
	db infradb.DB
}

func New(database infradb.DB) *Repository {
	return &Repository{db: database}
}

// 编译期断言：Repository 实现 schedule.Repository
var _ schedule.Repository = (*Repository)(nil)

// ── ToRow / FromRow ──

func scheduleToRow(sch *schedule.Schedule) (*SchedulePO, error) {
	var inputJSON []byte
	if len(sch.Input) > 0 {
		var err error
		inputJSON, err = json.Marshal(sch.Input)
		if err != nil {
			return nil, fmt.Errorf("schedule marshal input: %w", err)
		}
	}
	row := new(SchedulePO).ToPO(sch)
	row.Namespace = protocol.NormalizeNamespace(sch.Namespace)
	row.InputJSON = inputJSON
	return row, nil
}

func rowToSchedule(row *SchedulePO) (*schedule.Schedule, error) {
	var input map[string]any
	if len(row.InputJSON) > 0 {
		if err := json.Unmarshal(row.InputJSON, &input); err != nil {
			return nil, fmt.Errorf("schedule unmarshal input: %w", err)
		}
	}
	return &schedule.Schedule{
		ID:                  row.ID,
		Namespace:           row.Namespace,
		WorkflowType:        row.WorkflowType,
		Description:         row.Description,
		CronExpression:      row.CronExpression,
		Timezone:            row.Timezone,
		Input:               input,
		Paused:              row.Paused,
		OverlapPolicy:       row.OverlapPolicy,
		LastRunAt:           row.LastRunAt,
		NextRunAt:           row.NextRunAt,
		LastRunID:           row.LastRunID,
		RunCount:            row.RunCount,
		CatchupWindow:       row.CatchupWindow,
		MissedCatchupWindow: row.MissedCatchupWindow,
		BufferDropped:       row.BufferDropped,
		StartFailed:         row.StartFailed,
		BufferDropFrom:      row.BufferDropFrom,
		BufferDropUntil:     row.BufferDropUntil,
		CreatedAt:           row.CreatedAt,
		UpdatedAt:           row.UpdatedAt,
	}, nil
}

// ── schedule.Repository 方法 ──

func (s *Repository) CreateSchedule(entity *schedule.Schedule) error {
	row, err := scheduleToRow(entity)
	if err != nil {
		return err
	}
	if err := gsql.InsertInto(ScheduleSchema).Value(row).Exec(s.db); err != nil {
		if errors.Is(err, gorm.ErrDuplicatedKey) {
			return fmt.Errorf("%w: %s/%s", schedule.ErrAlreadyExists, row.Namespace, entity.ID)
		}
		return err
	}
	entity.ClearPatch()
	return nil
}

func (s *Repository) GetSchedule(namespace, id string) (*schedule.Schedule, error) {
	namespace = protocol.NormalizeNamespace(namespace)
	row, err := gsql.From[SchedulePO](ScheduleSchema).
		Where(
			ScheduleSchema.Namespace.Eq(namespace),
			ScheduleSchema.ID.Eq(id),
		).Take(s.db)
	if err != nil {
		return nil, err
	}
	if row == nil {
		return nil, fmt.Errorf("%w: %s/%s", schedule.ErrNotFound, namespace, id)
	}
	return rowToSchedule(row)
}

func (s *Repository) UpdateSchedule(entity *schedule.Schedule) error {
	row, err := scheduleToRow(entity)
	if err != nil {
		return err
	}
	patch := new(SchedulePO).ToPatch(entity)
	if entity.ExportPatch().Input.IsPresent() {
		patch["input_json"] = row.InputJSON
	}
	query := func() *gsql.QueryBuilderG[SchedulePO] {
		return gsql.From[SchedulePO](ScheduleSchema).Where(
			ScheduleSchema.Namespace.Eq(row.Namespace),
			ScheduleSchema.ID.Eq(entity.ID),
		)
	}
	var affected int64
	if len(patch) > 0 {
		result := query().Update(s.db, patch)
		if result.Error != nil {
			return result.Error
		}
		affected = result.RowsAffected
	}
	if affected == 0 {
		// 值没有变化的 UPDATE 在 MySQL 上同样报告 0 行，按行是否存在区分。
		existing, err := query().Take(s.db)
		if err != nil {
			return err
		}
		if existing == nil {
			return fmt.Errorf("%w: %s/%s", schedule.ErrNotFound, row.Namespace, entity.ID)
		}
	}
	entity.ClearPatch()
	return nil
}

func (s *Repository) DeleteSchedule(namespace, id string) error {
	namespace = protocol.NormalizeNamespace(namespace)
	result := gsql.From[SchedulePO](ScheduleSchema).
		Where(
			ScheduleSchema.Namespace.Eq(namespace),
			ScheduleSchema.ID.Eq(id),
		).Delete(s.db)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return fmt.Errorf("%w: %s/%s", schedule.ErrNotFound, namespace, id)
	}
	return nil
}

func (s *Repository) ListSchedules(namespace string) ([]*schedule.Schedule, error) {
	rows, err := infradb.Find(s.db, gsql.From[SchedulePO](ScheduleSchema).
		Where(ScheduleSchema.Namespace.Eq(protocol.NormalizeNamespace(namespace))).
		Order(ScheduleSchema.CreatedAt).Order(ScheduleSchema.ID))
	if err != nil {
		return nil, err
	}
	result := make([]*schedule.Schedule, 0, len(rows))
	for i := range rows {
		sch, err := rowToSchedule(rows[i])
		if err != nil {
			return nil, err
		}
		result = append(result, sch)
	}
	return result, nil
}

// ListDueSchedules 返回 next_run_at <= now 且 paused=false 的 schedule。
func (s *Repository) ListDueSchedules(now time.Time) ([]*schedule.Schedule, error) {
	rows, err := infradb.Find(s.db, gsql.From[SchedulePO](ScheduleSchema).
		Where(
			ScheduleSchema.NextRunAt.Lte(&now),
			ScheduleSchema.Paused.Eq(false),
		).
		Order(ScheduleSchema.NextRunAt).Order(ScheduleSchema.ID))
	if err != nil {
		return nil, err
	}
	result := make([]*schedule.Schedule, 0, len(rows))
	for i := range rows {
		sch, err := rowToSchedule(rows[i])
		if err != nil {
			return nil, err
		}
		result = append(result, sch)
	}
	return result, nil
}

// UpdateScheduleNextRunAtCAS 使用 CAS 原子推进 next_run_at，防止并发双触发。
// oldNextRunAt 为 nil 时匹配 IS NULL；否则匹配等于旧值。
// 返回 (true, nil) 表示更新成功；(false, nil) 表示被其他实例抢先推进了（幂等跳过）。
func (s *Repository) UpdateScheduleNextRunAtCAS(entity *schedule.Schedule, oldNextRunAt *time.Time) (bool, error) {
	row, err := scheduleToRow(entity)
	if err != nil {
		return false, err
	}

	query := gsql.From[SchedulePO](ScheduleSchema).
		Where(
			ScheduleSchema.Namespace.Eq(row.Namespace),
			ScheduleSchema.ID.Eq(entity.ID),
		)
	if oldNextRunAt == nil {
		query = query.Where(ScheduleSchema.NextRunAt.IsNull())
	} else {
		query = query.Where(ScheduleSchema.NextRunAt.Eq(oldNextRunAt))
	}
	result := query.Update(s.db, map[string]any{
		"next_run_at": row.NextRunAt,
		"updated_at":  row.UpdatedAt,
	})
	if result.Error != nil {
		return false, result.Error
	}
	if result.RowsAffected == 0 {
		return false, nil
	}
	// CAS 只消费实际写入的两个字段，其余待保存变更继续保留。
	patch := entity.ExportPatch()
	patch.NextRunAt = mo.None[*time.Time]()
	patch.UpdatedAt = mo.None[time.Time]()
	return true, nil
}
