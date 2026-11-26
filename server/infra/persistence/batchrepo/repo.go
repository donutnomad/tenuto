package batchrepo

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/donutnomad/gsql"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/server/biz/domain/batch"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	infradb "github.com/donutnomad/tenuto/server/infra/db"
	"gorm.io/gorm"
)

type Repository struct {
	db infradb.DB
}

func New(database infradb.DB) *Repository {
	return &Repository{db: database}
}

// 编译期断言：Repository 实现 batch.Repository
var _ batch.Repository = (*Repository)(nil)

// ── ToRow / FromRow ──

func operationToRow(op *batch.Operation) (*BatchOperationPO, error) {
	filterJSON, err := json.Marshal(op.Filter)
	if err != nil {
		return nil, fmt.Errorf("batch operation marshal filter: %w", err)
	}
	var errorsJSON []byte
	if len(op.Errors) > 0 {
		if errorsJSON, err = json.Marshal(op.Errors); err != nil {
			return nil, fmt.Errorf("batch operation marshal errors: %w", err)
		}
	}
	row := new(BatchOperationPO).ToPO(op)
	row.Namespace = protocol.NormalizeNamespace(op.Namespace)
	row.FilterJSON = filterJSON
	row.ErrorsJSON = errorsJSON
	if op.Cursor != nil {
		row.CursorCreatedAt = &op.Cursor.CreatedAt
		row.CursorRunID = op.Cursor.RunID
	}
	return row, nil
}

func rowToOperation(row *BatchOperationPO) (*batch.Operation, error) {
	var filter execution.RunListFilter
	if err := json.Unmarshal(row.FilterJSON, &filter); err != nil {
		return nil, fmt.Errorf("batch operation unmarshal filter: %w", err)
	}
	var runErrors []batch.RunError
	if len(row.ErrorsJSON) > 0 {
		if err := json.Unmarshal(row.ErrorsJSON, &runErrors); err != nil {
			return nil, fmt.Errorf("batch operation unmarshal errors: %w", err)
		}
	}
	var cursor *execution.RunCursor
	if row.CursorCreatedAt != nil {
		cursor = &execution.RunCursor{CreatedAt: *row.CursorCreatedAt, RunID: row.CursorRunID}
	}
	return &batch.Operation{
		ID:             row.ID,
		Namespace:      row.Namespace,
		Type:           batch.Type(row.Type),
		Filter:         filter,
		ResetType:      execution.ResetType(row.ResetType),
		Reason:         row.Reason,
		Status:         batch.Status(row.Status),
		TotalCount:     row.TotalCount,
		SucceededCount: row.SucceededCount,
		FailedCount:    row.FailedCount,
		SkippedCount:   row.SkippedCount,
		Errors:         runErrors,
		Cursor:         cursor,
		Version:        row.Version,
		CreatedAt:      row.CreatedAt,
		StartedAt:      row.StartedAt,
		FinishedAt:     row.FinishedAt,
		UpdatedAt:      row.UpdatedAt,
	}, nil
}

func rowsToOperations(rows []*BatchOperationPO) ([]*batch.Operation, error) {
	result := make([]*batch.Operation, 0, len(rows))
	for _, row := range rows {
		op, err := rowToOperation(row)
		if err != nil {
			return nil, err
		}
		result = append(result, op)
	}
	return result, nil
}

// ── batch.Repository 方法 ──

func (s *Repository) CreateOperation(op *batch.Operation) error {
	row, err := operationToRow(op)
	if err != nil {
		return err
	}
	if err := gsql.InsertInto(BatchOperationSchema).Value(row).Exec(s.db); err != nil {
		if errors.Is(err, gorm.ErrDuplicatedKey) {
			return fmt.Errorf("%w: %s/%s", batch.ErrAlreadyExists, row.Namespace, op.ID)
		}
		return err
	}
	op.ClearPatch()
	return nil
}

func (s *Repository) GetOperation(namespace, id string) (*batch.Operation, error) {
	namespace = protocol.NormalizeNamespace(namespace)
	row, err := s.byKey(namespace, id).Take(s.db)
	if err != nil {
		return nil, err
	}
	if row == nil {
		return nil, fmt.Errorf("%w: %s/%s", batch.ErrNotFound, namespace, id)
	}
	return rowToOperation(row)
}

// UpdateOperation 以 version 做乐观并发控制：条件里带上读取时的 version，0 行受影响时按行是否存在区分
// 不存在与并发修改。
func (s *Repository) UpdateOperation(op *batch.Operation) error {
	row, err := operationToRow(op)
	if err != nil {
		return err
	}
	patch := new(BatchOperationPO).ToPatch(op)
	changed := op.ExportPatch()
	if changed.Filter.IsPresent() {
		patch["filter_json"] = row.FilterJSON
	}
	if changed.Errors.IsPresent() {
		patch["errors_json"] = row.ErrorsJSON
	}
	if changed.Cursor.IsPresent() {
		patch["cursor_created_at"] = row.CursorCreatedAt
		patch["cursor_run_id"] = row.CursorRunID
	}
	if len(patch) == 0 {
		return nil
	}
	result := s.byKey(row.Namespace, op.ID).
		Where(BatchOperationSchema.Version.Eq(op.ExpectedVersion())).
		Update(s.db, patch)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		existing, err := s.byKey(row.Namespace, op.ID).Take(s.db)
		if err != nil {
			return err
		}
		if existing == nil {
			return fmt.Errorf("%w: %s/%s", batch.ErrNotFound, row.Namespace, op.ID)
		}
		return fmt.Errorf("%w: %s/%s at version %d, stored %d", batch.ErrConcurrentUpdate, row.Namespace, op.ID, op.ExpectedVersion(), existing.Version)
	}
	op.ClearPatch()
	return nil
}

func (s *Repository) ListOperations(filter batch.ListFilter) ([]*batch.Operation, int, error) {
	query := gsql.From[BatchOperationPO](BatchOperationSchema).
		Where(BatchOperationSchema.Namespace.Eq(protocol.NormalizeNamespace(filter.Namespace)))
	total, err := query.Count(s.db)
	if err != nil {
		return nil, 0, err
	}
	limit := filter.Limit
	if limit <= 0 {
		limit = 50
	}
	rows, err := infradb.Find(s.db, query.Order(BatchOperationSchema.CreatedAt, false).Order(BatchOperationSchema.ID, false).
		Offset(max(filter.Offset, 0)).Limit(limit))
	if err != nil {
		return nil, 0, err
	}
	ops, err := rowsToOperations(rows)
	return ops, int(total), err
}

func (s *Repository) ListActiveOperations() ([]*batch.Operation, error) {
	rows, err := infradb.Find(s.db, gsql.From[BatchOperationPO](BatchOperationSchema).
		Where(BatchOperationSchema.Status.In(string(batch.StatusEnums.Pending), string(batch.StatusEnums.Running))).
		Order(BatchOperationSchema.CreatedAt).Order(BatchOperationSchema.ID))
	if err != nil {
		return nil, err
	}
	return rowsToOperations(rows)
}

func (s *Repository) byKey(namespace, id string) *gsql.QueryBuilderG[BatchOperationPO] {
	return gsql.From[BatchOperationPO](BatchOperationSchema).Where(
		BatchOperationSchema.Namespace.Eq(namespace),
		BatchOperationSchema.ID.Eq(id),
	)
}
