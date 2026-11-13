package memory

import (
	"cmp"
	"fmt"
	"slices"
	"strings"
	"sync"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/server/biz/domain/batch"
)

// BatchRepository 是批量操作的内存实现，单进程部署与测试使用。
type BatchRepository struct {
	mu  sync.RWMutex
	ops map[string]*batch.Operation // key = "namespace/id"
}

var _ batch.Repository = (*BatchRepository)(nil)

func NewBatchRepository() *BatchRepository {
	return &BatchRepository{ops: make(map[string]*batch.Operation)}
}

func batchKey(namespace, id string) string {
	return protocol.NormalizeNamespace(namespace) + "/" + id
}

// cloneBatchOperation 返回与 op 不共享切片、map 与指针的副本：namespace 归一，待保存变更清空。
// 存入与读出都经过它，调用方改动自己手里的实体不影响仓储里的记录。
func cloneBatchOperation(op *batch.Operation) *batch.Operation {
	clone := *op
	clone.Namespace = protocol.NormalizeNamespace(op.Namespace)
	clone.Filter.RunIDs = slices.Clone(op.Filter.RunIDs)
	clone.Filter.Statuses = slices.Clone(op.Filter.Statuses)
	clone.Filter.SearchAttributes = cloneStringMap(op.Filter.SearchAttributes)
	clone.Errors = slices.Clone(op.Errors)
	if op.Cursor != nil {
		cursor := *op.Cursor
		clone.Cursor = &cursor
	}
	clone.StartedAt = cloneTime(op.StartedAt)
	clone.FinishedAt = cloneTime(op.FinishedAt)
	clone.ClearPatch()
	return &clone
}

func (s *BatchRepository) CreateOperation(op *batch.Operation) error {
	key := batchKey(op.Namespace, op.ID)
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.ops[key]; exists {
		return fmt.Errorf("%w: %s", batch.ErrAlreadyExists, key)
	}
	s.ops[key] = cloneBatchOperation(op)
	op.ClearPatch()
	return nil
}

func (s *BatchRepository) GetOperation(namespace, id string) (*batch.Operation, error) {
	key := batchKey(namespace, id)
	s.mu.RLock()
	defer s.mu.RUnlock()
	op, ok := s.ops[key]
	if !ok {
		return nil, fmt.Errorf("%w: %s", batch.ErrNotFound, key)
	}
	return cloneBatchOperation(op), nil
}

// UpdateOperation 只写入经领域方法改过的字段，Version 不符时拒绝，与 gorm 仓储一致。
func (s *BatchRepository) UpdateOperation(op *batch.Operation) error {
	key := batchKey(op.Namespace, op.ID)
	s.mu.Lock()
	defer s.mu.Unlock()
	stored, ok := s.ops[key]
	if !ok {
		return fmt.Errorf("%w: %s", batch.ErrNotFound, key)
	}
	if stored.Version != op.ExpectedVersion() {
		return fmt.Errorf("%w: %s at version %d, stored %d", batch.ErrConcurrentUpdate, key, op.ExpectedVersion(), stored.Version)
	}
	updated := *stored
	applyPatch(&updated, op, op.ExportPatch())
	s.ops[key] = cloneBatchOperation(&updated)
	op.ClearPatch()
	return nil
}

func (s *BatchRepository) ListOperations(filter batch.ListFilter) ([]*batch.Operation, int, error) {
	namespace := protocol.NormalizeNamespace(filter.Namespace)
	s.mu.RLock()
	defer s.mu.RUnlock()
	var matched []*batch.Operation
	for _, op := range s.ops {
		if op.Namespace == namespace {
			matched = append(matched, op)
		}
	}
	slices.SortFunc(matched, func(a, b *batch.Operation) int { return -compareBatchOperations(a, b) })
	limit := filter.Limit
	if limit <= 0 {
		limit = 50
	}
	page := matched[min(max(filter.Offset, 0), len(matched)):]
	page = page[:min(limit, len(page))]
	result := make([]*batch.Operation, 0, len(page))
	for _, op := range page {
		result = append(result, cloneBatchOperation(op))
	}
	return result, len(matched), nil
}

func (s *BatchRepository) ListActiveOperations() ([]*batch.Operation, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var result []*batch.Operation
	for _, op := range s.ops {
		if op.IsActive() {
			result = append(result, cloneBatchOperation(op))
		}
	}
	slices.SortFunc(result, compareBatchOperations)
	return result, nil
}

// compareBatchOperations 按 (CreatedAt, ID) 升序比较。
func compareBatchOperations(a, b *batch.Operation) int {
	return cmp.Or(a.CreatedAt.Compare(b.CreatedAt), strings.Compare(a.ID, b.ID))
}
