package batch

import "errors"

var (
	// ErrNotFound 表示 namespace 下没有这个 ID 的批量操作。
	ErrNotFound = errors.New("batch operation not found")
	// ErrAlreadyExists 表示 namespace 下已有相同 ID 的批量操作。
	ErrAlreadyExists = errors.New("batch operation already exists")
	// ErrConcurrentUpdate 表示批量操作在本次读取之后被其他写入方保存过，重新读取后再改。
	ErrConcurrentUpdate = errors.New("batch operation was concurrently modified")
	// ErrInvalidTransition 表示批量操作的当前状态不允许这次变更，例如取消一个已经结束的批量操作。
	ErrInvalidTransition = errors.New("invalid batch operation status transition")
)

// ListFilter 是批量操作列表的分页条件；Limit 不大于 0 时取 50。
type ListFilter struct {
	Namespace string
	Offset    int
	Limit     int
}

// Repository 保存批量操作。消费方是 usecase/batches。
// namespace 一律按 protocol.NormalizeNamespace 归一：空 namespace 与 "default" 是同一个。
type Repository interface {
	// CreateOperation 在 ID 已存在时返回满足 errors.Is(err, ErrAlreadyExists) 的错误。
	CreateOperation(op *Operation) error
	// GetOperation 在批量操作不存在时返回满足 errors.Is(err, ErrNotFound) 的错误。
	GetOperation(namespace, id string) (*Operation, error)
	// UpdateOperation 只写入经领域方法改过的字段，且仅在存储中的 Version 等于 op.ExpectedVersion() 时写入；
	// Version 不符返回满足 errors.Is(err, ErrConcurrentUpdate) 的错误，不存在返回 ErrNotFound。
	UpdateOperation(op *Operation) error
	// ListOperations 按 (CreatedAt, ID) 降序分页返回 namespace 下的批量操作，以及不分页的总数。
	ListOperations(filter ListFilter) ([]*Operation, int, error)
	// ListActiveOperations 按 (CreatedAt, ID) 升序返回全部 namespace 下 pending 与 running 的批量操作。
	ListActiveOperations() ([]*Operation, error)
}
