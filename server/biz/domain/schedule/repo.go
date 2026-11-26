package schedule

import (
	"context"
	"errors"
	"time"

	"github.com/donutnomad/tenuto/protocol/history"
)

var (
	// ErrNotFound 表示 namespace 下没有这个 ID 的 Schedule。
	ErrNotFound = errors.New("schedule not found")
	// ErrAlreadyExists 表示 namespace 下已有相同 ID 的 Schedule。
	ErrAlreadyExists = errors.New("schedule already exists")
	// ErrConcurrentUpdate 表示 Schedule 在本次操作读取之后被其他写入方改过，重试即可。
	ErrConcurrentUpdate = errors.New("schedule was concurrently modified")
	// ErrOverlapSkipped 表示 Overlap Policy 为 SKIP 且上一次触发的 Run 仍在运行，这次触发没有启动 Run。
	ErrOverlapSkipped = errors.New("previous scheduled run is still running")
)

// Repository 保存 Schedule。消费方是 usecase/schedules。
// namespace 一律按 protocol.NormalizeNamespace 归一：空 namespace 与 "default" 是同一个。
type Repository interface {
	// CreateSchedule 在 ID 已存在时返回满足 errors.Is(err, ErrAlreadyExists) 的错误。
	CreateSchedule(schedule *Schedule) error
	// GetSchedule 在 Schedule 不存在时返回满足 errors.Is(err, ErrNotFound) 的错误。
	GetSchedule(namespace, id string) (*Schedule, error)
	// UpdateSchedule 只写入自读取以来经领域方法改过的字段，其余字段保持存储中的值。
	UpdateSchedule(schedule *Schedule) error
	// DeleteSchedule 在 Schedule 不存在时返回满足 errors.Is(err, ErrNotFound) 的错误。
	DeleteSchedule(namespace, id string) error
	// ListSchedules 按 (CreatedAt, ID) 升序返回 namespace 下的 Schedule。
	ListSchedules(namespace string) ([]*Schedule, error)
	// ListDueSchedules 按 (NextRunAt, ID) 升序返回未暂停且 NextRunAt <= now 的 Schedule。
	ListDueSchedules(now time.Time) ([]*Schedule, error)
	// UpdateScheduleNextRunAtCAS 仅在存储中的 NextRunAt 等于 oldNextRunAt 时写入 NextRunAt 与 UpdatedAt，
	// 返回 false 表示已被其他写入方推进。oldNextRunAt 为 nil 时匹配未设置的 NextRunAt。
	UpdateScheduleNextRunAtCAS(schedule *Schedule, oldNextRunAt *time.Time) (bool, error)
}

// Firer 是触发 Schedule 时用到的执行域能力。消费方是 usecase/schedules。
type Firer interface {
	// StartWorkflowForCron 按 Workflow Type 启动一个 Initiator=Cron、带 searchAttributes 的 Run，返回 runID。启动按 workflowID
	// 去重：workflowID 的 current Run（运行中或已结束）由同一个 workflowID 的 Cron 启动创建时返回这个 Run，不启动新 Run。
	// current Run 是其他启动创建、仍在运行时不启动，返回满足 errors.Is(err, execution.ErrCurrentRunConflict) 的错误；
	// 已结束时新 Run 接替它。
	StartWorkflowForCron(ctx context.Context, namespace, workflowType, workflowID string, input map[string]any, searchAttributes map[string]string) (string, error)
	// CurrentRunStatus 返回 runID 所在 Workflow ID 的 current Run 的状态：runID 经 continue-as-new、重试或 reset 接续时
	// 是接续的那一代。runID 不存在时返回满足 errors.Is(err, execution.ErrRunNotFound) 的错误。
	CurrentRunStatus(runID string) (history.Status, error)
}
