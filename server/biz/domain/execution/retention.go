package execution

import "time"

type DeletionTask struct {
	TreeID    string
	Namespace string
	FireAt    time.Time
	Attempts  int
}

type WorkflowKey struct {
	Namespace  string
	WorkflowID string
}

type RetentionGCStore interface {
	ListDueDeletionTasks(now time.Time, limit int) ([]DeletionTask, error)
	// DeleteTree 删除整棵树；树里还有未终态的 Run 时返回满足 errors.Is(err, ErrTreeNotTerminal) 的错误。
	DeleteTree(treeID string) error
	DeferDeletionTask(treeID string, fireAt time.Time, countAttempt bool, cause error) error
	TreeWorkflowKeys(treeID string) ([]WorkflowKey, error)
	// DeletionBacklog 返回到期（fire_at <= now）仍未处理的任务数与其中最早的 fire_at。
	DeletionBacklog(now time.Time) (DeletionBacklog, error)
	// ScavengeOrphans 是兜底扫描：回收最后活动时间早于 now-minAge、主路径够不着的孤儿数据，每类至多 limit 条。
	ScavengeOrphans(now time.Time, minAge time.Duration, limit int) (ScavengeResult, error)
}

// DeletionBacklog 是到期未处理的删除任务：Overdue 为任务数，OldestFireAt 为最早的 fire_at（Overdue 为 0 时是零值）。
type DeletionBacklog struct {
	Overdue      int
	OldestFireAt time.Time
}

type ScavengeResult struct {
	OrphanEventRuns int
	OrphanTreeRows  int
}

func (r ScavengeResult) Empty() bool {
	return r.OrphanEventRuns == 0 && r.OrphanTreeRows == 0
}
