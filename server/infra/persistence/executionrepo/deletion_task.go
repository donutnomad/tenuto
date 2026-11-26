package executionrepo

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// DeletionTaskPO 是 retention GC 的投递载体，一行代表「一棵 history tree 到期后应被整体删除」。
//
// 主键是 tree_id 而非 run_id：reset 出的新 run 与老 run **共享同一棵 tree**
// （ForkBranch 用 TreeID: sourceBranch.TreeID），而 DeleteBranch 只按 tree_id 查
// history_tree、完全不看 tenuto_runs。若按 run 删除，GC 老 run 会删掉它的 tree row，
// 仍活着的新 run 把老 branch 列为 ancestor —— 引用信息永久丢失，等新 run 到期时
// DeleteBranch 的 allBranches 里已无老 branch，会回溯把仍被引用的历史全删。
// 这是潜伏型数据丢失：第一次 GC 看似正常，第二次才炸。
//
// 同树多个 run 陆续终态时，Upsert 取 fire_at 的**最大值**，保证整树最后一个 run
// 到期后才删。
// RetentionConfig 控制终态数据的保留与回收。零值 Retention 表示 GC 关闭：
// 不投递任何 deletion task，终态数据一直保留。
type RetentionConfig struct {
	// Retention 是终态事件落库后到该 tree 可被回收之间的最小间隔。<=0 表示关闭 GC。
	Retention time.Duration
	// Jitter 是叠加在 fire_at 上的随机抖动上界，
	// 防止批量关闭的 workflow 在同一秒集中打爆 DB。<=0 表示不抖动。
	Jitter time.Duration
}

// MinRetention 是 Retention 的下界，防止误设成秒级导致数据被瞬间删除。
const MinRetention = time.Hour

// Validate 校验 retention 配置。Retention<=0（关闭）永远合法。
func (c RetentionConfig) Validate() error {
	if c.Retention <= 0 {
		return nil
	}
	if c.Retention < MinRetention {
		return fmt.Errorf("workflow retention must be >= %s when enabled, got %s", MinRetention, c.Retention)
	}
	if c.Jitter < 0 {
		return fmt.Errorf("workflow retention jitter must not be negative, got %s", c.Jitter)
	}
	return nil
}

// enabled 报告是否应当投递 deletion task。
func (c RetentionConfig) enabled() bool { return c.Retention > 0 }

// fireAt 计算某个终态事件对应的到期时刻：closeTime + retention + rand(0, jitter)。
func (c RetentionConfig) fireAt(closeTime time.Time) time.Time {
	deadline := closeTime.Add(c.Retention)
	if c.Jitter > 0 {
		deadline = deadline.Add(time.Duration(rand.Int64N(int64(c.Jitter))))
	}
	return deadline
}

// isTerminalEvent 报告该事件是否让 run 进入终态。
//
// 这五种与 execution.ApplyEventToExecutionState 的终态分支**逐一对应**，是同一份事实的
// 两处表述。新增终态事件类型时两处必须同步，否则该终态的 tree 永不投递、永不回收。
func isTerminalEvent(event history.Event) bool {
	switch {
	case event.WorkflowCompleted != nil,
		event.WorkflowFailed != nil,
		event.WorkflowCancelled != nil,
		event.WorkflowTerminated != nil,
		event.WorkflowContinuedAsNew != nil:
		return true
	default:
		return false
	}
}

// enqueueDeletionTask 在**调用方的事务内**投递/推迟一棵 tree 的删除任务。
//
// 必须与终态事件同事务：否则存在「关闭成功但任务没排上」的窗口，那棵 tree 会永久泄漏
// （没有任何其它路径会重新发现它）。
//
// 冲突语义取 max(existing, new)：同树的多个 run 陆续终态时，整树的到期时刻由**最晚**
// 关闭的那个 run 决定。ON DUPLICATE KEY UPDATE 的 SET 子句引用表自身的列，
// 一条语句完成，无需先读后写（那会引入 TOCTOU）。
func enqueueDeletionTask(db *gorm.DB, treeID, namespace string, fireAt, now time.Time) error {
	if treeID == "" {
		return fmt.Errorf("deletion task requires a tree id")
	}
	row := &DeletionTaskPO{
		TreeID:    treeID,
		Namespace: protocol.NormalizeNamespace(namespace),
		FireAt:    fireAt,
		CreatedAt: now,
		UpdatedAt: now,
	}
	table := DeletionTaskPO{}.TableName()
	// 撞主键 tree_id 时只推迟 fire_at（取较晚者）并刷新 updated_at。
	return db.Clauses(clause.OnConflict{
		Columns: []clause.Column{{Name: "tree_id"}},
		DoUpdates: clause.Assignments(map[string]any{
			"fire_at": gorm.Expr(
				"CASE WHEN ? > "+table+".fire_at THEN ? ELSE "+table+".fire_at END",
				fireAt, fireAt),
			"updated_at": now,
		}),
	}).Create(row).Error
}

// terminalEventTypes 是 isTerminalEvent 的五种终态在 events.type 列上的取值。
// 与 isTerminalEvent / ApplyEventToExecutionState 同源，三处必须同步。
// 整树删除用它以历史事件判定 run 是否终态。
func terminalEventTypes() []string {
	return []string{
		string(history.EventWorkflowCompleted),
		string(history.EventWorkflowFailed),
		string(history.EventWorkflowCancelled),
		string(history.EventWorkflowTerminated),
		string(history.EventWorkflowContinuedAsNew),
	}
}

// errAffectedRowsProbeRollback 让 CheckDuplicateKeyAffectedRows 的事务回滚，探测行不留在表里。
var errAffectedRowsProbeRollback = errors.New("affected rows probe rollback")

// CheckDuplicateKeyAffectedRows 确认连接对「撞键、不改列」的 upsert 报告 affected rows = 0。仓储用这个值区分
// 新写入与去重命中（事件 dedupe、state 的 insertOrPatch），连接带 CLIENT_FOUND_ROWS（DSN 的 clientFoundRows=true）
// 时 MySQL 报告 1，这些判定全部失效。探测在一个回滚的事务里对 tenuto_deletion_tasks 插入同一行两次。
func (s *Repository) CheckDuplicateKeyAffectedRows(ctx context.Context) error {
	now := s.clock.Now()
	row := &DeletionTaskPO{TreeID: "tenuto:affected-rows-probe", Namespace: protocol.DefaultNamespace, FireAt: now, CreatedAt: now, UpdatedAt: now}
	var affected int64
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		for range 2 {
			created := tx.Clauses(onDuplicateKeyNoop("tree_id")).Create(row)
			if created.Error != nil {
				return created.Error
			}
			affected = created.RowsAffected
		}
		return errAffectedRowsProbeRollback
	})
	if !errors.Is(err, errAffectedRowsProbeRollback) {
		return fmt.Errorf("probe duplicate-key affected rows: %w", err)
	}
	if affected != 0 {
		return fmt.Errorf("MySQL connection reports %d affected rows for a no-op duplicate-key upsert; open it without clientFoundRows=true", affected)
	}
	return nil
}
