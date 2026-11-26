package executionrepo

import (
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/donutnomad/gsql"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	infradb "github.com/donutnomad/tenuto/server/infra/db"
	"github.com/donutnomad/tenuto/server/infra/persistence/dispatchrepo"
	"gorm.io/gorm"
)

// 保留期回收：到期任务的读取与推后、整树删除、孤儿兜底扫描（retention_scavenger.go）。
// 派发记录属于派发聚合，整树删除经 dispatchrepo.DeleteRunDispatches 在同一事务内删除。
var _ execution.RetentionGCStore = (*Repository)(nil)

// ListDueDeletionTasks 返回 fire_at <= now 的任务，按 fire_at 升序，最多 limit 条。
// 命中 idx_deletion_tasks_fire_at。
func (s *Repository) ListDueDeletionTasks(now time.Time, limit int) ([]execution.DeletionTask, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("deletion task scan limit must be positive")
	}
	rows, err := infradb.Find(s.db, gsql.From[DeletionTaskPO](DeletionTaskSchema).
		Where(DeletionTaskSchema.FireAt.Lte(now)).
		Order(DeletionTaskSchema.FireAt).
		Limit(limit))
	if err != nil {
		return nil, err
	}
	tasks := make([]execution.DeletionTask, 0, len(rows))
	for _, row := range rows {
		tasks = append(tasks, execution.DeletionTask{
			TreeID:    row.TreeID,
			Namespace: row.Namespace,
			FireAt:    row.FireAt,
			Attempts:  row.Attempts,
		})
	}
	return tasks, nil
}

// DeletionBacklog 统计 fire_at <= now 的任务数与最早的 fire_at，两条语句都走 idx_deletion_tasks_fire_at。
func (s *Repository) DeletionBacklog(now time.Time) (execution.DeletionBacklog, error) {
	count, err := gsql.From[DeletionTaskPO](DeletionTaskSchema).
		Where(DeletionTaskSchema.FireAt.Lte(now)).Count(s.db)
	if err != nil {
		return execution.DeletionBacklog{}, err
	}
	if count == 0 {
		return execution.DeletionBacklog{}, nil
	}
	oldest, err := gsql.From[DeletionTaskPO](DeletionTaskSchema).
		Where(DeletionTaskSchema.FireAt.Lte(now)).
		Order(DeletionTaskSchema.FireAt).Take(s.db)
	if err != nil {
		return execution.DeletionBacklog{}, err
	}
	backlog := execution.DeletionBacklog{Overdue: int(count)}
	if oldest != nil {
		backlog.OldestFireAt = oldest.FireAt
	}
	return backlog, nil
}

// TreeWorkflowKeys 返回该 tree 涉及的全部 (namespace, workflowID)，稳定排序。
//
// 扫描器用它取 keyedMu。一棵 tree 可能横跨多个 workflowID（child workflow、CAN），
// 必须把它们全锁住才能保证删除期间没有并发的用户操作看到中间态。
// 排序是死锁防线：两个 GC 实例处理有交集的 tree 时按同一顺序取锁。
func (s *Repository) TreeWorkflowKeys(treeID string) ([]execution.WorkflowKey, error) {
	runIDs, err := treeRunIDs(s.db.Session(&gorm.Session{}), treeID)
	if err != nil {
		return nil, err
	}
	if len(runIDs) == 0 {
		return nil, nil
	}
	rows, err := infradb.Find(s.db, gsql.From[RunPO](RunSchema).
		Where(RunSchema.ID.In(runIDs...)))
	if err != nil {
		return nil, err
	}
	seen := make(map[execution.WorkflowKey]bool, len(rows))
	keys := make([]execution.WorkflowKey, 0, len(rows))
	for _, row := range rows {
		key := execution.WorkflowKey{
			Namespace:  protocol.NormalizeNamespace(row.Namespace),
			WorkflowID: row.WorkflowID,
		}
		if seen[key] {
			continue
		}
		seen[key] = true
		keys = append(keys, key)
	}
	slices.SortFunc(keys, func(a, b execution.WorkflowKey) int {
		if c := strings.Compare(a.Namespace, b.Namespace); c != 0 {
			return c
		}
		return strings.Compare(a.WorkflowID, b.WorkflowID)
	})
	return keys, nil
}

// DeferDeletionTask 把任务推后到 fireAt 并记录一次失败。
//
// 之所以是「推后」而不是「原地重试」：任务按 fire_at 升序扫描，一个反复失败的任务若
// 不推后就会永远排在最前，占满每一轮的窗口，正常任务永远排不上 —— 活锁。
func (s *Repository) DeferDeletionTask(treeID string, fireAt time.Time, countAttempt bool, cause error) error {
	updates := map[string]any{
		"fire_at":    fireAt,
		"updated_at": time.Now(),
	}
	if countAttempt {
		updates["attempts"] = DeletionTaskSchema.Attempts.Add(1)
	}
	if cause != nil {
		updates["last_error"] = truncateDeletionError(cause.Error())
	}
	return gsql.From[DeletionTaskPO](DeletionTaskSchema).
		Where(DeletionTaskSchema.TreeID.Eq(treeID)).
		Update(s.db, updates).Error
}

// truncateDeletionError 限制 last_error 的长度，避免一条巨大的驱动错误撑爆行。
func truncateDeletionError(msg string) string {
	const limit = 2000
	if len(msg) <= limit {
		return msg
	}
	return msg[:limit]
}

// DeletionTaskAttempts 返回该 tree 当前的失败次数，供扫描器算退避；任务不存在返回 (0,false)。
func (s *Repository) DeletionTaskAttempts(treeID string) (int, bool, error) {
	row, err := gsql.From[DeletionTaskPO](DeletionTaskSchema).
		Where(DeletionTaskSchema.TreeID.Eq(treeID)).Take(s.db)
	if err != nil {
		return 0, false, err
	}
	if row == nil {
		return 0, false, nil
	}
	return row.Attempts, true, nil
}

// treeRunIDs 返回该 tree 下所有 run 的 ID。
//
// 数据来源是 tenuto_events.tree_id：run 与 tree 的关联只存在于 branch token（一个
// LONGBLOB，无法索引查询）和事件行上，而事件行有现成的 tree_id 列。任何在这棵树上
// 写过事件的 run 都会被找到，这正是「整树全部 run」需要的集合。
func treeRunIDs(db gsql.IDB, treeID string) ([]string, error) {
	rows, err := infradb.Find(db, gsql.PluckG[string](EventSchema.RunID).
		From(EventSchema).
		Where(EventSchema.TreeID.Eq(treeID)).
		Distinct())
	if err != nil {
		return nil, err
	}
	runIDs := make([]string, 0, len(rows))
	for _, row := range rows {
		runIDs = append(runIDs, *row)
	}
	return runIDs, nil
}

// treeBranchIDs 返回该 tree 在 history_tree 里登记的所有 branch。
func treeBranchIDs(db gsql.IDB, treeID string) ([]string, error) {
	rows, err := infradb.Find(db, gsql.PluckG[string](HistoryTreeSchema.BranchID).
		From(HistoryTreeSchema).
		Where(HistoryTreeSchema.TreeID.Eq(treeID)))
	if err != nil {
		return nil, err
	}
	branchIDs := make([]string, 0, len(rows))
	for _, row := range rows {
		branchIDs = append(branchIDs, *row)
	}
	return branchIDs, nil
}

// runTerminalFromHistory 以**历史事件**判定 run 是否终态。
//
// 为什么不能只读 run.Status：continue-as-new 与带重试的失败先写终态事件，Run 的终态状态随接续 Run
// 的事务提交，二者之间存在「终态事件已落库 + deletion task 已排上 + run.Status 仍是 Running」的
// 窗口，接续 Run 建不出来时窗口一直存在。若 Step 0
// 只看 run.Status，落进这个窗口的 run 会被**永久跳过** —— 它的 status 此后再也没有
// 别的路径去修正，那棵 tree 就永远不回收。
//
// 反过来「事件说终态、status 说 running」时以事件为准是安全的：事件是唯一的持久事实源，
// reconcileRunStatusFromHistory 走的也是同一判据。
//
// 只查该 run 自己写下的终态事件（run_id 维度），不做 branch 祖先链解析：终态事件必然由
// run 自己写入，祖先链上的终态属于**别的** run，不能算在这个 run 头上。
func runTerminalFromHistory(db gsql.IDB, runID string) (bool, error) {
	return gsql.From[EventPO](EventSchema).
		Where(EventSchema.RunID.Eq(runID), EventSchema.Type.In(terminalEventTypes()...)).
		Limit(1).
		Exist(db)
}

// forkCommitGrace 是 ForkBranch 写下新 branch 行到 reset 事务写出新 run 首条事件之间的最长间隔。
// 两步在同一次 reset 请求里相邻执行；超过这个时长仍没有事件的 branch 行属于 reset 提交失败
// 留下的残留，随整树一起回收。
const forkCommitGrace = time.Hour

// DeleteTree 整棵回收一个 history tree，是 retention GC 的执行体。
//
// 单位是 tree 而非 run：见 DeletionTaskPO 的注释。一棵 tree 上**全部 run 都终态**
// 才删，否则返回 execution.ErrTreeNotTerminal。
//
// 删除顺序：
//
//	Step 0 锁住 tree 行，校验整树终态（以历史事件为准）且没有进行中的 fork
//	Step 1 删 task_dispatches
//	Step 2 删 runs（含 is_current/current_marker）+ execution/activity/timer/child 状态
//	Step 3 删 events + history_tree 行
//	Step 4 删 deletion task 自身
//
// **核心不变式：宁可留下不可访问的孤儿事件，也不要留下可访问但没有事件的 run。**
// 因此 run 必须先于 events 消失。把 Step 3 提前会在任意中断点留下「run 还在、历史没了」
// 的不一致态 —— 那是用户可见的损坏；反之留下的孤儿事件无人可达，由兜底扫描慢慢清。
//
// 每一步都是幂等的（DELETE 天然幂等，DeleteBranch 已幂等化），所以中断后重跑安全，
// 不需要额外的进度位掩码。
//
// **与并发 reset 的关系**（reset = ForkBranch 写 tree 行，随后另一个事务建新 run 并写首条事件）：
//
//   - Step 0~2 的事务第一条语句是 lockHistoryTree。ForkBranch 的 INSERT 与这把锁互相等待。
//   - fork 先提交：本事务拿到锁后读到新 branch 行。新 run 已写出事件时它是非终态 run；
//     还没写出事件时它是一条 forkCommitGrace 内的空 branch 行。两种情况都返回 execution.ErrTreeNotTerminal。
//   - 本事务先提交：源 run 已删除，ForkBranch 在自己的事务里复查源 run 时失败并回滚。
//   - 终态校验放在取锁之后：InnoDB 的一致性快照建立于事务内第一次普通读，取锁期间提交的
//     fork 因此对校验可见。
func (s *Repository) DeleteTree(treeID string) error {
	if treeID == "" {
		return fmt.Errorf("delete tree: tree id is required")
	}

	err := s.db.Transaction(func(tx *gorm.DB) error {
		branches, err := LockHistoryTree(tx, treeID)
		if err != nil {
			return fmt.Errorf("delete tree %s: lock history tree: %w", treeID, err)
		}

		runIDs, err := treeRunIDs(tx, treeID)
		if err != nil {
			return err
		}

		// Step 0：整树终态校验。有任一 run 未终态就整树跳过。
		for _, runID := range runIDs {
			terminal, err := runTerminalFromHistory(tx, runID)
			if err != nil {
				return err
			}
			if !terminal {
				return fmt.Errorf("%w: tree %s run %s", execution.ErrTreeNotTerminal, treeID, runID)
			}
		}
		// 刚 fork 出来、新 run 还没写出首条事件的 branch：reset 正在进行，整树跳过。
		forkCutoff := time.Now().Add(-forkCommitGrace)
		for _, branch := range branches {
			if branch.CreatedAt.Before(forkCutoff) {
				continue
			}
			hasEvents, err := gsql.From[EventPO](EventSchema).
				Where(EventSchema.BranchID.Eq(branch.BranchID), EventSchema.TreeID.Eq(treeID)).
				Limit(1).
				Exist(tx)
			if err != nil {
				return err
			}
			if !hasEvents {
				return fmt.Errorf("%w: tree %s branch %s was forked and its run has no events yet", execution.ErrTreeNotTerminal, treeID, branch.BranchID)
			}
		}

		if len(runIDs) == 0 {
			return nil
		}
		// Step 1 + Step 2。
		if err := s.deleteRunRows(tx, runIDs); err != nil {
			return fmt.Errorf("delete tree %s: %w", treeID, err)
		}
		return nil
	})
	if err != nil {
		return err
	}

	// Step 3：删整棵树的事件与 tree 行。逐 branch 走 DeleteBranch 而不是直接
	// `DELETE FROM events WHERE tree_id=?`：DeleteBranch 是引用计数的唯一真源，
	// 走它可以在「整树删除」与「单叶子删除」之间共用同一份已验证过的语义。
	// 整树删除时每删一个 branch，剩余 branch 的引用会自然收敛到零，最终全清。
	branchIDs, err := treeBranchIDs(s.db.Session(&gorm.Session{}), treeID)
	if err != nil {
		return fmt.Errorf("delete tree %s: list branches: %w", treeID, err)
	}
	for _, branchID := range branchIDs {
		if err := s.DeleteBranch(treeID, branchID); err != nil {
			return fmt.Errorf("delete tree %s: branch %s: %w", treeID, branchID, err)
		}
	}
	// 兜底：没有 tree row 的 branch（run 从未 fork 过，tree 行只在 fork 时写入）不会被上面的循环覆盖，
	// 但它们的事件同样属于这棵树且此刻已无人可达。按 tree_id 分批删，走 idx_workflow_events_tree_run。
	if err := s.DeleteEventsBatched(EventSchema.TreeID.Eq(treeID)); err != nil {
		return fmt.Errorf("delete tree %s: residual events: %w", treeID, err)
	}
	if err := gsql.From[HistoryTreePO](HistoryTreeSchema).
		Where(HistoryTreeSchema.TreeID.Eq(treeID)).Delete(s.db).Error; err != nil {
		return fmt.Errorf("delete tree %s: residual tree rows: %w", treeID, err)
	}

	// Step 4：任务自身最后删。前面任何一步失败都会留着任务，下轮重试。
	if err := gsql.From[DeletionTaskPO](DeletionTaskSchema).
		Where(DeletionTaskSchema.TreeID.Eq(treeID)).Delete(s.db).Error; err != nil {
		return fmt.Errorf("delete tree %s: deletion task: %w", treeID, err)
	}
	return nil
}

// deleteRunRows 在事务内删掉一批 run 的 dispatch、附属状态与 run 行。
//
// 顺序固定：task_dispatches 最先 —— 它是唯一会把已删 run 重新拉回执行路径的表，中间态里
// dispatcher 不能捞到一个状态已被删掉的 run；随后是附属状态，最后是 run 行。
// runs 行带 is_current/current_marker，删掉它即释放该 workflowID 的 current 槽位。
// 附属状态表以 run_id 关联，tenuto_runs 的主键列是 id。
func (s *Repository) deleteRunRows(tx *gorm.DB, runIDs []string) error {
	if err := dispatchrepo.DeleteRunDispatches(tx, runIDs); err != nil {
		return fmt.Errorf("task dispatches: %w", err)
	}
	if err := gsql.From[WorkflowExecutionStatePO](WorkflowExecutionStateSchema).
		Where(WorkflowExecutionStateSchema.RunID.IN(runIDs...)).Delete(tx).Error; err != nil {
		return fmt.Errorf("workflow execution state: %w", err)
	}
	if err := gsql.From[ActivityStatePO](ActivityStateSchema).
		Where(ActivityStateSchema.RunID.IN(runIDs...)).Delete(tx).Error; err != nil {
		return fmt.Errorf("activity state: %w", err)
	}
	if err := gsql.From[TimerStatePO](TimerStateSchema).
		Where(TimerStateSchema.RunID.IN(runIDs...)).Delete(tx).Error; err != nil {
		return fmt.Errorf("timer state: %w", err)
	}
	if err := gsql.From[ChildWorkflowStatePO](ChildWorkflowStateSchema).
		Where(ChildWorkflowStateSchema.RunID.IN(runIDs...)).Delete(tx).Error; err != nil {
		return fmt.Errorf("child workflow state: %w", err)
	}
	if err := gsql.From[RunPO](RunSchema).Where(RunSchema.ID.IN(runIDs...)).Delete(tx).Error; err != nil {
		return fmt.Errorf("runs: %w", err)
	}
	return nil
}
