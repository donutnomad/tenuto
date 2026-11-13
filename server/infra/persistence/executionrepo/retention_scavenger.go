package executionrepo

import (
	"fmt"
	"time"

	"github.com/donutnomad/gsql"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
)

// 兜底扫描（scavenger）回收主 GC 路径**结构上够不着**的残留。
//
// 主 GC 的单位是 history tree：终态事件落库时按 tree_id 投递一条 deletion task，到期后
// 整树删除。「tree 已经删完但还剩东西」的数据主路径看不到 —— 它们不在
// tenuto_deletion_tasks 里，也就没有任何触发器，由兜底扫描低频回收：min_age 过滤、分页、单轮上限。
//
// **两类残留**
//
//   - 孤儿事件：run 已经不存在、事件还在。整树删除是分步的（Step 2 删 run、Step 3 删
//     events），中途崩溃就会停在这个中间态；DeleteBranch 的事件删除在事务外分批执行，
//     同样可能留下残留。
//   - 孤儿 tree 行：history_tree 里还有行、这棵树一条事件都不剩。
//
// **为什么 min_age 不可省**：孤儿事件的判定依赖「查不到关联行」，而正常写入过程中存在
// 短暂的「事件已落、run 行还没提交可见」之类的窗口。min_age 把窗口整个排除在外 ——
// 只处理老到不可能是在途写入的数据，默认 7 天（见 DefaultScavengeMinAge）。

// minAge / batch 的默认值定义在 biz/usecase/retention（DefaultScavengeMinAge、DefaultScavengeBatch），
// 由扫描器传入 —— 存储层只负责执行，不决定策略。

// ScavengeOrphans 跑一轮兜底扫描。幂等、可重复跑：每一步都是按谓词删除，不依赖游标，
// 中断后重跑只是从剩下的继续。
//
// cutoff 语义：只回收「最后活动时间 <= now - minAge」的数据。
func (s *Repository) ScavengeOrphans(now time.Time, minAge time.Duration, limit int) (execution.ScavengeResult, error) {
	var result execution.ScavengeResult
	if limit <= 0 {
		return result, fmt.Errorf("scavenge limit must be positive")
	}
	if minAge <= 0 {
		return result, fmt.Errorf("scavenge min age must be positive")
	}

	eventRuns, err := s.scavengeOrphanEvents(now, minAge, limit)
	if err != nil {
		return result, fmt.Errorf("scavenge orphan events: %w", err)
	}
	result.OrphanEventRuns = eventRuns

	treeRows, err := s.scavengeOrphanTreeRows(limit)
	if err != nil {
		return result, fmt.Errorf("scavenge orphan tree rows: %w", err)
	}
	result.OrphanTreeRows = treeRows
	return result, nil
}

// scavengeOrphanEvents 回收孤儿事件：run 行已不存在、事件还留着。
//
// 判据是「事件的 run_id 在 tenuto_runs 里查不到，且该 run 最后一条事件早于 cutoff」。
// 这里**只用 minAge**、不叠加 retention：能走到这一步说明 run 行已经没了，数据本就不可
// 访问，retention 承诺的「关闭后 N 天内可查历史」对它已经无从谈起。
//
// 用 NOT EXISTS 子查询而不是把 run id 全捞到内存再比：运行中的实例可能有几十万 run，
// 全量拉取既慢又占内存，而 NOT EXISTS 能直接吃 tenuto_runs 的主键索引。
func (s *Repository) scavengeOrphanEvents(now time.Time, minAge time.Duration, limit int) (int, error) {
	cutoff := now.Add(-minAge)

	events := EventSchema.As("events")
	runs := RunSchema.As("runs")
	runExists := gsql.Select(gsql.Lit(1).As("_")).
		From(runs).
		Where(runs.ID.EqF(events.RunID))
	candidates, err := gsql.SelectG[orphanEventCandidate](
		events.RunID,
	).
		From(events).
		Where(events.RunID.Not(""), gsql.NotExists(runExists)).
		GroupBy(events.RunID).
		Having(events.Timestamp.Max().Lte(cutoff)).
		Limit(limit).
		Find(s.db)
	if err != nil {
		return 0, err
	}

	cleaned := 0
	for _, candidate := range candidates {
		runID := candidate.RunID
		if err := s.DeleteEventsBatched(EventSchema.RunID.Eq(runID)); err != nil {
			return cleaned, fmt.Errorf("run %s: %w", runID, err)
		}
		cleaned++
	}
	return cleaned, nil
}

// scavengeOrphanTreeRows 清掉「history_tree 里还有行、但这棵树一条事件都不剩」的残留。
//
// 这类行是 DeleteTree / DeleteBranch 中断的产物，本身不占多少空间，但会**继续参与引用
// 计数**：DeleteBranch 把仍在 history_tree 里的 branch 一律当作活引用，一行僵尸 tree row
// 会永久保护住一段已经没人要的历史，让后续删除删不干净。清掉它是让引用计数恢复准确。
//
// 判据不需要 min_age：条件是「整棵树一条事件都没有」，而一棵活着的树必然有事件
// （root branch 至少有 WorkflowStarted）。不存在「在途写入恰好没有任何事件」的窗口 ——
// tree row 与它的首条事件由 ForkBranch / append 各自的事务写入，读到 tree row 却读不到
// 任何事件只可能是删除留下的残局。
func (s *Repository) scavengeOrphanTreeRows(limit int) (int, error) {
	trees := HistoryTreeSchema.As("trees")
	events := EventSchema.As("events")
	eventsForTree := gsql.Select(gsql.Lit(1).As("_")).
		From(events).
		Where(events.TreeID.EqF(trees.TreeID))
	treeRows, err := gsql.PluckG[string](trees.TreeID).
		From(trees).
		Where(gsql.NotExists(eventsForTree)).
		GroupBy(trees.TreeID).
		Limit(limit).
		Find(s.db)
	if err != nil {
		return 0, err
	}
	treeIDs := make([]string, 0, len(treeRows))
	for _, row := range treeRows {
		treeIDs = append(treeIDs, *row)
	}
	if len(treeIDs) == 0 {
		return 0, nil
	}

	// 顺带把这些树上可能残留的 deletion task 一并清掉：树已经空了，任务再触发也无事可做，
	// 留着只会让扫描器每轮都白跑一次 DeleteTree。
	if err := gsql.From[HistoryTreePO](HistoryTreeSchema).
		Where(HistoryTreeSchema.TreeID.In(treeIDs...)).Delete(s.db).Error; err != nil {
		return 0, err
	}
	if err := gsql.From[DeletionTaskPO](DeletionTaskSchema).
		Where(DeletionTaskSchema.TreeID.In(treeIDs...)).Delete(s.db).Error; err != nil {
		return 0, err
	}
	return len(treeIDs), nil
}

type orphanEventCandidate struct {
	RunID string `gorm:"column:run_id"`
}
