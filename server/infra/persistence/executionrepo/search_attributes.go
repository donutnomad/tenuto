package executionrepo

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/donutnomad/gsql"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	infradb "github.com/donutnomad/tenuto/server/infra/db"
	"gorm.io/gorm"
)

// searchAttributeHash 是 term 在 attr_hash 列上的值：SHA-256(键的字节长度 ‖ 键 ‖ 值)。长度前缀让不同的 (键, 值) 拼接不会相同。
func searchAttributeHash(term execution.SearchAttributeTerm) []byte {
	data := binary.BigEndian.AppendUint64(nil, uint64(len(term.Key)))
	data = append(append(data, term.Key...), term.Value...)
	sum := sha256.Sum256(data)
	return sum[:]
}

// writeRunSearchAttributes 把 runID 在 tenuto_run_search_attributes 里的行换成 attrs 的 term：先删后插。
// 调用方与 execution state 的写入在同一个事务里。
func writeRunSearchAttributes(db *gorm.DB, runID, namespace string, createdAt time.Time, attrs map[string]string) error {
	if err := gsql.From[RunSearchAttributePO](RunSearchAttributeSchema).
		Where(RunSearchAttributeSchema.RunID.Eq(runID)).Delete(db).Error; err != nil {
		return fmt.Errorf("run %s search attributes: %w", runID, err)
	}
	terms := execution.SearchAttributeTerms(attrs)
	if len(terms) == 0 {
		return nil
	}
	rows := make([]*RunSearchAttributePO, 0, len(terms))
	for _, term := range terms {
		rows = append(rows, &RunSearchAttributePO{RunID: runID, AttrHash: searchAttributeHash(term), Namespace: namespace, CreatedAt: createdAt})
	}
	if err := db.Create(rows).Error; err != nil {
		return fmt.Errorf("run %s search attributes: %w", runID, err)
	}
	return nil
}

type runKeyRow struct {
	Namespace string    `gorm:"column:namespace"`
	CreatedAt time.Time `gorm:"column:created_at"`
}

// rewriteRunSearchAttributes 在搜索属性改变后重写 runID 的行；namespace 与 created_at 读自 Run 行（Run 创建后不变）。
func rewriteRunSearchAttributes(db *gorm.DB, runID string, attrs map[string]string) error {
	rows, err := infradb.Find(db, gsql.SelectG[runKeyRow](RunSchema.Namespace, RunSchema.CreatedAt).
		From(RunSchema).Where(RunSchema.ID.Eq(runID)))
	if err != nil {
		return err
	}
	if len(rows) == 0 {
		return fmt.Errorf("run %s: %w", runID, execution.ErrRunNotFound)
	}
	return writeRunSearchAttributes(db, runID, rows[0].Namespace, rows[0].CreatedAt, attrs)
}

// listBySearchAttributes 是带搜索属性条件的 List：以按键排序的第一个 term 沿
// idx_run_search_attributes_lookup (namespace, attr_hash, created_at, run_id) 的顺序读，其余 term 经主键 EXISTS 判定，
// Run 级条件经主键关联 tenuto_runs 判定。数据库按索引顺序在凑够 limit+1 行时停止；游标与 CreatedBefore 落在
// 索引列上，翻页从游标处开始读。代价与第一个 term 的匹配数成正比。
func (s *Repository) listBySearchAttributes(runConds, stateConds []gsql.Expression, filter execution.StoreListFilter, limit int) (execution.StoreListResult, error) {
	terms := execution.FilterSearchAttributeTerms(filter.SearchAttributes)
	lead := RunSearchAttributeSchema.As("sa0")
	conds := []gsql.Expression{lead.AttrHash.EqAny(searchAttributeHash(terms[0]))}
	if filter.Namespace != "" {
		conds = append(conds, lead.Namespace.Eq(filter.Namespace))
	}
	if filter.CreatedBefore != nil {
		conds = append(conds, lead.CreatedAt.Lt(*filter.CreatedBefore))
	}
	for i, term := range terms[1:] {
		other := RunSearchAttributeSchema.As(fmt.Sprintf("sa%d", i+1))
		conds = append(conds, gsql.Exists(gsql.Select(gsql.Lit(1).As("_")).From(other).
			Where(other.RunID.EqF(lead.RunID), other.AttrHash.EqAny(searchAttributeHash(term)))))
	}
	if len(stateConds) > 0 {
		states := WorkflowExecutionStateSchema
		conds = append(conds, gsql.Exists(gsql.Select(gsql.Lit(1).As("_")).From(states).
			Where(states.RunID.EqF(lead.RunID)).Where(stateConds...)))
	}
	query := func() *gsql.QueryBuilderG[RunPO] {
		return gsql.SelectG[RunPO](RunSchema.Star()).From(lead).
			Join(gsql.InnerJoin(RunSchema).On(RunSchema.ID.EqF(lead.RunID))).
			Where(conds...).Where(runConds...)
	}
	var total int64
	if !filter.SkipTotal {
		var err error
		if total, err = query().Count(s.db); err != nil {
			return execution.StoreListResult{}, err
		}
	}
	page := query()
	if filter.After != nil {
		page = page.Where(cursorCondition(lead.CreatedAt, lead.RunID, filter.After, filter.OrderDesc))
	}
	rows, err := infradb.Find(s.db, page.Order(lead.CreatedAt, !filter.OrderDesc).Order(lead.RunID, !filter.OrderDesc).Limit(limit+1))
	if err != nil {
		return execution.StoreListResult{}, err
	}
	hasMore := len(rows) > limit
	runs, err := runRowsToDomain(rows[:min(len(rows), limit)])
	if err != nil {
		return execution.StoreListResult{}, err
	}
	return execution.StoreListResult{Runs: runs, Total: int(total), HasMore: hasMore}, nil
}
