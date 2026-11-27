package integration

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/infra/persistence/executionrepo"
	"github.com/donutnomad/tenuto/server/infra/persistence/mysqltest"
	"gorm.io/gorm"
)

// 按搜索属性过滤的 Run 列表的查询计划：20 万个 Run、两个 namespace、95% 已结束，每个 Run 有 Schedule ID（100 个取值）、
// TemporalChangeVersion 与 CustomerId 三个 term。执行仓储实际生成的 SQL（不是手写的等价语句）并 EXPLAIN：
// 列表页沿 idx_run_search_attributes_lookup 的顺序读，tenuto_runs 与其余 term 走主键；出现 filesort、临时表或全表扫描都失败。
// BlockedOnly 不在其中：blocked 的 Run 很少时优化器从 idx_workflow_execution_states_blocked 读起再排序，代价与 blocked 数成正比。
func TestRunListAttributeQueryPlans(t *testing.T) {
	db := mysqltest.NewDB(t)
	if err := db.Exec("SET SESSION cte_max_recursion_depth = 300000").Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Exec(`INSERT INTO tenuto_runs (id, namespace, workflow_id, workflow_type, workflow_config_version, status, created_at, attempt)
WITH RECURSIVE seq(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM seq WHERE n < 200000)
SELECT CONCAT('run-', n), IF(n % 2 = 0, 'tenant-a', 'tenant-b'), CONCAT('wf-', n), 'flow', 'v1',
       IF(n % 20 = 0, 'running', 'completed'), TIMESTAMPADD(SECOND, n, '2026-01-01'), 1 FROM seq`).Error; err != nil {
		t.Fatal(err)
	}
	for _, attr := range []struct{ key, value string }{
		{history.ScheduledByIDSearchAttribute, "CONCAT('sch-', CAST(SUBSTRING(id, 5) AS UNSIGNED) % 100)"},
		{history.ChangeVersionSearchAttribute, "IF(CAST(SUBSTRING(id, 5) AS UNSIGNED) % 50 = 0, 'policy-3', 'policy-4')"},
		{"CustomerId", "CONCAT('c-', CAST(SUBSTRING(id, 5) AS UNSIGNED) % 1000)"},
	} {
		if err := db.Exec(`INSERT INTO tenuto_run_search_attributes (run_id, attr_hash, namespace, created_at)
SELECT id, UNHEX(SHA2(CONCAT(UNHEX(LPAD(HEX(LENGTH(?)), 16, '0')), ?, `+attr.value+`), 256)), namespace, created_at FROM tenuto_runs`,
			attr.key, attr.key).Error; err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Exec("ANALYZE TABLE tenuto_runs, tenuto_run_search_attributes, tenuto_execution_states").Error; err != nil {
		t.Fatal(err)
	}

	// 记下仓储发出的每条查询（SQL 与参数），用同一组参数 EXPLAIN。
	type statement struct {
		sql  string
		vars []any
	}
	var captured []statement
	capture := func(tx *gorm.DB) {
		captured = append(captured, statement{sql: tx.Statement.SQL.String(), vars: append([]any(nil), tx.Statement.Vars...)})
	}
	if err := db.Callback().Query().After("gorm:query").Register("plan:capture", capture); err != nil {
		t.Fatal(err)
	}
	if err := db.Callback().Row().After("gorm:row").Register("plan:capture-row", capture); err != nil {
		t.Fatal(err)
	}
	repo, err := executionrepo.New(db, executionrepo.Config{})
	if err != nil {
		t.Fatal(err)
	}
	cursor := execution.RunCursor{CreatedAt: time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC), RunID: "run-86400"}
	createdBefore := time.Date(2026, 1, 3, 0, 0, 0, 0, time.UTC)
	schedule := map[string]string{history.ScheduledByIDSearchAttribute: "sch-8"}
	filters := []struct {
		name   string
		filter execution.StoreListFilter
	}{
		{"one attribute, all statuses, total", execution.StoreListFilter{SearchAttributes: schedule, IncludeTerminal: true, OrderDesc: true}},
		{"one attribute, open runs", execution.StoreListFilter{SearchAttributes: schedule, SkipTotal: true}},
		{"one attribute, one status", execution.StoreListFilter{SearchAttributes: schedule, Statuses: []history.Status{history.StatusRunning}, SkipTotal: true}},
		{"one attribute, cursor", execution.StoreListFilter{SearchAttributes: schedule, IncludeTerminal: true, After: &cursor, SkipTotal: true}},
		{"one attribute, cursor, descending", execution.StoreListFilter{SearchAttributes: schedule, IncludeTerminal: true, After: &cursor, OrderDesc: true, SkipTotal: true}},
		{"change version member", execution.StoreListFilter{SearchAttributes: map[string]string{history.ChangeVersionSearchAttribute: "policy-3"}, IncludeTerminal: true, SkipTotal: true}},
		{"two attributes", execution.StoreListFilter{SearchAttributes: map[string]string{history.ScheduledByIDSearchAttribute: "sch-8", "CustomerId": "c-8"}, IncludeTerminal: true, SkipTotal: true}},
		{"batch page", execution.StoreListFilter{SearchAttributes: schedule, IncludeTerminal: true, CreatedBefore: &createdBefore, After: &cursor, SkipTotal: true}},
	}
	for _, item := range filters {
		item.filter.Namespace, item.filter.Limit = "tenant-a", 50
		captured = nil
		if _, err := repo.List(item.filter); err != nil {
			t.Fatal(err)
		}
		if len(captured) == 0 {
			t.Fatalf("%s: no query captured", item.name)
		}
		for _, stmt := range captured {
			var rows []map[string]any
			if err := db.Raw("EXPLAIN "+stmt.sql, stmt.vars...).Scan(&rows).Error; err != nil {
				t.Fatalf("%s: explain %s: %v", item.name, stmt.sql, err)
			}
			for _, row := range rows {
				line := fmt.Sprintf("table=%v type=%v key=%v rows=%v extra=%v", row["table"], row["type"], row["key"], row["rows"], row["Extra"])
				t.Logf("%s: %s", item.name, line)
				if strings.Contains(line, "filesort") || strings.Contains(line, "temporary") || fmt.Sprint(row["type"]) == "ALL" {
					t.Errorf("%s sorts, aggregates into a temporary table or scans the whole table: %s\n%s", item.name, line, stmt.sql)
				}
			}
		}
	}
}
