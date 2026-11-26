package integration

import (
	"fmt"
	"strings"
	"testing"

	"github.com/donutnomad/tenuto/server/infra/persistence/mysqltest"
)

// Run 列表与统计的查询计划：20 万行、两个 namespace、95% 已结束。列表按 (namespace[, status], created_at, id)
// 索引顺序读一页，不排序全部 Run；run-stats 只扫一个 namespace 的索引区间；list-namespaces 按索引顺序分组，
// 不建临时表。任何一条出现 filesort、临时表或全表扫描都失败。
func TestRunListAndStatsQueryPlans(t *testing.T) {
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
	if err := db.Exec("ANALYZE TABLE tenuto_runs").Error; err != nil {
		t.Fatal(err)
	}
	queries := []struct{ name, sql string }{
		{"list page, all statuses", "SELECT * FROM tenuto_runs WHERE namespace = 'tenant-a' ORDER BY created_at DESC, id DESC LIMIT 51"},
		{"list page, one status", "SELECT * FROM tenuto_runs WHERE namespace = 'tenant-a' AND status IN ('running') ORDER BY created_at DESC, id DESC LIMIT 51"},
		{"list page, open runs", "SELECT * FROM tenuto_runs WHERE namespace = 'tenant-a' AND status NOT IN ('completed','failed','cancelled','terminated','continued_as_new') ORDER BY created_at DESC, id DESC LIMIT 51"},
		{"list page, cursor", "SELECT * FROM tenuto_runs WHERE namespace = 'tenant-a' AND (created_at > '2026-01-02' OR (created_at = '2026-01-02' AND id > 'run-1')) ORDER BY created_at, id LIMIT 51"},
		{"list total", "SELECT COUNT(*) FROM tenuto_runs WHERE namespace = 'tenant-a'"},
		{"run-stats", "SELECT namespace, status, COUNT(id) AS count FROM tenuto_runs WHERE namespace = 'tenant-a' GROUP BY namespace, status"},
		{"list-namespaces", "SELECT namespace, status, COUNT(id) AS count FROM tenuto_runs GROUP BY namespace, status"},
	}
	for _, query := range queries {
		var rows []map[string]any
		if err := db.Raw("EXPLAIN " + query.sql).Scan(&rows).Error; err != nil {
			t.Fatal(err)
		}
		for _, row := range rows {
			line := fmt.Sprintf("type=%v key=%v rows=%v extra=%v", row["type"], row["key"], row["rows"], row["Extra"])
			t.Logf("%s: %s", query.name, line)
			if strings.Contains(line, "filesort") || strings.Contains(line, "temporary") || fmt.Sprint(row["type"]) == "ALL" {
				t.Errorf("%s sorts, aggregates into a temporary table or scans the whole table: %s", query.name, line)
			}
		}
	}
}
