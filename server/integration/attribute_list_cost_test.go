package integration

import (
	"strconv"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/infra/persistence/executionrepo"
	"github.com/donutnomad/tenuto/server/infra/persistence/mysqltest"
	"gorm.io/gorm"
)

func sessionCounter(t *testing.T, db *gorm.DB, name string) int64 {
	t.Helper()
	var row struct {
		VariableName string `gorm:"column:Variable_name"`
		Value        string `gorm:"column:Value"`
	}
	if err := db.Raw("SHOW SESSION STATUS LIKE ?", name).Scan(&row).Error; err != nil {
		t.Fatal(err)
	}
	v, _ := strconv.ParseInt(row.Value, 10, 64)
	return v
}

// attributeHashSQL 在 SQL 里算出与 executionrepo.searchAttributeHash 相同的 attr_hash：SHA-256(8 字节大端键长 ‖ 键 ‖ 值)。
const attributeHashSQL = "UNHEX(SHA2(CONCAT(UNHEX(LPAD(HEX(LENGTH(?)), 16, '0')), ?, ?), 256))"

// 按搜索属性过滤的 Run 列表（Console 的 Schedule「Run Count」链接、TemporalChangeVersion 存量查询、按属性过滤的批量操作）
// 沿 tenuto_run_search_attributes 的索引读：20 万个已结束的 Run 里只有最早的 5 个匹配时，一页的读行数与发给 Server 的
// 字节数与匹配数成正比，不随 namespace 的 Run 数增长。会话计数器取自同一连接。
func TestAttributeFilteredRunListReadsOnlyMatches(t *testing.T) {
	db := mysqltest.NewDB(t)
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	sqlDB.SetMaxOpenConns(1) // 会话计数器只在同一连接上可比

	const total = 200000
	if err := db.Exec("SET SESSION cte_max_recursion_depth = 300000").Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Exec(`INSERT INTO tenuto_runs (id, namespace, workflow_id, workflow_type, workflow_config_version, status, created_at, attempt)
WITH RECURSIVE seq(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM seq WHERE n < ?)
SELECT CONCAT('run-', LPAD(n, 7, '0')), 'tenant-a', CONCAT('wf-', n), 'flow', 'v1', 'completed', TIMESTAMPADD(SECOND, n, '2026-01-01'), 1 FROM seq`, total).Error; err != nil {
		t.Fatal(err)
	}
	// 5 个最早的 Run 属于目标 Schedule、带旧版本标记；其余属于别的 Schedule。
	for _, attr := range []struct{ key, target, other string }{
		{history.ScheduledByIDSearchAttribute, "sch-target", "sch-other"},
		{history.ChangeVersionSearchAttribute, "charge-policy-3", "charge-policy-4"},
		{"CustomerId", "c-1", "c-2"},
	} {
		if err := db.Exec(`INSERT INTO tenuto_run_search_attributes (run_id, attr_hash, namespace, created_at)
SELECT id, IF(id <= 'run-0000005', `+attributeHashSQL+`, `+attributeHashSQL+`), namespace, created_at FROM tenuto_runs`,
			attr.key, attr.key, attr.target, attr.key, attr.key, attr.other).Error; err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Exec("ANALYZE TABLE tenuto_runs, tenuto_run_search_attributes").Error; err != nil {
		t.Fatal(err)
	}
	repo, err := executionrepo.New(db, executionrepo.Config{})
	if err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name      string
		attrs     map[string]string
		skipTotal bool
	}{
		{"schedule_id (Console Run Count 链接)", map[string]string{history.ScheduledByIDSearchAttribute: "sch-target"}, true},
		{"TemporalChangeVersion (版本存量查询)", map[string]string{history.ChangeVersionSearchAttribute: "charge-policy-3"}, true},
		{"two attributes with total", map[string]string{history.ScheduledByIDSearchAttribute: "sch-target", "CustomerId": "c-1"}, false},
	}
	for _, c := range cases {
		readsBefore := sessionCounter(t, db, "Handler_read_next") + sessionCounter(t, db, "Handler_read_prev") + sessionCounter(t, db, "Handler_read_key")
		sentBefore := sessionCounter(t, db, "Bytes_sent")
		started := time.Now()
		result, err := repo.List(execution.StoreListFilter{
			Namespace: "tenant-a", SearchAttributes: c.attrs, IncludeTerminal: true,
			OrderDesc: true, Limit: 50, SkipTotal: c.skipTotal,
		})
		if err != nil {
			t.Fatal(err)
		}
		elapsed := time.Since(started)
		reads := sessionCounter(t, db, "Handler_read_next") + sessionCounter(t, db, "Handler_read_prev") + sessionCounter(t, db, "Handler_read_key") - readsBefore
		sent := sessionCounter(t, db, "Bytes_sent") - sentBefore
		t.Logf("%s: %d runs returned (total %d), %d index/row reads, %d bytes sent to Server, %s", c.name, len(result.Runs), result.Total, reads, sent, elapsed.Round(time.Millisecond))
		if len(result.Runs) != 5 || result.HasMore || (!c.skipTotal && result.Total != 5) {
			t.Fatalf("%s: runs=%d total=%d has_more=%v, want the 5 matching runs", c.name, len(result.Runs), result.Total, result.HasMore)
		}
		if reads > 200 || sent > 64<<10 {
			t.Fatalf("%s: %d index/row reads and %d bytes for 5 matches among %d runs; the page must read only matching rows", c.name, reads, sent, total)
		}
	}
}
