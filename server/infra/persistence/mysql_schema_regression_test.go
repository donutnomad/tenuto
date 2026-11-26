package persistence

import (
	"reflect"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	historypkg "github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	infradb "github.com/donutnomad/tenuto/server/infra/db"
	tenutosql "github.com/donutnomad/tenuto/server/infra/sql"
	"gorm.io/gorm/schema"
)

// ============================================================================
// 2026-08-03 四个 MySQL 阻断缺陷的回归测试
// ============================================================================
// 这些用例只在 TENUTO_TEST_MYSQL=1 时运行，且容器使用 MySQL 8 的**默认** sql_mode
// （含 NO_ZERO_DATE + STRICT_TRANS_TABLES），与生产一致。
// 不要为了让测试变绿而放宽 sql_mode——那正是上一轮把缺陷 4 测没了的原因。
// ============================================================================

// TestMySQLUsesDefaultStrictSQLMode 是"容器没被偷偷放宽"的看门测试。
//
// 上一轮为绕开零值 time.Time 写入失败，曾在容器启动参数里去掉 NO_ZERO_DATE 与
// STRICT_TRANS_TABLES。那不是修复：生产仍跑默认 sql_mode，照炸不误，而测试却变绿了。
// 这条断言把"必须用默认严格模式"变成硬约束——谁再放宽，这里当场失败。
func TestMySQLUsesDefaultStrictSQLMode(t *testing.T) {
	store := requireMySQL(t)

	var mode string
	if err := store.db.Raw("SELECT @@SESSION.sql_mode").Scan(&mode).Error; err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{"STRICT_TRANS_TABLES", "NO_ZERO_DATE"} {
		if !strings.Contains(mode, required) {
			t.Fatalf("sql_mode = %q, 缺少 %s。\n"+
				"容器必须使用 MySQL 8 默认 sql_mode 才能暴露零值 time.Time 落库问题；"+
				"放宽 sql_mode 等于把生产上真实存在的缺陷测没了。", mode, required)
		}
	}
	t.Logf("sql_mode = %s", mode)
}

// TestClearStickyExecutionUnderDefaultSQLMode 覆盖缺陷 4：
// 零值 time.Time 落库会被写成 '0000-00-00'，在默认 sql_mode 下报
// Error 1292: Incorrect datetime value —— 即使列 nullable 也拒绝。
//
// StickyExpiresAt 已改为 *time.Time，清 sticky 时写 nil（SQL NULL）。
// 本用例跑完整链路：Create → 赋 sticky → 清 sticky → 回读，全程不得报错。
func TestClearStickyExecutionUnderDefaultSQLMode(t *testing.T) {
	store := requireMySQL(t)

	run := &execution.Run{
		ID: "run-sticky-zero", WorkflowID: "wf-sticky", Namespace: protocol.DefaultNamespace,
		Status: history.StatusRunning, CreatedAt: time.Now(),
	}
	// store.Create 本身就是上一轮"~40 个用例全挂"的那一步，必须先跑通。
	if err := store.Executions.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(rootBranch(t, run)) }); err != nil {
		t.Fatalf("store.Create 在默认 sql_mode 下失败: %v", err)
	}

	state, err := store.Executions.GetWorkflowExecutionState(run.ID)
	if err != nil {
		t.Fatalf("初始化 execution state: %v", err)
	}

	gen, assigned, err := store.Executions.AssignStickyExecution(
		run.ID, state.ExecutionGeneration, state.StickyGeneration, "worker-a", "build-a")
	if err != nil || !assigned {
		t.Fatalf("AssignStickyExecution: assigned=%v err=%v", assigned, err)
	}
	// 有 sticky 绑定时调度的 WFT 带 sticky schedule-to-start 期限。
	if _, inserted, err := store.Executions.AppendWorkflowTaskScheduled(run.ID, run.ID, run.ID, history.NewWorkflowTaskScheduledEvent(1000)); err != nil || !inserted {
		t.Fatalf("AppendWorkflowTaskScheduled: inserted=%v err=%v", inserted, err)
	}

	assignedState, err := store.Executions.GetWorkflowExecutionState(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if assignedState.StickyExpiresAt == nil {
		t.Fatal("赋 sticky 后 StickyExpiresAt 仍为 nil")
	}
	if !assignedState.StickyValidAt(time.Now()) {
		t.Fatalf("sticky 应处于有效期内, 实际 expires=%v", *assignedState.StickyExpiresAt)
	}

	// 关键一步：清 sticky 会写零值 —— 修复前这里直接 Error 1292。
	if _, cleared, err := store.Executions.ClearStickyExecution(run.ID, assignedState.ExecutionGeneration, gen); err != nil {
		t.Fatalf("ClearStickyExecution 在默认 sql_mode 下失败（缺陷 4 回归）: %v", err)
	} else if !cleared {
		t.Fatal("ClearStickyExecution 未生效")
	}

	clearedState, err := store.Executions.GetWorkflowExecutionState(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if clearedState.StickyExpiresAt != nil {
		t.Fatalf("清 sticky 后 StickyExpiresAt = %v, want nil", *clearedState.StickyExpiresAt)
	}
	if clearedState.StickyWorkerIdentity != "" {
		t.Fatalf("清 sticky 后 identity = %q, want 空", clearedState.StickyWorkerIdentity)
	}
	// nil 必须真的落成 SQL NULL，而不是被写成某个哨兵值。
	var nullCount int64
	if err := store.db.Raw(
		"SELECT COUNT(*) FROM tenuto_execution_states WHERE run_id = ? AND sticky_expires_at IS NULL",
		run.ID).Scan(&nullCount).Error; err != nil {
		t.Fatal(err)
	}
	if nullCount != 1 {
		t.Fatalf("sticky_expires_at IS NULL 的行数 = %d, want 1", nullCount)
	}
}

// TestReleaseTaskDispatchUnderDefaultSQLMode 覆盖缺陷 4 的第二处零值写入：
// ReleaseTaskDispatch 清租约时写 lease_expires_at 零值，同样触发 Error 1292。
//
// ⚠️ 这里必须用**已过期**的租约（负 TTL）。本用例关心的是"清租约这个写操作在默认
// sql_mode 下不炸"，而不是"什么时候该清"——后者由 ReleaseTaskDispatch 的 lease 守卫
// 决定（见 mysql_dispatch_lease_guard_test.go）。守卫加入后，未过期的活跃租约会被
// 刻意保护、Release 不再改写该行，用负 TTL 才能真正走到零值写入那条路径。
func TestReleaseTaskDispatchUnderDefaultSQLMode(t *testing.T) {
	store := requireMySQL(t)

	task := protocol.Task{
		Namespace: protocol.DefaultNamespace, Type: protocol.TaskTypeWorkflow,
		RunID: "run-release-zero", ScheduledEventID: 11,
	}
	if _, acquired, err := store.Dispatches.AcquireTaskDispatch(task, -time.Minute); err != nil || !acquired {
		t.Fatalf("AcquireTaskDispatch: acquired=%v err=%v", acquired, err)
	}
	if err := store.Dispatches.ReleaseTaskDispatch(task, "queue down"); err != nil {
		t.Fatalf("ReleaseTaskDispatch 在默认 sql_mode 下失败（缺陷 4 回归）: %v", err)
	}

	var nullCount int64
	if err := store.db.Raw(
		"SELECT COUNT(*) FROM tenuto_task_dispatches WHERE run_id = ? AND lease_expires_at IS NULL",
		task.RunID).Scan(&nullCount).Error; err != nil {
		t.Fatal(err)
	}
	if nullCount != 1 {
		t.Fatalf("lease_expires_at IS NULL 的行数 = %d, want 1", nullCount)
	}
}

// TestSchemaSQLBuildsEveryTableOnMySQL 在 MySQL 上执行全部迁移（mysqltest.NewDB；迁移与 schema.sql 的一致由
// server/infra/sql 的 TestTenutoMigrationsBuildSchemaSQL 核对），再按 information_schema 核对：建出的表恰好是
// PO 的表、schemaOnlyTables 与迁移记录表，每个 PO 的列都在，
// retention GC 的 tree_id 索引列序正确。建表语句中途报错（如主键超出 InnoDB 3072 字节上限的
// Error 1071）时后面的表都建不出来，这里会列出缺的表。
func TestSchemaSQLBuildsEveryTableOnMySQL(t *testing.T) {
	store := requireMySQL(t)
	dbName := currentDatabaseName(t, store.db)
	tables := databaseTableColumns(t, store.db, dbName)

	want := append([]string{tenutosql.MigrationTable}, schemaOnlyTables...)
	for table := range poTableColumns(t) {
		want = append(want, table)
	}
	slices.Sort(want)
	if got := sortedKeys(tables); !slices.Equal(got, want) {
		t.Errorf("迁移建出的表 = %v, want %v", got, want)
	}
	for table, poCols := range poTableColumns(t) {
		var missing []string
		for col := range poCols {
			if !tables[table][col] {
				missing = append(missing, col)
			}
		}
		if len(missing) > 0 {
			slices.Sort(missing)
			t.Errorf("表 %s 缺列 %v", table, missing)
		}
	}

	wantTreeIndex := []string{"tree_id", "run_id"}
	if got := indexColumns(t, store.db, dbName, "tenuto_events", eventsTreeRunIndex); !slices.Equal(got, wantTreeIndex) {
		t.Errorf("%s 列 = %v, want %v", eventsTreeRunIndex, got, wantTreeIndex)
	}
}

// TestSchemaSQLAcceptsNullForNilableFields 核对 Go 字段可为 nil 的列（指针、切片、map、接口）在
// schema.sql 建出的库上可以写 NULL：这些字段为 nil 时 GORM 写入 NULL，NOT NULL 列会报 Column cannot be null。
// requiredJSONColumns 里的列每次写入都给出序列化结果，NOT NULL 与写入一致。
func TestSchemaSQLAcceptsNullForNilableFields(t *testing.T) {
	store := requireMySQL(t)
	nullable := databaseNullableColumns(t, store.db, currentDatabaseName(t, store.db))
	for key := range nilableModelColumns(t) {
		columnNullable, ok := nullable[key]
		if !ok {
			continue // 缺列由 TestSchemaSQLBuildsEveryTableOnMySQL 报告
		}
		if !columnNullable && !slices.Contains(requiredJSONColumns, key) {
			t.Errorf("%s：Go 字段可为 nil（写入 NULL），schema.sql 声明 NOT NULL", key)
		}
	}
}

// requiredJSONColumns 是 Go 类型为 []byte、仓储每次写入都给出序列化结果的列，schema.sql 的 NOT NULL 与写入一致。
var requiredJSONColumns = []string{
	"tenuto_configs.config_json",
	"tenuto_history_tree.data_json",
	"tenuto_events.event_json",
	// attr_hash 每次写入都是 32 字节的 SHA-256。
	"tenuto_run_search_attributes.attr_hash",
}

// TestSchemaSQLDatabaseAcceptsLongErrorsAndReleasedLeases 在按 schema.sql 建出的库上执行两类写入：
// 带长错误文本的失败事件，以及带长错误文本的派发租约释放（lease_expires_at 写 NULL）。
func TestSchemaSQLDatabaseAcceptsLongErrorsAndReleasedLeases(t *testing.T) {
	store := requireMySQL(t)
	longError := strings.Repeat("activity failed: upstream returned an unexpected response; ", 40)

	createTestRun(t, store, "run-long-error")
	if _, err := store.Executions.Append("run-long-error", "run-long-error", "run-long-error",
		historypkg.NewWorkflowFailedEvent(longError, "")); err != nil {
		t.Fatalf("append failed event with a %d-byte error: %v", len(longError), err)
	}

	task := protocol.Task{
		Namespace: protocol.DefaultNamespace, Type: protocol.TaskTypeWorkflow,
		RunID: "run-long-error", ScheduledEventID: 7,
	}
	if _, acquired, err := store.Dispatches.AcquireTaskDispatch(task, -time.Minute); err != nil || !acquired {
		t.Fatalf("AcquireTaskDispatch: acquired=%v err=%v", acquired, err)
	}
	if err := store.Dispatches.ReleaseTaskDispatch(task, longError); err != nil {
		t.Fatalf("release dispatch with a %d-byte error: %v", len(longError), err)
	}
}

// nilableModelColumns 返回 Go 字段类型可以取 nil 的列（指针、切片、map、接口），键为 "表.列"。
// 这些字段为 nil 时 GORM 写入 NULL。
func nilableModelColumns(t *testing.T) map[string]bool {
	t.Helper()
	result := make(map[string]bool)
	for _, model := range poModels {
		parsed, err := schema.Parse(model, &sync.Map{}, schema.NamingStrategy{})
		if err != nil {
			t.Fatalf("parse model %T: %v", model, err)
		}
		for _, field := range parsed.Fields {
			switch field.FieldType.Kind() {
			case reflect.Pointer, reflect.Slice, reflect.Map, reflect.Interface:
				result[parsed.Table+"."+field.DBName] = true
			}
		}
	}
	return result
}

// databaseNullableColumns 读 information_schema，返回 "表.列" -> 是否可空。
func databaseNullableColumns(t *testing.T, db infradb.DB, dbName string) map[string]bool {
	t.Helper()
	type row struct {
		TableName  string
		ColumnName string
		IsNullable string
	}
	var rows []row
	if err := db.Raw(`
		SELECT TABLE_NAME AS table_name, COLUMN_NAME AS column_name, IS_NULLABLE AS is_nullable
		FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ?`, dbName).Scan(&rows).Error; err != nil {
		t.Fatal(err)
	}
	result := make(map[string]bool, len(rows))
	for _, r := range rows {
		result[r.TableName+"."+r.ColumnName] = r.IsNullable == "YES"
	}
	return result
}

const eventsTreeRunIndex = "idx_workflow_events_tree_run"

// indexColumns 按 SEQ_IN_INDEX 返回某索引的列，索引不存在时返回空。
func indexColumns(t *testing.T, db infradb.DB, dbName, table, index string) []string {
	t.Helper()
	var columns []string
	if err := db.Raw(
		"SELECT COLUMN_NAME FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND INDEX_NAME = ? ORDER BY SEQ_IN_INDEX",
		dbName, table, index,
	).Scan(&columns).Error; err != nil {
		t.Fatal(err)
	}
	return columns
}

// TestWorkerVersionsPrimaryKeyFitsInnoDBLimit 覆盖缺陷 1 的直接成因：
// 复合主键字节数必须落在 InnoDB 3072 上限内。直接查 information_schema 算账，
// 而不是仅依赖"建表没报错"——后者在别的字符集下可能侥幸通过。
func TestWorkerVersionsPrimaryKeyFitsInnoDBLimit(t *testing.T) {
	store := requireMySQL(t)
	dbName := currentDatabaseName(t, store.db)

	type colInfo struct {
		ColumnName string
		MaxLen     int64
	}
	var cols []colInfo
	if err := store.db.Raw(`
		SELECT c.COLUMN_NAME AS column_name, c.CHARACTER_MAXIMUM_LENGTH AS max_len
		FROM information_schema.STATISTICS s
		JOIN information_schema.COLUMNS c
		  ON c.TABLE_SCHEMA = s.TABLE_SCHEMA AND c.TABLE_NAME = s.TABLE_NAME
		 AND c.COLUMN_NAME = s.COLUMN_NAME
		WHERE s.TABLE_SCHEMA = ? AND s.TABLE_NAME = 'tenuto_activity_worker_versions'
		  AND s.INDEX_NAME = 'PRIMARY'`, dbName).Scan(&cols).Error; err != nil {
		t.Fatal(err)
	}
	if len(cols) == 0 {
		t.Fatal("未取到 tenuto_activity_worker_versions 的主键列——表可能没建出来")
	}

	// utf8mb4 每字符最多 4 字节。
	const bytesPerChar = 4
	const innoDBKeyLimit = 3072
	var total int64
	for _, c := range cols {
		total += c.MaxLen * bytesPerChar
		t.Logf("主键列 %-14s varchar(%d) → %d 字节", c.ColumnName, c.MaxLen, c.MaxLen*bytesPerChar)
	}
	if total > innoDBKeyLimit {
		t.Fatalf("复合主键 %d 字节 > InnoDB 上限 %d——建表会报 Error 1071", total, innoDBKeyLimit)
	}
	t.Logf("复合主键合计 %d 字节 ≤ %d，余量 %d", total, innoDBKeyLimit, innoDBKeyLimit-total)
}

// ── helpers ──

func currentDatabaseName(t *testing.T, db infradb.DB) string {
	t.Helper()
	var name string
	if err := db.Raw("SELECT DATABASE()").Scan(&name).Error; err != nil {
		t.Fatal(err)
	}
	return name
}

// databaseTableColumns 读 information_schema，返回 表名 -> 列名集合。
func databaseTableColumns(t *testing.T, db infradb.DB, dbName string) map[string]map[string]bool {
	t.Helper()
	type row struct {
		TableName  string
		ColumnName string
	}
	var rows []row
	if err := db.Raw(`
		SELECT TABLE_NAME AS table_name, COLUMN_NAME AS column_name
		FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ?`, dbName).Scan(&rows).Error; err != nil {
		t.Fatal(err)
	}
	result := make(map[string]map[string]bool)
	for _, r := range rows {
		if result[r.TableName] == nil {
			result[r.TableName] = make(map[string]bool)
		}
		result[r.TableName][r.ColumnName] = true
	}
	return result
}

func sortedKeys(m map[string]map[string]bool) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
