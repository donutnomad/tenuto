package persistence

import (
	"regexp"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/donutnomad/tenuto/server/infra/persistence/batchrepo"
	"github.com/donutnomad/tenuto/server/infra/persistence/definitionrepo"
	"github.com/donutnomad/tenuto/server/infra/persistence/dispatchrepo"
	"github.com/donutnomad/tenuto/server/infra/persistence/executionrepo"
	"github.com/donutnomad/tenuto/server/infra/persistence/schedulerepo"
	tenutosql "github.com/donutnomad/tenuto/server/infra/sql"
	"gorm.io/gorm/schema"
)

// 表只由 server/infra/sql/migrations 的迁移建出，schema.sql 是执行完全部迁移后的完整表结构
// （二者的一致由 server/infra/sql 的 TestTenutoMigrationsBuildSchemaSQL 在 MySQL 上核对）。
// 本文件的守卫让 PO、PO 的 MysqlCreateTable() 与 schema.sql 保持一致：
//   - 每个 PO 的 MysqlCreateTable() 与 schema.sql 里同名表的语句相同；
//   - schema.sql 的每张表都有 PO，或登记在 schemaOnlyTables；
//   - 每个 PO 的 column tag 在它的表里都有对应的列，表里的列也都有 PO 字段。

// poModels 是各仓储的全部 PO。新增 PO 时登记在这里，守卫才会检查它。
var poModels = []any{
	&executionrepo.RunPO{},
	&executionrepo.EventPO{},
	&executionrepo.WorkflowExecutionStatePO{},
	&executionrepo.HistoryTreePO{},
	&executionrepo.ActivityStatePO{},
	&executionrepo.TimerStatePO{},
	&executionrepo.ChildWorkflowStatePO{},
	&executionrepo.DeletionTaskPO{},
	&definitionrepo.WorkflowConfigPO{},
	&definitionrepo.WorkflowConfigActivePO{},
	&dispatchrepo.TaskDispatchPO{},
	&dispatchrepo.ActivityWorkerVersionPO{},
	&schedulerepo.SchedulePO{},
	&batchrepo.BatchOperationPO{},
}

// schemaOnlyTables 是 schema.sql 里没有 PO 的表：leader 选主的两张表由 dbleader 读写，
// 它的行类型不导出。
var schemaOnlyTables = []string{
	"tenuto_leader_lock",
	"tenuto_leader_lock_members",
}

// schemaOnlyColumns 记录允许只出现在 schema.sql、没有 PO 字段的列。目前为空：
// 任何这类差异都应被视为漂移并显式登记，而不是默默放过。
var schemaOnlyColumns = map[string][]string{}

// tableNamer 与 mysqlDDL 由每个 PO 实现。
type tableNamer interface {
	TableName() string
}

type mysqlDDL interface {
	MysqlCreateTable() string
}

// createTableRe 匹配 schema.sql 里的建表语句，兼容可选的 IF NOT EXISTS 与反引号。
var createTableRe = regexp.MustCompile("(?i)^CREATE\\s+TABLE\\s+(?:IF\\s+NOT\\s+EXISTS\\s+)?`?([a-z0-9_]+)`?")

// columnDefRe 从建表语句里抓取列定义行的列名（形如 "`col_name` TYPE ..."）。
// 约束行以 PRIMARY KEY / INDEX / UNIQUE / KEY 开头，不以反引号开头，天然排除。
var columnDefRe = regexp.MustCompile("^`([a-z0-9_]+)`\\s+[A-Za-z]")

// schemaSQLStatements 按表名返回 schema.sql 里的建表语句（已去掉整行注释与结尾分号）。
func schemaSQLStatements(t *testing.T) map[string]string {
	t.Helper()
	statements := make(map[string]string)
	for _, statement := range tenutosql.Statements(tenutosql.Schema) {
		match := createTableRe.FindStringSubmatch(statement)
		if match == nil {
			t.Fatalf("schema.sql 里有非建表语句：%s", statement)
		}
		if _, dup := statements[match[1]]; dup {
			t.Fatalf("schema.sql 重复建表 %s", match[1])
		}
		statements[match[1]] = statement
	}
	if len(statements) == 0 {
		t.Fatal("schema.sql 没有建表语句")
	}
	return statements
}

// schemaSQLTableColumns 解析 schema.sql，返回 表名 -> 列名集合。
func schemaSQLTableColumns(t *testing.T) map[string]map[string]bool {
	t.Helper()
	result := make(map[string]map[string]bool)
	for table, statement := range schemaSQLStatements(t) {
		cols := make(map[string]bool)
		for _, line := range strings.Split(statement, "\n") {
			if m := columnDefRe.FindStringSubmatch(strings.TrimSpace(line)); m != nil {
				cols[m[1]] = true
			}
		}
		if len(cols) == 0 {
			t.Fatalf("parsed 0 columns for table %q in schema.sql —— 解析失真，请检查 columnDefRe 与文件格式", table)
		}
		result[table] = cols
	}
	return result
}

// poTableColumns 用 GORM 的 schema.Parse 读出每个 PO 的表名与列名（来自 column tag）。
func poTableColumns(t *testing.T) map[string]map[string]bool {
	t.Helper()
	result := make(map[string]map[string]bool, len(poModels))
	for _, model := range poModels {
		parsed, err := schema.Parse(model, &sync.Map{}, schema.NamingStrategy{})
		if err != nil {
			t.Fatalf("parse PO %T: %v", model, err)
		}
		if parsed.Table != model.(tableNamer).TableName() {
			t.Fatalf("PO %T: parsed table %q, TableName() %q", model, parsed.Table, model.(tableNamer).TableName())
		}
		cols := make(map[string]bool, len(parsed.DBNames))
		for _, name := range parsed.DBNames {
			cols[name] = true
		}
		result[parsed.Table] = cols
	}
	return result
}

// normalizeDDL 去掉注释、反引号、IF NOT EXISTS 与空白差异，得到可逐字比较的建表语句。
func normalizeDDL(ddl string) string {
	var lines []string
	for _, line := range strings.Split(ddl, "\n") {
		if i := strings.Index(line, "--"); i >= 0 {
			line = line[:i]
		}
		line = strings.TrimSpace(line)
		if line != "" {
			lines = append(lines, line)
		}
	}
	text := strings.Join(lines, " ")
	text = strings.ReplaceAll(text, "`", "")
	text = regexp.MustCompile(`(?i)CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS`).ReplaceAllString(text, "CREATE TABLE")
	text = regexp.MustCompile(`\s+`).ReplaceAllString(text, " ")
	text = strings.ReplaceAll(text, "( ", "(")
	text = strings.ReplaceAll(text, " )", ")")
	return strings.TrimSuffix(strings.TrimSpace(text), ";")
}

// TestMysqlCreateTableMatchesSchemaSQL 断言每个 PO 的 MysqlCreateTable() 与 schema.sql 里同名表的语句
// 除注释、反引号、IF NOT EXISTS 与空白外逐字相同。
func TestMysqlCreateTableMatchesSchemaSQL(t *testing.T) {
	statements := schemaSQLStatements(t)
	for _, model := range poModels {
		table := model.(tableNamer).TableName()
		ddl, ok := model.(mysqlDDL)
		if !ok {
			t.Errorf("%T 没有 MysqlCreateTable()", model)
			continue
		}
		want, ok := statements[table]
		if !ok {
			t.Errorf("schema.sql 没有表 %s", table)
			continue
		}
		if got, want := normalizeDDL(ddl.MysqlCreateTable()), normalizeDDL(want); got != want {
			t.Errorf("表 %s 的 MysqlCreateTable() 与 schema.sql 不一致\nPO:         %s\nschema.sql: %s", table, got, want)
		}
	}
}

// TestSchemaSQLTablesHavePOs 断言 schema.sql 的每张表都有 PO 或登记在 schemaOnlyTables，
// 每个 PO 的表都在 schema.sql 里，白名单里的表也都还在 schema.sql 里。
func TestSchemaSQLTablesHavePOs(t *testing.T) {
	statements := schemaSQLStatements(t)
	poTables := make(map[string]bool, len(poModels))
	for _, model := range poModels {
		poTables[model.(tableNamer).TableName()] = true
	}
	for table := range poTables {
		if _, ok := statements[table]; !ok {
			t.Errorf("PO 的表 %s 不在 schema.sql 里：按 schema.sql 建的库没有这张表", table)
		}
	}
	for table := range statements {
		if !poTables[table] && !slices.Contains(schemaOnlyTables, table) {
			t.Errorf("schema.sql 的表 %s 没有 PO：在 poModels 里登记它的 PO，或（由别的组件读写时）加入 schemaOnlyTables", table)
		}
	}
	for _, table := range schemaOnlyTables {
		if _, ok := statements[table]; !ok {
			t.Errorf("schemaOnlyTables 里的 %q 已不在 schema.sql 中，请清理白名单", table)
		}
		if poTables[table] {
			t.Errorf("schemaOnlyTables 里的 %q 已有 PO，请从白名单移除", table)
		}
	}
}

// TestPOColumnsMatchSchemaSQL 是列级守卫：PO 的每个 column tag 在它的表里都有列
// （否则读写报 Unknown column），表里的每一列也都有 PO 字段（否则是废弃列或漏映射的字段，
// 确属预期时登记进 schemaOnlyColumns）。
func TestPOColumnsMatchSchemaSQL(t *testing.T) {
	schemaCols := schemaSQLTableColumns(t)
	for table, poCols := range poTableColumns(t) {
		got, ok := schemaCols[table]
		if !ok {
			continue // TestSchemaSQLTablesHavePOs 报告缺表
		}
		var missing []string
		for col := range poCols {
			if !got[col] {
				missing = append(missing, col)
			}
		}
		if len(missing) > 0 {
			slices.Sort(missing)
			t.Errorf("表 %s：PO 有、schema.sql 没有的列 %v——读写该表时报 Unknown column，请补进 schema.sql 与 MysqlCreateTable()", table, missing)
		}
		var extra []string
		for col := range got {
			if !poCols[col] && !slices.Contains(schemaOnlyColumns[table], col) {
				extra = append(extra, col)
			}
		}
		if len(extra) > 0 {
			slices.Sort(extra)
			t.Errorf("表 %s：schema.sql 有、PO 没有的列 %v——补上 PO 字段，或登记进 schemaOnlyColumns", table, extra)
		}
	}
}
