package sql_test

import (
	"context"
	"log/slog"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/fstest"

	"github.com/donutnomad/tenuto/server/infra/persistence/mysqltest"
	tenutosql "github.com/donutnomad/tenuto/server/infra/sql"
	"gorm.io/gorm"
)

// 本文件的测试需要 MySQL（TENUTO_TEST_MYSQL=1，见 mysqltest），没有时跳过。

func TestMain(m *testing.M) {
	code := m.Run()
	mysqltest.Terminate()
	os.Exit(code)
}

var discard = slog.New(slog.DiscardHandler)

// showCreateTables 返回库里每张表的 SHOW CREATE TABLE，不含迁移记录表。
func showCreateTables(t *testing.T, db *gorm.DB) map[string]string {
	t.Helper()
	var tables []string
	if err := db.Raw("SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE()").Scan(&tables).Error; err != nil {
		t.Fatal(err)
	}
	result := make(map[string]string, len(tables))
	for _, table := range tables {
		if table == tenutosql.MigrationTable {
			continue
		}
		var name, ddl string
		if err := db.Raw("SHOW CREATE TABLE `"+table+"`").Row().Scan(&name, &ddl); err != nil {
			t.Fatalf("show create table %s: %v", table, err)
		}
		result[table] = ddl
	}
	return result
}

func assertSameTables(t *testing.T, fromMigrations, fromSchema map[string]string, schemaName string) {
	t.Helper()
	if len(fromSchema) == 0 {
		t.Fatalf("%s 没有建出表", schemaName)
	}
	for table, want := range fromSchema {
		got, ok := fromMigrations[table]
		if !ok {
			t.Errorf("执行完全部迁移的库缺少 %s 里的表 %s：新增一个建表迁移", schemaName, table)
			continue
		}
		if got != want {
			t.Errorf("表 %s：执行完全部迁移的结果与 %s 不同\nmigrations:\n%s\n%s:\n%s", table, schemaName, got, schemaName, want)
		}
	}
	for table := range fromMigrations {
		if _, ok := fromSchema[table]; !ok {
			t.Errorf("迁移建出了 %s 里没有的表 %s：把它写进 %s", schemaName, table, schemaName)
		}
	}
}

// TestTenutoMigrationsBuildSchemaSQL 断言在空库上执行全部迁移得到的表与按 schema.sql 建出的表逐字相同（SHOW CREATE TABLE）。
func TestTenutoMigrationsBuildSchemaSQL(t *testing.T) {
	migrated := mysqltest.NewDB(t)
	reference := mysqltest.NewEmptyDB(t)
	if err := tenutosql.Exec(t.Context(), reference, tenutosql.Schema); err != nil {
		t.Fatalf("apply schema.sql: %v", err)
	}
	assertSameTables(t, showCreateTables(t, migrated), showCreateTables(t, reference), "schema.sql")
}

// TestTaskQMigrationsBuildTaskQSchemaSQL 断言执行全部 TaskQ 迁移得到的表与按本次构建所用 TaskQ 模块的 schema.sql 建出的表相同。
// 升级 TaskQ 带来表结构变化时这里失败：在 taskq_migrations 里新增迁移。
func TestTaskQMigrationsBuildTaskQSchemaSQL(t *testing.T) {
	script, err := mysqltest.TaskQSchema()
	if err != nil {
		t.Fatal(err)
	}
	migrated := mysqltest.NewTaskQDB(t)
	reference := mysqltest.NewEmptyDB(t)
	if err := tenutosql.Exec(t.Context(), reference, script); err != nil {
		t.Fatalf("apply TaskQ schema.sql: %v", err)
	}
	assertSameTables(t, showCreateTables(t, migrated), showCreateTables(t, reference), "TaskQ schema.sql")
}

// TestConcurrentMigrateAppliesOnce 模拟多个副本同时执行 migrate：命名锁让它们依次进行，迁移只执行一次，
// 再次执行什么也不做；Workflow 与 TaskQ 共用一个库时两组迁移各自记录。
func TestConcurrentMigrateAppliesOnce(t *testing.T) {
	db := mysqltest.NewEmptyDB(t)
	const replicas = 4
	var wg sync.WaitGroup
	results := make([][]string, replicas)
	errs := make([]error, replicas)
	for i := range replicas {
		wg.Go(func() {
			results[i], _, errs[i] = tenutosql.TenutoMigrations.Migrate(t.Context(), db, discard)
		})
	}
	wg.Wait()
	var applied []string
	for i := range replicas {
		if errs[i] != nil {
			t.Fatalf("replica %d: %v", i, errs[i])
		}
		applied = append(applied, results[i]...)
	}
	var want []string
	for _, migration := range tenutosql.TenutoMigrations.List {
		want = append(want, migration.Name)
	}
	if !slices.Equal(applied, want) {
		t.Fatalf("applied across replicas = %v, want each of %v exactly once", applied, want)
	}

	if again, _, err := tenutosql.TenutoMigrations.Migrate(t.Context(), db, discard); err != nil || len(again) != 0 {
		t.Fatalf("second migrate applied %v, err = %v", again, err)
	}
	if _, err := tenutosql.TaskQMigrations.Check(t.Context(), db); err == nil {
		t.Fatal("TaskQ check passed before TaskQ migrations ran")
	}
	if _, _, err := tenutosql.TaskQMigrations.Migrate(t.Context(), db, discard); err != nil {
		t.Fatal(err)
	}
	for _, migrations := range []tenutosql.Migrations{tenutosql.TenutoMigrations, tenutosql.TaskQMigrations} {
		if unknown, err := migrations.Check(t.Context(), db); err != nil || len(unknown) != 0 {
			t.Fatalf("%s check: unknown = %v, err = %v", migrations.Component, unknown, err)
		}
	}
}

// TestCheckReportsUninitializedAheadAndEditedSchemas 覆盖启动检查的三种结果：未初始化、库比二进制新、已执行的迁移被改过。
func TestCheckReportsUninitializedAheadAndEditedSchemas(t *testing.T) {
	ctx := context.Background()
	migrations := tenutosql.TenutoMigrations
	db := mysqltest.NewEmptyDB(t)
	if _, err := migrations.Check(ctx, db); err == nil || !strings.Contains(err.Error(), "tenuto-server migrate") {
		t.Fatalf("check on an empty database: %v", err)
	}
	if _, _, err := migrations.Migrate(ctx, db, discard); err != nil {
		t.Fatal(err)
	}

	future := migrations.Latest() + 1
	if err := db.Exec("INSERT INTO "+tenutosql.MigrationTable+" (component, version, name, checksum, statements_applied, applied_at) VALUES (?, ?, 'future', 'x', 1, NOW(3))",
		migrations.Component, future).Error; err != nil {
		t.Fatal(err)
	}
	if unknown, err := migrations.Check(ctx, db); err != nil || !slices.Equal(unknown, []int{future}) {
		t.Fatalf("check on a newer database: unknown = %v, err = %v", unknown, err)
	}
	if _, unknown, err := migrations.Migrate(ctx, db, discard); err != nil || !slices.Equal(unknown, []int{future}) {
		t.Fatalf("migrate on a newer database: unknown = %v, err = %v", unknown, err)
	}

	if err := db.Exec("UPDATE "+tenutosql.MigrationTable+" SET checksum = 'edited' WHERE component = ? AND version = 1", migrations.Component).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := migrations.Check(ctx, db); err == nil || !strings.Contains(err.Error(), "must never be edited") {
		t.Fatalf("check with an edited migration: %v", err)
	}
	if _, _, err := migrations.Migrate(ctx, db, discard); err == nil || !strings.Contains(err.Error(), "must never be edited") {
		t.Fatalf("migrate with an edited migration: %v", err)
	}
}

// TestMigrateResumesAfterAFailedStatement：第 2 条语句失败时第 1 条已提交；启动检查报告落后；
// 修复原因后再次执行，从第 2 条继续（第 1 条若重新执行会因表已存在而失败）。
func TestMigrateResumesAfterAFailedStatement(t *testing.T) {
	ctx := context.Background()
	migrations, err := tenutosql.LoadMigrations("resume_test", fstest.MapFS{
		"m/0001_three_steps.sql": {Data: []byte("CREATE TABLE first_table (id INT);\nINSERT INTO missing_table VALUES (1);\nCREATE TABLE third_table (id INT);\n")},
	}, "m")
	if err != nil {
		t.Fatal(err)
	}
	db := mysqltest.NewEmptyDB(t)
	if _, _, err := migrations.Migrate(ctx, db, discard); err == nil || !strings.Contains(err.Error(), "statement 2 of 3") {
		t.Fatalf("migrate with a failing statement: %v", err)
	}
	if _, err := migrations.Check(ctx, db); err == nil || !strings.Contains(err.Error(), "0001_three_steps") {
		t.Fatalf("check after a failed migration: %v", err)
	}
	if err := db.Exec("CREATE TABLE missing_table (id INT)").Error; err != nil {
		t.Fatal(err)
	}
	applied, _, err := migrations.Migrate(ctx, db, discard)
	if err != nil || !slices.Equal(applied, []string{"0001_three_steps"}) {
		t.Fatalf("resumed migrate: applied = %v, err = %v", applied, err)
	}
	var rows int64
	if err := db.Raw("SELECT COUNT(*) FROM missing_table").Scan(&rows).Error; err != nil || rows != 1 {
		t.Fatalf("missing_table rows = %d, err = %v", rows, err)
	}
	if !db.Migrator().HasTable("third_table") {
		t.Fatal("third_table was not created")
	}
	if _, err := migrations.Check(ctx, db); err != nil {
		t.Fatal(err)
	}
}
