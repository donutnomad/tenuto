package persistence

import (
	"os"
	"strings"
	"testing"

	"github.com/donutnomad/tenuto/server/infra/persistence/executionrepo"
	"github.com/donutnomad/tenuto/server/infra/persistence/mysqltest"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

// 本包的测试验证 GORM 仓储在 MySQL 上的行为，经 mysqltest 取一个执行完全部迁移的独立库：
//
//	TENUTO_TEST_MYSQL=1 TENUTO_TEST_MYSQL_DSN='root:pw@tcp([IP地址]:3306)/' go test ./server/infra/persistence/... -count=1
//
// 不设 TENUTO_TEST_MYSQL 时这些测试跳过。仓储的领域契约（executiontest、dispatchtest 等）在
// server/infra/memory 上同样运行，默认的 go test ./... 覆盖内存后端。

// TestMain 负责在全部测试结束后销毁共享容器。
func TestMain(m *testing.M) {
	code := m.Run()
	mysqltest.Terminate()
	os.Exit(code)
}

// newMySQLStore 在独立的 MySQL 库上按 cfg 建 Store；没有开启 MySQL 后端测试时跳过 t。
func newMySQLStore(t *testing.T, cfg Config) *Store {
	t.Helper()
	store, err := New(mysqltest.NewDB(t), cfg)
	if err != nil {
		t.Fatalf("new mysql store: %v", err)
	}
	return store
}

// requireMySQL 返回独立 MySQL 库上的 Store，供并发、行锁与 sql_mode 相关的用例使用。
func requireMySQL(t *testing.T) *Store {
	t.Helper()
	return newMySQLStore(t, Config{})
}

// newUnconnectedDB 返回一个还没有建立连接的 MySQL 句柄：database/sql 在第一次查询时才拨号。
// 只检查构造参数或连接失败路径的用例用它，不需要服务端。
func newUnconnectedDB(t *testing.T, cfg *gorm.Config) *gorm.DB {
	t.Helper()
	cfg.DisableAutomaticPing = true
	db, err := gorm.Open(mysql.New(mysql.Config{
		DSN:                       "tenuto:unused@tcp(127.0.0.1:1)/tenuto?parseTime=true",
		SkipInitializeWithVersion: true,
	}), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if pool, err := db.DB(); err == nil {
			_ = pool.Close()
		}
	})
	return db
}

// TestMySQLBackendIsActuallyMySQL 确认门控打开时连的是 MySQL 8 上按测试分配的库。
func TestMySQLBackendIsActuallyMySQL(t *testing.T) {
	store := requireMySQL(t)

	var version string
	if err := store.db.Raw("SELECT VERSION()").Scan(&version).Error; err != nil {
		t.Fatal(err)
	}
	if version == "" || !strings.HasPrefix(version, "8.") {
		t.Fatalf("unexpected server version %q, want MySQL 8.x", version)
	}

	var dbName string
	if err := store.db.Raw("SELECT DATABASE()").Scan(&dbName).Error; err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(dbName, "tenuto_test_") {
		t.Fatalf("unexpected database %q, want per-test tenuto_test_* schema", dbName)
	}

	// 行锁（FOR UPDATE）依赖 InnoDB。
	var engineName string
	if err := store.db.Raw(
		"SELECT ENGINE FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
		dbName, executionrepo.EventPO{}.TableName(),
	).Scan(&engineName).Error; err != nil {
		t.Fatal(err)
	}
	if !strings.EqualFold(engineName, "InnoDB") {
		t.Fatalf("events table engine = %q, want InnoDB (row locks required)", engineName)
	}

	t.Logf("connected to MySQL version=%s database=%s events_engine=%s", version, dbName, engineName)
}
