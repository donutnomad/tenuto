// Package mysqltest 给 MySQL 门控的测试分配独立的库：每个测试一个新库，经生产同一条迁移路径（tenuto-server migrate 执行的迁移）建好表。
//
// GORM 持久化、应用入口与端到端测试共用本包。TENUTO_TEST_MYSQL=1 打开门控：同时设置
// TENUTO_TEST_MYSQL_DSN 时连接该服务端，否则启动一个共享的 mysql:8.0 testcontainer。
// 不设 TENUTO_TEST_MYSQL 时，经本包取库的测试跳过。
//
// 本包只在测试中使用，但不是 _test.go 文件：跨包引用要求它是普通包文件。
package mysqltest

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	infradb "github.com/donutnomad/tenuto/server/infra/db"
	tenutosql "github.com/donutnomad/tenuto/server/infra/sql"
	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/testcontainers/testcontainers-go"
	tcmysql "github.com/testcontainers/testcontainers-go/modules/mysql"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

// EnvVar 是开关环境变量。不设时 MySQL 门控的测试跳过，go test ./... 只跑内存后端。
//
//	TENUTO_TEST_MYSQL=1 go test ./server/... -count=1
//
// 选环境变量而非 build tag：build tag 容易被忘记（go test ./... 静默不跑），
// 且 IDE/gopls 对非默认 tag 支持差。
const EnvVar = "TENUTO_TEST_MYSQL"

// DSNEnvVar 指向一个已存在的 MySQL 服务端（如本地实例或 CI 的 service container），
// 设置后不启动容器，测试直接在该服务端上建独立库：
//
//	TENUTO_TEST_MYSQL=1 TENUTO_TEST_MYSQL_DSN='root:pw@tcp([IP地址]:3306)/' go test ./server/... -count=1
//
// DSN 带 host、port 与凭据，库名由本包分配；parseTime 与 interpolateParams 由本包按 Server 的要求补齐
// （infradb.ConfigureMySQL），其余参数（如 loc）按原样保留。
const DSNEnvVar = "TENUTO_TEST_MYSQL_DSN"

// image 固定用本地已有的 mysql:8.0，避免指定别的 tag 触发镜像 pull。
const image = "mysql:8.0"

// taskQModule 是 TaskQ 的模块路径；它的建表脚本在模块目录的 store/gormstore/schema.sql。
const taskQModule = "github.com/donutnomad/taskq"

// Enabled 报告是否开启了 MySQL 后端测试。
func Enabled() bool {
	v := os.Getenv(EnvVar)
	return v == "1" || strings.EqualFold(v, "true")
}

// Require 在没有开启 MySQL 后端测试时跳过 t。
func Require(t testing.TB) {
	t.Helper()
	if !Enabled() {
		t.Skipf("skipping MySQL test: set %s=1 (and %s to use an existing server)", EnvVar, DSNEnvVar)
	}
}

var (
	// 容器全进程复用一次：几十个测试各起一个容器慢到没法用。
	once      sync.Once
	container *tcmysql.MySQLContainer
	baseDSN   string // 不含 database 名的 DSN 前缀，形如 user:pass@tcp(host:port)/
	startErr  error

	// dbSeq 给每个测试分配独立 database，保证互不污染。
	dbSeq atomic.Int64

	taskQSchemaOnce sync.Once
	taskQSchema     string
	taskQSchemaErr  error
)

// AdminDSN 返回连接服务端本身（不带库名）的 DSN，用于执行 CREATE/DROP DATABASE。
// 设置 DSNEnvVar 时连外部服务端，否则首次调用时起共享容器。
func AdminDSN(ctx context.Context) (string, error) {
	once.Do(func() {
		if dsn := os.Getenv(DSNEnvVar); dsn != "" {
			base, err := normalizeDSN(dsn)
			if err != nil {
				startErr = err
				return
			}
			baseDSN = base
			return
		}
		c, err := tcmysql.Run(ctx, image,
			tcmysql.WithDatabase("tenuto_root"),
			tcmysql.WithUsername("root"),
			tcmysql.WithPassword("test"),
			testcontainers.WithEnv(map[string]string{
				"MYSQL_ROOT_HOST": "%",
			}),
			// 不要在这里放宽 sql_mode。容器使用 MySQL 8 的默认 sql_mode
			// （含 NO_ZERO_DATE + STRICT_TRANS_TABLES），与生产一致；放宽后生产上会失败的写入在测试里通过。
		)
		if err != nil {
			startErr = fmt.Errorf("start mysql container: %w", err)
			return
		}
		container = c

		host, err := c.Host(ctx)
		if err != nil {
			startErr = fmt.Errorf("mysql container host: %w", err)
			return
		}
		port, err := c.MappedPort(ctx, "3306/tcp")
		if err != nil {
			startErr = fmt.Errorf("mysql container port: %w", err)
			return
		}
		baseDSN = fmt.Sprintf("root:test@tcp(%s:%s)/", host, port.Port())
	})
	return baseDSN, startErr
}

// normalizeDSN 把 `user:pass@tcp(host:port)/db?params` 收敛成不含库名的 DSN 前缀。
func normalizeDSN(dsn string) (string, error) {
	cfg, err := mysqldriver.ParseDSN(dsn)
	if err != nil {
		return "", fmt.Errorf("parse %s: %w", DSNEnvVar, err)
	}
	cfg.DBName = ""
	if err := infradb.ConfigureMySQL(cfg); err != nil {
		return "", fmt.Errorf("%s: %w", DSNEnvVar, err)
	}
	return cfg.FormatDSN(), nil
}

// NewDB 为单个测试建一个独立的库，执行 Tenuto 的全部迁移（server/infra/sql/migrations），返回连到该库的 *gorm.DB。
// 没有开启 MySQL 后端测试时跳过 t。连接与库在测试结束时关闭、删除。
//
// 隔离方案选型：每个测试独立 database（而非清表 / 表前缀）。
//   - 独立 database：建库有开销（实测每库 ~40ms，可接受），但不依赖测试执行顺序、
//     天然支持 t.Parallel()、且不必维护"清哪些表"的清单（新增表不会漏清）。
//   - 清表：快，但要保证顺序，且漏清新增表会造成串扰。
func NewDB(t *testing.T) *gorm.DB {
	t.Helper()
	return newMigratedDB(t, tenutosql.TenutoMigrations)
}

// NewTaskQDB 为单个测试建一个独立的库，执行 TaskQ 的全部迁移（server/infra/sql/taskq_migrations）。
// 没有开启 MySQL 后端测试时跳过 t。
func NewTaskQDB(t *testing.T) *gorm.DB {
	t.Helper()
	return newMigratedDB(t, tenutosql.TaskQMigrations)
}

func newMigratedDB(t *testing.T, migrations tenutosql.Migrations) *gorm.DB {
	t.Helper()
	db := NewEmptyDB(t)
	if _, _, err := migrations.Migrate(t.Context(), db, slog.New(slog.DiscardHandler)); err != nil {
		t.Fatalf("migrate %s schema: %v", migrations.Component, err)
	}
	return db
}

// NewEmptyDB 为单个测试建一个没有表的独立库。没有开启 MySQL 后端测试时跳过 t。
func NewEmptyDB(t *testing.T) *gorm.DB {
	t.Helper()
	Require(t)

	ctx := t.Context()
	base, err := AdminDSN(ctx)
	if err != nil {
		// 开关已显式打开，说明调用方明确要求跑 MySQL；此时静默 skip 属于掩盖。
		t.Fatalf("%s=1 but the MySQL server is unavailable: %v", EnvVar, err)
	}

	dbName := fmt.Sprintf("tenuto_test_%d_%d", time.Now().UnixNano()%1e6, dbSeq.Add(1))

	admin, err := gorm.Open(mysql.Open(base), &gorm.Config{})
	if err != nil {
		t.Fatalf("connect mysql admin: %v", err)
	}
	if err := admin.Exec("CREATE DATABASE `" + dbName + "` DEFAULT CHARSET utf8mb4").Error; err != nil {
		t.Fatalf("create database %s: %v", dbName, err)
	}
	t.Cleanup(func() { dropDatabase(t, dbName) })
	if adminDB, err := admin.DB(); err == nil {
		_ = adminDB.Close()
	}

	// TranslateError 与生产入口一致：驱动把唯一约束冲突翻译为 gorm.ErrDuplicatedKey，
	// 仓储按它识别 Run ID、Schedule ID、current 标记与事件 dedupe 的冲突。
	db, err := gorm.Open(mysql.Open(withDatabase(base, dbName)), &gorm.Config{TranslateError: true})
	if err != nil {
		t.Fatalf("connect mysql db %s: %v", dbName, err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatalf("mysql sql.DB: %v", err)
	}
	// 留足连接让 goroutine 物理并行，并发与行锁相关的断言才有意义。
	sqlDB.SetMaxOpenConns(32)
	sqlDB.SetMaxIdleConns(8)
	// 在 dropDatabase 之前关闭（Cleanup 后进先出）。
	t.Cleanup(func() { _ = sqlDB.Close() })
	return db
}

// TaskQSchema 读 TaskQ 模块目录里的 store/gormstore/schema.sql，供迁移守卫测试与 TaskQ 的迁移对照。
// 模块目录由 go list 给出，与本次构建使用的 TaskQ 版本一致。
func TaskQSchema() (string, error) {
	taskQSchemaOnce.Do(func() {
		cmd := exec.Command("go", "list", "-m", "-f", "{{.Dir}}", taskQModule)
		cmd.Env = append(os.Environ(), "GOWORK=off")
		out, err := cmd.Output()
		if err != nil {
			taskQSchemaErr = fmt.Errorf("locate %s: %w", taskQModule, err)
			return
		}
		dir := strings.TrimSpace(string(out))
		if dir == "" {
			taskQSchemaErr = fmt.Errorf("locate %s: module directory is empty (module not downloaded?)", taskQModule)
			return
		}
		raw, err := os.ReadFile(filepath.Join(dir, "store", "gormstore", "schema.sql"))
		if err != nil {
			taskQSchemaErr = err
			return
		}
		taskQSchema = string(raw)
	})
	return taskQSchema, taskQSchemaErr
}

// dropDatabase 删除测试库。失败只记入测试日志：库删不掉不该让已通过的业务断言变红。
func dropDatabase(t testing.TB, dbName string) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	adminDSN, err := AdminDSN(ctx)
	if err != nil {
		return
	}
	admin, err := gorm.Open(mysql.Open(adminDSN), &gorm.Config{})
	if err != nil {
		return
	}
	if sqlDB, err := admin.DB(); err == nil {
		defer sqlDB.Close()
	}
	if err := admin.Exec("DROP DATABASE IF EXISTS `" + dbName + "`").Error; err != nil {
		t.Logf("drop test database %s: %v", dbName, err)
	}
}

// withDatabase 在 DSN 前缀上换出库名。前缀由 normalizeDSN 或容器给出，均已是
// FormatDSN 形式，可无损往返。
func withDatabase(base, dbName string) string {
	clone, err := mysqldriver.ParseDSN(base)
	if err != nil {
		return base + dbName
	}
	clone.DBName = dbName
	if err := infradb.ConfigureMySQL(clone); err != nil {
		return base + dbName
	}
	return clone.FormatDSN()
}

// Terminate 销毁共享容器，由各包的 TestMain 在全部测试结束后调用。
// 容器未启动时是 no-op。
func Terminate() {
	if container == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	_ = container.Terminate(ctx)
	container = nil
}
