package sql

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"embed"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"regexp"
	"slices"
	"strconv"
	"strings"

	infradb "github.com/donutnomad/tenuto/server/infra/db"
)

// MigrationTable 记录各组件已执行的迁移。它由迁移器自己建出（CREATE TABLE IF NOT EXISTS），不在 schema.sql 与迁移文件里。
// Workflow 库与 TaskQ 库是同一个库时，两个组件的记录在同一张表里，按 component 区分。
const MigrationTable = "tenuto_schema_migrations"

const createMigrationTable = "CREATE TABLE IF NOT EXISTS `" + MigrationTable + "` (\n" +
	"  `component`          VARCHAR(32)  NOT NULL COMMENT '迁移所属组件：tenuto 或 taskq',\n" +
	"  `version`            INT          NOT NULL COMMENT '迁移编号，取自文件名的数字前缀',\n" +
	"  `name`               VARCHAR(191) NOT NULL COMMENT '迁移文件名，不含 .sql',\n" +
	"  `checksum`           CHAR(64)     NOT NULL COMMENT '迁移文件内容的 SHA-256（换行统一为 LF）',\n" +
	"  `statements_applied` INT          NOT NULL COMMENT '已执行成功的语句数',\n" +
	"  `applied_at`         DATETIME(3)  DEFAULT NULL COMMENT '全部语句执行完的时刻；NULL 表示迁移中途失败，下次从 statements_applied 之后继续',\n" +
	"  PRIMARY KEY (`component`, `version`)\n" +
	") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"

//go:embed migrations/*.sql
var tenutoMigrationFiles embed.FS

//go:embed taskq_migrations/*.sql
var taskQMigrationFiles embed.FS

// TenutoMigrations 建出并升级 Workflow 库（WORKFLOW_DB_DSN），执行完全部迁移的库与 schema.sql 建出的库相同。
var TenutoMigrations = mustLoadMigrations("tenuto", tenutoMigrationFiles, "migrations")

// TaskQMigrations 建出并升级 TaskQ 库（TASKQ_DB_DSN），执行完全部迁移的库与 TaskQ 模块的 store/gormstore/schema.sql 建出的库相同。
var TaskQMigrations = mustLoadMigrations("taskq", taskQMigrationFiles, "taskq_migrations")

// Migration 是一个迁移文件。
type Migration struct {
	Version  int
	Name     string // 文件名，不含 .sql，如 0001_init
	Script   string
	Checksum string
}

// Migrations 是一个组件按编号排好的全部迁移，编号从 1 开始连续。
type Migrations struct {
	Component string
	List      []Migration
}

// Latest 返回最后一个迁移的编号。
func (m Migrations) Latest() int {
	if len(m.List) == 0 {
		return 0
	}
	return m.List[len(m.List)-1].Version
}

// migrationFileRe 是迁移文件名：四位编号、下划线、小写字母数字与下划线组成的名字。
var migrationFileRe = regexp.MustCompile(`^(\d{4})_([a-z0-9_]+)\.sql$`)

// LoadMigrations 读取 fsys 的 dir 目录下的迁移文件。目录里只能有迁移文件，文件名符合 NNNN_name.sql，
// 编号从 0001 开始连续、不重复，每个文件至少有一条语句。
func LoadMigrations(component string, fsys fs.FS, dir string) (Migrations, error) {
	entries, err := fs.ReadDir(fsys, dir)
	if err != nil {
		return Migrations{}, err
	}
	result := Migrations{Component: component}
	for _, entry := range entries { // fs.ReadDir 按文件名排序
		match := migrationFileRe.FindStringSubmatch(entry.Name())
		if entry.IsDir() || match == nil {
			return Migrations{}, fmt.Errorf("%s migrations: %s is not a migration file (want NNNN_name.sql)", component, entry.Name())
		}
		version, _ := strconv.Atoi(match[1])
		if want := len(result.List) + 1; version != want {
			return Migrations{}, fmt.Errorf("%s migrations: %s has version %d, want %d (versions start at 0001 and have no gaps or duplicates)", component, entry.Name(), version, want)
		}
		raw, err := fs.ReadFile(fsys, dir+"/"+entry.Name())
		if err != nil {
			return Migrations{}, err
		}
		script := strings.ReplaceAll(string(raw), "\r\n", "\n")
		if len(Statements(script)) == 0 {
			return Migrations{}, fmt.Errorf("%s migrations: %s has no statements", component, entry.Name())
		}
		sum := sha256.Sum256([]byte(script))
		result.List = append(result.List, Migration{
			Version:  version,
			Name:     strings.TrimSuffix(entry.Name(), ".sql"),
			Script:   script,
			Checksum: hex.EncodeToString(sum[:]),
		})
	}
	if len(result.List) == 0 {
		return Migrations{}, fmt.Errorf("%s migrations: no migration files", component)
	}
	return result, nil
}

func mustLoadMigrations(component string, fsys fs.FS, dir string) Migrations {
	migrations, err := LoadMigrations(component, fsys, dir)
	if err != nil {
		panic(err)
	}
	return migrations
}

// migrationRow 是 MigrationTable 的一行。
type migrationRow struct {
	Version           int
	Name              string
	Checksum          string
	StatementsApplied int
	Applied           bool // applied_at 非 NULL
}

// migrationPlan 是库里的记录与 Migrations 比较的结果。
type migrationPlan struct {
	current int         // 连续执行完的最大编号
	pending []Migration // 待执行（含中途失败）的迁移，按编号排序
	resume  int         // pending[0] 已执行成功的语句数
	unknown []int       // 库里有、本二进制不认识的更新编号
}

// plan 比较库里的记录与 migrations。已执行迁移的 checksum 与文件不同、记录不连续、
// 或库里有更新的迁移同时本二进制还有未执行的迁移时返回错误。
func (m Migrations) plan(rows []migrationRow) (migrationPlan, error) {
	byVersion := make(map[int]migrationRow, len(rows))
	var result migrationPlan
	for _, row := range rows {
		byVersion[row.Version] = row
		if row.Version > m.Latest() {
			result.unknown = append(result.unknown, row.Version)
		}
	}
	slices.Sort(result.unknown)
	for _, migration := range m.List {
		row, recorded := byVersion[migration.Version]
		if recorded && row.Checksum != migration.Checksum {
			return migrationPlan{}, fmt.Errorf("%s migration %s was applied with checksum %s, but this binary has %s: an applied migration must never be edited; add a new migration instead",
				m.Component, migration.Name, row.Checksum, migration.Checksum)
		}
		switch {
		case recorded && row.Applied && len(result.pending) == 0:
			result.current = migration.Version
		case recorded && row.Applied:
			return migrationPlan{}, fmt.Errorf("%s migration %s is applied but an earlier migration %s is not", m.Component, migration.Name, result.pending[0].Name)
		case recorded && len(result.pending) > 0:
			return migrationPlan{}, fmt.Errorf("%s migration %s is partially applied but an earlier migration %s is not", m.Component, migration.Name, result.pending[0].Name)
		case recorded:
			result.resume = row.StatementsApplied
			result.pending = append(result.pending, migration)
		default:
			result.pending = append(result.pending, migration)
		}
	}
	if len(result.unknown) > 0 && len(result.pending) > 0 {
		return migrationPlan{}, fmt.Errorf("%s schema has migrations %v that this binary does not know while its own migration %s is not applied",
			m.Component, result.unknown, result.pending[0].Name)
	}
	return result, nil
}

// queryer 是 *sql.DB 与 *sql.Conn 共有的查询方法。
type queryer interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

func (m Migrations) recorded(ctx context.Context, db queryer) ([]migrationRow, error) {
	rows, err := db.QueryContext(ctx, "SELECT version, name, checksum, statements_applied, applied_at IS NOT NULL FROM `"+MigrationTable+
		"` WHERE component = ? ORDER BY version", m.Component)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", MigrationTable, err)
	}
	defer rows.Close()
	var result []migrationRow
	for rows.Next() {
		var row migrationRow
		if err := rows.Scan(&row.Version, &row.Name, &row.Checksum, &row.StatementsApplied, &row.Applied); err != nil {
			return nil, fmt.Errorf("read %s: %w", MigrationTable, err)
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read %s: %w", MigrationTable, err)
	}
	return result, nil
}

// Check 在启动时确认库已执行完本二进制的全部迁移，不加锁、不改库。
// 库落后（缺迁移表、有未执行或中途失败的迁移）或已执行迁移的 checksum 与文件不同时返回错误，错误信息提示运行 tenuto-server migrate。
// 库里有本二进制不认识的更新迁移时返回这些编号，由调用方记录警告。
func (m Migrations) Check(ctx context.Context, db infradb.DB) (unknown []int, err error) {
	sqlDB, err := db.DB()
	if err != nil {
		return nil, err
	}
	var tables int
	if err := sqlDB.QueryRowContext(ctx, "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = ?",
		MigrationTable).Scan(&tables); err != nil {
		return nil, fmt.Errorf("look up %s: %w", MigrationTable, err)
	}
	if tables == 0 {
		return nil, fmt.Errorf("%s schema is not initialized (table %s is missing): run `tenuto-server migrate` before starting the server", m.Component, MigrationTable)
	}
	rows, err := m.recorded(ctx, sqlDB)
	if err != nil {
		return nil, err
	}
	plan, err := m.plan(rows)
	if err != nil {
		return nil, err
	}
	if len(plan.pending) > 0 {
		names := make([]string, len(plan.pending))
		for i, migration := range plan.pending {
			names[i] = migration.Name
		}
		return nil, fmt.Errorf("%s schema is at version %d, this binary needs %d (pending: %s): run `tenuto-server migrate` before starting the server",
			m.Component, plan.current, m.Latest(), strings.Join(names, ", "))
	}
	return plan.unknown, nil
}

// Migrate 执行库里还没有执行的迁移，返回本次执行的迁移名与库里本二进制不认识的更新编号。
//
// 并发：整个过程持有 MySQL 命名锁（GET_LOCK，按库名区分），多个副本同时执行时依次进行，后到的看到迁移已执行后什么也不做。
// 命名锁属于连接，过程在同一条连接上执行；进程退出或连接断开时 MySQL 释放它。
//
// 失败：MySQL 的 DDL 隐式提交，迁移不能整体回滚。每条语句执行成功后记录 statements_applied，
// 全部执行完再写 applied_at。某条语句失败时之前的语句已生效，返回的错误给出迁移名与失败语句的序号；
// 修复原因（例如清理违反新唯一键的数据）后再次执行 Migrate，从失败的那条语句继续。
func (m Migrations) Migrate(ctx context.Context, db infradb.DB, logger *slog.Logger) (applied []string, unknown []int, err error) {
	sqlDB, err := db.DB()
	if err != nil {
		return nil, nil, err
	}
	conn, err := sqlDB.Conn(ctx)
	if err != nil {
		return nil, nil, err
	}
	defer conn.Close()
	if err := acquireMigrationLock(ctx, conn, logger); err != nil {
		return nil, nil, err
	}
	// 释放失败（如 ctx 已取消、连接已断开）时，连接关闭同样释放命名锁。
	defer conn.ExecContext(context.WithoutCancel(ctx), "DO RELEASE_LOCK("+migrationLockName+")")

	if _, err := conn.ExecContext(ctx, createMigrationTable); err != nil {
		return nil, nil, fmt.Errorf("create %s: %w", MigrationTable, err)
	}
	rows, err := m.recorded(ctx, conn)
	if err != nil {
		return nil, nil, err
	}
	plan, err := m.plan(rows)
	if err != nil {
		return nil, nil, err
	}
	resume := plan.resume
	for _, migration := range plan.pending {
		if err := m.apply(ctx, conn, migration, resume, logger); err != nil {
			return applied, plan.unknown, err
		}
		applied = append(applied, migration.Name)
		resume = 0
	}
	return applied, plan.unknown, nil
}

// migrationLockName 是迁移用的 MySQL 命名锁。命名锁在整个 MySQL 服务端范围内生效，名字带上库名的摘要，
// 不同库的迁移互不等待；名字长度 15+40 个字符，在 MySQL 的 64 字符上限之内。
const migrationLockName = "CONCAT('tenuto_migrate:', SHA1(DATABASE()))"

func acquireMigrationLock(ctx context.Context, conn *sql.Conn, logger *slog.Logger) error {
	var acquired sql.NullInt64
	if err := conn.QueryRowContext(ctx, "SELECT GET_LOCK("+migrationLockName+", 0)").Scan(&acquired); err != nil {
		return fmt.Errorf("acquire migration lock: %w", err)
	}
	if acquired.Int64 == 1 {
		return nil
	}
	logger.Info("another migration holds the lock on this database; waiting for it to finish")
	// 超时 -1 表示一直等待；ctx 取消时驱动断开连接，等待随之结束。
	if err := conn.QueryRowContext(ctx, "SELECT GET_LOCK("+migrationLockName+", -1)").Scan(&acquired); err != nil {
		return fmt.Errorf("acquire migration lock: %w", err)
	}
	if acquired.Int64 != 1 {
		return errors.New("acquire migration lock: GET_LOCK did not return 1")
	}
	return nil
}

// apply 从第 skip 条语句之后执行 migration，每条语句成功后记录进度，全部执行完写 applied_at。
func (m Migrations) apply(ctx context.Context, conn *sql.Conn, migration Migration, skip int, logger *slog.Logger) error {
	statements := Statements(migration.Script)
	if skip > len(statements) {
		return fmt.Errorf("%s migration %s records %d applied statements but has %d", m.Component, migration.Name, skip, len(statements))
	}
	if skip == 0 {
		_, err := conn.ExecContext(ctx, "INSERT INTO `"+MigrationTable+"` (component, version, name, checksum, statements_applied) VALUES (?, ?, ?, ?, 0)",
			m.Component, migration.Version, migration.Name, migration.Checksum)
		if err != nil {
			return fmt.Errorf("record %s migration %s: %w", m.Component, migration.Name, err)
		}
		logger.Info("applying migration", "component", m.Component, "migration", migration.Name, "statements", len(statements))
	} else {
		logger.Info("resuming partially applied migration", "component", m.Component, "migration", migration.Name,
			"statements", len(statements), "already_applied", skip)
	}
	for i := skip; i < len(statements); i++ {
		if _, err := conn.ExecContext(ctx, statements[i]); err != nil {
			return fmt.Errorf("%s migration %s failed at statement %d of %d (%s): %w; statements before it are applied and committed (MySQL DDL cannot be rolled back); fix the cause and run migrate again to continue from statement %d",
				m.Component, migration.Name, i+1, len(statements), firstLine(statements[i]), err, i+1)
		}
		_, err := conn.ExecContext(ctx, "UPDATE `"+MigrationTable+"` SET statements_applied = ? WHERE component = ? AND version = ?",
			i+1, m.Component, migration.Version)
		if err != nil {
			return fmt.Errorf("record progress of %s migration %s after statement %d: %w", m.Component, migration.Name, i+1, err)
		}
	}
	_, err := conn.ExecContext(ctx, "UPDATE `"+MigrationTable+"` SET applied_at = NOW(3) WHERE component = ? AND version = ?",
		m.Component, migration.Version)
	if err != nil {
		return fmt.Errorf("record %s migration %s as applied: %w", m.Component, migration.Name, err)
	}
	return nil
}
