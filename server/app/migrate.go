package app

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	tenutosql "github.com/donutnomad/tenuto/server/infra/sql"
)

// Migrate 执行 tenuto-server migrate：在 WORKFLOW_DB_DSN 指向的库上执行 Tenuto 的迁移，在 TASKQ_DB_DSN
// （默认与 WORKFLOW_DB_DSN 相同）指向的库上执行 TaskQ 的迁移。只读这两个环境变量，其余 Server 配置不需要。
// 已执行完的库上什么也不做；多个进程同时执行时依次进行（见 tenutosql.Migrations.Migrate）。
func Migrate(ctx context.Context, logger *slog.Logger) error {
	return migrate(ctx, os.LookupEnv, logger)
}

func migrate(ctx context.Context, lookup envLookup, logger *slog.Logger) error {
	if backend := envOrDefault(lookup, "WORKFLOW_BACKEND", string(BackendMySQLTaskQ)); backend != string(BackendMySQLTaskQ) {
		return fmt.Errorf("migrate needs WORKFLOW_BACKEND=%s (got %q): the memory backend has no database", BackendMySQLTaskQ, backend)
	}
	workflowDSN, err := requiredEnv(lookup, "WORKFLOW_DB_DSN")
	if err != nil {
		return err
	}
	taskQDSN := envOrDefault(lookup, "TASKQ_DB_DSN", workflowDSN)
	if err := migrateDatabase(ctx, workflowDSN, tenutosql.TenutoMigrations, logger); err != nil {
		return err
	}
	return migrateDatabase(ctx, taskQDSN, tenutosql.TaskQMigrations, logger)
}

func migrateDatabase(ctx context.Context, dsn string, migrations tenutosql.Migrations, logger *slog.Logger) error {
	database, sqlDB, err := openMySQL(ctx, dsn, DatabaseConfig{MaxOpenConns: 2, MaxIdleConns: 1})
	if err != nil {
		return fmt.Errorf("open %s database: %w", migrations.Component, err)
	}
	defer sqlDB.Close()
	applied, unknown, err := migrations.Migrate(ctx, database, logger)
	if err != nil {
		return err
	}
	if len(unknown) > 0 {
		logger.Warn("database schema is newer than this binary; nothing to apply",
			"component", migrations.Component, "binary_schema_version", migrations.Latest(), "unknown_migrations", unknown)
		return nil
	}
	logger.Info("database schema is up to date", "component", migrations.Component,
		"schema_version", migrations.Latest(), "applied", applied)
	return nil
}
