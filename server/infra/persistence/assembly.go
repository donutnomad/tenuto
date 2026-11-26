package persistence

import (
	"context"
	"fmt"
	"reflect"

	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/clock"
	infradb "github.com/donutnomad/tenuto/server/infra/db"
	"github.com/donutnomad/tenuto/server/infra/persistence/batchrepo"
	"github.com/donutnomad/tenuto/server/infra/persistence/definitionrepo"
	"github.com/donutnomad/tenuto/server/infra/persistence/dispatchrepo"
	"github.com/donutnomad/tenuto/server/infra/persistence/executionrepo"
	"github.com/donutnomad/tenuto/server/infra/persistence/schedulerepo"
)

type Config struct {
	HistoryLimits       history.HistoryLimits
	HistoryPageTokenKey []byte
	Retention           executionrepo.RetentionConfig
	// Clock 是执行仓储的时间来源，应与 Server 使用同一个时钟；留空时使用系统时钟。
	Clock clock.Clock
}

// Store 持有同一个数据库上的各仓储。调用方按字段取用所需的仓储。
type Store struct {
	db infradb.DB

	Executions  *executionrepo.Repository
	Definitions *definitionrepo.Repository
	// Dispatches 同时实现 dispatch.Repository 与 dispatch.WorkerVersionRepository。
	Dispatches *dispatchrepo.Repository
	Schedules  *schedulerepo.Repository
	Batches    *batchrepo.Repository
}

// New 在 db 上建立各仓储。db 是 MySQL 连接，表由 tenuto-server migrate 预先建好；New 不建表。
// db 必须以 gorm.Config{TranslateError: true} 打开：仓储按 gorm.ErrDuplicatedKey
// 识别唯一键冲突（Run ID、Schedule ID、current 标记、事件 dedupe），未翻译的驱动错误会让这些判定失效。
// 连接不能带 clientFoundRows=true：仓储按 affected rows 区分新写入与去重命中，New 在库上探测一次，不符时返回错误。
func New(db infradb.DB, cfg Config) (*Store, error) {
	if db == nil || reflect.ValueOf(db).Kind() == reflect.Ptr && reflect.ValueOf(db).IsNil() {
		return nil, fmt.Errorf("workflow persistence: db is required")
	}
	if !db.WithContext(context.Background()).TranslateError {
		return nil, fmt.Errorf("workflow persistence: db must be opened with gorm.Config{TranslateError: true}")
	}
	runs, err := executionrepo.New(db, executionrepo.Config{
		HistoryLimits: cfg.HistoryLimits, HistoryPageTokenKey: cfg.HistoryPageTokenKey, Retention: cfg.Retention,
		Clock: cfg.Clock,
	})
	if err != nil {
		return nil, err
	}
	if err := runs.CheckDuplicateKeyAffectedRows(context.Background()); err != nil {
		return nil, fmt.Errorf("workflow persistence: %w", err)
	}
	store := &Store{
		db:          db,
		Executions:  runs,
		Definitions: definitionrepo.New(db),
		Dispatches:  dispatchrepo.New(db),
		Schedules:   schedulerepo.New(db),
		Batches:     batchrepo.New(db),
	}
	return store, nil
}
