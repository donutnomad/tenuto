package batchrepo

import "time"

// BatchOperationPO
// @Gsql
// @Setter(setter=false, patch=v2)
type BatchOperationPO struct {
	ID              string     `gorm:"column:id"`
	Namespace       string     `gorm:"column:namespace"`
	Type            string     `gorm:"column:type"`
	FilterJSON      []byte     `gorm:"column:filter_json"`
	ResetType       string     `gorm:"column:reset_type"`
	Reason          string     `gorm:"column:reason"`
	Status          string     `gorm:"column:status"`
	TotalCount      int        `gorm:"column:total_count"`
	SucceededCount  int        `gorm:"column:succeeded_count"`
	FailedCount     int        `gorm:"column:failed_count"`
	SkippedCount    int        `gorm:"column:skipped_count"`
	ErrorsJSON      []byte     `gorm:"column:errors_json"`
	CursorCreatedAt *time.Time `gorm:"column:cursor_created_at"`
	CursorRunID     string     `gorm:"column:cursor_run_id"`
	Version         int64      `gorm:"column:version"`
	CreatedAt       time.Time  `gorm:"column:created_at"`
	StartedAt       *time.Time `gorm:"column:started_at"`
	FinishedAt      *time.Time `gorm:"column:finished_at"`
	UpdatedAt       time.Time  `gorm:"column:updated_at"`
}

func (BatchOperationPO) TableName() string { return "tenuto_batch_operations" }

func (BatchOperationPO) MysqlCreateTable() string {
	return `
CREATE TABLE IF NOT EXISTS tenuto_batch_operations (
  id                VARCHAR(191)  NOT NULL,
  namespace         VARCHAR(191)  NOT NULL,
  type              VARCHAR(32)   NOT NULL,
  filter_json       LONGBLOB,
  reset_type        VARCHAR(64)   NOT NULL DEFAULT '',
  reason            VARCHAR(1024) NOT NULL DEFAULT '',
  status            VARCHAR(32)   NOT NULL,
  total_count       BIGINT        NOT NULL DEFAULT 0,
  succeeded_count   BIGINT        NOT NULL DEFAULT 0,
  failed_count      BIGINT        NOT NULL DEFAULT 0,
  skipped_count     BIGINT        NOT NULL DEFAULT 0,
  errors_json       LONGBLOB,
  cursor_created_at DATETIME(3),
  cursor_run_id     VARCHAR(191)  NOT NULL DEFAULT '',
  version           BIGINT        NOT NULL DEFAULT 0,
  created_at        DATETIME(3)   NOT NULL,
  started_at        DATETIME(3),
  finished_at       DATETIME(3),
  updated_at        DATETIME(3)   NOT NULL,
  PRIMARY KEY (namespace, id),
  INDEX idx_tenuto_batch_operations_status (status, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
`
}
