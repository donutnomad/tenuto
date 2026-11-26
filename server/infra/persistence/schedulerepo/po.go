package schedulerepo

import "time"

// SchedulePO
// @Gsql
// @Setter(setter=false, patch=v2)
type SchedulePO struct {
	ID             string     `gorm:"column:id"`
	Namespace      string     `gorm:"column:namespace"`
	WorkflowType   string     `gorm:"column:workflow_type"`
	Description    string     `gorm:"column:description"`
	CronExpression string     `gorm:"column:cron_expression"`
	Timezone       string     `gorm:"column:timezone"`
	InputJSON      []byte     `gorm:"column:input_json"`
	Paused         bool       `gorm:"column:paused"`
	OverlapPolicy  string     `gorm:"column:overlap_policy"`
	LastRunAt      *time.Time `gorm:"column:last_run_at"`
	NextRunAt      *time.Time `gorm:"column:next_run_at"`
	LastRunID      string     `gorm:"column:last_run_id"`
	RunCount       int        `gorm:"column:run_count"`
	// CatchupWindow 以纳秒保存。
	CatchupWindow       time.Duration `gorm:"column:catchup_window_ns"`
	MissedCatchupWindow int64         `gorm:"column:missed_catchup_window"`
	BufferDropped       int64         `gorm:"column:buffer_dropped"`
	CreatedAt           time.Time     `gorm:"column:created_at"`
	UpdatedAt           time.Time     `gorm:"column:updated_at"`
}

func (SchedulePO) TableName() string { return "tenuto_schedules" }

func (SchedulePO) MysqlCreateTable() string {
	return `
CREATE TABLE IF NOT EXISTS tenuto_schedules (
  id              VARCHAR(191) NOT NULL,
  namespace       VARCHAR(191) NOT NULL,
  workflow_type     VARCHAR(191) NOT NULL,
  -- 必须是 VARCHAR 而非 TEXT：MySQL 不允许 BLOB/TEXT 列带 DEFAULT，
  -- 写成 TEXT NOT NULL DEFAULT '' 会报
  -- "Error 1101: BLOB, TEXT, GEOMETRY or JSON column 'description' can't have a default value"，
  -- 使本脚本在此**中断**，这张表及其后的 leader lock 两表全部建不出来。
  description     VARCHAR(191) NOT NULL DEFAULT '',
  cron_expression VARCHAR(191) NOT NULL,
  timezone        VARCHAR(64)  NOT NULL DEFAULT '',
  input_json      LONGBLOB,
  paused          TINYINT(1)   NOT NULL DEFAULT 0,
  overlap_policy  VARCHAR(64)  NOT NULL DEFAULT '',
  last_run_at     DATETIME(3),
  next_run_at     DATETIME(3),
  last_run_id     VARCHAR(191) NOT NULL DEFAULT '',
  run_count       INT          NOT NULL DEFAULT 0,
  -- catchup_window_ns 是补触发窗口（纳秒）；missed_catchup_window 与 buffer_dropped 是跳过的到期点计数。
  catchup_window_ns     BIGINT NOT NULL,
  missed_catchup_window BIGINT NOT NULL DEFAULT 0,
  buffer_dropped        BIGINT NOT NULL DEFAULT 0,
  created_at      DATETIME(3)  NOT NULL,
  updated_at      DATETIME(3)  NOT NULL,
  PRIMARY KEY (namespace, id),
  INDEX idx_workflow_schedules_next_run_at (next_run_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;
`
}
