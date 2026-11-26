package dispatchrepo

import "time"

// TaskDispatchPO
// @Gsql
// @Setter(setter=false, patch=v2)
type TaskDispatchPO struct {
	Namespace        string `gorm:"column:namespace"`
	TaskType         string `gorm:"column:task_type"`
	RunID            string `gorm:"column:run_id"`
	ScheduledEventID int64  `gorm:"column:scheduled_event_id"`
	Attempt          int    `gorm:"column:attempt"`
	ActivityID       string `gorm:"column:activity_id"`
	ActivityType     string `gorm:"column:activity_type"`
	TaskQueue        string `gorm:"column:task_queue"`
	Status           string `gorm:"column:status"`
	LeaseID          string `gorm:"column:lease_id"`
	// Nil represents no lease; a zero time.Time is invalid for strict MySQL dates.
	LeaseExpiresAt   *time.Time `gorm:"column:lease_expires_at"`
	Attempts         int        `gorm:"column:attempts"`
	LastError        string     `gorm:"column:last_error"`
	LastDispatchedAt *time.Time `gorm:"column:last_dispatched_at"`
	CreatedAt        time.Time  `gorm:"column:created_at"`
	UpdatedAt        time.Time  `gorm:"column:updated_at"`
}

func (TaskDispatchPO) TableName() string { return "tenuto_task_dispatches" }

func (TaskDispatchPO) MysqlCreateTable() string {
	return `
CREATE TABLE IF NOT EXISTS tenuto_task_dispatches (
  namespace          VARCHAR(191) NOT NULL DEFAULT 'default',
  task_type          VARCHAR(32)  NOT NULL,
  run_id             VARCHAR(191) NOT NULL,
  scheduled_event_id BIGINT       NOT NULL,
  attempt            INT          NOT NULL,
  activity_id        VARCHAR(191) NOT NULL DEFAULT '',
  activity_type      VARCHAR(191) NOT NULL DEFAULT '',
  task_queue         VARCHAR(191) NOT NULL DEFAULT '',
  status             VARCHAR(32)  NOT NULL,
  lease_id           VARCHAR(191) NOT NULL DEFAULT '',
  lease_expires_at   DATETIME(3)  NULL,
  attempts           INT          NOT NULL DEFAULT 0,
  last_error         LONGTEXT,
  last_dispatched_at DATETIME(3),
  created_at         DATETIME(3)  NOT NULL,
  updated_at         DATETIME(3)  NOT NULL,
  PRIMARY KEY (namespace, task_type, run_id, scheduled_event_id, attempt),
  INDEX idx_task_dispatches_namespace (namespace),
  INDEX idx_task_dispatches_run_id (run_id),
  INDEX idx_task_dispatches_task_queue (task_queue),
  INDEX idx_task_dispatches_status (status),
  INDEX idx_task_dispatches_lease_id (lease_id),
  INDEX idx_task_dispatches_lease_expires_at (lease_expires_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
`
}

// ActivityWorkerVersionPO
// @Gsql
// @Setter(setter=false, patch=v2)
type ActivityWorkerVersionPO struct {
	Namespace    string `gorm:"column:namespace"`
	ActivityType string `gorm:"column:activity_type"`
	App          string `gorm:"column:app"`
	Identity     string `gorm:"column:identity"`
	BuildID      string `gorm:"column:build_id"`
	TaskQueue    string `gorm:"column:task_queue"`
	IsDefault    bool   `gorm:"column:is_default"`
	Weight       int    `gorm:"column:weight"`
	HeartbeatTTL string `gorm:"column:heartbeat_ttl"`
	// ContractsJSON 保存 worker 上报的 Activity 契约元数据，跨服务重启保留。
	ContractsJSON   []byte    `gorm:"column:contracts_json"`
	LastHeartbeatAt time.Time `gorm:"column:last_heartbeat_at"`
	ExpiresAt       time.Time `gorm:"column:expires_at"`
	CreatedAt       time.Time `gorm:"column:created_at"`
	UpdatedAt       time.Time `gorm:"column:updated_at"`
}

func (ActivityWorkerVersionPO) TableName() string { return "tenuto_activity_worker_versions" }

func (ActivityWorkerVersionPO) MysqlCreateTable() string {
	return `
CREATE TABLE IF NOT EXISTS tenuto_activity_worker_versions (
  namespace         VARCHAR(64)  NOT NULL DEFAULT 'default',
  activity_type     VARCHAR(128) NOT NULL,
  app               VARCHAR(191) NOT NULL DEFAULT '',
  identity          VARCHAR(128) NOT NULL,
  build_id          VARCHAR(64)  NOT NULL,
  task_queue        VARCHAR(64)  NOT NULL,
  is_default        TINYINT(1)   NOT NULL DEFAULT 0,
  weight            INT          NOT NULL DEFAULT 0,
  heartbeat_ttl     VARCHAR(64)  NOT NULL DEFAULT '',
  contracts_json    LONGBLOB,
  last_heartbeat_at DATETIME(3)  NOT NULL,
  expires_at        DATETIME(3)  NOT NULL,
  created_at        DATETIME(3)  NOT NULL,
  updated_at        DATETIME(3)  NOT NULL,
  PRIMARY KEY (namespace, activity_type, identity, build_id, task_queue),
  INDEX idx_activity_worker_versions_namespace (namespace),
  INDEX idx_activity_worker_versions_app (app),
  INDEX idx_activity_worker_versions_identity (identity),
  INDEX idx_activity_worker_versions_build_id (build_id),
  INDEX idx_activity_worker_versions_task_queue (task_queue),
  INDEX idx_activity_worker_versions_expires_at (expires_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
`
}
