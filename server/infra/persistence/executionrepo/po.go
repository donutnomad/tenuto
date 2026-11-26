package executionrepo

import "time"

// RunPO
// @Gsql
// @Setter(setter=false, patch=v2)
type RunPO struct {
	ID                     string     `gorm:"column:id"`
	Namespace              string     `gorm:"column:namespace"`
	WorkflowID             string     `gorm:"column:workflow_id"`
	WorkflowType           string     `gorm:"column:workflow_type"`
	WorkflowConfigVersion  string     `gorm:"column:workflow_config_version"`
	OriginalConfigVersion  string     `gorm:"column:original_workflow_config_version"`
	Policy                 string     `gorm:"column:policy"`
	PolicySource           string     `gorm:"column:policy_source"`
	TargetWorkerIdentity   string     `gorm:"column:target_worker_identity"`
	UpgradeError           string     `gorm:"column:upgrade_error"`
	ConfigError            string     `gorm:"column:config_error"`
	ParentRunID            string     `gorm:"column:parent_run_id"`
	ParentChildID          string     `gorm:"column:parent_child_id"`
	ParentScheduledEventID int64      `gorm:"column:parent_scheduled_event_id"`
	FirstRunID             string     `gorm:"column:first_run_id"`
	ContinuedRunID         string     `gorm:"column:continued_run_id"`
	ResetRunID             string     `gorm:"column:reset_run_id"`
	BranchTokenJSON        []byte     `gorm:"column:branch_token_json"`
	Status                 string     `gorm:"column:status"`
	InputJSON              []byte     `gorm:"column:input_json"`
	OutputJSON             []byte     `gorm:"column:output_json"`
	SearchAttributesJSON   []byte     `gorm:"column:search_attributes_json"`
	CreatedAt              time.Time  `gorm:"column:created_at"`
	DeadlineAt             *time.Time `gorm:"column:deadline_at"`
	RunDeadlineAt          *time.Time `gorm:"column:run_deadline_at"`
	Attempt                int        `gorm:"column:attempt"`
	Initiator              string     `gorm:"column:initiator"`
	IsCurrent              bool       `gorm:"column:is_current"`
	// The unique marker enforces at most one current run per namespace/workflow.
	CurrentMarker *string `gorm:"column:current_marker"`
}

func (RunPO) TableName() string { return "tenuto_runs" }

func (RunPO) MysqlCreateTable() string {
	return `
CREATE TABLE IF NOT EXISTS tenuto_runs (
  id                        VARCHAR(191) NOT NULL,
  namespace                 VARCHAR(191) NOT NULL DEFAULT 'default',
  workflow_id               VARCHAR(191) NOT NULL,
  workflow_type             VARCHAR(191) NOT NULL DEFAULT '',
  workflow_config_version   VARCHAR(64)  NOT NULL,
  -- original 记录启动来源；current 由 workflow_config_version 表达。
  original_workflow_config_version VARCHAR(64) NOT NULL DEFAULT '',
  policy                    VARCHAR(32)  NOT NULL DEFAULT '',
  policy_source             VARCHAR(32)  NOT NULL DEFAULT '',
  -- target_worker_identity 是 Run 固定路由快照，内容升级不覆盖。
  target_worker_identity    VARCHAR(191) NOT NULL DEFAULT '',
  upgrade_error             VARCHAR(1024) NOT NULL DEFAULT '',
  config_error              VARCHAR(1024) NOT NULL DEFAULT '',
  parent_run_id             VARCHAR(191) NOT NULL DEFAULT '',
  parent_child_id           VARCHAR(191) NOT NULL DEFAULT '',
  parent_scheduled_event_id BIGINT       NOT NULL DEFAULT 0,
  first_run_id              VARCHAR(191) NOT NULL DEFAULT '',
  continued_run_id          VARCHAR(191) NOT NULL DEFAULT '',
  -- reset_run_id 是最近一次 reset 出的 Run；branch_token_json 是 Run 的 History branch token。
  reset_run_id              VARCHAR(191) NOT NULL DEFAULT '',
  branch_token_json         LONGBLOB,
  status                    VARCHAR(64)  NOT NULL,
  input_json                LONGBLOB,
  output_json               LONGBLOB,
  search_attributes_json    LONGBLOB,
  created_at                DATETIME(3)  NOT NULL,
  deadline_at               DATETIME(3),
  run_deadline_at           DATETIME(3),
  attempt                   INT          NOT NULL DEFAULT 1,
  initiator                 VARCHAR(64)  NOT NULL DEFAULT '',
  -- is_current 标记该 run 是否为所属 workflow 的"最后一代"（对标 Temporal current execution）。
  -- 终态后仍保留，只在 retry/cron/CAN/reset 产生新代时由 SetCurrentRunCAS 真条件切换。
  is_current                BOOLEAN      NOT NULL DEFAULT FALSE,
  -- current_marker 是"每 workflow 至多一个 current"的 DB 唯一约束兜底：
  -- is_current=true 时存 namespace+'/'+workflow_id（非 NULL），is_current=false 时为 NULL。
  -- MySQL 唯一索引忽略 NULL，故多个 false 行不冲突；首设并发 TOCTOU 漏判时，
  -- 第二个置 current 写入相同 marker 会撞唯一约束失败，SetCurrentRunCAS 据此返回 false。
  current_marker            VARCHAR(383) DEFAULT NULL,
  PRIMARY KEY (id),
  -- Run 列表按 namespace（与状态）过滤、按 (created_at, id) 排序翻页；run-stats 按 namespace 分组计数。
  INDEX idx_workflow_runs_namespace (namespace, created_at, id),
  INDEX idx_workflow_runs_namespace_status (namespace, status, created_at, id),
  INDEX idx_workflow_runs_workflow_id (workflow_id),
  INDEX idx_workflow_runs_workflow_type (workflow_type),
  INDEX idx_workflow_runs_parent_run_id (parent_run_id),
  INDEX idx_workflow_runs_first_run_id (first_run_id),
  INDEX idx_workflow_runs_continued_run_id (continued_run_id),
  -- 周期补偿：(status, created_at, id) 支撑 Running Run 的游标翻页，两个截止时间索引支撑 ListRecoveryRunIDs 的到期查询。
  INDEX idx_workflow_runs_status (status, created_at, id),
  INDEX idx_workflow_runs_status_deadline (status, deadline_at),
  INDEX idx_workflow_runs_status_run_deadline (status, run_deadline_at),
  INDEX idx_workflow_runs_is_current (namespace, workflow_id, is_current),
  UNIQUE INDEX uniq_run_current_marker (current_marker)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;
`
}

// EventPO 一行是一条事件。event_json 是整条事件（history.CanonicalEventJSON 的输出），其余列只服务查询：
// tree_id / branch_id / node_id 定位与排序，run_id 与 type 供回收和终态判定过滤，dedupe_key 做幂等，
// timestamp 供孤儿事件回收判断年龄，event_size_bytes 供历史字节数求和。
// @Gsql
// @Setter(setter=false, patch=v2)
type EventPO struct {
	ID             int64     `gorm:"column:id"`
	TreeID         string    `gorm:"column:tree_id"`
	BranchID       string    `gorm:"column:branch_id"`
	NodeID         int64     `gorm:"column:node_id"`
	RunID          string    `gorm:"column:run_id"`
	Type           string    `gorm:"column:type"`
	DedupeKey      *string   `gorm:"column:dedupe_key"`
	Timestamp      time.Time `gorm:"column:timestamp"`
	EventSizeBytes int64     `gorm:"column:event_size_bytes"`
	EventJSON      []byte    `gorm:"column:event_json"`
}

func (EventPO) TableName() string { return "tenuto_events" }

func (EventPO) MysqlCreateTable() string {
	return `
CREATE TABLE IF NOT EXISTS tenuto_events (
  id               BIGINT       NOT NULL AUTO_INCREMENT,
  tree_id          VARCHAR(191) NOT NULL DEFAULT '',
  branch_id        VARCHAR(191) NOT NULL DEFAULT '',
  node_id          BIGINT       NOT NULL DEFAULT 0,
  run_id           VARCHAR(191) NOT NULL,
  type             VARCHAR(128) NOT NULL,
  dedupe_key       VARCHAR(400),
  timestamp        DATETIME(3)  NOT NULL,
  event_size_bytes BIGINT       NOT NULL DEFAULT 0,
  -- event_json 是整条事件的 JSON，字节原样保存；event_size_bytes 是它的长度。
  event_json       LONGBLOB     NOT NULL,
  PRIMARY KEY (id),
  INDEX idx_workflow_events_run_id (run_id),
  -- retention GC 与兜底扫描只按 tree_id 过滤（整树的 run 集合、残留事件删除、空树判定）。
  INDEX idx_workflow_events_tree_run (tree_id, run_id),
  -- node_id 是逻辑事件序（engine Event.ID），取自持有行锁的 execution state；读取一律 ORDER BY node_id。
  -- dedupe 的范围是 branch：reset 出的 Run 不与 fork 来的祖先前缀去重。
  UNIQUE INDEX idx_workflow_events_branch_node (branch_id, node_id),
  UNIQUE INDEX idx_workflow_events_branch_dedupe (branch_id, dedupe_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;
`
}

// HistoryTreePO
// @Gsql
// @Setter(setter=false, patch=v2)
type HistoryTreePO struct {
	TreeID    string    `gorm:"column:tree_id"`
	BranchID  string    `gorm:"column:branch_id"`
	DataJSON  []byte    `gorm:"column:data_json"`
	Info      string    `gorm:"column:info"`
	CreatedAt time.Time `gorm:"column:created_at"`
}

func (HistoryTreePO) TableName() string { return "tenuto_history_tree" }

func (HistoryTreePO) MysqlCreateTable() string {
	return `
CREATE TABLE IF NOT EXISTS tenuto_history_tree (
  tree_id    VARCHAR(191) NOT NULL,
  branch_id  VARCHAR(191) NOT NULL,
  data_json  LONGBLOB     NOT NULL,
  info       VARCHAR(191) NOT NULL DEFAULT '',
  created_at DATETIME(3)  NOT NULL,
  PRIMARY KEY (tree_id, branch_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;
`
}

// WorkflowExecutionStatePO
// @Gsql
// @Setter(setter=false, patch=v2)
type WorkflowExecutionStatePO struct {
	RunID                    string     `gorm:"column:run_id"`
	NextEventID              int64      `gorm:"column:next_event_id"`
	LastEventID              int64      `gorm:"column:last_event_id"`
	HistoryEventCount        int64      `gorm:"column:history_event_count"`
	HistorySizeBytes         int64      `gorm:"column:history_size_bytes"`
	Status                   string     `gorm:"column:status"`
	ClosedAt                 *time.Time `gorm:"column:closed_at"`
	SearchAttributesJSON     []byte     `gorm:"column:search_attributes_json"`
	CurrentWaitingJSON       []byte     `gorm:"column:current_waiting_json"`
	OpenWaitsJSON            []byte     `gorm:"column:open_waits_json"`
	FailureReason            string     `gorm:"column:failure_reason"`
	IsReset                  bool       `gorm:"column:is_reset"`
	BlockedJSON              []byte     `gorm:"column:blocked_json"`
	Blocked                  bool       `gorm:"column:blocked"`
	NeedsWorkflowTask        bool       `gorm:"column:needs_workflow_task"`
	OpenWorkflowTaskEventID  int64      `gorm:"column:open_workflow_task_event_id"`
	ExecutionGeneration      int64      `gorm:"column:execution_generation"`
	CurrentActivationID      string     `gorm:"column:current_activation_id"`
	CurrentActivationAttempt int        `gorm:"column:current_activation_attempt"`
	StickyWorkerIdentity     string     `gorm:"column:sticky_worker_identity"`
	StickyWorkerBuildID      string     `gorm:"column:sticky_worker_build_id"`
	StickyGeneration         int64      `gorm:"column:sticky_generation"`
	// Nil represents no sticky lease; a zero time.Time is invalid for strict MySQL dates.
	StickyExpiresAt *time.Time `gorm:"column:sticky_expires_at"`
	StateVersion    int64      `gorm:"column:state_version"`
	UpdatedAt       time.Time  `gorm:"column:updated_at"`
}

func (WorkflowExecutionStatePO) TableName() string { return "tenuto_execution_states" }

func (WorkflowExecutionStatePO) MysqlCreateTable() string {
	return `
CREATE TABLE IF NOT EXISTS tenuto_execution_states (
  run_id                         VARCHAR(191) NOT NULL,
  next_event_id                  BIGINT       NOT NULL,
  last_event_id                  BIGINT       NOT NULL DEFAULT 0,
  history_event_count            BIGINT       NOT NULL DEFAULT 0,
  history_size_bytes             BIGINT       NOT NULL DEFAULT 0,
  status                         VARCHAR(64)  NOT NULL,
  closed_at                      DATETIME(3),
  search_attributes_json         LONGBLOB,
  current_waiting_json           LONGBLOB,
  open_waits_json                LONGBLOB,
  failure_reason                 LONGTEXT,
  is_reset                       BOOLEAN      NOT NULL DEFAULT FALSE,
  blocked_json                   LONGBLOB,
  blocked                        BOOLEAN      NOT NULL DEFAULT FALSE,
  -- needs_workflow_task 投影 WorkflowExecutionState.NeedsWorkflowTask()：有新事实却没有 open WFT。
  needs_workflow_task            BOOLEAN      NOT NULL DEFAULT FALSE,
  open_workflow_task_event_id    BIGINT       NOT NULL DEFAULT 0,
  execution_generation           BIGINT       NOT NULL DEFAULT 1,
  current_activation_id          VARCHAR(191) NOT NULL DEFAULT '',
  current_activation_attempt     INT          NOT NULL DEFAULT 0,
  sticky_worker_identity         VARCHAR(191) NOT NULL DEFAULT '',
  sticky_worker_build_id         VARCHAR(191) NOT NULL DEFAULT '',
  sticky_generation              BIGINT       NOT NULL DEFAULT 1,
  sticky_expires_at              DATETIME(3),
  state_version                  BIGINT       NOT NULL DEFAULT 0,
  updated_at                     DATETIME(3)  NOT NULL,
  PRIMARY KEY (run_id),
  INDEX idx_workflow_execution_states_status (status),
  INDEX idx_workflow_execution_states_blocked (blocked),
  INDEX idx_workflow_execution_states_open_wft (open_workflow_task_event_id),
  INDEX idx_workflow_execution_states_needs_wft (needs_workflow_task),
  INDEX idx_workflow_execution_states_is_reset (is_reset),
  -- sticky 回落扫描（ListExpiredStickyExecutions）的定向索引：
  -- 前缀列 sticky_worker_identity 绝大多数行是 ''（无 sticky），选择性良好；
  -- 第二列 sticky_expires_at 支撑过期范围扫描。
  INDEX idx_wf_exec_states_sticky_fallback (sticky_worker_identity, sticky_expires_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;
`
}

// ActivityStatePO
// @Gsql
// @Setter(setter=false, patch=v2)
type ActivityStatePO struct {
	TargetWorkerIdentity     string     `gorm:"column:target_worker_identity"`
	RunID                    string     `gorm:"column:run_id"`
	ScheduledEventID         int64      `gorm:"column:scheduled_event_id"`
	ActivityID               string     `gorm:"column:activity_id"`
	ActivityType             string     `gorm:"column:activity_type"`
	TaskQueue                string     `gorm:"column:task_queue"`
	Attempt                  int        `gorm:"column:attempt"`
	Stamp                    int64      `gorm:"column:stamp"`
	StartedEventID           int64      `gorm:"column:started_event_id"`
	ActivityReset            bool       `gorm:"column:activity_reset"`
	CancelRequested          bool       `gorm:"column:cancel_requested"`
	WorkerIdentity           string     `gorm:"column:worker_identity"`
	WorkerBuildID            string     `gorm:"column:worker_build_id"`
	LastHeartbeatJSON        []byte     `gorm:"column:last_heartbeat_json"`
	LastHeartbeatAt          *time.Time `gorm:"column:last_heartbeat_at"`
	RetryLastFailure         string     `gorm:"column:retry_last_failure"`
	NextAttemptAt            *time.Time `gorm:"column:next_attempt_at"`
	TimeoutAt                *time.Time `gorm:"column:timeout_at"`
	Status                   string     `gorm:"column:status"`
	ScheduledTime            time.Time  `gorm:"column:scheduled_time"`
	StartedTime              *time.Time `gorm:"column:started_time"`
	ScheduleToStartTimeoutMs int64      `gorm:"column:schedule_to_start_timeout_ms"`
	StartToCloseTimeoutMs    int64      `gorm:"column:start_to_close_timeout_ms"`
	ScheduleToCloseTimeoutMs int64      `gorm:"column:schedule_to_close_timeout_ms"`
	HeartbeatTimeoutMs       int64      `gorm:"column:heartbeat_timeout_ms"`
	RetryPolicyJSON          []byte     `gorm:"column:retry_policy_json"`
	InputJSON                []byte     `gorm:"column:input_json"`
	TraceContextJSON         *string    `gorm:"column:trace_context_json"`
	CreatedAt                time.Time  `gorm:"column:created_at"`
	UpdatedAt                time.Time  `gorm:"column:updated_at"`
}

func (ActivityStatePO) TableName() string { return "tenuto_activity_states" }

func (ActivityStatePO) MysqlCreateTable() string {
	return `
CREATE TABLE IF NOT EXISTS tenuto_activity_states (
  run_id                         VARCHAR(191) NOT NULL,
  scheduled_event_id             BIGINT       NOT NULL,
  activity_id                    VARCHAR(191) NOT NULL DEFAULT '',
  activity_type                  VARCHAR(191) NOT NULL DEFAULT '',
  task_queue                     VARCHAR(191) NOT NULL DEFAULT '',
  target_worker_identity VARCHAR(191) NOT NULL DEFAULT '',
  attempt                        INT          NOT NULL DEFAULT 0,
  stamp                          BIGINT       NOT NULL DEFAULT 0,
  started_event_id               BIGINT       NOT NULL DEFAULT 0,
  activity_reset                 BOOLEAN      NOT NULL DEFAULT FALSE,
  cancel_requested               BOOLEAN      NOT NULL DEFAULT FALSE,
  worker_identity                VARCHAR(191) NOT NULL DEFAULT '',
  worker_build_id                VARCHAR(191) NOT NULL DEFAULT '',
  last_heartbeat_json            LONGBLOB,
  last_heartbeat_at              DATETIME(3),
  retry_last_failure             LONGTEXT,
  next_attempt_at                DATETIME(3),
  -- timeout_at 是已生效超时里最早的截止时间（execution.ActivityTimeoutAt），由每次写入按整份 state 重算。
  timeout_at                     DATETIME(3),
  status                         VARCHAR(32)  NOT NULL,
  scheduled_time                 DATETIME(3)  NOT NULL,
  started_time                   DATETIME(3),
  schedule_to_start_timeout_ms   BIGINT       NOT NULL DEFAULT 0,
  start_to_close_timeout_ms      BIGINT       NOT NULL DEFAULT 0,
  schedule_to_close_timeout_ms   BIGINT       NOT NULL DEFAULT 0,
  heartbeat_timeout_ms           BIGINT       NOT NULL DEFAULT 0,
  retry_policy_json              LONGBLOB,
  input_json                     LONGBLOB,
  trace_context_json             JSON,
  created_at                     DATETIME(3)  NOT NULL,
  updated_at                     DATETIME(3)  NOT NULL,
  PRIMARY KEY (run_id, scheduled_event_id),
  INDEX idx_activity_states_run_id (run_id),
  INDEX idx_activity_states_activity_id (activity_id),
  INDEX idx_activity_states_status (status, timeout_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;
`
}

// TimerStatePO
// @Gsql
// @Setter(setter=false, patch=v2)
type TimerStatePO struct {
	RunID            string    `gorm:"column:run_id"`
	ScheduledEventID int64     `gorm:"column:scheduled_event_id"`
	TimerID          string    `gorm:"column:timer_id"`
	Deadline         time.Time `gorm:"column:deadline"`
	Status           string    `gorm:"column:status"`
	Kind             string    `gorm:"column:kind"`
	TraceContextJSON *string   `gorm:"column:trace_context_json"`
	CreatedAt        time.Time `gorm:"column:created_at"`
	UpdatedAt        time.Time `gorm:"column:updated_at"`
}

func (TimerStatePO) TableName() string { return "tenuto_timer_states" }

func (TimerStatePO) MysqlCreateTable() string {
	return `
CREATE TABLE IF NOT EXISTS tenuto_timer_states (
  run_id             VARCHAR(191) NOT NULL,
  scheduled_event_id BIGINT       NOT NULL,
  timer_id           VARCHAR(191) NOT NULL DEFAULT '',
  deadline           DATETIME(3)  NOT NULL,
  status             VARCHAR(32)  NOT NULL,
  kind               VARCHAR(32)  NOT NULL DEFAULT '',
  trace_context_json  JSON,
  created_at         DATETIME(3)  NOT NULL,
  updated_at         DATETIME(3)  NOT NULL,
  PRIMARY KEY (run_id, scheduled_event_id),
  INDEX idx_timer_states_run_id (run_id),
  INDEX idx_timer_states_timer_id (timer_id),
  INDEX idx_timer_states_status (status, deadline)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;
`
}

// ChildWorkflowStatePO
// @Gsql
// @Setter(setter=false, patch=v2)
type ChildWorkflowStatePO struct {
	RunID                 string    `gorm:"column:run_id"`
	ScheduledEventID      int64     `gorm:"column:scheduled_event_id"`
	ChildID               string    `gorm:"column:child_id"`
	ChildRunID            string    `gorm:"column:child_run_id"`
	WorkflowType          string    `gorm:"column:workflow_type"`
	WorkflowConfigVersion string    `gorm:"column:workflow_config_version"`
	Status                string    `gorm:"column:status"`
	ParentClosePolicy     string    `gorm:"column:parent_close_policy"`
	CancelRequested       bool      `gorm:"column:cancel_requested"`
	InputJSON             []byte    `gorm:"column:input_json"`
	OutputJSON            []byte    `gorm:"column:output_json"`
	Error                 string    `gorm:"column:error"`
	Reason                string    `gorm:"column:reason"`
	CreatedAt             time.Time `gorm:"column:created_at"`
	UpdatedAt             time.Time `gorm:"column:updated_at"`
}

func (ChildWorkflowStatePO) TableName() string { return "tenuto_child_workflow_states" }

func (ChildWorkflowStatePO) MysqlCreateTable() string {
	return `
CREATE TABLE IF NOT EXISTS tenuto_child_workflow_states (
  run_id                  VARCHAR(191) NOT NULL,
  scheduled_event_id      BIGINT       NOT NULL,
  child_id                VARCHAR(191) NOT NULL DEFAULT '',
  child_run_id            VARCHAR(191) NOT NULL DEFAULT '',
  workflow_type           VARCHAR(191) NOT NULL DEFAULT '',
  workflow_config_version VARCHAR(191) NOT NULL DEFAULT '',
  status                  VARCHAR(32)  NOT NULL,
  parent_close_policy     VARCHAR(32)  NOT NULL DEFAULT '',
  cancel_requested        BOOLEAN      NOT NULL DEFAULT FALSE,
  input_json              LONGBLOB,
  output_json             LONGBLOB,
  error                   LONGTEXT,
  reason                  LONGTEXT,
  created_at              DATETIME(3)  NOT NULL,
  updated_at              DATETIME(3)  NOT NULL,
  PRIMARY KEY (run_id, scheduled_event_id),
  INDEX idx_workflow_child_workflow_states_run_id (run_id),
  INDEX idx_workflow_child_workflow_states_child_id (child_id),
  INDEX idx_workflow_child_workflow_states_child_run_id (child_run_id),
  INDEX idx_workflow_child_workflow_states_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;
`
}

// DeletionTaskPO
// @Gsql
// @Setter(setter=false, patch=v2)
type DeletionTaskPO struct {
	TreeID    string    `gorm:"column:tree_id"`
	Namespace string    `gorm:"column:namespace"`
	FireAt    time.Time `gorm:"column:fire_at"`
	Attempts  int       `gorm:"column:attempts"`
	LastError string    `gorm:"column:last_error"`
	CreatedAt time.Time `gorm:"column:created_at"`
	UpdatedAt time.Time `gorm:"column:updated_at"`
}

func (DeletionTaskPO) TableName() string { return "tenuto_deletion_tasks" }

func (DeletionTaskPO) MysqlCreateTable() string {
	return `
CREATE TABLE IF NOT EXISTS tenuto_deletion_tasks (
  tree_id    VARCHAR(191) NOT NULL,
  namespace  VARCHAR(191) NOT NULL DEFAULT 'default',
  -- fire_at = 终态事件时刻 + retention + rand(0, jitter)。jitter 防批量关闭同秒打爆 DB。
  fire_at    DATETIME(3)  NOT NULL,
  attempts   INT          NOT NULL DEFAULT 0,
  last_error LONGTEXT,
  created_at DATETIME(3)  NOT NULL,
  updated_at DATETIME(3)  NOT NULL,
  PRIMARY KEY (tree_id),
  INDEX idx_deletion_tasks_fire_at (fire_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;
`
}
