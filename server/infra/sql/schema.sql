-- Workflow 库当前的完整表结构，等于执行完 migrations/ 全部迁移的结果，供阅读与守卫测试对照。
-- 部署不执行本文件：表由 tenuto-server migrate 按 migrations/ 建出并升级。改表时新增迁移文件，
-- 同步修改本文件、PO 与 MysqlCreateTable()，见 CONTRIBUTING.md。

CREATE TABLE `tenuto_runs` (
  `id`                        VARCHAR(191) NOT NULL,
  `namespace`                 VARCHAR(191) NOT NULL DEFAULT 'default',
  `workflow_id`               VARCHAR(191) NOT NULL,
  `workflow_type`             VARCHAR(191) NOT NULL DEFAULT '',
  `workflow_config_version`   VARCHAR(64)  NOT NULL,
  -- original 记录启动来源；current 由 workflow_config_version 表达，AutoUpgrade 提交后才更新。
  `original_workflow_config_version` VARCHAR(64) NOT NULL DEFAULT '',
  `policy`                    VARCHAR(32)  NOT NULL DEFAULT '',
  `policy_source`             VARCHAR(32)  NOT NULL DEFAULT '',
  -- target_worker_identity 是 Run 固定路由快照，内容升级不覆盖。
  `target_worker_identity`    VARCHAR(191) NOT NULL DEFAULT '',
  -- upgrade_error 记录最近一次 AutoUpgrade 失败诊断，不阻塞重试；成功后清空，current 仍是最后成功版本。
  `upgrade_error`             VARCHAR(1024) NOT NULL DEFAULT '',
  -- config_error 非空=确定性配置/契约/target 冲突，暂停推进等待人工修复。
  `config_error`              VARCHAR(1024) NOT NULL DEFAULT '',
  `parent_run_id`             VARCHAR(191) NOT NULL DEFAULT '',
  `parent_child_id`           VARCHAR(191) NOT NULL DEFAULT '',
  `parent_scheduled_event_id` BIGINT       NOT NULL DEFAULT 0,
  `first_run_id`              VARCHAR(191) NOT NULL DEFAULT '',
  `continued_run_id`          VARCHAR(191) NOT NULL DEFAULT '',
  -- reset_run_id 是最近一次 reset 出的 Run；branch_token_json 是 Run 的 History branch token。
  `reset_run_id`              VARCHAR(191) NOT NULL DEFAULT '',
  `branch_token_json`         LONGBLOB,
  `status`                    VARCHAR(64)  NOT NULL,
  `input_json`                LONGBLOB,
  `output_json`               LONGBLOB,
  `search_attributes_json`    LONGBLOB,
  `created_at`                DATETIME(3)  NOT NULL,
  `deadline_at`               DATETIME(3),
  `run_deadline_at`           DATETIME(3),
  `attempt`                   INT          NOT NULL DEFAULT 1,
  `initiator`                 VARCHAR(64)  NOT NULL DEFAULT '',
  -- is_current 标记该 run 是否为所属 workflow 的"最后一代"（对标 Temporal current execution）。
  -- 终态后仍保留，只在 retry/cron/CAN/reset 产生新代时由 SetCurrentRunCAS 真条件切换。
  `is_current`                BOOLEAN      NOT NULL DEFAULT FALSE,
  -- current_marker 是"每 workflow 至多一个 current"的 DB 唯一约束兜底：
  -- is_current=true 时存 namespace+'/'+workflow_id（非 NULL），is_current=false 时为 NULL。
  -- MySQL 唯一索引忽略 NULL，故多个 false 行不冲突；首设并发 TOCTOU 漏判时，
  -- 第二个置 current 写入相同 marker 会撞唯一约束失败，SetCurrentRunCAS 据此返回 false。
  `current_marker`            VARCHAR(383) DEFAULT NULL,
  PRIMARY KEY (`id`),
  INDEX `idx_workflow_runs_namespace` (`namespace`),
  INDEX `idx_workflow_runs_workflow_id` (`workflow_id`),
  INDEX `idx_workflow_runs_workflow_type` (`workflow_type`),
  INDEX `idx_workflow_runs_parent_run_id` (`parent_run_id`),
  INDEX `idx_workflow_runs_first_run_id` (`first_run_id`),
  INDEX `idx_workflow_runs_continued_run_id` (`continued_run_id`),
  -- 周期补偿：(status, created_at, id) 支撑 Running Run 的游标翻页，两个截止时间索引支撑 ListRecoveryRunIDs 的到期查询。
  INDEX `idx_workflow_runs_status` (`status`, `created_at`, `id`),
  INDEX `idx_workflow_runs_status_deadline` (`status`, `deadline_at`),
  INDEX `idx_workflow_runs_status_run_deadline` (`status`, `run_deadline_at`),
  INDEX `idx_workflow_runs_is_current` (`namespace`, `workflow_id`, `is_current`),
  UNIQUE INDEX `uniq_run_current_marker` (`current_marker`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `tenuto_events` (
  `id`               BIGINT       NOT NULL AUTO_INCREMENT,
  `tree_id`          VARCHAR(191) NOT NULL DEFAULT '',
  `branch_id`        VARCHAR(191) NOT NULL DEFAULT '',
  `node_id`          BIGINT       NOT NULL DEFAULT 0,
  `run_id`           VARCHAR(191) NOT NULL,
  `type`             VARCHAR(128) NOT NULL,
  `dedupe_key`       VARCHAR(400),
  `timestamp`        DATETIME(3)  NOT NULL,
  `event_size_bytes` BIGINT       NOT NULL DEFAULT 0,
  -- event_json 是整条事件的 JSON，字节原样保存；event_size_bytes 是它的长度。
  `event_json`       LONGBLOB     NOT NULL,
  PRIMARY KEY (`id`),
  INDEX `idx_workflow_events_run_id` (`run_id`),
  -- retention GC 与兜底扫描只按 tree_id 过滤（整树的 run 集合、残留事件删除、空树判定）。
  INDEX `idx_workflow_events_tree_run` (`tree_id`, `run_id`),
  -- node_id 是逻辑事件序（engine Event.ID），取自持有行锁的 execution state；读取一律 ORDER BY node_id。
  -- dedupe 的范围是 branch：reset 出的 Run 不与 fork 来的祖先前缀去重。
  UNIQUE INDEX `idx_workflow_events_branch_node` (`branch_id`, `node_id`),
  UNIQUE INDEX `idx_workflow_events_branch_dedupe` (`branch_id`, `dedupe_key`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `tenuto_execution_states` (
  `run_id`                         VARCHAR(191) NOT NULL,
  `next_event_id`                  BIGINT       NOT NULL,
  `last_event_id`                  BIGINT       NOT NULL DEFAULT 0,
  `history_event_count`            BIGINT       NOT NULL DEFAULT 0,
  `history_size_bytes`             BIGINT       NOT NULL DEFAULT 0,
  `status`                         VARCHAR(64)  NOT NULL,
  `closed_at`                      DATETIME(3),
  `search_attributes_json`         LONGBLOB,
  `current_waiting_json`           LONGBLOB,
  `open_waits_json`                LONGBLOB,
  `failure_reason`                 LONGTEXT,
  `is_reset`                       BOOLEAN      NOT NULL DEFAULT FALSE,
  `blocked_json`                   LONGBLOB,
  `blocked`                        BOOLEAN      NOT NULL DEFAULT FALSE,
  -- needs_workflow_task 投影 WorkflowExecutionState.NeedsWorkflowTask()：有新事实却没有 open WFT。
  `needs_workflow_task`            BOOLEAN      NOT NULL DEFAULT FALSE,
  `open_workflow_task_event_id`    BIGINT       NOT NULL DEFAULT 0,
  `execution_generation`           BIGINT       NOT NULL DEFAULT 1,
  `current_activation_id`          VARCHAR(191) NOT NULL DEFAULT '',
  `current_activation_attempt`     INT          NOT NULL DEFAULT 0,
  `sticky_worker_identity`         VARCHAR(191) NOT NULL DEFAULT '',
  `sticky_worker_build_id`         VARCHAR(191) NOT NULL DEFAULT '',
  `sticky_generation`              BIGINT       NOT NULL DEFAULT 1,
  `sticky_expires_at`              DATETIME(3),
  `state_version`                  BIGINT       NOT NULL DEFAULT 0,
  `updated_at`                     DATETIME(3)  NOT NULL,
  PRIMARY KEY (`run_id`),
  INDEX `idx_workflow_execution_states_status` (`status`),
  INDEX `idx_workflow_execution_states_blocked` (`blocked`),
  INDEX `idx_workflow_execution_states_open_wft` (`open_workflow_task_event_id`),
  INDEX `idx_workflow_execution_states_needs_wft` (`needs_workflow_task`),
  INDEX `idx_workflow_execution_states_is_reset` (`is_reset`),
  -- sticky 回落扫描（ListExpiredStickyExecutions）的定向索引：
  -- 前缀列 sticky_worker_identity 绝大多数行是 ''（无 sticky），选择性良好；
  -- 第二列 sticky_expires_at 支撑过期范围扫描。
  INDEX `idx_wf_exec_states_sticky_fallback` (`sticky_worker_identity`, `sticky_expires_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- tenuto_history_tree 持久化 history branch 拓扑（fork/reset 的核心表）。
CREATE TABLE `tenuto_history_tree` (
  `tree_id`    VARCHAR(191) NOT NULL,
  `branch_id`  VARCHAR(191) NOT NULL,
  `data_json`  LONGBLOB     NOT NULL,
  `info`       VARCHAR(191) NOT NULL DEFAULT '',
  `created_at` DATETIME(3)  NOT NULL,
  PRIMARY KEY (`tree_id`, `branch_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- workflow_child_workflow_states 持久化 child workflow 的调度/完成状态。
-- 对应 ChildWorkflowStatePO；(run_id, scheduled_event_id) 复合主键。
CREATE TABLE `tenuto_child_workflow_states` (
  `run_id`                  VARCHAR(191) NOT NULL,
  `scheduled_event_id`      BIGINT       NOT NULL,
  `child_id`                VARCHAR(191) NOT NULL DEFAULT '',
  `child_run_id`            VARCHAR(191) NOT NULL DEFAULT '',
  `workflow_type`           VARCHAR(191) NOT NULL DEFAULT '',
  `workflow_config_version` VARCHAR(191) NOT NULL DEFAULT '',
  `status`                  VARCHAR(32)  NOT NULL,
  `parent_close_policy`     VARCHAR(32)  NOT NULL DEFAULT '',
  `cancel_requested`        BOOLEAN      NOT NULL DEFAULT FALSE,
  `input_json`              LONGBLOB,
  `output_json`             LONGBLOB,
  `error`                   LONGTEXT,
  `reason`                  LONGTEXT,
  `created_at`              DATETIME(3)  NOT NULL,
  `updated_at`              DATETIME(3)  NOT NULL,
  PRIMARY KEY (`run_id`, `scheduled_event_id`),
  INDEX `idx_workflow_child_workflow_states_run_id` (`run_id`),
  INDEX `idx_workflow_child_workflow_states_child_id` (`child_id`),
  INDEX `idx_workflow_child_workflow_states_child_run_id` (`child_run_id`),
  INDEX `idx_workflow_child_workflow_states_status` (`status`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `tenuto_configs` (
  `namespace`     VARCHAR(191) NOT NULL DEFAULT 'default',
  `workflow_type` VARCHAR(191) NOT NULL,
  `version`       VARCHAR(64)  NOT NULL,
  `config_json`   LONGBLOB     NOT NULL,
  `created_at`    DATETIME(3)  NOT NULL,
  PRIMARY KEY (`namespace`, `workflow_type`, `version`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `tenuto_config_active` (
  `namespace`     VARCHAR(191) NOT NULL DEFAULT 'default',
  `workflow_type` VARCHAR(191) NOT NULL,
  `version`       VARCHAR(64)  NOT NULL,
  `updated_at`    DATETIME(3)  NOT NULL,
  PRIMARY KEY (`namespace`, `workflow_type`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `tenuto_task_dispatches` (
  `namespace`          VARCHAR(191) NOT NULL DEFAULT 'default',
  `task_type`          VARCHAR(32)  NOT NULL,
  `run_id`             VARCHAR(191) NOT NULL,
  `scheduled_event_id` BIGINT       NOT NULL,
  `attempt`            INT          NOT NULL,
  `activity_id`        VARCHAR(191) NOT NULL DEFAULT '',
  `activity_type`      VARCHAR(191) NOT NULL DEFAULT '',
  `task_queue`         VARCHAR(191) NOT NULL DEFAULT '',
  `status`             VARCHAR(32)  NOT NULL,
  `lease_id`           VARCHAR(191) NOT NULL DEFAULT '',
  `lease_expires_at`   DATETIME(3)  NULL,
  `attempts`           INT          NOT NULL DEFAULT 0,
  `last_error`         LONGTEXT,
  `last_dispatched_at` DATETIME(3),
  `created_at`         DATETIME(3)  NOT NULL,
  `updated_at`         DATETIME(3)  NOT NULL,
  PRIMARY KEY (`namespace`, `task_type`, `run_id`, `scheduled_event_id`, `attempt`),
  INDEX `idx_task_dispatches_namespace` (`namespace`),
  INDEX `idx_task_dispatches_run_id` (`run_id`),
  INDEX `idx_task_dispatches_task_queue` (`task_queue`),
  INDEX `idx_task_dispatches_status` (`status`),
  INDEX `idx_task_dispatches_lease_id` (`lease_id`),
  INDEX `idx_task_dispatches_lease_expires_at` (`lease_expires_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `tenuto_activity_states` (
  `run_id`                         VARCHAR(191) NOT NULL,
  `scheduled_event_id`             BIGINT       NOT NULL,
  `activity_id`                    VARCHAR(191) NOT NULL DEFAULT '',
  `activity_type`                  VARCHAR(191) NOT NULL DEFAULT '',
  `task_queue`                     VARCHAR(191) NOT NULL DEFAULT '',
  `target_worker_identity` VARCHAR(191) NOT NULL DEFAULT '',
  `attempt`                        INT          NOT NULL DEFAULT 0,
  `stamp`                          BIGINT       NOT NULL DEFAULT 0,
  `started_event_id`               BIGINT       NOT NULL DEFAULT 0,
  `activity_reset`                 BOOLEAN      NOT NULL DEFAULT FALSE,
  `cancel_requested`               BOOLEAN      NOT NULL DEFAULT FALSE,
  `worker_identity`                VARCHAR(191) NOT NULL DEFAULT '',
  `worker_build_id`                VARCHAR(191) NOT NULL DEFAULT '',
  `last_heartbeat_json`            LONGBLOB,
  `last_heartbeat_at`              DATETIME(3),
  `retry_last_failure`             LONGTEXT,
  `next_attempt_at`                DATETIME(3),
  -- timeout_at 是已生效超时里最早的截止时间（execution.ActivityTimeoutAt），由每次写入按整份 state 重算。
  `timeout_at`                     DATETIME(3),
  `status`                         VARCHAR(32)  NOT NULL,
  `scheduled_time`                 DATETIME(3)  NOT NULL,
  `started_time`                   DATETIME(3),
  `schedule_to_start_timeout_ms`   BIGINT       NOT NULL DEFAULT 0,
  `start_to_close_timeout_ms`      BIGINT       NOT NULL DEFAULT 0,
  `schedule_to_close_timeout_ms`   BIGINT       NOT NULL DEFAULT 0,
  `heartbeat_timeout_ms`           BIGINT       NOT NULL DEFAULT 0,
  `retry_policy_json`              LONGBLOB,
  `input_json`                     LONGBLOB,
  `trace_context_json`             JSON,
  `created_at`                     DATETIME(3)  NOT NULL,
  `updated_at`                     DATETIME(3)  NOT NULL,
  PRIMARY KEY (`run_id`, `scheduled_event_id`),
  INDEX `idx_activity_states_run_id` (`run_id`),
  INDEX `idx_activity_states_activity_id` (`activity_id`),
  INDEX `idx_activity_states_status` (`status`, `timeout_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `tenuto_timer_states` (
  `run_id`             VARCHAR(191) NOT NULL,
  `scheduled_event_id` BIGINT       NOT NULL,
  `timer_id`           VARCHAR(191) NOT NULL DEFAULT '',
  `deadline`           DATETIME(3)  NOT NULL,
  `status`             VARCHAR(32)  NOT NULL,
  `kind`               VARCHAR(32)  NOT NULL DEFAULT '',
  `trace_context_json`  JSON,
  `created_at`         DATETIME(3)  NOT NULL,
  `updated_at`         DATETIME(3)  NOT NULL,
  PRIMARY KEY (`run_id`, `scheduled_event_id`),
  INDEX `idx_timer_states_run_id` (`run_id`),
  INDEX `idx_timer_states_timer_id` (`timer_id`),
  INDEX `idx_timer_states_status` (`status`, `deadline`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ⚠️ 主键列宽受 InnoDB 索引键 3072 字节上限约束，改动前先算账。
-- utf8mb4 每字符最多 4 字节，故 5 列复合主键的字节数 = (各列长度之和) × 4。
-- 原先 5 列均为 VARCHAR(191)：191×5×4 = 3820 > 3072 → 建表直接 ERROR 1071，
-- 且脚本会在此**中断**，本表及其后的所有表全都建不出来（实测只建出 13 张）。
-- 现按各列真实语义收窄：(64 + 128 + 128 + 64 + 64) × 4 = 448 × 4 = 1792 ≤ 3072（留 42% 余量）。
-- 非主键列（app 等）不受索引长度限制，保持 191。
CREATE TABLE `tenuto_activity_worker_versions` (
  `namespace`         VARCHAR(64)  NOT NULL DEFAULT 'default',
  `activity_type`     VARCHAR(128) NOT NULL,
  `app`               VARCHAR(191) NOT NULL DEFAULT '',
  `identity`          VARCHAR(128) NOT NULL,
  `build_id`          VARCHAR(64)  NOT NULL,
  `task_queue`        VARCHAR(64)  NOT NULL,
  `is_default`        TINYINT(1)   NOT NULL DEFAULT 0,
  `weight`            INT          NOT NULL DEFAULT 0,
  `heartbeat_ttl`     VARCHAR(64)  NOT NULL DEFAULT '',
  `contracts_json`    LONGBLOB,
  `last_heartbeat_at` DATETIME(3)  NOT NULL,
  `expires_at`        DATETIME(3)  NOT NULL,
  `created_at`        DATETIME(3)  NOT NULL,
  `updated_at`        DATETIME(3)  NOT NULL,
  PRIMARY KEY (`namespace`, `activity_type`, `identity`, `build_id`, `task_queue`),
  INDEX `idx_activity_worker_versions_namespace` (`namespace`),
  INDEX `idx_activity_worker_versions_app` (`app`),
  INDEX `idx_activity_worker_versions_identity` (`identity`),
  INDEX `idx_activity_worker_versions_build_id` (`build_id`),
  INDEX `idx_activity_worker_versions_task_queue` (`task_queue`),
  INDEX `idx_activity_worker_versions_expires_at` (`expires_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `tenuto_schedules` (
  `id`              VARCHAR(191) NOT NULL,
  `namespace`       VARCHAR(191) NOT NULL,
  `workflow_type`   VARCHAR(191) NOT NULL,
  -- 必须是 VARCHAR 而非 TEXT：MySQL 不允许 BLOB/TEXT 列带 DEFAULT，
  -- 写成 `TEXT NOT NULL DEFAULT ''` 会报
  -- "Error 1101: BLOB, TEXT, GEOMETRY or JSON column 'description' can't have a default value"，
  -- 使本脚本在此**中断**，这张表及其后的 leader lock 两表全部建不出来。
  `description`     VARCHAR(191) NOT NULL DEFAULT '',
  `cron_expression` VARCHAR(191) NOT NULL,
  `timezone`        VARCHAR(64)  NOT NULL DEFAULT '',
  `input_json`      LONGBLOB,
  `paused`          TINYINT(1)   NOT NULL DEFAULT 0,
  `overlap_policy`  VARCHAR(64)  NOT NULL DEFAULT '',
  `last_run_at`     DATETIME(3),
  `next_run_at`     DATETIME(3),
  `last_run_id`     VARCHAR(191) NOT NULL DEFAULT '',
  `run_count`       INT          NOT NULL DEFAULT 0,
  `created_at`      DATETIME(3)  NOT NULL,
  `updated_at`      DATETIME(3)  NOT NULL,
  PRIMARY KEY (`namespace`, `id`),
  INDEX `idx_workflow_schedules_next_run_at` (`next_run_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- tenuto_batch_operations 是批量操作（取消、终止、Reset）的持久化记录。HTTP 只写入 pending 记录，
-- 后台驱动（leader 副本）按 (created_at, id) 游标分页处理选中的 Run，每页处理完后把计数与
-- cursor_created_at / cursor_run_id 一起保存，崩溃或换 leader 后从游标继续。
-- version 是乐观并发控制：每次保存加一，基于过期读取的写入被拒绝（例如取消与驱动保存进度同时发生）。
CREATE TABLE IF NOT EXISTS `tenuto_batch_operations` (
  `id`                VARCHAR(191)  NOT NULL,
  `namespace`         VARCHAR(191)  NOT NULL,
  `type`              VARCHAR(32)   NOT NULL,
  `filter_json`       LONGBLOB,
  `reset_type`        VARCHAR(64)   NOT NULL DEFAULT '',
  `reason`            VARCHAR(1024) NOT NULL DEFAULT '',
  `status`            VARCHAR(32)   NOT NULL,
  `total_count`       BIGINT        NOT NULL DEFAULT 0,
  `succeeded_count`   BIGINT        NOT NULL DEFAULT 0,
  `failed_count`      BIGINT        NOT NULL DEFAULT 0,
  `skipped_count`     BIGINT        NOT NULL DEFAULT 0,
  `errors_json`       LONGBLOB,
  `cursor_created_at` DATETIME(3),
  `cursor_run_id`     VARCHAR(191)  NOT NULL DEFAULT '',
  `version`           BIGINT        NOT NULL DEFAULT 0,
  `created_at`        DATETIME(3)   NOT NULL,
  `started_at`        DATETIME(3),
  `finished_at`       DATETIME(3),
  `updated_at`        DATETIME(3)   NOT NULL,
  PRIMARY KEY (`namespace`, `id`),
  INDEX `idx_tenuto_batch_operations_status` (`status`, `created_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- tenuto_deletion_tasks 是 retention GC 的投递载体：一行 = 「一棵 history tree 到期后整体删除」。
--
-- 主键是 tree_id 而非 run_id：reset 出的新 run 与老 run 共享同一棵 tree
-- （ForkBranch 用 TreeID: sourceBranch.TreeID），而 DeleteBranch 只按 tree_id 查
-- tenuto_history_tree、完全不看 tenuto_runs。按 run 删除时，GC 老 run 会删掉它的 tree row，
-- 仍活着的新 run 把老 branch 列为 ancestor —— 引用信息永久丢失，等新 run 到期时
-- DeleteBranch 的 allBranches 里已无老 branch，会回溯把仍被引用的历史全删。
-- 这是潜伏型数据丢失：第一次 GC 看似正常，第二次才炸。
--
-- 同树多个 run 陆续终态时，upsert 取 fire_at 的最大值，保证整树最后一个 run 到期后才删。
CREATE TABLE `tenuto_deletion_tasks` (
  `tree_id`    VARCHAR(191) NOT NULL,
  `namespace`  VARCHAR(191) NOT NULL DEFAULT 'default',
  -- fire_at = 终态事件时刻 + retention + rand(0, jitter)。jitter 防批量关闭同秒打爆 DB。
  `fire_at`    DATETIME(3)  NOT NULL,
  `attempts`   INT          NOT NULL DEFAULT 0,
  `last_error` LONGTEXT,
  `created_at` DATETIME(3)  NOT NULL,
  `updated_at` DATETIME(3)  NOT NULL,
  PRIMARY KEY (`tree_id`),
  INDEX `idx_deletion_tasks_fire_at` (`fire_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `tenuto_leader_lock` (
  `lock_name`   VARCHAR(64)     NOT NULL COMMENT '锁名称/组件名',
  `leader_ip`   VARCHAR(64)     NOT NULL COMMENT '当前持有锁的节点IP和端口',
  `expire_time` DATETIME(3)     NOT NULL COMMENT '锁的绝对过期时间',
  `version`     BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '每次易主 +1，Fencing Token',
  PRIMARY KEY (`lock_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `tenuto_leader_lock_members` (
  `lock_name`       VARCHAR(64)  NOT NULL COMMENT '锁名称,与主锁表对应',
  `instance_id`     CHAR(36)     NOT NULL COMMENT '进程实例唯一 ID (UUID),每次启动重新生成',
  `node_ip`         VARCHAR(64)  NOT NULL COMMENT '节点 IP:PORT (展示用)',
  `mac_address`     VARCHAR(32)  NOT NULL DEFAULT '' COMMENT '第一个非回环网卡 MAC',
  `hostname`        VARCHAR(128) NOT NULL DEFAULT '' COMMENT 'os.Hostname()',
  `pid`             INT UNSIGNED NOT NULL DEFAULT 0 COMMENT '进程号',
  `os`              VARCHAR(16)  NOT NULL DEFAULT '' COMMENT 'runtime.GOOS',
  `go_version`      VARCHAR(32)  NOT NULL DEFAULT '' COMMENT 'runtime.Version()',
  `service_version` VARCHAR(64)  NOT NULL DEFAULT '' COMMENT '业务服务版本,用户传入',
  `description`     VARCHAR(256) NOT NULL DEFAULT '' COMMENT '用户传入备注',
  `start_time`      DATETIME(3)  NOT NULL COMMENT '进程启动时间',
  `last_seen`       DATETIME(3)  NOT NULL COMMENT '最近一次心跳时间,leader 据此判活',
  PRIMARY KEY (`lock_name`, `instance_id`),
  KEY `idx_lock_lastseen` (`lock_name`, `last_seen`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
