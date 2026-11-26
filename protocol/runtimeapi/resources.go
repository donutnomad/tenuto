package runtimeapi

import (
	"time"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
)

// 本文件是端点应答与查询入参里的资源形状：Run、History 分页、查询结果、Activity 状态、Worker、
// 配置、Schedule 与批量操作。字段与 JSON 名同服务端读模型一致，服务端在 handler 里显式转换。

// WorkflowConfigRef 标识一个已登记的 Workflow 配置版本。
type WorkflowConfigRef struct {
	Namespace    string `json:"namespace"`
	WorkflowType string `json:"workflow_type"`
	Version      string `json:"version"`
}

// Run 是一次 Workflow 执行。
type Run struct {
	ID           string `json:"id"`
	Namespace    string `json:"namespace"`
	WorkflowID   string `json:"workflow_id"`
	WorkflowType string `json:"workflow_type,omitempty"`
	// WorkflowConfigVersion 是 Run 当前采用的内容版本（Program 版本）。
	WorkflowConfigVersion string `json:"workflow_config_version"`
	// OriginalWorkflowConfigVersion 是 Run 首次执行所选的内容版本。
	OriginalWorkflowConfigVersion string `json:"original_workflow_config_version,omitempty"`
	// Policy 是 Run 的有效升级策略，PolicySource 是它的来源。
	Policy       string `json:"policy,omitempty"`
	PolicySource string `json:"policy_source,omitempty"`
	// TargetWorkerIdentity 是 Run 固定的 Activity Worker 路由。
	TargetWorkerIdentity string `json:"target_worker_identity,omitempty"`
	// UpgradeError 是最近一次自动升级失败的诊断；成功后清空。
	UpgradeError string `json:"upgrade_error,omitempty"`
	// ConfigError 非空表示配置、契约或目标 Worker 冲突，Run 暂停推进等待人工修复。
	ConfigError            string            `json:"config_error,omitempty"`
	ParentRunID            string            `json:"parent_run_id,omitempty"`
	ParentChildID          string            `json:"parent_child_id,omitempty"`
	ParentScheduledEventID int64             `json:"parent_scheduled_event_id,omitempty"`
	FirstRunID             string            `json:"first_run_id"`
	ContinuedRunID         string            `json:"continued_run_id,omitempty"`
	ResetRunID             string            `json:"reset_run_id,omitempty"`
	BranchToken            []byte            `json:"branch_token,omitempty"`
	Status                 history.Status    `json:"status"`
	Input                  map[string]any    `json:"input,omitempty"`
	Output                 map[string]any    `json:"output,omitempty"`
	SearchAttributes       map[string]string `json:"search_attributes,omitempty"`
	CreatedAt              time.Time         `json:"created_at"`
	DeadlineAt             *time.Time        `json:"deadline_at,omitempty"`
	RunDeadlineAt          *time.Time        `json:"run_deadline_at,omitempty"`
	Attempt                int               `json:"attempt,omitempty"`
	Initiator              string            `json:"initiator,omitempty"`
	IsCurrent              bool              `json:"is_current,omitempty"`
}

// BlockedInfo 描述 Run 因 Workflow Task 连续失败而停滞的状态。
type BlockedInfo struct {
	Blocked             bool       `json:"blocked"`
	ConsecutiveFailures int        `json:"consecutive_failures,omitempty"`
	Cause               string     `json:"cause,omitempty"`
	Reason              string     `json:"reason,omitempty"`
	Since               *time.Time `json:"since,omitempty"`
}

// WaitingState 描述 Run 当前在等什么。
type WaitingState struct {
	Type             string `json:"type,omitempty"`
	ActivityID       string `json:"activity_id,omitempty"`
	ChildID          string `json:"child_id,omitempty"`
	ActivityType     string `json:"activity_type,omitempty"`
	TaskQueue        string `json:"task_queue,omitempty"`
	TimerID          string `json:"timer_id,omitempty"`
	ChildRunID       string `json:"child_run_id,omitempty"`
	ScheduledEventID int64  `json:"scheduled_event_id,omitempty"`
}

// RunWithBlocked 是 get-run 的应答。
type RunWithBlocked struct {
	Run     *Run        `json:"run"`
	Blocked BlockedInfo `json:"blocked"`
	// TerminalFailure 取自历史里的 WorkflowFailed 事件。
	TerminalFailure *protocol.Failure `json:"terminal_failure,omitempty"`
}

// RunListFilter 是 Run 列表的查询条件。
type RunListFilter struct {
	Namespace        string            `json:"namespace,omitempty"`
	RunIDs           []string          `json:"run_ids,omitempty"`
	WorkflowID       string            `json:"workflow_id,omitempty"`
	Statuses         []history.Status  `json:"statuses,omitempty"`
	SearchAttributes map[string]string `json:"search_attributes,omitempty"`
	IncludeTerminal  bool              `json:"include_terminal,omitempty"`
	Limit            int               `json:"limit,omitempty"`
	OrderDesc        bool              `json:"order_desc,omitempty"`
	BlockedOnly      bool              `json:"blocked_only,omitempty"`
	// SkipTotal 为 true 时 Server 不计算 Total（应答 0），翻页看 NextPageToken。
	SkipTotal bool `json:"skip_total,omitempty"`
	// PageToken 是上一页应答的 next_page_token，为空时从第一页开始；过滤条件变了再用返回 400。
	PageToken string `json:"page_token,omitempty"`
}

// RunListResult 是一页 Run。Total 是满足条件的 Run 总数（SkipTotal 时为 0），HasMore 表示这一页之后还有 Run，
// 此时 NextPageToken 是下一页的 page_token。
type RunListResult struct {
	Items         []RunListItem `json:"items"`
	Total         int           `json:"total"`
	HasMore       bool          `json:"has_more"`
	NextPageToken string        `json:"next_page_token,omitempty"`
}

type RunListItem struct {
	RunID                 string         `json:"run_id"`
	Namespace             string         `json:"namespace,omitempty"`
	WorkflowID            string         `json:"workflow_id"`
	WorkflowType          string         `json:"workflow_type"`
	Status                history.Status `json:"status"`
	WorkflowConfigVersion string         `json:"workflow_config_version,omitempty"`
	ParentRunID           string         `json:"parent_run_id,omitempty"`
	ResetRunID            string         `json:"reset_run_id,omitempty"`
	IsReset               bool           `json:"is_reset,omitempty"`
	CreatedAt             time.Time      `json:"created_at"`
	// ClosedAt 是 Run 结束的时间；Run 未结束时省略。
	ClosedAt         *time.Time        `json:"closed_at,omitempty"`
	SearchAttributes map[string]string `json:"search_attributes,omitempty"`
	CurrentWaiting   WaitingState      `json:"current_waiting,omitempty"`
	FailureReason    string            `json:"failure_reason,omitempty"`
	HistoryLength    int               `json:"history_length"`
	Blocked          *BlockedInfo      `json:"blocked,omitempty"`
}

type RunStats struct {
	Total          int `json:"total"`
	Running        int `json:"running"`
	Completed      int `json:"completed"`
	Failed         int `json:"failed"`
	Cancelled      int `json:"cancelled"`
	Terminated     int `json:"terminated"`
	ContinuedAsNew int `json:"continued_as_new"`
}

// HistoryPageRequest 分页读取 Run 的 History。Direction 取 "ascending" 或 "descending"，空值为升序。
type HistoryPageRequest struct {
	RunID     string `json:"run_id"`
	PageSize  int    `json:"page_size,omitempty"`
	Direction string `json:"direction,omitempty"`
	PageToken string `json:"page_token,omitempty"`
}

type HistoryPage struct {
	Events             []history.Event `json:"events"`
	NextPageToken      string          `json:"next_page_token,omitempty"`
	SnapshotMaxEventID int64           `json:"snapshot_max_event_id"`
	HistoryEventCount  int64           `json:"history_event_count"`
	HistorySizeBytes   int64           `json:"history_size_bytes"`
}

// WorkflowQueryResult 是 query 的应答：Run 的当前状态与查询处理函数的结果。
type WorkflowQueryResult struct {
	Run              *Run                           `json:"run"`
	QueryName        string                         `json:"query_name,omitempty"`
	HistoryLength    int                            `json:"history_length"`
	SearchAttributes map[string]string              `json:"search_attributes,omitempty"`
	CurrentWaiting   WaitingState                   `json:"current_waiting,omitempty"`
	FailureReason    string                         `json:"failure_reason,omitempty"`
	Signals          []history.WorkflowSignalRecord `json:"signals,omitempty"`
	Updates          []history.WorkflowUpdateRecord `json:"updates,omitempty"`

	QueryResult   map[string]any `json:"query_result,omitempty"`
	QueryRejected bool           `json:"query_rejected,omitempty"`
	QueryError    string         `json:"query_error,omitempty"`
}

// ActivityState 是一个 Activity 的调度、执行与重试状态。
type ActivityState struct {
	TargetWorkerIdentity string `json:"targetWorkerIdentity,omitempty"`
	RunID                string `json:"runId"`
	ScheduledEventID     int64  `json:"scheduledEventId"`
	ActivityID           string `json:"activityId"`
	ActivityType         string `json:"activityType"`
	TaskQueue            string `json:"taskQueue"`

	Attempt        int   `json:"attempt"`
	Stamp          int64 `json:"stamp"`
	StartedEventID int64 `json:"startedEventId"`
	ActivityReset  bool  `json:"activityReset"`
	// CancelRequested 表示 Workflow 已请求取消这个 Activity。
	CancelRequested bool `json:"cancelRequested,omitempty"`

	StartedTime    *time.Time `json:"startedTime,omitempty"`
	WorkerIdentity string     `json:"workerIdentity,omitempty"`
	WorkerBuildID  string     `json:"workerBuildId,omitempty"`
	// StartRequestID 是开始当前 attempt 的 Begin 请求标识。
	StartRequestID   string         `json:"startRequestId,omitempty"`
	LastHeartbeat    map[string]any `json:"lastHeartbeat,omitempty"`
	LastHeartbeatAt  *time.Time     `json:"lastHeartbeatAt,omitempty"`
	RetryLastFailure string         `json:"retryLastFailure,omitempty"`
	// NextAttemptAt 是当前 attempt 最早可派发的时刻；nil 表示调度时即可派发。
	NextAttemptAt *time.Time `json:"nextAttemptAt,omitempty"`

	Status        string    `json:"status"`
	ScheduledTime time.Time `json:"scheduledTime"`

	// 超时以毫秒计，0 表示未设置。
	ScheduleToStartTimeoutMs int64                `json:"scheduleToStartTimeoutMs,omitempty"`
	StartToCloseTimeoutMs    int64                `json:"startToCloseTimeoutMs,omitempty"`
	ScheduleToCloseTimeoutMs int64                `json:"scheduleToCloseTimeoutMs,omitempty"`
	HeartbeatTimeoutMs       int64                `json:"heartbeatTimeoutMs,omitempty"`
	RetryPolicy              *history.RetryPolicy `json:"retryPolicy,omitempty"`

	Input        map[string]any    `json:"input,omitempty"`
	TraceContext map[string]string `json:"traceContext,omitempty"`

	CreatedAt time.Time `json:"createdAt"`
	UpdatedAt time.Time `json:"updatedAt"`
}

type WorkflowDispatchResult struct {
	WorkflowTasks int `json:"workflow_tasks"`
	ActivityTasks int `json:"activity_tasks"`
}

type WorkerListFilter struct {
	Namespace string `json:"namespace,omitempty"`
}

type WorkerListResult struct {
	Items []WorkerListItem `json:"items"`
	Total int              `json:"total"`
}

type WorkerListItem struct {
	Namespace       string                           `json:"namespace,omitempty"`
	App             string                           `json:"app,omitempty"`
	Identity        string                           `json:"identity,omitempty"`
	BuildID         string                           `json:"build_id,omitempty"`
	TaskQueues      []string                         `json:"task_queues,omitempty"`
	ActivityTypes   []string                         `json:"activity_types,omitempty"`
	Default         bool                             `json:"default,omitempty"`
	Weight          int                              `json:"weight,omitempty"`
	LastHeartbeatAt time.Time                        `json:"last_heartbeat_at,omitempty"`
	ExpiresAt       time.Time                        `json:"expires_at,omitempty"`
	StickyCache     *protocol.ExecutionCacheSnapshot `json:"sticky_cache,omitempty"`
}

type WorkflowConfigListResult struct {
	Items []WorkflowConfigSummary `json:"items"`
	Total int                     `json:"total"`
}

type WorkflowConfigSummary struct {
	TargetWorkerIdentity string `json:"target_worker_identity,omitempty"`
	TargetWorkerStatus   string `json:"target_worker_status,omitempty"`
	WorkflowType         string `json:"workflow_type"`
	Namespace            string `json:"namespace"`
	Name                 string `json:"name,omitempty"`
	LatestVersion        string `json:"latest_version"`
	Policy               string `json:"policy,omitempty"`
	WorkflowVersion      string `json:"workflow_version,omitempty"`
	VersionCount         int    `json:"version_count"`
	ActivityCount        int    `json:"activity_count"`
	Active               bool   `json:"active"`
}

type NamespaceListResult struct {
	Items []NamespaceInfo `json:"items"`
	Total int             `json:"total"`
}

type NamespaceInfo struct {
	Name          string `json:"name"`
	RunCount      int    `json:"run_count"`
	WorkflowCount int    `json:"workflow_count"`
	WorkerCount   int    `json:"worker_count"`
	ScheduleCount int    `json:"schedule_count"`
}

type TaskQueueListResult struct {
	Items []TaskQueueInfo `json:"items"`
	Total int             `json:"total"`
}

type TaskQueueInfo struct {
	Name          string   `json:"name"`
	Namespace     string   `json:"namespace"`
	ActiveWorkers int      `json:"active_workers"`
	WorkerIDs     []string `json:"worker_ids,omitempty"`
	ActivityTypes []string `json:"activity_types,omitempty"`
}

// Schedule 按 cron 表达式周期启动 Workflow。
type Schedule struct {
	ID             string         `json:"id"`
	Namespace      string         `json:"namespace"`
	WorkflowType   string         `json:"workflow_type"`
	Description    string         `json:"description,omitempty"`
	CronExpression string         `json:"cron_expression"`
	Timezone       string         `json:"timezone,omitempty"`
	Input          map[string]any `json:"input,omitempty"`
	Paused         bool           `json:"paused"`
	OverlapPolicy  string         `json:"overlap_policy,omitempty"`
	// CatchupWindow 是到期点之后仍会补触发的最长延迟（纳秒），逐个到期点判定。
	CatchupWindow time.Duration `json:"catchup_window"`
	LastRunAt     *time.Time    `json:"last_run_at,omitempty"`
	NextRunAt     *time.Time    `json:"next_run_at,omitempty"`
	LastRunID     string        `json:"last_run_id,omitempty"`
	RunCount      int           `json:"run_count"`
	// MissedCatchupWindow 是因超出 catchup window 没有触发的到期点数。
	MissedCatchupWindow int64 `json:"missed_catchup_window"`
	// BufferDropped 是补触发时 buffer（1000 个）满之后到达而被丢弃的到期点数。
	BufferDropped int64 `json:"buffer_dropped"`
	// StartFailed 是启动失败而丢弃的到期点数：配置错误立即丢弃，暂时性错误重试 1 小时后丢弃。
	StartFailed int64     `json:"start_failed"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

// ScheduleCreateRequest 是 create-schedule 的请求体。
type ScheduleCreateRequest struct {
	Namespace      string         `json:"namespace,omitempty"`
	WorkflowType   string         `json:"workflow_type"`
	Description    string         `json:"description,omitempty"`
	CronExpression string         `json:"cron_expression"`
	Timezone       string         `json:"timezone,omitempty"`
	Input          map[string]any `json:"input,omitempty"`
	OverlapPolicy  string         `json:"overlap_policy,omitempty"`
	Paused         bool           `json:"paused,omitempty"`
	// CatchupWindow（纳秒）为 0 时取 365 天，最小 10 秒。
	CatchupWindow time.Duration `json:"catchup_window,omitempty"`
}

// ScheduleUpdate 只改写非 nil 的字段。
type ScheduleUpdate struct {
	Description    *string         `json:"description,omitempty"`
	CronExpression *string         `json:"cron_expression,omitempty"`
	Timezone       *string         `json:"timezone,omitempty"`
	Input          *map[string]any `json:"input,omitempty"`
	OverlapPolicy  *string         `json:"overlap_policy,omitempty"`
	CatchupWindow  *time.Duration  `json:"catchup_window,omitempty"`
}

type ScheduleListFilter struct {
	Namespace    string `json:"namespace,omitempty"`
	WorkflowType string `json:"workflow_type,omitempty"`
	Paused       *bool  `json:"paused,omitempty"`
	Offset       int    `json:"offset,omitempty"`
	Limit        int    `json:"limit,omitempty"`
}

type ScheduleListResult struct {
	Items []Schedule `json:"items"`
	Total int        `json:"total"`
}

// BatchStartRequest 选出一批 Run 并对它们执行同一个动作。Type 取 "cancel"、"terminate" 或 "reset"。
type BatchStartRequest struct {
	Namespace string        `json:"namespace,omitempty"`
	Type      string        `json:"type"`
	Filter    RunListFilter `json:"filter"`
	Reason    string        `json:"reason,omitempty"`
	ResetType string        `json:"reset_type,omitempty"`
}

// BatchOperation 是一次批量操作的状态与进度。
type BatchOperation struct {
	ID        string        `json:"id"`
	Namespace string        `json:"namespace"`
	Type      string        `json:"type"`
	Filter    RunListFilter `json:"filter"`
	ResetType string        `json:"reset_type,omitempty"`
	Reason    string        `json:"reason,omitempty"`
	Status    string        `json:"status"`
	// TotalCount 在处理第一页时取当时匹配的 Run 数；结束时改为实际处理的 Run 数。
	TotalCount     int             `json:"total_count"`
	SucceededCount int             `json:"succeeded_count"`
	FailedCount    int             `json:"failed_count"`
	SkippedCount   int             `json:"skipped_count"`
	Errors         []BatchRunError `json:"errors,omitempty"`
	CreatedAt      time.Time       `json:"created_at"`
	StartedAt      *time.Time      `json:"started_at,omitempty"`
	FinishedAt     *time.Time      `json:"finished_at,omitempty"`
	UpdatedAt      time.Time       `json:"updated_at"`
}

type BatchRunError struct {
	RunID   string `json:"run_id"`
	Message string `json:"message"`
}

type BatchListResult struct {
	Items []BatchOperation `json:"items"`
	Total int              `json:"total"`
}
