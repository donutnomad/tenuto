// Package history 是 Server 与 Worker 共用的已发布语言：事件、命令、Run 状态与历史度量的纯函数投影。
package history

import (
	"time"

	"github.com/donutnomad/tenuto/protocol"
)

type ContinueAsNewReason = protocol.ContinueAsNewReason
type Payloads = protocol.Payloads
type UpdateOutcome = protocol.UpdateOutcome

// EventType 标识事件类型，用于序列化存储和日志。
type EventType string

const (
	EventWorkflowStarted               EventType = "WorkflowStarted"               // Run 启动
	EventWorkflowCompleted             EventType = "WorkflowCompleted"             // 编排正常完成（终态）
	EventWorkflowFailed                EventType = "WorkflowFailed"                // 编排失败（终态）
	EventWorkflowCancelled             EventType = "WorkflowCancelled"             // 编排以取消错误结束（终态）
	EventWorkflowTerminated            EventType = "WorkflowTerminated"            // 外部强制终止（终态）
	EventWorkflowContinuedAsNew        EventType = "WorkflowContinuedAsNew"        // 截断历史，开下一代 Run（终态）
	EventWorkflowCancellationRequested EventType = "WorkflowCancellationRequested" // 协作式取消请求

	EventWorkflowTaskScheduled EventType = "WorkflowTaskScheduled" // 投递一轮决策任务（WFT）
	EventWorkflowTaskStarted   EventType = "WorkflowTaskStarted"   // Worker 领取 WFT
	EventWorkflowTaskCompleted EventType = "WorkflowTaskCompleted" // WFT 决策完成，提交一批 Command
	EventWorkflowTaskTimedOut  EventType = "WorkflowTaskTimedOut"  // WFT 超时未提交
	EventWorkflowTaskFailed    EventType = "WorkflowTaskFailed"    // WFT 失败（Reset 产生）

	EventActivityScheduled       EventType = "ActivityScheduled"       // 编排调度一个 Activity
	EventActivityStarted         EventType = "ActivityStarted"         // Activity Worker 开始执行
	EventActivityAttemptFailed   EventType = "ActivityAttemptFailed"   // 本次 attempt 失败，可能重试
	EventActivityTimedOut        EventType = "ActivityTimedOut"        // 本次 attempt 超时
	EventActivityCompleted       EventType = "ActivityCompleted"       // Activity 成功（终态）
	EventActivityFailed          EventType = "ActivityFailed"          // 重试耗尽，最终失败（终态）
	EventActivityReset           EventType = "ActivityReset"           // 手动重置 activity attempt 回 1
	EventActivityCancelRequested EventType = "ActivityCancelRequested" // 编排请求取消 Activity
	EventActivityCancelled       EventType = "ActivityCancelled"       // Activity 按取消结束（终态）

	EventTimerScheduled EventType = "TimerScheduled" // 启动 durable 定时器
	EventTimerFired     EventType = "TimerFired"     // 定时器到期
	EventTimerCancelled EventType = "TimerCancelled" // 编排取消定时器

	EventChildWorkflowStarted         EventType = "ChildWorkflowStarted"         // 启动子 Workflow
	EventChildWorkflowCompleted       EventType = "ChildWorkflowCompleted"       // 子 Workflow 成功
	EventChildWorkflowFailed          EventType = "ChildWorkflowFailed"          // 子 Workflow 失败
	EventChildWorkflowCancelled       EventType = "ChildWorkflowCancelled"       // 子 Workflow 被取消
	EventChildWorkflowTerminated      EventType = "ChildWorkflowTerminated"      // 子 Workflow 被终止
	EventChildWorkflowCancelRequested EventType = "ChildWorkflowCancelRequested" // 编排请求取消子 Workflow

	EventWorkflowSignaled        EventType = "WorkflowSignaled"        // 外部 Signal 注入数据
	EventWorkflowUpdateAccepted  EventType = "WorkflowUpdateAccepted"  // 外部 Update 请求被接受
	EventWorkflowUpdateCompleted EventType = "WorkflowUpdateCompleted" // Update 处理完成

	EventMarkerRecorded           EventType = "MarkerRecorded"           // 固化非确定性值（SideEffect/GetVersion）
	EventSearchAttributesUpserted EventType = "SearchAttributesUpserted" // 更新业务索引字段
)

// Event 是事件历史的单条记录。
//
// 有且仅有一个事件指针字段非 nil，由它自描述事件类型。
// Type 字段由 EventType() 从非 nil 指针推导，序列化层使用。
// ID 和 Timestamp 是公共字段，所有事件共享。构造函数不填 Timestamp：
// 事件写入 History 时由追加它的 Server 按自己的时钟打上时间。
type Event struct {
	ID        int64     `json:"id"`
	Timestamp time.Time `json:"timestamp"`

	WorkflowStarted        *WorkflowStartedEvent        `json:"workflowStarted,omitempty"`
	WorkflowCompleted      *WorkflowCompletedEvent      `json:"workflowCompleted,omitempty"`
	WorkflowFailed         *WorkflowFailedEvent         `json:"workflowFailed,omitempty"`
	WorkflowCancelled      *WorkflowCancelledEvent      `json:"workflowCancelled,omitempty"`
	WorkflowTerminated     *WorkflowTerminatedEvent     `json:"workflowTerminated,omitempty"`
	WorkflowContinuedAsNew *WorkflowContinuedAsNewEvent `json:"workflowContinuedAsNew,omitempty"`

	WorkflowTaskScheduled *WorkflowTaskScheduledEvent `json:"workflowTaskScheduled,omitempty"`
	WorkflowTaskStarted   *WorkflowTaskStartedEvent   `json:"workflowTaskStarted,omitempty"`
	WorkflowTaskCompleted *WorkflowTaskCompletedEvent `json:"workflowTaskCompleted,omitempty"`
	WorkflowTaskTimedOut  *WorkflowTaskTimedOutEvent  `json:"workflowTaskTimedOut,omitempty"`
	WorkflowTaskFailed    *WorkflowTaskFailedEvent    `json:"workflowTaskFailed,omitempty"`

	ActivityScheduled       *ActivityScheduledEvent       `json:"activityScheduled,omitempty"`
	ActivityStarted         *ActivityStartedEvent         `json:"activityStarted,omitempty"`
	ActivityAttemptFailed   *ActivityAttemptFailedEvent   `json:"activityAttemptFailed,omitempty"`
	ActivityTimedOut        *ActivityTimedOutEvent        `json:"activityTimedOut,omitempty"`
	ActivityCompleted       *ActivityCompletedEvent       `json:"activityCompleted,omitempty"`
	ActivityFailed          *ActivityFailedEvent          `json:"activityFailed,omitempty"`
	ActivityReset           *ActivityResetEvent           `json:"activityReset,omitempty"`
	ActivityCancelRequested *ActivityCancelRequestedEvent `json:"activityCancelRequested,omitempty"`
	ActivityCancelled       *ActivityCancelledEvent       `json:"activityCancelled,omitempty"`

	TimerScheduled *TimerScheduledEvent `json:"timerScheduled,omitempty"`
	TimerFired     *TimerFiredEvent     `json:"timerFired,omitempty"`
	TimerCancelled *TimerCancelledEvent `json:"timerCancelled,omitempty"`

	ChildWorkflowStarted         *ChildWorkflowStartedEvent         `json:"childWorkflowStarted,omitempty"`
	ChildWorkflowCompleted       *ChildWorkflowCompletedEvent       `json:"childWorkflowCompleted,omitempty"`
	ChildWorkflowFailed          *ChildWorkflowFailedEvent          `json:"childWorkflowFailed,omitempty"`
	ChildWorkflowCancelled       *ChildWorkflowCancelledEvent       `json:"childWorkflowCancelled,omitempty"`
	ChildWorkflowTerminated      *ChildWorkflowTerminatedEvent      `json:"childWorkflowTerminated,omitempty"`
	ChildWorkflowCancelRequested *ChildWorkflowCancelRequestedEvent `json:"childWorkflowCancelRequested,omitempty"`

	WorkflowSignaled        *WorkflowSignaledEvent        `json:"workflowSignaled,omitempty"`
	WorkflowUpdateAccepted  *WorkflowUpdateAcceptedEvent  `json:"workflowUpdateAccepted,omitempty"`
	WorkflowUpdateCompleted *WorkflowUpdateCompletedEvent `json:"workflowUpdateCompleted,omitempty"`

	WorkflowCancellationRequested *WorkflowCancellationRequestedEvent `json:"workflowCancellationRequested,omitempty"`
	MarkerRecorded                *MarkerRecordedEvent                `json:"markerRecorded,omitempty"`
	SearchAttributesUpserted      *SearchAttributesUpsertedEvent      `json:"searchAttributesUpserted,omitempty"`
}

// EventType 从非 nil 指针推导事件类型。
func (e *Event) EventType() EventType {
	switch {
	case e.WorkflowStarted != nil:
		return EventWorkflowStarted
	case e.WorkflowCompleted != nil:
		return EventWorkflowCompleted
	case e.WorkflowFailed != nil:
		return EventWorkflowFailed
	case e.WorkflowCancelled != nil:
		return EventWorkflowCancelled
	case e.WorkflowTerminated != nil:
		return EventWorkflowTerminated
	case e.WorkflowContinuedAsNew != nil:
		return EventWorkflowContinuedAsNew
	case e.WorkflowTaskScheduled != nil:
		return EventWorkflowTaskScheduled
	case e.WorkflowTaskStarted != nil:
		return EventWorkflowTaskStarted
	case e.WorkflowTaskCompleted != nil:
		return EventWorkflowTaskCompleted
	case e.WorkflowTaskTimedOut != nil:
		return EventWorkflowTaskTimedOut
	case e.WorkflowTaskFailed != nil:
		return EventWorkflowTaskFailed
	case e.ActivityScheduled != nil:
		return EventActivityScheduled
	case e.ActivityStarted != nil:
		return EventActivityStarted
	case e.ActivityAttemptFailed != nil:
		return EventActivityAttemptFailed
	case e.ActivityTimedOut != nil:
		return EventActivityTimedOut
	case e.ActivityCompleted != nil:
		return EventActivityCompleted
	case e.ActivityFailed != nil:
		return EventActivityFailed
	case e.ActivityReset != nil:
		return EventActivityReset
	case e.ActivityCancelRequested != nil:
		return EventActivityCancelRequested
	case e.ActivityCancelled != nil:
		return EventActivityCancelled
	case e.TimerScheduled != nil:
		return EventTimerScheduled
	case e.TimerFired != nil:
		return EventTimerFired
	case e.TimerCancelled != nil:
		return EventTimerCancelled
	case e.ChildWorkflowStarted != nil:
		return EventChildWorkflowStarted
	case e.ChildWorkflowCompleted != nil:
		return EventChildWorkflowCompleted
	case e.ChildWorkflowFailed != nil:
		return EventChildWorkflowFailed
	case e.ChildWorkflowCancelled != nil:
		return EventChildWorkflowCancelled
	case e.ChildWorkflowTerminated != nil:
		return EventChildWorkflowTerminated
	case e.ChildWorkflowCancelRequested != nil:
		return EventChildWorkflowCancelRequested
	case e.WorkflowSignaled != nil:
		return EventWorkflowSignaled
	case e.WorkflowUpdateAccepted != nil:
		return EventWorkflowUpdateAccepted
	case e.WorkflowUpdateCompleted != nil:
		return EventWorkflowUpdateCompleted
	case e.WorkflowCancellationRequested != nil:
		return EventWorkflowCancellationRequested
	case e.MarkerRecorded != nil:
		return EventMarkerRecorded
	case e.SearchAttributesUpserted != nil:
		return EventSearchAttributesUpserted
	default:
		return ""
	}
}

// ── Workflow 生命周期 ──

type WorkflowStartedEvent struct {
	WorkflowID         string         `json:"workflowId,omitempty"`
	Input              map[string]any `json:"input,omitempty"`
	FirstRunID         string         `json:"firstRunId,omitempty"`
	ContinuedRunID     string         `json:"continuedRunId,omitempty"`
	Name               string         `json:"name,omitempty"`
	ExecutionTimeoutMs int64          `json:"executionTimeoutMs,omitempty"`
	Attempt            int            `json:"attempt,omitempty"`   // 从 1 起，初始 run=1
	Initiator          string         `json:"initiator,omitempty"` // ""/Retry/Cron/ContinueAsNew
	// SearchAttributes 是启动时的搜索属性，重试与 Reset 以它为底（照 Temporal startAttr.SearchAttributes）。
	// 空 map 表示启动时没有搜索属性，原样写入 History，故不加 omitempty。
	SearchAttributes map[string]string `json:"searchAttributes"`
	// RequestID 是启动请求的幂等键（SDK 每次 ExecuteWorkflow 生成一个，重试时不变）：同一 Workflow ID 的 current Run
	// 由同一个 RequestID 启动时，重发的启动请求得到这个 Run。
	RequestID string `json:"requestId,omitempty"`
}

type WorkflowCompletedEvent struct {
	Output map[string]any `json:"output,omitempty"`
}

type WorkflowFailedEvent struct {
	// Error 与 Failure.Message 相同，供按字符串读取失败原因的查询使用。
	Error string `json:"error,omitempty"`
	// Failure 是 Workflow body 返回应用错误时的结构化载荷；普通 error 只有 message。
	Failure           *protocol.Failure `json:"failure,omitempty"`
	TimeoutType       string            `json:"timeoutType,omitempty"`
	NewExecutionRunID string            `json:"newExecutionRunId,omitempty"` // 将在下一代 run 中重试时设置
	RetryState        string            `json:"retryState,omitempty"`        // InProgress/MaximumAttemptsReached/NonRetryableFailure/Timeout
}

// WorkflowCancelledEvent 是编排返回取消错误时的终态；Details 是该取消错误的 details。
type WorkflowCancelledEvent struct {
	Details Payloads `json:"details,omitempty"`
}

type WorkflowTerminatedEvent struct {
	Reason string `json:"reason,omitempty"`
}

type WorkflowContinuedAsNewEvent struct {
	Name                  string         `json:"name,omitempty"`
	NextRunID             string         `json:"nextRunId,omitempty"`
	WorkflowConfigVersion string         `json:"workflowConfigVersion,omitempty"`
	Input                 map[string]any `json:"input,omitempty"`
}

type WorkflowCancellationRequestedEvent struct {
	Cause string `json:"cause,omitempty"`
}

// ── WorkflowTask ──

type WorkflowTaskScheduledEvent struct {
	TimeoutMs    int64             `json:"timeoutMs,omitempty"`
	Attempt      int               `json:"attempt,omitempty"`
	TraceContext map[string]string `json:"traceContext,omitempty"`
}

type WorkflowTaskStartedEvent struct {
	ScheduledEventID int64  `json:"scheduledEventId,omitempty"`
	WorkerIdentity   string `json:"workerIdentity,omitempty"`
	WorkerBuildID    string `json:"workerBuildId,omitempty"`
	// ProgramVersion 记录本次 WFT 实际采用的 Workflow 内容版本；AutoUpgrade 提交成功后
	// 由它把 current 提升到该版本，失败时作为“本次候选版本”在 history 中可查。
	ProgramVersion string `json:"programVersion,omitempty"`
	// TimeoutMs 是本次领取的 start-to-close 超时：Server 从事件时间起计时，到期仍未关闭的 WFT 记为超时。
	TimeoutMs            int64                 `json:"timeoutMs,omitempty"`
	HistoryEventCount    int64                 `json:"historyEventCount,omitempty"`
	HistorySizeBytes     int64                 `json:"historySizeBytes,omitempty"`
	ContinueAsNewReasons []ContinueAsNewReason `json:"continueAsNewReasons,omitempty"`
}

type WorkflowTaskCompletedEvent struct {
	ScheduledEventID int64  `json:"scheduledEventId,omitempty"`
	WorkerIdentity   string `json:"workerIdentity,omitempty"`
}

type WorkflowTaskTimedOutEvent struct {
	ScheduledEventID int64  `json:"scheduledEventId,omitempty"`
	Error            string `json:"error,omitempty"`
}

type WorkflowTaskFailedEvent struct {
	ScheduledEventID int64             `json:"scheduledEventId,omitempty"`
	StartedEventID   int64             `json:"startedEventId,omitempty"`
	Cause            string            `json:"cause,omitempty"`
	BaseRunID        string            `json:"baseRunId,omitempty"`
	ResetRunID       string            `json:"resetRunId,omitempty"`
	Reason           string            `json:"reason,omitempty"`
	TraceContext     map[string]string `json:"traceContext,omitempty"`
}

// ── Activity ──

type ActivityScheduledEvent struct {
	ActivityID   string            `json:"activityId,omitempty"`
	ActivityType string            `json:"activityType,omitempty"`
	TaskQueue    string            `json:"taskQueue,omitempty"`
	Input        map[string]any    `json:"input,omitempty"`
	TraceContext map[string]string `json:"traceContext,omitempty"`

	// 超时与重试策略（毫秒，0 表示未设置）
	ScheduleToStartTimeoutMs int64        `json:"scheduleToStartTimeoutMs,omitempty"`
	StartToCloseTimeoutMs    int64        `json:"startToCloseTimeoutMs,omitempty"`
	ScheduleToCloseTimeoutMs int64        `json:"scheduleToCloseTimeoutMs,omitempty"`
	HeartbeatTimeoutMs       int64        `json:"heartbeatTimeoutMs,omitempty"`
	RetryPolicy              *RetryPolicy `json:"retryPolicy,omitempty"`
}

type RetryPolicy struct {
	MaxAttempts        int      `json:"maxAttempts,omitempty"`
	InitialIntervalMs  int64    `json:"initialIntervalMs,omitempty"`
	BackoffCoefficient float64  `json:"backoffCoefficient"`
	MaxIntervalMs      int64    `json:"maxIntervalMs,omitempty"`
	NonRetryableErrors []string `json:"nonRetryableErrors,omitempty"`
	// NonRetryableErrorTypes 按 Failure.Type 完全相等匹配（大小写敏感）。
	NonRetryableErrorTypes []string `json:"nonRetryableErrorTypes,omitempty"`
}

type ActivityStartedEvent struct {
	ActivityID       string `json:"activityId,omitempty"`
	ScheduledEventID int64  `json:"scheduledEventId,omitempty"`
	Attempt          int    `json:"attempt,omitempty"`
	Stamp            int64  `json:"stamp,omitempty"` // generation token（dedupe 防 reset 撞键）
	ActivityType     string `json:"activityType,omitempty"`
	TaskQueue        string `json:"taskQueue,omitempty"`

	WorkerIdentity string `json:"workerIdentity,omitempty"`
	WorkerBuildID  string `json:"workerBuildId,omitempty"`
	LastError      string `json:"lastError,omitempty"`
}

type ActivityCompletedEvent struct {
	ActivityID       string `json:"activityId,omitempty"`
	ScheduledEventID int64  `json:"scheduledEventId,omitempty"`
	StartedEventID   int64  `json:"startedEventId,omitempty"`
	Attempt          int    `json:"attempt,omitempty"`
	ActivityType     string `json:"activityType,omitempty"`
	TaskQueue        string `json:"taskQueue,omitempty"`
	Identity         string `json:"identity,omitempty"`

	Output map[string]any `json:"output,omitempty"`
}

type ActivityFailedEvent struct {
	ActivityID       string `json:"activityId,omitempty"`
	ScheduledEventID int64  `json:"scheduledEventId,omitempty"`
	StartedEventID   int64  `json:"startedEventId,omitempty"`
	Attempt          int    `json:"attempt,omitempty"`
	ActivityType     string `json:"activityType,omitempty"`
	TaskQueue        string `json:"taskQueue,omitempty"`
	Identity         string `json:"identity,omitempty"`

	Error      string `json:"error,omitempty"`
	RetryState string `json:"retryState,omitempty"`
	// Failure 是终态结构化失败（message/type/nonRetryable/details/cause）。
	Failure *protocol.Failure `json:"failure,omitempty"`
	// LastHeartbeatDetails 是 Workflow Reset 打断已开始的 Activity 时，旧 Run 里这个 Activity 最后一次心跳的详情
	// （Temporal ResetWorkflowFailureInfo.last_heartbeat_details）。
	LastHeartbeatDetails map[string]any `json:"lastHeartbeatDetails,omitempty"`
}

type ActivityAttemptFailedEvent struct {
	ActivityID       string `json:"activityId,omitempty"`
	ScheduledEventID int64  `json:"scheduledEventId,omitempty"`
	Attempt          int    `json:"attempt,omitempty"`
	Stamp            int64  `json:"stamp,omitempty"`
	Error            string `json:"error,omitempty"`
	// Failure 保留中间 attempt 的结构化失败，重试耗尽与冷恢复都从这里读。
	Failure *protocol.Failure `json:"failure,omitempty"`
}

type ActivityResetEvent struct {
	ActivityID       string `json:"activityId,omitempty"`
	ScheduledEventID int64  `json:"scheduledEventId,omitempty"`
	Attempt          int    `json:"attempt,omitempty"` // 重置前的 attempt（审计用）
	Identity         string `json:"identity,omitempty"`
	Reason           string `json:"reason,omitempty"`
	ResetHeartbeat   bool   `json:"resetHeartbeat,omitempty"`
}

type ActivityTimedOutEvent struct {
	ActivityID       string `json:"activityId,omitempty"`
	ScheduledEventID int64  `json:"scheduledEventId,omitempty"`
	StartedEventID   int64  `json:"startedEventId,omitempty"`
	Attempt          int    `json:"attempt,omitempty"`
	Stamp            int64  `json:"stamp,omitempty"`
	ActivityType     string `json:"activityType,omitempty"`
	TaskQueue        string `json:"taskQueue,omitempty"`
	// TimeoutType 是超时种类；给人读的说明在 Failure.Message。
	TimeoutType protocol.TimeoutType `json:"timeoutType,omitempty"`
	RetryState  string               `json:"retryState,omitempty"`
	Failure     *protocol.Failure    `json:"failure,omitempty"`
	// LastHeartbeatDetails 是超时时 Activity 状态里最后一次心跳的详情（Temporal
	// TimeoutFailureInfo.last_heartbeat_details）。心跳不写 History，Activity 结束后从这里看到它。
	LastHeartbeatDetails map[string]any `json:"lastHeartbeatDetails,omitempty"`
}

// ActivityCancelRequestedEvent 记录编排请求取消一个 Activity。已开始的 attempt 经心跳得知取消；
// 尚未开始的 Activity 在同一事务里以 ActivityCancelled 结束。
type ActivityCancelRequestedEvent struct {
	ActivityID       string `json:"activityId,omitempty"`
	ScheduledEventID int64  `json:"scheduledEventId,omitempty"`
}

// ActivityCancelledEvent 是 Activity 按取消请求结束的终态：Worker 确认取消，或取消时 Activity 尚未开始。
type ActivityCancelledEvent struct {
	ActivityID             string `json:"activityId,omitempty"`
	ScheduledEventID       int64  `json:"scheduledEventId,omitempty"`
	StartedEventID         int64  `json:"startedEventId,omitempty"`
	CancelRequestedEventID int64  `json:"cancelRequestedEventId,omitempty"`
	Attempt                int    `json:"attempt,omitempty"`
	ActivityType           string `json:"activityType,omitempty"`
	TaskQueue              string `json:"taskQueue,omitempty"`
	Identity               string `json:"identity,omitempty"`
	// Details 是 Activity 确认取消时返回的取消错误 details。
	Details Payloads `json:"details,omitempty"`
}

// ── Timer ──

type TimerScheduledEvent struct {
	TimerID      string            `json:"timerId,omitempty"`
	DelayInput   map[string]any    `json:"delayInput,omitempty"`
	DelayMs      int64             `json:"delayMs,omitempty"`
	TraceContext map[string]string `json:"traceContext,omitempty"`
}

type TimerFiredEvent struct {
	TimerID          string `json:"timerId,omitempty"`
	ScheduledEventID int64  `json:"scheduledEventId,omitempty"`
}

// TimerCancelledEvent 记录编排取消一个尚未触发的 timer。
type TimerCancelledEvent struct {
	TimerID          string `json:"timerId,omitempty"`
	ScheduledEventID int64  `json:"scheduledEventId,omitempty"`
}

// ── Child Workflow ──

type ChildWorkflowStartedEvent struct {
	Name                  string         `json:"name,omitempty"`
	ChildID               string         `json:"childId,omitempty"`
	ChildRunID            string         `json:"childRunId,omitempty"`
	WorkflowConfigVersion string         `json:"workflowConfigVersion,omitempty"`
	Input                 map[string]any `json:"input,omitempty"`
	// ParentClosePolicy 是父 Run 关闭时对这个子 Workflow 的处理，记录归一化后的值（空值不出现）。
	ParentClosePolicy protocol.ParentClosePolicy `json:"parentClosePolicy,omitempty"`
}

type ChildWorkflowCompletedEvent struct {
	ChildID          string         `json:"childId,omitempty"`
	ChildRunID       string         `json:"childRunId,omitempty"`
	ScheduledEventID int64          `json:"scheduledEventId,omitempty"`
	Output           map[string]any `json:"output,omitempty"`
}

type ChildWorkflowFailedEvent struct {
	ChildID          string `json:"childId,omitempty"`
	ChildRunID       string `json:"childRunId,omitempty"`
	ScheduledEventID int64  `json:"scheduledEventId,omitempty"`
	// Error 与 Failure.Message 相同，供按字符串读取失败原因的查询使用。
	Error string `json:"error,omitempty"`
	// Failure 是子工作流终态的结构化失败，与 ActivityFailed/WorkflowFailed 同形。
	Failure *protocol.Failure `json:"failure,omitempty"`
}

type ChildWorkflowCancelledEvent struct {
	ChildID          string `json:"childId,omitempty"`
	ChildRunID       string `json:"childRunId,omitempty"`
	ScheduledEventID int64  `json:"scheduledEventId,omitempty"`
}

type ChildWorkflowTerminatedEvent struct {
	ChildID          string `json:"childId,omitempty"`
	ChildRunID       string `json:"childRunId,omitempty"`
	ScheduledEventID int64  `json:"scheduledEventId,omitempty"`
	Reason           string `json:"reason,omitempty"`
}

// ChildWorkflowCancelRequestedEvent 记录编排请求取消一个子 Workflow；子 Run 随后收到取消请求，
// 由子 Workflow 的代码决定它的终态。
type ChildWorkflowCancelRequestedEvent struct {
	ChildID          string `json:"childId,omitempty"`
	ChildRunID       string `json:"childRunId,omitempty"`
	ScheduledEventID int64  `json:"scheduledEventId,omitempty"`
}

// ── Signal / Update ──

type WorkflowSignaledEvent struct {
	Name  string         `json:"name,omitempty"`
	Input map[string]any `json:"input,omitempty"`
	// RequestID 是 Signal 请求的幂等键（SDK 每次调用生成一个，重试时不变）：同一个 Run 上同一个 RequestID 只记一条。
	RequestID string `json:"requestId,omitempty"`
}

type WorkflowUpdateAcceptedEvent struct {
	Name     string    `json:"name,omitempty"`
	UpdateID string    `json:"updateId,omitempty"`
	Args     *Payloads `json:"args,omitempty"`
}

// WorkflowUpdateCompletedEvent 只有一个结果来源：Outcome（成功=Success，失败=Failure）。
type WorkflowUpdateCompletedEvent struct {
	Name     string         `json:"name,omitempty"`
	UpdateID string         `json:"updateId,omitempty"`
	Outcome  *UpdateOutcome `json:"outcome,omitempty"`
}

// ── Marker / SearchAttributes ──

type MarkerRecordedEvent struct {
	MarkerName string         `json:"markerName,omitempty"`
	MarkerID   string         `json:"markerId,omitempty"`
	Value      map[string]any `json:"value,omitempty"`
}

type SearchAttributesUpsertedEvent struct {
	Attributes map[string]any `json:"attributes,omitempty"`
}

// ── 构造函数 ──

func NewWorkflowStartedEvent(input map[string]any, firstRunID, continuedRunID, workflowID string, name string, executionTimeoutMs int64) Event {
	return Event{WorkflowStarted: &WorkflowStartedEvent{WorkflowID: workflowID, Input: input, FirstRunID: firstRunID, ContinuedRunID: continuedRunID, Name: name, ExecutionTimeoutMs: executionTimeoutMs}}
}

func NewWorkflowCompletedEvent(output map[string]any) Event {
	return Event{WorkflowCompleted: &WorkflowCompletedEvent{Output: output}}
}

// NewWorkflowFailedEvent 是 message-only Failure 的便捷构造（等同 Encode(errors.New(errMsg))）：
// 没有 details，标准化不可能失败，因此保持单返回值签名。带结构化载荷用 NewWorkflowFailedFailureEvent。
func NewWorkflowFailedEvent(errMsg, timeoutType string) Event {
	event, _ := NewWorkflowFailedFailureEvent(protocol.Failure{Message: errMsg}, timeoutType)
	return event
}

// NewWorkflowFailedFailureEvent 用结构化 Failure 创建终态失败事件（Error 镜像 Failure.Message）。
// 载荷在这一处统一校验与 canonical 化：retry 路径与直接失败路径共用同一个入口，
// 超限时在写状态/事件之前返回错误。
func NewWorkflowFailedFailureEvent(failure protocol.Failure, timeoutType string) (Event, error) {
	normalized, err := failure.Normalize()
	if err != nil {
		return Event{}, protocol.InvalidRequestf("normalize workflow failure payload: %w", err)
	}
	return Event{WorkflowFailed: &WorkflowFailedEvent{
		Error: normalized.Message, Failure: &normalized, TimeoutType: timeoutType,
	}}, nil
}

func NewWorkflowCancelledEvent(details Payloads) Event {
	return Event{WorkflowCancelled: &WorkflowCancelledEvent{Details: details}}
}

func NewWorkflowTerminatedEvent(reason string) Event {
	return Event{WorkflowTerminated: &WorkflowTerminatedEvent{Reason: reason}}
}

func NewWorkflowContinuedAsNewEvent(name, nextRunID, configVersion string, input map[string]any) Event {
	return Event{WorkflowContinuedAsNew: &WorkflowContinuedAsNewEvent{Name: name, NextRunID: nextRunID, WorkflowConfigVersion: configVersion, Input: input}}
}

func NewWorkflowCancellationRequestedEvent(cause string) Event {
	return Event{WorkflowCancellationRequested: &WorkflowCancellationRequestedEvent{Cause: cause}}
}

func NewWorkflowTaskScheduledEvent(timeoutMs int64) Event {
	return Event{WorkflowTaskScheduled: &WorkflowTaskScheduledEvent{TimeoutMs: timeoutMs, Attempt: 1}}
}

func NewWorkflowTaskStartedEvent(scheduledEventID int64, workerIdentity, workerBuildID string, historySizeBytes int64) Event {
	return Event{WorkflowTaskStarted: &WorkflowTaskStartedEvent{ScheduledEventID: scheduledEventID, WorkerIdentity: workerIdentity, WorkerBuildID: workerBuildID, HistorySizeBytes: historySizeBytes}}
}

func NewWorkflowTaskCompletedEvent(scheduledEventID int64, workerIdentity string) Event {
	return Event{WorkflowTaskCompleted: &WorkflowTaskCompletedEvent{ScheduledEventID: scheduledEventID, WorkerIdentity: workerIdentity}}
}

func NewWorkflowTaskTimedOutEvent(scheduledEventID int64, errMsg string) Event {
	return Event{WorkflowTaskTimedOut: &WorkflowTaskTimedOutEvent{ScheduledEventID: scheduledEventID, Error: errMsg}}
}

func NewWorkflowTaskFailedEvent(e WorkflowTaskFailedEvent) Event {
	return Event{WorkflowTaskFailed: &e}
}

func NewActivityScheduledEvent(e ActivityScheduledEvent) Event {
	return Event{ActivityScheduled: &e}
}

func NewActivityStartedEvent(e ActivityStartedEvent) Event {
	return Event{ActivityStarted: &e}
}

func NewActivityCompletedEvent(e ActivityCompletedEvent) Event {
	return Event{ActivityCompleted: &e}
}

func NewActivityFailedEvent(e ActivityFailedEvent) Event {
	return Event{ActivityFailed: &e}
}

func NewActivityAttemptFailedEvent(e ActivityAttemptFailedEvent) Event {
	return Event{ActivityAttemptFailed: &e}
}

func NewActivityTimedOutEvent(e ActivityTimedOutEvent) Event {
	return Event{ActivityTimedOut: &e}
}

func NewActivityResetEvent(e ActivityResetEvent) Event {
	return Event{ActivityReset: &e}
}

func NewActivityCancelRequestedEvent(activityID string, scheduledEventID int64) Event {
	return Event{ActivityCancelRequested: &ActivityCancelRequestedEvent{ActivityID: activityID, ScheduledEventID: scheduledEventID}}
}

func NewActivityCancelledEvent(e ActivityCancelledEvent) Event {
	return Event{ActivityCancelled: &e}
}

func NewTimerCancelledEvent(timerID string, scheduledEventID int64) Event {
	return Event{TimerCancelled: &TimerCancelledEvent{TimerID: timerID, ScheduledEventID: scheduledEventID}}
}

func NewChildWorkflowCancelRequestedEvent(childID, childRunID string, scheduledEventID int64) Event {
	return Event{ChildWorkflowCancelRequested: &ChildWorkflowCancelRequestedEvent{ChildID: childID, ChildRunID: childRunID, ScheduledEventID: scheduledEventID}}
}

func NewTimerScheduledEvent(timerID string, delayInput map[string]any, delayMs int64) Event {
	return Event{TimerScheduled: &TimerScheduledEvent{TimerID: timerID, DelayInput: delayInput, DelayMs: delayMs}}
}

func NewTimerFiredEvent(timerID string, scheduledEventID int64) Event {
	return Event{TimerFired: &TimerFiredEvent{TimerID: timerID, ScheduledEventID: scheduledEventID}}
}

func NewChildWorkflowStartedEvent(name, childID, childRunID, configVersion string, input map[string]any) Event {
	return Event{ChildWorkflowStarted: &ChildWorkflowStartedEvent{Name: name, ChildID: childID, ChildRunID: childRunID, WorkflowConfigVersion: configVersion, Input: input}}
}

func NewChildWorkflowCompletedEvent(childID, childRunID string, scheduledEventID int64, output map[string]any) Event {
	return Event{ChildWorkflowCompleted: &ChildWorkflowCompletedEvent{ChildID: childID, ChildRunID: childRunID, ScheduledEventID: scheduledEventID, Output: output}}
}

func NewChildWorkflowFailedEvent(childID, childRunID string, scheduledEventID int64, failure protocol.Failure) Event {
	return Event{ChildWorkflowFailed: &ChildWorkflowFailedEvent{
		ChildID: childID, ChildRunID: childRunID, ScheduledEventID: scheduledEventID,
		Error: failure.Message, Failure: &failure,
	}}
}

func NewChildWorkflowCancelledEvent(childID, childRunID string, scheduledEventID int64) Event {
	return Event{ChildWorkflowCancelled: &ChildWorkflowCancelledEvent{
		ChildID: childID, ChildRunID: childRunID, ScheduledEventID: scheduledEventID,
	}}
}

func NewChildWorkflowTerminatedEvent(childID, childRunID string, scheduledEventID int64, reason string) Event {
	return Event{ChildWorkflowTerminated: &ChildWorkflowTerminatedEvent{
		ChildID: childID, ChildRunID: childRunID, ScheduledEventID: scheduledEventID, Reason: reason,
	}}
}

func NewWorkflowSignaledEvent(name string, input map[string]any) Event {
	return Event{WorkflowSignaled: &WorkflowSignaledEvent{Name: name, Input: input}}
}

func NewWorkflowUpdateAcceptedEvent(name, updateID string, args Payloads) Event {
	if args == nil {
		args = Payloads{}
	}
	return Event{WorkflowUpdateAccepted: &WorkflowUpdateAcceptedEvent{Name: name, UpdateID: updateID, Args: &args}}
}

func NewWorkflowUpdateCompletedOutcomeEvent(name, updateID string, outcome UpdateOutcome) Event {
	return Event{WorkflowUpdateCompleted: &WorkflowUpdateCompletedEvent{Name: name, UpdateID: updateID, Outcome: &outcome}}
}

func NewMarkerRecordedEvent(markerName, markerID string, value map[string]any) Event {
	return Event{MarkerRecorded: &MarkerRecordedEvent{MarkerName: markerName, MarkerID: markerID, Value: value}}
}

func NewSearchAttributesUpsertedEvent(attrs map[string]any) Event {
	return Event{SearchAttributesUpserted: &SearchAttributesUpsertedEvent{Attributes: attrs}}
}

// ── 类型判断辅助 ──

func IsWorkflowTerminalEventType(typ EventType) bool {
	switch typ {
	case EventWorkflowCompleted, EventWorkflowFailed, EventWorkflowCancelled, EventWorkflowTerminated, EventWorkflowContinuedAsNew:
		return true
	default:
		return false
	}
}

func IsActivityCloseEventType(typ EventType) bool {
	switch typ {
	case EventActivityCompleted, EventActivityFailed, EventActivityCancelled:
		return true
	default:
		return false
	}
}

func IsActivityAttemptEventType(typ EventType) bool {
	switch typ {
	case EventActivityAttemptFailed, EventActivityTimedOut:
		return true
	default:
		return false
	}
}

func IsWorkflowTaskCloseEventType(typ EventType) bool {
	switch typ {
	case EventWorkflowTaskCompleted, EventWorkflowTaskTimedOut, EventWorkflowTaskFailed:
		return true
	default:
		return false
	}
}

// FlatScheduledEventID 返回事件所关联的调度事件 ID；Event 是 tagged union，取自唯一非 nil 的子事件。
func (e *Event) FlatScheduledEventID() int64 {
	switch {
	case e.WorkflowTaskStarted != nil:
		return e.WorkflowTaskStarted.ScheduledEventID
	case e.WorkflowTaskCompleted != nil:
		return e.WorkflowTaskCompleted.ScheduledEventID
	case e.WorkflowTaskTimedOut != nil:
		return e.WorkflowTaskTimedOut.ScheduledEventID
	case e.WorkflowTaskFailed != nil:
		return e.WorkflowTaskFailed.ScheduledEventID
	case e.ActivityStarted != nil:
		return e.ActivityStarted.ScheduledEventID
	case e.ActivityCompleted != nil:
		return e.ActivityCompleted.ScheduledEventID
	case e.ActivityFailed != nil:
		return e.ActivityFailed.ScheduledEventID
	case e.ActivityAttemptFailed != nil:
		return e.ActivityAttemptFailed.ScheduledEventID
	case e.ActivityTimedOut != nil:
		return e.ActivityTimedOut.ScheduledEventID
	case e.ActivityCancelRequested != nil:
		return e.ActivityCancelRequested.ScheduledEventID
	case e.ActivityCancelled != nil:
		return e.ActivityCancelled.ScheduledEventID
	case e.TimerFired != nil:
		return e.TimerFired.ScheduledEventID
	case e.TimerCancelled != nil:
		return e.TimerCancelled.ScheduledEventID
	case e.ChildWorkflowCancelRequested != nil:
		return e.ChildWorkflowCancelRequested.ScheduledEventID
	case e.ChildWorkflowCompleted != nil:
		return e.ChildWorkflowCompleted.ScheduledEventID
	case e.ChildWorkflowFailed != nil:
		return e.ChildWorkflowFailed.ScheduledEventID
	case e.ChildWorkflowCancelled != nil:
		return e.ChildWorkflowCancelled.ScheduledEventID
	case e.ChildWorkflowTerminated != nil:
		return e.ChildWorkflowTerminated.ScheduledEventID
	default:
		return 0
	}
}
