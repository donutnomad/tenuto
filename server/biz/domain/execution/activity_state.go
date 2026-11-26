package execution

import (
	"time"

	"github.com/donutnomad/tenuto/protocol/history"
)

const (
	EmptyEventID     int64 = 0
	TransientEventID int64 = -124
)

type ActivityStatus string

// 字符串枚举 + var 结构体形式（项目规范）。Completed、Failed、Cancelled 与 RunClosed 是终态；RunClosed 表示
// Run 结束时 Activity 仍未完成，随 Run 关闭，不写事件（Temporal 关闭 Run 时随 mutable state 删除 pending activity）。
var ActivityStatusEnums = struct {
	Scheduled ActivityStatus
	Started   ActivityStatus
	Completed ActivityStatus
	Failed    ActivityStatus
	Cancelled ActivityStatus
	RunClosed ActivityStatus
}{
	Scheduled: "scheduled",
	Started:   "started",
	Completed: "completed",
	Failed:    "failed",
	Cancelled: "cancelled",
	RunClosed: "run_closed",
}

// ActivityState 是一个 activity（以 ScheduledEventID 为生命周期 key）的"当前状态"
// 唯一权威来源，对标 Temporal ActivityInfo 精简版。判断"第几次 attempt / 是否 started /
// 是否终态 / 是否被 reset" 一律读这里，不再扫 history。
// ActivityState
// @Setter
type ActivityState struct {
	TargetWorkerIdentity string `json:"targetWorkerIdentity,omitempty"`
	RunID                string `json:"runId"`
	ScheduledEventID     int64  `json:"scheduledEventId"` // 复合主键之二，贯穿生命周期不变
	ActivityID           string `json:"activityId"`
	ActivityType         string `json:"activityType"`
	TaskQueue            string `json:"taskQueue"`

	Attempt        int   `json:"attempt"`        // 当前 attempt（reset 时归 1）
	Stamp          int64 `json:"stamp"`          // generation token，reset/retry 时 ++，旧 task 失效
	StartedEventID int64 `json:"startedEventId"` // EmptyEventID/TransientEventID/真实 id（见 sentinels.go）
	ActivityReset  bool  `json:"activityReset"`  // 该 activity 是否经历过 reset（替代 lastResetID 历史扫描）
	// CancelRequested 表示 Workflow 已请求取消这个 Activity（ActivityCancelRequested）：不再重试，
	// 已开始的 attempt 经心跳得知取消。
	CancelRequested bool `json:"cancelRequested,omitempty"`

	StartedTime    *time.Time `json:"startedTime,omitempty"`
	WorkerIdentity string     `json:"workerIdentity,omitempty"` // started 时 worker 上报
	WorkerBuildID  string     `json:"workerBuildId,omitempty"`  // started 时 worker 上报；只用于观测，不参与路由
	// StartRequestID 是开始当前 attempt 的 Begin 请求标识（Worker 每个 lease 生成一次，对标 Temporal
	// ActivityInfo.RequestId）：同一标识的重发得到原来的开始结果，其他投递得到 attempt_already_started。
	StartRequestID   string         `json:"startRequestId,omitempty"`
	LastHeartbeat    map[string]any `json:"lastHeartbeat,omitempty"`
	LastHeartbeatAt  *time.Time     `json:"lastHeartbeatAt,omitempty"`
	RetryLastFailure string         `json:"retryLastFailure,omitempty"` // 上次 attempt 失败原因
	// NextAttemptAt 是当前 attempt 最早可派发的时刻：失败后排定下一次 attempt 时写入重试退避的到期时间，
	// reset 时写入 reset 的时间，attempt 开始时清空。nil 表示首个 attempt，调度时即可派发。
	NextAttemptAt *time.Time `json:"nextAttemptAt,omitempty"`

	Status        ActivityStatus `json:"status"`
	ScheduledTime time.Time      `json:"scheduledTime"`

	// 超时与重试（毫秒，0=未设置），供崩溃恢复重建 timer 句柄（tracker E）。
	ScheduleToStartTimeoutMs int64                `json:"scheduleToStartTimeoutMs,omitempty"`
	StartToCloseTimeoutMs    int64                `json:"startToCloseTimeoutMs,omitempty"`
	ScheduleToCloseTimeoutMs int64                `json:"scheduleToCloseTimeoutMs,omitempty"`
	HeartbeatTimeoutMs       int64                `json:"heartbeatTimeoutMs,omitempty"`
	RetryPolicy              *history.RetryPolicy `json:"retryPolicy,omitempty"`

	Input        map[string]any    `json:"input,omitempty"` // 重试/恢复重派需要
	TraceContext map[string]string `json:"traceContext,omitempty"`

	CreatedAt time.Time `json:"createdAt"`
	UpdatedAt time.Time `json:"updatedAt"`
	patch     ActivityStatePatch
}

func (s *ActivityState) IsScheduled() bool { return s.Status == ActivityStatusEnums.Scheduled }
func (s *ActivityState) IsStarted() bool   { return s.Status == ActivityStatusEnums.Started }

// DispatchableAt 返回当前 attempt 可派发的时刻：首个 attempt 是 ScheduledTime，之后是 NextAttemptAt。
// ScheduleToStart 从这一刻起算。
func (s *ActivityState) DispatchableAt() time.Time {
	if s.NextAttemptAt != nil {
		return *s.NextAttemptAt
	}
	return s.ScheduledTime
}

// RetryBackoffPending 报告 Scheduled 的 attempt 在 now 时刻是否仍在重试退避期内。
func (s *ActivityState) RetryBackoffPending(now time.Time) bool {
	return s.IsScheduled() && s.NextAttemptAt != nil && s.NextAttemptAt.After(now)
}

func (s *ActivityState) IsTerminal() bool {
	return s.Status == ActivityStatusEnums.Completed || s.Status == ActivityStatusEnums.Failed ||
		s.Status == ActivityStatusEnums.Cancelled || s.Status == ActivityStatusEnums.RunClosed
}

// ActivityStateStore 是 activity_states 表的 CRUD。返回的 *ActivityState 必须是深拷贝
// （实现侧负责），调用方修改不得影响存储内部。
type ActivityStateStore interface {
	GetActivityState(runID string, scheduledEventID int64) (*ActivityState, bool, error)
	UpsertActivityState(state *ActivityState) error
	ListActivityStates(runID string) ([]*ActivityState, error)
}
