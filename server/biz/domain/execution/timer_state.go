package execution

import "time"

type TimerStatus string

// 字符串枚举 + var 结构体形式（项目规范）。timer 一次性 fire，无 generation/reset 语义：
// Scheduled（已调度，待 fire）→ Fired（已到期触发，终态）、Cancelled（Workflow 取消，写 TimerCancelled，终态）
// 或 RunClosed（Run 结束时仍未触发，随 Run 关闭，不写事件，终态）。
var TimerStatusEnums = struct {
	Scheduled TimerStatus
	Fired     TimerStatus
	Cancelled TimerStatus
	RunClosed TimerStatus
}{
	Scheduled: "scheduled",
	Fired:     "fired",
	Cancelled: "cancelled",
	RunClosed: "run_closed",
}

type TimerKind string

// TimerKindEnums 是 timer 的种类，触发时按种类分发：
// UserTimer: 用户通过 CmdScheduleTimer 命令创建的 durable timer；
// RetryBackoff: workflow retry 的 backoff 延迟 timer（延迟新 run 首个 WFT）。
var TimerKindEnums = struct {
	UserTimer    TimerKind
	RetryBackoff TimerKind
}{
	UserTimer:    "UserTimer",
	RetryBackoff: "RetryBackoff",
}

// TimerState 是一个 durable timer（以 ScheduledEventID = TimerScheduled 事件 ID 为生命周期 key）
// 的"当前状态"唯一权威来源。判断"是否已 fire / 是否可被 recovery 重建"一律读这里，不扫 history。
//
// 与 ActivityState 不同：timer 无 Attempt/Stamp（一次性 fire，无重试/reset/generation），
// 故无 generation-aware dedupe；幂等由 Status 终态在 RunInTx 内的 GetTimerStateForUpdate 保证。
//
// Kind 在创建时写入，触发时按它分发到用户 timer 或重试退避的处理。
// TimerState
// @Setter
type TimerState struct {
	RunID            string    `json:"runId"`
	ScheduledEventID int64     `json:"scheduledEventId"` // 复合主键之二 = TimerScheduled 事件 ID，贯穿生命周期不变
	TimerID          string    `json:"timerId"`          // workflow 内的用户可见 timer 标识；RetryBackoff 置空
	Kind             TimerKind `json:"kind"`

	Deadline     time.Time         `json:"deadline"` // 到期时刻；崩溃恢复算 remaining 重建 time.AfterFunc
	Status       TimerStatus       `json:"status"`
	TraceContext map[string]string `json:"traceContext,omitempty"`

	CreatedAt time.Time `json:"createdAt"`
	UpdatedAt time.Time `json:"updatedAt"`
	patch     TimerStatePatch
}

func (s *TimerState) IsScheduled() bool { return s.Status == TimerStatusEnums.Scheduled }

// IsTerminal 报告 timer 是否已触发、已取消或随 Run 关闭。崩溃恢复跳过终态 timer；
// TimerFired 写入前在 RunInTx 内校验非终态（幂等，替代 timerAlreadyClosed 历史扫描）。
func (s *TimerState) IsTerminal() bool {
	switch s.Status {
	case TimerStatusEnums.Fired, TimerStatusEnums.Cancelled, TimerStatusEnums.RunClosed:
		return true
	}
	return false
}

// TimerStateStore 是 timer_states 表的 CRUD。返回的 *TimerState 必须是深拷贝（实现侧负责），
// 调用方修改不得影响存储内部。方法签名与 ActivityStateStore 同构（复合主键 RunID+ScheduledEventID）。
type TimerStateStore interface {
	GetTimerState(runID string, scheduledEventID int64) (*TimerState, bool, error)
	UpsertTimerState(state *TimerState) error
	ListTimerStates(runID string) ([]*TimerState, error)
}
