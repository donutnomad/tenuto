package execution

import (
	"fmt"
	"time"

	"github.com/donutnomad/tenuto/protocol/history"
)

// eventTime 返回事件时间的副本，供 state 的时间指针字段使用。
func eventTime(event history.Event) *time.Time {
	at := event.Timestamp
	return &at
}

func cloneStringMap(src map[string]string) map[string]string {
	if src == nil {
		return nil
	}
	dst := make(map[string]string, len(src))
	for key, value := range src {
		dst[key] = value
	}
	return dst
}

// WorkflowExecutionState
// @Setter
type WorkflowExecutionState struct {
	RunID             string         `json:"run_id"`
	NextEventID       int64          `json:"next_event_id"`
	LastEventID       int64          `json:"last_event_id"`
	HistoryEventCount int64          `json:"history_event_count"`
	HistorySizeBytes  int64          `json:"history_size_bytes"`
	Status            history.Status `json:"status"`
	// ClosedAt 是终态事件的时间；Run 未结束时为 nil。
	ClosedAt                 *time.Time        `json:"closed_at,omitempty"`
	SearchAttributes         map[string]string `json:"search_attributes,omitempty"`
	CurrentWaiting           WaitingState      `json:"current_waiting,omitempty"`
	FailureReason            string            `json:"failure_reason,omitempty"`
	IsReset                  bool              `json:"is_reset,omitempty"`
	Blocked                  BlockedInfo       `json:"blocked"`
	OpenWorkflowTaskEventID  int64             `json:"open_workflow_task_event_id,omitempty"`
	ExecutionGeneration      int64             `json:"execution_generation"`
	CurrentActivationID      string            `json:"current_activation_id,omitempty"`
	CurrentActivationAttempt int               `json:"current_activation_attempt,omitempty"`
	StickyWorkerIdentity     string            `json:"sticky_worker_identity,omitempty"`
	StickyWorkerBuildID      string            `json:"sticky_worker_build_id,omitempty"`
	StickyGeneration         int64             `json:"sticky_generation"`
	// StickyExpiresAt 用指针：nil 表示"无 sticky 会话"。
	// 非指针 time.Time 的零值落 MySQL 会写成 '0000-00-00'，而 MySQL 8 默认 sql_mode 含
	// NO_ZERO_DATE + STRICT_TRANS_TABLES，会直接报 Error 1292 拒绝写入（即使列 nullable）。
	// 用指针后清 sticky 写的是 SQL NULL，这是"无值"的正确表达。
	// 附带修正：omitempty 对非零大小的 struct 本就不生效，原先零值会被序列化成
	// "0001-01-01T00:00:00Z"；改指针后 nil 才真正被 omitempty 省略。
	StickyExpiresAt *time.Time             `json:"sticky_expires_at,omitempty"`
	StateVersion    int64                  `json:"state_version"`
	UpdatedAt       time.Time              `json:"updated_at"`
	OpenWaits       map[int64]WaitingState `json:"-"`
	// WorkflowTask 是 WFT 生命周期的投影，见 WorkflowTaskProjection。
	WorkflowTask WorkflowTaskProjection `json:"workflow_task"`
	// HasWorkflowStarted 表示 History 已有 WorkflowStarted 事件。
	HasWorkflowStarted bool `json:"has_workflow_started,omitempty"`
	// CancelRequested 表示 History 已有 WorkflowCancellationRequested 事件。
	CancelRequested bool `json:"cancel_requested,omitempty"`
	patch           WorkflowExecutionStatePatch
}

// WorkflowTaskProjection 记录调度与恢复 WFT 所需的当前事实。
type WorkflowTaskProjection struct {
	// StartedScheduledEventID 与 StartedProgramVersion 取自最近一次 WorkflowTaskStarted。
	StartedScheduledEventID int64  `json:"started_scheduled_event_id,omitempty"`
	StartedProgramVersion   string `json:"started_program_version,omitempty"`
	// StartToCloseDeadline 是已 started 且仍 open 的 WFT 的超时时刻，取自 WorkflowTaskStarted 的时间与 TimeoutMs；
	// WFT 关闭时清空。
	StartToCloseDeadline *time.Time `json:"start_to_close_deadline,omitempty"`
	// LastFailedAt 与 LastFailureTraceContext 取自当前连续失败串里最近一次 WorkflowTaskFailed。
	LastFailedAt            *time.Time        `json:"last_failed_at,omitempty"`
	LastFailureTraceContext map[string]string `json:"last_failure_trace_context,omitempty"`
	// ActivationPending 表示最近一次 WFT 生命周期事件之后出现了需要 Workflow 处理的新事实。
	ActivationPending bool `json:"activation_pending,omitempty"`
}

// WorkflowExecutionStateStore 读写 execution state。Run 不存在时各方法返回 ErrRunNotFound。
type WorkflowExecutionStateStore interface {
	GetWorkflowExecutionState(runID string) (*WorkflowExecutionState, error)
	ListWorkflowExecutionStates(runIDs []string) (map[string]*WorkflowExecutionState, error)
	// ListExpiredStickyExecutions 返回 sticky 已过期且仍有 open WFT 的 execution state，判据是
	// StickyWorkerIdentity != "" && !StickyValidAt(now) && OpenWorkflowTaskEventID != 0；limit > 0 时封顶。
	ListExpiredStickyExecutions(now time.Time, limit int) ([]*WorkflowExecutionState, error)
	AssignStickyExecution(runID string, executionGeneration, expectedStickyGeneration int64, workerIdentity, workerBuildID string, expiresAt time.Time) (int64, bool, error)
	ClearStickyExecution(runID string, executionGeneration, expectedStickyGeneration int64) (int64, bool, error)
}

func NewWorkflowExecutionState(run *Run) *WorkflowExecutionState {
	state := &WorkflowExecutionState{
		RunID:               run.ID,
		NextEventID:         1,
		Status:              run.Status,
		SearchAttributes:    cloneStringMap(run.SearchAttributes),
		ExecutionGeneration: 1,
		StickyGeneration:    1,
		OpenWaits:           make(map[int64]WaitingState),
		UpdatedAt:           run.CreatedAt,
	}
	if state.Status == "" {
		state.setStatus(history.StatusRunning)
	}
	return state
}

func CloneWorkflowExecutionState(state *WorkflowExecutionState) *WorkflowExecutionState {
	if state == nil {
		return nil
	}
	clone := *state
	clone.SearchAttributes = cloneStringMap(state.SearchAttributes)
	clone.OpenWaits = make(map[int64]WaitingState, len(state.OpenWaits))
	for id, waiting := range state.OpenWaits {
		clone.OpenWaits[id] = waiting
	}
	if state.Blocked.Since != nil {
		since := *state.Blocked.Since
		clone.Blocked.Since = &since
	}
	clone.WorkflowTask = cloneWorkflowTaskProjection(state.WorkflowTask)
	return &clone
}

func cloneWorkflowTaskProjection(task WorkflowTaskProjection) WorkflowTaskProjection {
	if task.LastFailedAt != nil {
		failedAt := *task.LastFailedAt
		task.LastFailedAt = &failedAt
	}
	if task.StartToCloseDeadline != nil {
		deadline := *task.StartToCloseDeadline
		task.StartToCloseDeadline = &deadline
	}
	task.LastFailureTraceContext = cloneStringMap(task.LastFailureTraceContext)
	return task
}

// SyncFromRun 把本次 Run 更新改过的状态与搜索属性同步到投影，两个存储后端的 UpdateRun 共用。
// 只同步经 setter 改过的字段：Run 对象可能是事务外读到的旧快照，其余字段以投影为准。
// 终态由 History 的终态事件写入后保持不变：持有旧快照的 Run 更新不会把它改回 Running。
func (s *WorkflowExecutionState) SyncFromRun(run *Run, now time.Time) {
	changed := run.ExportPatch()
	if changed.Status.IsPresent() && !history.IsTerminalStatus(s.Status) {
		s.setStatus(run.Status)
	}
	if changed.SearchAttributes.IsPresent() {
		s.setSearchAttributes(cloneStringMap(run.SearchAttributes))
	}
	s.setStateVersion(s.StateVersion + 1)
	s.setUpdatedAt(now)
}

// IsTerminal 报告 Run 是否已有终态。
func (s *WorkflowExecutionState) IsTerminal() bool { return history.IsTerminalStatus(s.Status) }

// sessionMatches 报告调用方持有的 execution 与 sticky 代数是否仍是当前值。
func (s *WorkflowExecutionState) sessionMatches(executionGeneration, stickyGeneration int64) bool {
	return s.ExecutionGeneration == executionGeneration && s.StickyGeneration == stickyGeneration
}

// BeginActivation 为 scheduledEventID 的 WFT 把 activationID 记为当前激活并累加尝试次数。该 WFT 已关闭、
// 代数已变，或另一个激活正在进行时返回 false，状态不变。WFT 是否仍 open 与写入激活在同一次状态修改里判定：
// 重复投递的 WFT 在另一份完成之后才走到这里时，不会在下一个 WFT 上留下陈旧的激活。
func (s *WorkflowExecutionState) BeginActivation(scheduledEventID, executionGeneration, stickyGeneration int64, activationID string, now time.Time) bool {
	if !s.WorkflowTaskOpen(scheduledEventID) || !s.sessionMatches(executionGeneration, stickyGeneration) {
		return false
	}
	if s.CurrentActivationID != "" && s.CurrentActivationID != activationID {
		return false
	}
	s.setCurrentActivationID(activationID)
	s.setCurrentActivationAttempt(s.CurrentActivationAttempt + 1)
	s.setStateVersion(s.StateVersion + 1)
	s.setUpdatedAt(now)
	return true
}

// AssignSticky 把 sticky 会话交给 Worker；换了 Worker 或 build 时 sticky 代数加一。代数已变时返回 false，状态不变。
func (s *WorkflowExecutionState) AssignSticky(executionGeneration, expectedStickyGeneration int64, workerIdentity, workerBuildID string, expiresAt, now time.Time) bool {
	if !s.sessionMatches(executionGeneration, expectedStickyGeneration) {
		return false
	}
	if s.StickyWorkerIdentity != "" && (s.StickyWorkerIdentity != workerIdentity || s.StickyWorkerBuildID != workerBuildID) {
		s.setStickyGeneration(s.StickyGeneration + 1)
	}
	s.setStickyWorkerIdentity(workerIdentity)
	s.setStickyWorkerBuildID(workerBuildID)
	s.setStickyExpiresAt(&expiresAt)
	s.setStateVersion(s.StateVersion + 1)
	s.setUpdatedAt(now)
	return true
}

// ClearSticky 结束 sticky 会话并把 sticky 代数加一。代数已变时返回 false，状态不变。
func (s *WorkflowExecutionState) ClearSticky(executionGeneration, expectedStickyGeneration int64, now time.Time) bool {
	if !s.sessionMatches(executionGeneration, expectedStickyGeneration) {
		return false
	}
	s.setStickyGeneration(s.StickyGeneration + 1)
	s.setStickyWorkerIdentity("")
	s.setStickyWorkerBuildID("")
	s.setStickyExpiresAt(nil)
	s.setStateVersion(s.StateVersion + 1)
	s.setUpdatedAt(now)
	return true
}

// WorkflowTaskOpen 报告 scheduledEventID 对应的 WFT 是否已调度且尚未关闭。
func (s *WorkflowExecutionState) WorkflowTaskOpen(scheduledEventID int64) bool {
	return s.OpenWaits[scheduledEventID].Type == WaitingWorkflowTask
}

// CurrentWorkflowTaskScheduledEventID 返回 open WFT 的调度事件 ID；没有 open WFT 时返回最近一次
// 已 started 的 WFT。
func (s *WorkflowExecutionState) CurrentWorkflowTaskScheduledEventID() int64 {
	if s.OpenWorkflowTaskEventID > 0 {
		return s.OpenWorkflowTaskEventID
	}
	return s.WorkflowTask.StartedScheduledEventID
}

// WorkflowTaskProgramVersion 返回 scheduledEventID 对应 WFT 在 started 时选中的 Program 版本。
func (s *WorkflowExecutionState) WorkflowTaskProgramVersion(scheduledEventID int64) string {
	if s.WorkflowTask.StartedScheduledEventID != scheduledEventID {
		return ""
	}
	return s.WorkflowTask.StartedProgramVersion
}

// WorkflowTaskSuperseded 报告 scheduledEventID 对应的 open WFT 在 started 之后是否出现了新事实。
func (s *WorkflowExecutionState) WorkflowTaskSuperseded(scheduledEventID int64) bool {
	return s.WorkflowTaskOpen(scheduledEventID) &&
		s.WorkflowTask.StartedScheduledEventID == scheduledEventID &&
		s.WorkflowTask.ActivationPending
}

// WorkflowTaskTimeoutDeadline 返回已 started 且仍 open 的 WFT 的调度事件 ID 与 start-to-close 超时时刻。
func (s *WorkflowExecutionState) WorkflowTaskTimeoutDeadline() (int64, time.Time, bool) {
	scheduledEventID := s.OpenWorkflowTaskEventID
	if scheduledEventID == 0 || s.WorkflowTask.StartedScheduledEventID != scheduledEventID || s.WorkflowTask.StartToCloseDeadline == nil {
		return 0, time.Time{}, false
	}
	return scheduledEventID, *s.WorkflowTask.StartToCloseDeadline, true
}

// WorkflowTaskTimedOutAt 报告 scheduledEventID 对应的 WFT 在 now 时刻是否已 started、仍 open 且超过 start-to-close 超时。
func (s *WorkflowExecutionState) WorkflowTaskTimedOutAt(scheduledEventID int64, now time.Time) bool {
	id, deadline, ok := s.WorkflowTaskTimeoutDeadline()
	return ok && id == scheduledEventID && !deadline.After(now)
}

// NeedsWorkflowTask 报告 Run 是否有待处理的新事实且没有 open WFT。
func (s *WorkflowExecutionState) NeedsWorkflowTask() bool {
	return s.OpenWorkflowTaskEventID == 0 && s.WorkflowTask.ActivationPending
}

// HadOrHasWorkflowTask 报告 Run 调度过 Workflow Task：现在有 open 的，或有过 started 的。
func (s *WorkflowExecutionState) HadOrHasWorkflowTask() bool {
	return s.OpenWorkflowTaskEventID != 0 || s.WorkflowTask.StartedScheduledEventID != 0
}

// PendingOnWorkflowTaskBackoff 报告重试 Run 仍在等首个 Workflow Task 的退避：RetryBackoff timer 未结束，
// 且 Run 还没有调度过 Workflow Task。对应 Temporal 的 IsWorkflowPendingOnWorkflowTaskBackoff。
func (s *WorkflowExecutionState) PendingOnWorkflowTaskBackoff(timers []*TimerState) bool {
	if s.HadOrHasWorkflowTask() {
		return false
	}
	for _, timer := range timers {
		if timer.Kind == TimerKindEnums.RetryBackoff && !timer.IsTerminal() {
			return true
		}
	}
	return false
}

// BlockedInfo 返回对外展示的 blocked 描述：只有处于 blocked 时才带失败次数与原因。
func (s *WorkflowExecutionState) BlockedInfo() BlockedInfo {
	if !s.Blocked.Blocked {
		return BlockedInfo{}
	}
	return s.Blocked
}

// WorkflowTaskFailureBackoff 返回当前连续失败次数对应的退避时长。
func (s *WorkflowExecutionState) WorkflowTaskFailureBackoff(base, cap time.Duration) time.Duration {
	return WorkflowTaskFailureBackoff(s.Blocked.ConsecutiveFailures, base, cap)
}

func ApplyEventToExecutionState(state *WorkflowExecutionState, event history.Event, eventSize int64) {
	if state.OpenWaits == nil {
		state.OpenWaits = make(map[int64]WaitingState)
	}
	state.setLastEventID(event.ID)
	state.setNextEventID(event.ID + 1)
	state.setHistoryEventCount(state.HistoryEventCount + 1)
	state.setHistorySizeBytes(state.HistorySizeBytes + eventSize)
	state.setStateVersion(state.StateVersion + 1)
	if event.Timestamp.After(state.UpdatedAt) {
		state.setUpdatedAt(event.Timestamp)
	}

	switch {
	case event.WorkflowStarted != nil:
		state.setHasWorkflowStarted(true)
	case event.WorkflowCancellationRequested != nil:
		state.setCancelRequested(true)
	case event.WorkflowCompleted != nil:
		state.setStatus(history.StatusCompleted)
		state.setClosedAt(eventTime(event))
		state.setFailureReason("")
	case event.WorkflowFailed != nil:
		state.setStatus(history.StatusFailed)
		state.setClosedAt(eventTime(event))
		state.setFailureReason(event.WorkflowFailed.Error)
	case event.WorkflowCancelled != nil:
		state.setStatus(history.StatusCancelled)
		state.setClosedAt(eventTime(event))
		state.setFailureReason("")
	case event.WorkflowTerminated != nil:
		state.setStatus(history.StatusTerminated)
		state.setClosedAt(eventTime(event))
		state.setFailureReason(event.WorkflowTerminated.Reason)
	case event.WorkflowContinuedAsNew != nil:
		state.setStatus(history.StatusContinuedAsNew)
		state.setClosedAt(eventTime(event))
	case event.SearchAttributesUpserted != nil:
		if state.SearchAttributes == nil {
			state.SearchAttributes = make(map[string]string)
		}
		for key, value := range event.SearchAttributesUpserted.Attributes {
			if key != "" {
				state.SearchAttributes[key] = fmt.Sprint(value)
			}
		}
		state.setSearchAttributes(state.SearchAttributes)
	case event.ActivityReset != nil:
		invalidateExecutionSession(state)
	case event.WorkflowTaskTimedOut != nil:
		// 超时的 WFT 不再交给原 sticky Worker：下一个 WFT 走普通队列，任何 Worker 都能领取。sticky 代数不变，
		// 原 Worker 迟到的上报仍能通过代数校验，按已关闭的 WFT 处理。
		clearStickyRoute(state)
	}

	applyWaitingEvent(state, event)
	applyBlockedEvent(state, event)
	applyWorkflowTaskEvent(state, event)
	state.setCurrentWaiting(latestWaiting(state.OpenWaits))
}

func applyWaitingEvent(state *WorkflowExecutionState, event history.Event) {
	switch {
	case event.WorkflowTaskScheduled != nil:
		state.setOpenWorkflowTaskEventID(event.ID)
		state.OpenWaits[event.ID] = WaitingState{Type: WaitingWorkflowTask, ScheduledEventID: event.ID}
	case history.IsWorkflowTaskCloseEventType(event.EventType()):
		id := event.FlatScheduledEventID()
		delete(state.OpenWaits, id)
		state.setCurrentActivationID("")
		state.setCurrentActivationAttempt(0)
	case event.ActivityScheduled != nil:
		state.OpenWaits[event.ID] = WaitingState{Type: WaitingActivity, ActivityID: event.ActivityScheduled.ActivityID, ActivityType: event.ActivityScheduled.ActivityType, TaskQueue: event.ActivityScheduled.TaskQueue, ScheduledEventID: event.ID}
	case event.ActivityCompleted != nil:
		delete(state.OpenWaits, event.ActivityCompleted.ScheduledEventID)
	case event.ActivityFailed != nil:
		delete(state.OpenWaits, event.ActivityFailed.ScheduledEventID)
	case event.ActivityCancelled != nil:
		delete(state.OpenWaits, event.ActivityCancelled.ScheduledEventID)
	case event.TimerScheduled != nil:
		state.OpenWaits[event.ID] = WaitingState{Type: WaitingTimer, TimerID: event.TimerScheduled.TimerID, ScheduledEventID: event.ID}
	case event.TimerFired != nil:
		delete(state.OpenWaits, event.TimerFired.ScheduledEventID)
	case event.TimerCancelled != nil:
		delete(state.OpenWaits, event.TimerCancelled.ScheduledEventID)
	case event.ChildWorkflowStarted != nil:
		state.OpenWaits[event.ID] = WaitingState{Type: WaitingChildWorkflow, ChildID: event.ChildWorkflowStarted.ChildID, ChildRunID: event.ChildWorkflowStarted.ChildRunID, ScheduledEventID: event.ID}
	case event.ChildWorkflowCompleted != nil:
		delete(state.OpenWaits, event.ChildWorkflowCompleted.ScheduledEventID)
	case event.ChildWorkflowFailed != nil:
		delete(state.OpenWaits, event.ChildWorkflowFailed.ScheduledEventID)
	case event.ChildWorkflowCancelled != nil:
		delete(state.OpenWaits, event.ChildWorkflowCancelled.ScheduledEventID)
	case event.ChildWorkflowTerminated != nil:
		delete(state.OpenWaits, event.ChildWorkflowTerminated.ScheduledEventID)
	}
	state.setOpenWaits(state.OpenWaits)
	state.setOpenWorkflowTaskEventID(latestWorkflowTaskWaiting(state.OpenWaits))
}

func applyBlockedEvent(state *WorkflowExecutionState, event history.Event) {
	blocked := state.Blocked
	if history.IsWorkflowTerminalEventType(event.EventType()) {
		// 已结束的 Run 不再调度 Workflow Task，blocked 只描述运行中的 Run。
		state.setBlocked(BlockedInfo{})
		return
	}
	switch event.EventType() {
	case history.EventWorkflowTaskScheduled, history.EventWorkflowTaskStarted:
		blocked.Blocked = false
		state.setBlocked(blocked)
	case history.EventWorkflowTaskCompleted, history.EventWorkflowTaskTimedOut:
		state.setBlocked(BlockedInfo{})
	case history.EventWorkflowTaskFailed:
		failed := event.WorkflowTaskFailed
		if failed.Cause == WorkflowTaskFailedCauseResetWorkflow {
			state.setIsReset(true)
			invalidateExecutionSession(state)
		}
		if isRedoWorkflowTaskFailure(failed.Cause) {
			return
		}
		blocked.Blocked = state.OpenWorkflowTaskEventID == 0
		blocked.ConsecutiveFailures++
		blocked.Cause = failed.Cause
		blocked.Reason = failed.Reason
		if blocked.Since == nil {
			since := event.Timestamp
			blocked.Since = &since
		}
		state.setBlocked(blocked)
	}
}

func applyWorkflowTaskEvent(state *WorkflowExecutionState, event history.Event) {
	task := state.WorkflowTask
	typ := event.EventType()
	// 被新事实取代或被恢复关闭的 WFT：待处理的事实仍在，ActivationPending 为 true，也不记失败时间。
	if typ == history.EventWorkflowTaskFailed && isRedoWorkflowTaskFailure(event.WorkflowTaskFailed.Cause) {
		task.ActivationPending = true
		task.StartToCloseDeadline = nil
		state.setWorkflowTask(task)
		return
	}
	switch {
	case isWorkflowActivationEvent(typ):
		task.ActivationPending = true
	case isWorkflowTaskLifecycleEvent(typ):
		task.ActivationPending = false
	default:
		return
	}
	switch typ {
	case history.EventWorkflowTaskStarted:
		task.StartedScheduledEventID = event.WorkflowTaskStarted.ScheduledEventID
		task.StartedProgramVersion = event.WorkflowTaskStarted.ProgramVersion
		task.StartToCloseDeadline = nil
		if timeoutMs := event.WorkflowTaskStarted.TimeoutMs; timeoutMs > 0 {
			deadline := event.Timestamp.Add(time.Duration(timeoutMs) * time.Millisecond)
			task.StartToCloseDeadline = &deadline
		}
	case history.EventWorkflowTaskCompleted, history.EventWorkflowTaskTimedOut:
		task.LastFailedAt = nil
		task.LastFailureTraceContext = nil
		task.StartToCloseDeadline = nil
	case history.EventWorkflowTaskFailed:
		failedAt := event.Timestamp
		task.LastFailedAt = &failedAt
		task.LastFailureTraceContext = cloneStringMap(event.WorkflowTaskFailed.TraceContext)
		task.StartToCloseDeadline = nil
	}
	state.setWorkflowTask(task)
}

func invalidateExecutionSession(state *WorkflowExecutionState) {
	state.setExecutionGeneration(state.ExecutionGeneration + 1)
	state.setCurrentActivationID("")
	state.setCurrentActivationAttempt(0)
	state.setStickyGeneration(state.StickyGeneration + 1)
	clearStickyRoute(state)
}

// clearStickyRoute 结束 sticky 会话，之后的 WFT 走普通队列。
func clearStickyRoute(state *WorkflowExecutionState) {
	state.setStickyWorkerIdentity("")
	state.setStickyWorkerBuildID("")
	state.setStickyExpiresAt(nil)
}

// StickyValidAt 报告 sticky 会话在 now 时刻是否仍然有效。
// 集中处理 nil（无 sticky 会话）：nil 一律视为已失效，等价于原先零值 time.Time
// 的 .After(now) 恒为 false 的行为，故调用方语义不变。
func (s *WorkflowExecutionState) StickyValidAt(now time.Time) bool {
	return s.StickyExpiresAt != nil && s.StickyExpiresAt.After(now)
}

func latestWaiting(open map[int64]WaitingState) WaitingState {
	var latest WaitingState
	for id, waiting := range open {
		if id > latest.ScheduledEventID {
			latest = waiting
		}
	}
	return latest
}

func latestWorkflowTaskWaiting(open map[int64]WaitingState) int64 {
	var latest int64
	for id, waiting := range open {
		if waiting.Type == WaitingWorkflowTask && id > latest {
			latest = id
		}
	}
	return latest
}

func BuildWorkflowExecutionState(run *Run, events []history.Event) (*WorkflowExecutionState, error) {
	state := NewWorkflowExecutionState(run)
	for _, event := range events {
		size, err := history.CanonicalEventSize(event)
		if err != nil {
			return nil, err
		}
		ApplyEventToExecutionState(state, event, size)
	}
	return state, nil
}
