package execution

import (
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
)

// ResolveResetNodeID maps a requested reset point to a workflow task boundary.
func ResolveResetNodeID(events []history.Event, req ResetWorkflowRequest) (int64, error) {
	if req.ResetEventID > 0 {
		maxNodeID := int64(0)
		var anchor *history.Event
		for i := range events {
			event := &events[i]
			if event.ID > maxNodeID {
				maxNodeID = event.ID
			}
			if event.ID == req.ResetEventID {
				anchor = event
			}
		}
		if req.ResetEventID > maxNodeID {
			return 0, protocol.InvalidRequestf("reset_event_id %d exceeds max event id %d", req.ResetEventID, maxNodeID)
		}
		if anchor == nil {
			return 0, protocol.InvalidRequestf("reset_event_id %d not found", req.ResetEventID)
		}
		typ := anchor.EventType()
		switch {
		case typ == history.EventWorkflowTaskStarted:
			return anchor.ID, nil
		case history.IsWorkflowTaskCloseEventType(typ):
			return resetTaskStartNode(events, *anchor)
		default:
			return 0, protocol.InvalidRequestf("reset_event_id %d is not a valid reset point: must be a workflow task boundary event (WorkflowTaskStarted/Completed/Failed/TimedOut)", req.ResetEventID)
		}
	}

	var closeNodeIDs []int64
	for _, event := range events {
		typ := event.EventType()
		if typ == history.EventWorkflowTaskCompleted || typ == history.EventWorkflowTaskTimedOut || typ == history.EventWorkflowTaskFailed {
			nodeID, err := resetTaskStartNode(events, event)
			if err != nil {
				return 0, err
			}
			closeNodeIDs = append(closeNodeIDs, nodeID)
		}
	}
	if len(closeNodeIDs) == 0 {
		return 0, protocol.InvalidRequestf("no workflow task close events found")
	}
	switch req.ResetType {
	case ResetTypeEnums.FirstWorkflowTask:
		return closeNodeIDs[0], nil
	case ResetTypeEnums.LastWorkflowTask:
		return closeNodeIDs[len(closeNodeIDs)-1], nil
	default:
		return 0, protocol.InvalidRequestf("unknown reset type: %s", req.ResetType)
	}
}

// resetTaskStartNode retains earlier committed batches and replays the selected
// task. Its close event must be excluded together with its command events.
// A workflow task is closed only after it started, so the Started event exists.
func resetTaskStartNode(events []history.Event, close history.Event) (int64, error) {
	scheduledID := close.FlatScheduledEventID()
	for i := len(events) - 1; i >= 0; i-- {
		event := events[i]
		if event.ID < close.ID && event.WorkflowTaskStarted != nil && event.WorkflowTaskStarted.ScheduledEventID == scheduledID {
			return event.ID, nil
		}
	}
	return 0, fmt.Errorf("workflow task close event %d has no WorkflowTaskStarted event", close.ID)
}

// ResetPoint 是 reset 在 base Run 的 History 上分叉的位置。
type ResetPoint struct {
	// NodeID 是被重放的 Workflow Task 的 WorkflowTaskStarted 事件；新分支保留到它为止的事件。
	NodeID int64
	// ScheduledEventID 是这个 Workflow Task 的 WorkflowTaskScheduled 事件。
	ScheduledEventID int64
}

// PlanReset 按顺序校验 reset 请求并返回分叉位置：请求的位置要能解析到一个 Workflow Task 边界（ResolveResetNodeID），
// 分叉点之前不能有尚未结束的子 Workflow（CheckPendingChildWorkflows）。分叉点上未结束的 Activity 与 Timer 由
// PlanResetPendingWork 带进新 Run。events 是 base Run 的 History，children 是它的子 Workflow 状态。
func PlanReset(events []history.Event, children []*ChildWorkflowState, req ResetWorkflowRequest) (ResetPoint, error) {
	nodeID, err := ResolveResetNodeID(events, req)
	if err != nil {
		return ResetPoint{}, err
	}
	if err := CheckPendingChildWorkflows(children, nodeID); err != nil {
		return ResetPoint{}, err
	}
	point := ResetPoint{NodeID: nodeID}
	for _, event := range events {
		if event.ID == nodeID {
			point.ScheduledEventID = event.WorkflowTaskStarted.ScheduledEventID
		}
	}
	return point, nil
}

// CheckPendingChildWorkflows 拒绝分叉点之前（含）启动、尚未结束的子 Workflow：reset 无法重建它们与新 Run 的关联。
func CheckPendingChildWorkflows(states []*ChildWorkflowState, resetNodeID int64) error {
	for _, state := range states {
		if state.ScheduledEventID <= resetNodeID && !state.IsTerminal() {
			return protocol.InvalidRequestf("cannot reset: pending child workflows exist at reset point")
		}
	}
	return nil
}

// NewResetRun forks the same attempt of the same execution: it keeps the
// attempt number and initiator of the Run it replaces. Both deadlines start
// again when the engine arms the new Run's timers, so a Run that already ran
// past its execution timeout can be reset and continue.
func NewResetRun(old *Run, newID, baseRunID string, branchToken []byte, now time.Time) *Run {
	next := NewRun(NewRunOptions{
		ID: newID, Namespace: old.Namespace, WorkflowID: old.WorkflowID,
		WorkflowType:          old.WorkflowType,
		WorkflowConfigVersion: old.WorkflowConfigVersion,
		ParentRunID:           old.ParentRunID, ParentChildID: old.ParentChildID,
		ParentScheduledEventID: old.ParentScheduledEventID, FirstRunID: old.FirstRunID,
		ContinuedRunID: baseRunID, BranchToken: branchToken,
		Input: old.Input, SearchAttributes: old.SearchAttributes,
		Attempt: old.Attempt, Initiator: old.Initiator, CreatedAt: now,
	})
	inheritRunUpgradeState(next, old)
	return next
}

type ResetRunCommit struct {
	NewRun          *Run
	ExpectedCurrent string
	BaseRunID       string
	TreeID          string
	BranchID        string
	FailedEvent     history.Event
	// Pending 是分叉点上未结束的 Activity 与 Timer（见 PlanResetPendingWork）：关闭事件写在 reset 标记之后，
	// state 行建在新 Run 上。
	Pending ResetPendingWork
	// ReapplyEvents 是 reset 点之后要搬到新 Run 的外部输入（见 ResetReapplyEvents），写在 Pending 的事件之后。
	ReapplyEvents []history.Event
	// Terminate 是被新 Run 取代、仍在运行的 Run：被 reset 的 Run 与 reset 历史代时的 current。
	Terminate []ResetTermination
	Now       time.Time
}

// ResetTermination 是 reset 在同一事务里终止的一个 Run。
type ResetTermination struct {
	RunID            string
	TreeID, BranchID string
}

// ResetTerminationReason 是 reset 终止被取代的 Run 时记录的原因。
const ResetTerminationReason = "reset"

// CommitResetRun terminates the Runs the reset replaces, creates a forked Run,
// moves current, records the reset link, writes the reset marker, the close
// events of the Activities the reset interrupts and the reapplied external
// inputs, creates the new Run's Activity and Timer states carried over from
// the fork point, and points the parent's open child link at the new Run, all
// in one transaction: a failed reset leaves the replaced Runs
// running. The replaced Runs do not report ChildWorkflowTerminated to their
// parent; the new Run takes over the parent's child link.
//
// The base Run, the expected current Run and the replaced Runs are read with
// GetRunForUpdate in ascending Run ID order before anything is written, so two
// resets of one workflow take their state row locks in the same order. The new Run takes
// its effective policy from the base Run read under the lock.
func CommitResetRun(tx ExecutionTx, request ResetRunCommit) error {
	runIDs := []string{request.BaseRunID}
	if request.ExpectedCurrent != "" {
		runIDs = append(runIDs, request.ExpectedCurrent)
	}
	for _, replaced := range request.Terminate {
		runIDs = append(runIDs, replaced.RunID)
	}
	slices.Sort(runIDs)
	locked := make(map[string]*Run, len(runIDs))
	for _, runID := range slices.Compact(runIDs) {
		run, err := tx.GetRunForUpdate(runID)
		if err != nil {
			return err
		}
		locked[runID] = run
	}
	request.NewRun.inheritPolicy(locked[request.BaseRunID])
	for _, replaced := range request.Terminate {
		if _, err := Terminate(tx, locked[replaced.RunID], replaced.TreeID, replaced.BranchID, ResetTerminationReason); err != nil {
			return err
		}
	}
	if err := tx.CreateRun(request.NewRun); err != nil {
		return err
	}
	ok, err := tx.SetCurrentRunCAS(request.NewRun.Namespace, request.NewRun.WorkflowID, request.ExpectedCurrent, request.NewRun.ID)
	if err != nil {
		return err
	}
	if !ok {
		return ErrCurrentRunConflict
	}
	if err := tx.SetResetRunID(request.BaseRunID, request.NewRun.ID); err != nil {
		return err
	}
	events := append([]history.Event{request.FailedEvent}, request.Pending.Events...)
	for _, event := range append(events, request.ReapplyEvents...) {
		if _, err := tx.Append(request.NewRun.ID, request.TreeID, request.BranchID, event); err != nil {
			return err
		}
	}
	for _, state := range request.Pending.Activities {
		if err := tx.UpsertActivityState(state); err != nil {
			return err
		}
	}
	for _, state := range request.Pending.Timers {
		if err := tx.UpsertTimerState(state); err != nil {
			return err
		}
	}
	return repointChildLink(tx, request.NewRun, request.Now)
}

// repointChildLink 让父 Run 仍打开的子关联指向接替的子 Run：新 Run 结束时父 Run 收到的是它的结果。
func repointChildLink(tx ExecutionTx, child *Run, now time.Time) error {
	if child.ParentRunID == "" || child.ParentScheduledEventID == 0 {
		return nil
	}
	state, found, err := tx.GetChildWorkflowStateForUpdate(child.ParentRunID, child.ParentScheduledEventID)
	if err != nil || !found || state.IsTerminal() {
		return err
	}
	state.setChildRunID(child.ID)
	state.setUpdatedAt(now)
	return tx.UpsertChildWorkflowState(state)
}

// ResetReapplyEvents 选出 reset 点之后的外部输入（Signal 与 Update-Accepted），excludeTypes 排除指定类型。
func ResetReapplyEvents(events []history.Event, resetNodeID int64, excludeTypes []string) []history.Event {
	excludeSignal := slices.Contains(excludeTypes, string(ResetReapplyExcludeTypeEnums.Signal))
	excludeUpdate := slices.Contains(excludeTypes, string(ResetReapplyExcludeTypeEnums.Update))
	var eligible []history.Event
	for _, event := range events {
		if event.ID > resetNodeID && ((event.WorkflowSignaled != nil && !excludeSignal) ||
			(event.WorkflowUpdateAccepted != nil && !excludeUpdate)) {
			eligible = append(eligible, event)
		}
	}
	return eligible
}

// ResetPendingWork 是分叉点上未结束的 Activity 与 Timer 在新 Run 里的处理结果（对标 Temporal
// workflow_resetter 的 prepareResetWorkflow 与 failInflightActivity）：
//
//   - 未开始的 Activity 以新 Run 的 state 行保留并重新派发：Attempt 从 1 开始，ScheduledTime 是 reset 时刻，
//     Schedule-To-Start 与 Schedule-To-Close 都从这里重新起算。
//   - 已开始的 Activity（保留的 History 里有当前 attempt 的 ActivityStarted）在分叉点之后写 ActivityFailed，
//     failure 类型 ResetWorkflowError、不可重试，带上旧 Run 里最后一次心跳的详情。
//   - 已请求取消、开始只是 transient 的 Activity 写 ActivityCancelled 结束，不再派发：Tenuto 里未开始的
//     Activity 被取消时同事务结束，新 Run 按同一规则处理。
//   - 未触发的 Timer 以新 Run 的 state 行保留，Deadline 沿用原值；已过期的由引擎立即触发。
type ResetPendingWork struct {
	// Events 是写在 reset 标记之后、重新应用的外部输入之前的 Activity 关闭事件。
	Events     []history.Event
	Activities []*ActivityState
	Timers     []*TimerState
}

// resetPendingActivity 是保留的 History 里一个未关闭 Activity 的位置。
type resetPendingActivity struct {
	started         *history.Event // 当前 attempt 的 ActivityStarted；被 AttemptFailed 或 ActivityReset 结束的 attempt 不算
	cancelRequested int64          // ActivityCancelRequested 事件 ID
	attempt         int            // 分叉点上的 attempt：上一次 AttemptFailed 之后的下一次，ActivityReset 归 1
}

// PlanResetPendingWork 按保留的 History（截止 cutoff）找出未结束的 Activity 与 Timer，按 ResetPendingWork 的规则
// 构造新 Run 的 state 与要补写的事件。静态字段（类型、队列、目标 Worker、超时、重试策略、输入、Trace）与 Timer
// 的 Deadline 取自 base Run 对应的 state 行，心跳详情取 base Run 的最新值（可能晚于分叉点）。
func PlanResetPendingWork(events []history.Event, cutoff int64, base []*ActivityState, baseTimers []*TimerState, newRunID, reason string, now time.Time) (ResetPendingWork, error) {
	activities := map[int64]*resetPendingActivity{}
	timers := map[int64]bool{}
	for i := range events {
		event := &events[i]
		if event.ID > cutoff {
			continue
		}
		switch {
		case event.ActivityScheduled != nil:
			activities[event.ID] = &resetPendingActivity{attempt: 1}
		case event.ActivityStarted != nil:
			if pending := activities[event.ActivityStarted.ScheduledEventID]; pending != nil {
				pending.started = event
			}
		case event.ActivityAttemptFailed != nil:
			if pending := activities[event.ActivityAttemptFailed.ScheduledEventID]; pending != nil {
				pending.started = nil
				pending.attempt = history.NormalizeActivityAttempt(event.ActivityAttemptFailed.Attempt) + 1
			}
		case event.ActivityReset != nil:
			if pending := activities[event.ActivityReset.ScheduledEventID]; pending != nil {
				pending.started = nil
				pending.attempt = 1
			}
		case event.ActivityCancelRequested != nil:
			if pending := activities[event.ActivityCancelRequested.ScheduledEventID]; pending != nil {
				pending.cancelRequested = event.ID
			}
		case event.ActivityCompleted != nil:
			delete(activities, event.ActivityCompleted.ScheduledEventID)
		case event.ActivityFailed != nil:
			delete(activities, event.ActivityFailed.ScheduledEventID)
		case event.ActivityCancelled != nil:
			delete(activities, event.ActivityCancelled.ScheduledEventID)
		case event.TimerScheduled != nil:
			timers[event.ID] = true
		case event.TimerFired != nil:
			delete(timers, event.TimerFired.ScheduledEventID)
		case event.TimerCancelled != nil:
			delete(timers, event.TimerCancelled.ScheduledEventID)
		}
	}

	var work ResetPendingWork
	baseActivities := make(map[int64]*ActivityState, len(base))
	for _, state := range base {
		baseActivities[state.ScheduledEventID] = state
	}
	for _, id := range slices.Sorted(maps.Keys(activities)) {
		pending := activities[id]
		old, ok := baseActivities[id]
		if !ok {
			return ResetPendingWork{}, fmt.Errorf("reset: activity scheduled at event %d has no state", id)
		}
		switch {
		case pending.started != nil:
			work.Events = append(work.Events, resetActivityFailedEvent(old, pending.started, reason))
		case pending.cancelRequested > 0:
			work.Events = append(work.Events, history.NewActivityCancelledEvent(history.ActivityCancelledEvent{
				ActivityID: old.ActivityID, ScheduledEventID: id, CancelRequestedEventID: pending.cancelRequested,
				Attempt: pending.attempt, ActivityType: old.ActivityType, TaskQueue: old.TaskQueue,
			}))
		default:
			work.Activities = append(work.Activities, resetScheduledActivity(old, newRunID, now))
		}
	}

	baseTimerStates := make(map[int64]*TimerState, len(baseTimers))
	for _, state := range baseTimers {
		baseTimerStates[state.ScheduledEventID] = state
	}
	for _, id := range slices.Sorted(maps.Keys(timers)) {
		old, ok := baseTimerStates[id]
		if !ok {
			return ResetPendingWork{}, fmt.Errorf("reset: timer scheduled at event %d has no state", id)
		}
		work.Timers = append(work.Timers, &TimerState{
			RunID: newRunID, ScheduledEventID: id, TimerID: old.TimerID, Kind: old.Kind,
			Deadline: old.Deadline, Status: TimerStatusEnums.Scheduled,
			TraceContext: cloneStringMap(old.TraceContext), CreatedAt: now, UpdatedAt: now,
		})
	}
	return work, nil
}

// resetScheduledActivity 是未开始的 Activity 在新 Run 里的首个 attempt：调度时间是 reset 时刻。
func resetScheduledActivity(old *ActivityState, newRunID string, now time.Time) *ActivityState {
	return &ActivityState{
		RunID: newRunID, ScheduledEventID: old.ScheduledEventID,
		ActivityID: old.ActivityID, ActivityType: old.ActivityType, TaskQueue: old.TaskQueue,
		TargetWorkerIdentity: old.TargetWorkerIdentity,
		Attempt:              1, Stamp: 1, StartedEventID: EmptyEventID,
		Status: ActivityStatusEnums.Scheduled, ScheduledTime: now,
		ScheduleToStartTimeoutMs: old.ScheduleToStartTimeoutMs,
		StartToCloseTimeoutMs:    old.StartToCloseTimeoutMs,
		ScheduleToCloseTimeoutMs: old.ScheduleToCloseTimeoutMs,
		HeartbeatTimeoutMs:       old.HeartbeatTimeoutMs,
		RetryPolicy:              old.RetryPolicy,
		Input:                    old.Input,
		TraceContext:             cloneStringMap(old.TraceContext),
		CreatedAt:                now, UpdatedAt: now,
	}
}

// resetActivityFailedEvent 是已开始的 Activity 被 reset 打断的终态失败（Temporal ResetWorkflowFailure）。
func resetActivityFailedEvent(old *ActivityState, started *history.Event, reason string) history.Event {
	message := reason
	if message == "" {
		message = "workflow reset"
	}
	failure := protocol.Failure{Message: message, Type: protocol.ResetWorkflowFailureType, NonRetryable: true}
	return history.NewActivityFailedEvent(history.ActivityFailedEvent{
		ActivityID: old.ActivityID, ScheduledEventID: old.ScheduledEventID,
		StartedEventID: started.ID, Attempt: history.NormalizeActivityAttempt(started.ActivityStarted.Attempt),
		ActivityType: old.ActivityType, TaskQueue: old.TaskQueue,
		Identity: started.ActivityStarted.WorkerIdentity, Error: message,
		RetryState: activityRetryStateNonRetryable, Failure: &failure,
		LastHeartbeatDetails: old.LastHeartbeat,
	})
}
