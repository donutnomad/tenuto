package execution

import (
	"time"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
)

// 取消规则对标 Temporal（service/history 的 workflowTaskCompletedHandler 与 MutableStateImpl）：
//
//   - RequestCancelActivity 记录 ActivityCancelRequested 并给 Activity 打上 CancelRequested。尚未开始的
//     attempt（含重试退避中的）在同一事务里以 ActivityCancelled 结束；已开始的 attempt 经心跳得知取消，
//     Worker 确认取消（Type 为 CanceledError 的失败上报）时以 ActivityCancelled 结束。确认之前到达的
//     完成仍记为 ActivityCompleted；失败与超时不再重试，以 retryState CancelRequested 结束；
//     Worker 停止交还的 attempt 不再派发，以 ActivityCancelled 结束。已结束的 Activity 只记录请求事件。
//   - CancelTimer 记录 TimerCancelled，timer 不再触发。
//   - RequestCancelChildWorkflow 记录 ChildWorkflowCancelRequested，子 Run 在提交后收到取消请求，
//     子 Workflow 的代码决定它的终态。

// ActivityCancelRetryState 是取消请求之后结束的 attempt 不再重试时 ActivityFailed/ActivityTimedOut 的 retryState。
const ActivityCancelRetryState = "CancelRequested"

// resolveCancelTargets 用 Run 的 open waits 把取消命令的事件补全为目标的调度事件 ID（子 Workflow 另补子 Run ID）。
// Worker 只知道 ActivityID、timer ID 与 child ID。目标已不在 open waits 里（已结束）时事件的调度事件 ID 为 0，
// 提交只记录这条请求。
func resolveCancelTargets(state *WorkflowExecutionState, commands []history.Command, events []history.Event) {
	eventIndex := 0
	for _, command := range commands {
		if command.Type == history.CmdFailWorkflow {
			continue
		}
		if eventIndex >= len(events) {
			return
		}
		event := events[eventIndex]
		eventIndex++
		for id, wait := range state.OpenWaits {
			switch {
			case event.ActivityCancelRequested != nil && wait.Type == WaitingActivity && wait.ActivityID == command.ActivityID:
				event.ActivityCancelRequested.ScheduledEventID = id
			case event.TimerCancelled != nil && wait.Type == WaitingTimer && wait.TimerID == command.TimerID:
				event.TimerCancelled.ScheduledEventID = id
			case event.ChildWorkflowCancelRequested != nil && wait.Type == WaitingChildWorkflow && wait.ChildID == command.ChildID:
				event.ChildWorkflowCancelRequested.ScheduledEventID = id
				event.ChildWorkflowCancelRequested.ChildRunID = wait.ChildRunID
			}
		}
	}
}

// requestActivityCancellation 处理已提交的 ActivityCancelRequested：cancelled 为 true 时 Activity 尚未开始，
// 已在本事务里以 ActivityCancelled 结束，调用方停掉它的 timer 并调度 Workflow Task。
func requestActivityCancellation(tx ExecutionTx, commit WorkflowTaskCommit, scheduledEventID, cancelRequestedEventID int64) (cancelled bool, err error) {
	if scheduledEventID == 0 {
		return false, nil
	}
	state, ok, err := tx.GetActivityStateForUpdate(commit.Run.ID, scheduledEventID)
	if err != nil || !ok || state.IsTerminal() {
		return false, err
	}
	state.setCancelRequested(true)
	state.setUpdatedAt(commit.Now)
	if state.IsScheduled() {
		transition := ActivityTransition{
			RunID: commit.Run.ID, TreeID: commit.TreeID, BranchID: commit.BranchID,
			ScheduledEventID: scheduledEventID, Attempt: state.Attempt, Stamp: state.Stamp, Now: commit.Now,
		}
		return true, cancelActivity(tx, transition, state, cancelRequestedEventID, nil)
	}
	return false, tx.UpdateActivityState(state)
}

// cancelActivity 以 ActivityCancelled 结束 Activity。
func cancelActivity(tx ExecutionTx, transition ActivityTransition, state *ActivityState, cancelRequestedEventID int64, details protocol.Payloads) error {
	if state.StartedEventID == TransientEventID {
		if err := backfillTransientStarted(tx, transition, state); err != nil {
			return err
		}
	}
	_, _, err := tx.AppendIdempotent(transition.RunID, transition.TreeID, transition.BranchID, history.NewActivityCancelledEvent(history.ActivityCancelledEvent{
		ActivityID: state.ActivityID, ScheduledEventID: state.ScheduledEventID,
		StartedEventID: state.StartedEventID, CancelRequestedEventID: cancelRequestedEventID,
		Attempt:      history.NormalizeActivityAttempt(state.Attempt),
		ActivityType: state.ActivityType, TaskQueue: state.TaskQueue,
		Identity: state.WorkerIdentity, Details: details,
	}))
	if err != nil {
		return err
	}
	state.setStatus(ActivityStatusEnums.Cancelled)
	state.setUpdatedAt(transition.Now)
	return tx.UpdateActivityState(state)
}

// cancelTimer 处理已提交的 TimerCancelled：timer 不再触发。
func cancelTimer(tx ExecutionTx, runID string, scheduledEventID int64, now time.Time) error {
	if scheduledEventID == 0 {
		return nil
	}
	state, ok, err := tx.GetTimerStateForUpdate(runID, scheduledEventID)
	if err != nil || !ok || state.IsTerminal() {
		return err
	}
	state.setStatus(TimerStatusEnums.Cancelled)
	state.setUpdatedAt(now)
	return tx.UpdateTimerState(state)
}

// requestChildCancellation 处理已提交的 ChildWorkflowCancelRequested：记下请求，子 Run 的取消请求在提交后发出。
func requestChildCancellation(tx ExecutionTx, runID string, scheduledEventID int64, now time.Time) error {
	if scheduledEventID == 0 {
		return nil
	}
	state, ok, err := tx.GetChildWorkflowStateForUpdate(runID, scheduledEventID)
	if err != nil || !ok || state.IsTerminal() {
		return err
	}
	state.setCancelRequested(true)
	state.setUpdatedAt(now)
	return tx.UpsertChildWorkflowState(state)
}

// ChildCancelPending 报告父 Workflow 请求取消的子 Workflow 当前 Run 是否还没有收到取消请求：
// 父 Run 提交请求后、向子 Run 发出之前进程退出，或子 Run 在请求之后才建好。
func ChildCancelPending(link *ChildWorkflowState, child *WorkflowExecutionState) bool {
	return link != nil && link.CancelRequested && !link.IsTerminal() &&
		child != nil && !child.IsTerminal() && !child.CancelRequested
}
