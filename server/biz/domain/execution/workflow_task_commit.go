package execution

import (
	"fmt"
	"slices"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
)

type ChildWorkflowTarget struct {
	RunID             string
	WorkflowType      string
	ConfigVersion     string
	ParentClosePolicy protocol.ParentClosePolicy
}

type WorkflowTaskCommit struct {
	// Run 是调用方在本事务里用 GetRunForUpdate 读到的 Run。Run 的改动与 History 同事务写入：提交成功时 current 版本
	// 提升为 ProgramVersion、UpgradeError 清空；CompleteWorkflow 与 CancelWorkflow 把 Run 置为对应终态。
	Run           *Run
	TreeID        string
	BranchID      string
	CloseEvent    history.Event
	CommandEvents []history.Event
	Commands      []history.Command
	ChildTargets  map[int]ChildWorkflowTarget
	TraceContext  map[string]string
	Now           time.Time
	// SeenThroughEventID 是 Worker 做决策时已看到的最后一个事件 ID；增量激活路径提供它，全量路径为 0。
	SeenThroughEventID int64
	// ProgramVersion 是本次 Workflow Task 采用的 Program 版本。
	ProgramVersion string
}

// CommitWorkflowTask writes a workflow decision and every resulting mutable
// state in one transaction. Queue delivery and timers start after commit.
//
// A fact that lands after WorkflowTaskStarted was not part of the worker's
// decision. Committing the completion would place that fact inside this
// task's replay batch, and no later task would process it. The task is closed
// as failed instead and the caller schedules a new one.
//
// A Run that already holds a terminal event accepts no further decision: the
// result reports nothing inserted and History stays unchanged.
//
// CompleteWorkflow and CancelWorkflow close the Run in this transaction.
// FailWorkflow has no command event; the caller appends WorkflowFailed in the
// same transaction with AppendWorkflowFailed. ContinueAsNew leaves the Run to
// CommitContinuedRun, which the caller runs in the same transaction.
func CommitWorkflowTask(tx ExecutionTx, commit WorkflowTaskCommit) (WorkflowTaskCompletionResult, error) {
	run := commit.Run
	if run == nil {
		return WorkflowTaskCompletionResult{}, fmt.Errorf("workflow task commit requires the run")
	}
	state, err := tx.GetWorkflowExecutionStateForUpdate(run.ID)
	if err != nil {
		return WorkflowTaskCompletionResult{}, err
	}
	if state.IsTerminal() {
		return WorkflowTaskCompletionResult{}, nil
	}
	scheduledEventID := commit.CloseEvent.FlatScheduledEventID()
	superseded := state.WorkflowTaskSuperseded(scheduledEventID)
	if commit.SeenThroughEventID > 0 {
		// Worker 报告了它看到的位置：只有在那之后落库的事实才是它没处理的。
		superseded = state.WorkflowTaskOpen(scheduledEventID) && state.LastEventID > commit.SeenThroughEventID
	}
	if superseded {
		_, inserted, err := tx.AppendWorkflowTaskClosed(run.ID, commit.TreeID, commit.BranchID, history.NewWorkflowTaskFailedEvent(history.WorkflowTaskFailedEvent{
			ScheduledEventID: scheduledEventID,
			Cause:            WorkflowTaskFailedCauseUnhandledEvents,
			Reason:           "history changed after the workflow task started",
			TraceContext:     cloneStringMap(commit.TraceContext),
		}))
		return WorkflowTaskCompletionResult{Superseded: inserted}, err
	}
	if err := checkActivityIDs(state, commit.Commands); err != nil {
		return WorkflowTaskCompletionResult{}, err
	}
	resolveCancelTargets(state, commit.Commands, commit.CommandEvents)
	result, err := tx.AppendWorkflowTaskCompleted(run.ID, commit.TreeID, commit.BranchID, commit.CloseEvent, commit.CommandEvents)
	if err != nil || !result.ClosedInserted {
		return result, err
	}
	runChanged := run.adoptProgramVersion(commit.ProgramVersion)
	if slices.ContainsFunc(commit.Commands, func(command history.Command) bool { return command.Type == history.CmdUpsertSearchAttributes }) {
		if err := tx.SyncRunSearchAttributes(run.ID); err != nil {
			return WorkflowTaskCompletionResult{}, err
		}
	}
	now := commit.Now
	actions, err := WorkflowTaskActions(commit.Commands, result)
	if err != nil {
		return WorkflowTaskCompletionResult{}, err
	}
	for _, action := range actions {
		i, command := action.Index, action.Command
		if command.Type == history.CmdFailWorkflow {
			continue
		}
		inserted := action.Event
		if !inserted.Inserted {
			continue
		}
		switch command.Type {
		case history.CmdCompleteWorkflow:
			err = run.MarkTerminal(history.StatusCompleted, command.Output)
			runChanged = true
		case history.CmdCancelWorkflow:
			err = run.MarkTerminal(history.StatusCancelled, nil)
			runChanged = true
		case history.CmdScheduleActivity:
			err = tx.UpsertActivityState(&ActivityState{
				RunID:                    run.ID,
				ScheduledEventID:         inserted.EventID,
				ActivityID:               command.ActivityID,
				ActivityType:             command.ActivityType,
				TaskQueue:                command.TaskQueue,
				TargetWorkerIdentity:     command.TargetWorkerIdentity,
				Attempt:                  1,
				Stamp:                    1,
				StartedEventID:           EmptyEventID,
				Status:                   ActivityStatusEnums.Scheduled,
				ScheduledTime:            now,
				ScheduleToStartTimeoutMs: command.ScheduleToStartTimeout.Milliseconds(),
				StartToCloseTimeoutMs:    command.StartToCloseTimeout.Milliseconds(),
				ScheduleToCloseTimeoutMs: command.ScheduleToCloseTimeout.Milliseconds(),
				HeartbeatTimeoutMs:       command.HeartbeatTimeout.Milliseconds(),
				RetryPolicy:              activityRetryPolicy(command),
				Input:                    command.Input,
				TraceContext:             cloneStringMap(commit.TraceContext),
				CreatedAt:                now,
				UpdatedAt:                now,
			})
		case history.CmdScheduleTimer:
			err = tx.UpsertTimerState(&TimerState{
				RunID:            run.ID,
				ScheduledEventID: inserted.EventID,
				TimerID:          command.TimerID,
				Kind:             TimerKindEnums.UserTimer,
				Deadline:         now.Add(command.Delay),
				Status:           TimerStatusEnums.Scheduled,
				TraceContext:     cloneStringMap(commit.TraceContext),
				CreatedAt:        now,
				UpdatedAt:        now,
			})
		case history.CmdRequestCancelActivity:
			var cancelled bool
			scheduledEventID := commit.CommandEvents[action.EventIndex].ActivityCancelRequested.ScheduledEventID
			cancelled, err = requestActivityCancellation(tx, commit, scheduledEventID, inserted.EventID)
			if cancelled {
				result.CancelledActivities = append(result.CancelledActivities, scheduledEventID)
			}
		case history.CmdCancelTimer:
			err = cancelTimer(tx, run.ID, commit.CommandEvents[action.EventIndex].TimerCancelled.ScheduledEventID, now)
		case history.CmdRequestCancelChildWorkflow:
			err = requestChildCancellation(tx, run.ID, commit.CommandEvents[action.EventIndex].ChildWorkflowCancelRequested.ScheduledEventID, now)
		case history.CmdStartChildWorkflow:
			target, ok := commit.ChildTargets[i]
			if !ok {
				return WorkflowTaskCompletionResult{}, fmt.Errorf("child workflow target missing for %s", command.ChildID)
			}
			err = tx.UpsertChildWorkflowState(&ChildWorkflowState{
				RunID:                 run.ID,
				ScheduledEventID:      inserted.EventID,
				ChildID:               command.ChildID,
				ChildRunID:            target.RunID,
				WorkflowType:          target.WorkflowType,
				WorkflowConfigVersion: target.ConfigVersion,
				Status:                ChildWorkflowStatusEnums.Initiated,
				ParentClosePolicy:     target.ParentClosePolicy,
				Input:                 command.Input,
				CreatedAt:             now,
				UpdatedAt:             now,
			})
		}
		if err != nil {
			return WorkflowTaskCompletionResult{}, err
		}
	}
	if runChanged {
		if err := tx.UpdateRun(run); err != nil {
			return WorkflowTaskCompletionResult{}, err
		}
	}
	if history.IsTerminalStatus(run.Status) {
		// 完成或取消 Workflow 的命令关闭了 Run：它仍未触发的 timer 随之关闭（含本批命令新建的）。
		if err := tx.CloseScheduledTimers(run.ID); err != nil {
			return WorkflowTaskCompletionResult{}, err
		}
	}
	return result, nil
}

// checkActivityIDs 拒绝与仍未关闭的 Activity 重复的 ActivityID（对标 Temporal 的 SCHEDULE_ACTIVITY_DUPLICATE_ID）：
// Run 的 open waits 里的 Activity 与本批排在前面的调度命令都算未关闭，已请求取消、尚未确认的也算。前一个
// Activity 关闭（完成、失败、超时或取消确认）之后同一 ID 可以再用。open waits 与 Worker 决策时看到的
// 历史一致（之后落库的事实使提交按 superseded 失败），所以判定是确定的。
func checkActivityIDs(state *WorkflowExecutionState, commands []history.Command) error {
	open := make(map[string]bool)
	for _, wait := range state.OpenWaits {
		if wait.Type == WaitingActivity {
			open[wait.ActivityID] = true
		}
	}
	for _, command := range commands {
		if command.Type != history.CmdScheduleActivity {
			continue
		}
		if open[command.ActivityID] {
			return fmt.Errorf("%w: activity %q (type %s): an activity with this ActivityID is still open in this run",
				ErrScheduleActivityDuplicateID, command.ActivityID, command.ActivityType)
		}
		open[command.ActivityID] = true
	}
	return nil
}
