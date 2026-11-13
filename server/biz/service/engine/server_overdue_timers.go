package engine

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/donutnomad/tenuto/protocol"

	"github.com/donutnomad/tenuto/server/biz/domain/dispatch"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
)

const (
	// DefaultOverdueTimerGrace 是到期扫描接管定时器前的等待时长，让持有内存定时器的存活副本优先触发。
	DefaultOverdueTimerGrace = 30 * time.Second
	// runScanPageSize 是按状态分页扫描 Run 的页大小，到期扫描与 reconcileRunningRuns 共用。
	runScanPageSize = 200
)

// OverdueTimerResult 统计一轮到期扫描发起的触发次数。
type OverdueTimerResult struct {
	RunTimeouts          int
	DurableTimers        int
	ActivityTimeouts     int
	WorkflowTaskTimeouts int
	// WorkflowTasks 统计退避到期的重调度与派发后没有被领取的 Workflow Task 重投；ActivityTasks 统计后者的 Activity。
	WorkflowTasks int
	ActivityTasks int
}

// FireOverdueTimers 扫描 Running 的 Run，触发持久化状态已过期超过 grace 的定时器与超时，
// 接管已下线副本持有的进程内定时器。
//
// 多副本可同时扫描：重复触发由各 handler 的校验收敛（durable timer 与 Run 超时校验持久化状态，
// Activity 超时在事务内校验 attempt/stamp 与本次超时类型的持久化截止时间，Workflow Task 超时在事务内
// 校验该 WFT 仍 open 且持久化的截止时间已过）。单个 Run 失败不影响其余 Run，
// 未完成的触发在下一轮重新扫描。
func (s *Server) FireOverdueTimers(ctx context.Context, grace time.Duration) (OverdueTimerResult, error) {
	var result OverdueTimerResult
	// 先收齐 ID 再逐个触发：触发会让 Run 离开 Running，边翻页边触发会漏掉 Run（见 runningRunIDs）。
	runIDs, err := s.runningRunIDs(ctx)
	errs := []error{err}
	for _, runID := range runIDs {
		if err := ctx.Err(); err != nil {
			errs = append(errs, err)
			break
		}
		run, err := s.mgr.Get(runID)
		if err == nil {
			err = s.fireOverdueForRun(ctx, run, grace, &result)
		}
		if err != nil {
			errs = append(errs, fmt.Errorf("fire overdue timers for run %s: %w", runID, err))
		}
	}
	return result, errors.Join(errs...)
}

// Reconcile 是周期补偿：一次扫描 Running 的 Run，对每个 Run 先做 DispatchPendingTasks 的补偿，再做
// FireOverdueTimers 的到期接管。两步的语义与单独调用相同，只是共用一份 Run 列表。
func (s *Server) Reconcile(ctx context.Context, grace time.Duration) (dispatch.WorkflowDispatchResult, OverdueTimerResult, error) {
	var dispatched dispatch.WorkflowDispatchResult
	var overdue OverdueTimerResult
	runIDs, err := s.runningRunIDs(ctx)
	errs := []error{err}
	for _, runID := range runIDs {
		if err := ctx.Err(); err != nil {
			errs = append(errs, err)
			break
		}
		result, err := s.reconcileRun(ctx, runID, false)
		if err != nil {
			errs = append(errs, fmt.Errorf("reconcile run %s: %w", runID, err))
		}
		dispatched.WorkflowTasks += result.WorkflowTasks
		dispatched.ActivityTasks += result.ActivityTasks
		run, err := s.mgr.Get(runID)
		if err == nil {
			err = s.fireOverdueForRun(ctx, run, grace, &overdue)
		}
		if err != nil {
			errs = append(errs, fmt.Errorf("fire overdue timers for run %s: %w", runID, err))
		}
	}
	s.telemetry.recordBacklog(ctx, dispatched.WorkflowTasks, dispatched.ActivityTasks)
	return dispatched, overdue, errors.Join(errs...)
}

// fireOverdueForRun 接管一个 Run 过期超过 grace 的定时器与超时，并重投派发后迟迟没有被领取的任务
// （见 redeliverUnclaimedTask）。
func (s *Server) fireOverdueForRun(ctx context.Context, run *execution.Run, grace time.Duration, result *OverdueTimerResult) error {
	now := s.clock.Now()
	cutoff := now.Add(-grace)
	if execution.WorkflowDeadlineExpired(run, cutoff) {
		s.handleWorkflowTimeout(run.ID)
		result.RunTimeouts++
		return nil
	}
	if execution.RunDeadlineExpired(run, cutoff) {
		s.handleRunTimeout(run.ID)
		result.RunTimeouts++
		return nil
	}
	timers, err := s.mgr.ListTimerStates(run.ID)
	if err != nil {
		return err
	}
	for _, timer := range timers {
		if execution.TimerRecoveryNeeded(timer) && timer.Deadline.Before(cutoff) {
			s.handleTimerFired(context.WithoutCancel(ctx), run.ID, timer.ScheduledEventID)
			result.DurableTimers++
		}
	}
	activities, err := s.mgr.ListActivityStates(run.ID)
	if err != nil {
		return err
	}
	for _, activity := range activities {
		if reason, overdue := execution.OverdueActivityTimeout(activity, cutoff); overdue {
			s.handleActivityTimeoutAttempt(run.ID, activity.ScheduledEventID, activity.Attempt, reason)
			result.ActivityTimeouts++
			continue
		}
		if execution.ActivityRecoveryFor(activity, now) == execution.ActivityRecoveryRedispatch {
			redelivered, err := s.redeliverUnclaimedTask(ctx, s.activityTaskFromState(activity), now, grace)
			if err != nil {
				return err
			}
			if redelivered {
				result.ActivityTasks++
			}
		}
	}
	state, err := s.mgr.GetWorkflowExecutionState(run.ID)
	if err != nil {
		return err
	}
	if scheduledEventID, deadline, ok := state.WorkflowTaskTimeoutDeadline(); ok && deadline.Before(cutoff) {
		s.handleWorkflowTaskTimeoutFired(context.WithoutCancel(ctx), run.ID, scheduledEventID)
		result.WorkflowTaskTimeouts++
		return nil
	}
	// ConfigError 暂停的 Run 等待人工修复，重调度不会产生 WFT。
	if run.ConfigError != "" {
		return nil
	}
	// open 且没有 started 的 WFT：它的投递可能已在队列里丢失。已 started 的由上面的 start-to-close 超时接管。
	if open := state.OpenWorkflowTaskEventID; open != 0 && state.WorkflowTask.StartedScheduledEventID != open {
		task, err := s.openWorkflowTask(run, open)
		if err != nil {
			return err
		}
		redelivered, err := s.redeliverUnclaimedTask(ctx, task, now, grace)
		if err != nil {
			return err
		}
		if redelivered {
			result.WorkflowTasks++
		}
		return nil
	}
	return s.fireOverdueWorkflowTask(ctx, run.ID, state, cutoff, result)
}

// redeliverUnclaimedTask 重投一个派发记录已是 dispatched、却超过等待时长仍没有被领取的任务（等待时长见
// dispatch.State.RedeliveryDue，从 grace 起按派发次数翻倍）。已投递的派发记录挡住重复派发，前提是队列里的
// 那份投递最终会被领取；投递在队列里丢失时，没有这一步任务就永远不再派发，Run 永久停住。调用方只传仍待领取的
// 任务。多出的投递在领取时按 state 丢弃：Workflow Task 已 started 或已关闭时由 Server 确认丢弃，Activity
// attempt 已 started 时跳过。
func (s *Server) redeliverUnclaimedTask(ctx context.Context, task protocol.Task, now time.Time, grace time.Duration) (bool, error) {
	record, found, err := s.dispatches.GetTaskDispatch(task)
	if err != nil || !found || !record.RedeliveryDue(now, grace) {
		return false, err
	}
	s.logger.Warn("task dispatched but not claimed; redelivering",
		slog.String("run_id", task.RunID),
		slog.String("task_type", string(task.Type)),
		slog.Int64("scheduled_event_id", task.ScheduledEventID),
		slog.Int("attempt", task.Attempt),
		slog.Time("last_dispatched_at", record.LastDispatchedAt),
		slog.Int("dispatches", record.Attempts),
	)
	if err := s.dispatches.ReleaseTaskDispatch(task, "dispatched but not claimed; redelivering"); err != nil {
		return false, err
	}
	ctx = context.WithoutCancel(ctx)
	if task.Type == protocol.TaskTypeActivity {
		return s.dispatchActivityTask(ctx, task)
	}
	return s.dispatchWorkflowTask(ctx, task)
}

// fireOverdueWorkflowTask 重调度退避已到期的失败 WFT。这类 Run 没有 open WFT，DispatchPendingTasks
// 不会补投，延迟 timer 丢失时由这里接管。是否 blocked、失败时间与退避档位都读 execution state。
func (s *Server) fireOverdueWorkflowTask(ctx context.Context, runID string, state *execution.WorkflowExecutionState, cutoff time.Time, result *OverdueTimerResult) error {
	if !state.Blocked.Blocked {
		return nil
	}
	var failedAt time.Time
	if state.WorkflowTask.LastFailedAt != nil {
		failedAt = *state.WorkflowTask.LastFailedAt
	}
	if !failedAt.Add(state.WorkflowTaskFailureBackoff(s.wftBackoffBase, s.wftBackoffCap)).Before(cutoff) {
		return nil
	}
	result.WorkflowTasks++
	ctx = s.telemetry.extract(context.WithoutCancel(ctx), state.WorkflowTask.LastFailureTraceContext)
	return s.scheduleWorkflowTaskIfRunning(ctx, runID)
}
