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
	// DefaultReconcileLookahead 是默认周期补偿间隔（30s）下的 lookahead：两个间隔内到期的定时器在到期前至少被一轮挂上。
	DefaultReconcileLookahead = time.Minute
	// runScanPageSize 是全量扫描 Running Run 时每页的 Run 数（见 forEachRunningRunPage）。
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

// FireOverdueTimers 触发持久化状态已过期超过 grace 的定时器与超时，接管已下线副本持有的进程内定时器，
// 并重投派发后迟迟没有被领取的任务。只处理 ListRecoveryRunIDs(now-grace) 选出的 Run，代价与到期或待处理的
// 事项数成正比。
//
// 多副本可同时扫描：重复触发由各 handler 的校验收敛（durable timer 与 Run 超时校验持久化状态，
// Activity 超时在事务内校验 attempt/stamp 与本次超时类型的持久化截止时间，Workflow Task 超时在事务内
// 校验该 WFT 仍 open 且持久化的截止时间已过）。单个 Run 失败不影响其余 Run，
// 未完成的触发在下一轮重新扫描。
func (s *Server) FireOverdueTimers(ctx context.Context, grace time.Duration) (OverdueTimerResult, error) {
	var result OverdueTimerResult
	runIDs, err := s.mgr.ListRecoveryRunIDs(s.clock.Now().Add(-grace))
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

// Reconcile 是周期补偿：只处理 ListRecoveryRunIDs(now+lookahead) 选出的 Run，即截止时间、timer 或 Activity
// 超时在 lookahead 内到期，或有 Scheduled Activity、open 或待调度的 WFT、blocked 的 Running Run。对每个 Run 先做
// reconcileRun 的补偿（补投任务、把 lookahead 内到期的定时器挂到本进程），再做 FireOverdueTimers 的到期接管。
// 代价与到期或待处理的事项数成正比，与 Running Run 总数无关。lookahead 不短于两次调用的间隔时，定时器在到期前
// 至少被一轮挂上，下线副本持有的定时器仍按原截止时间触发。
//
// 没有到期索引的崩溃窗口（子 Run 补建、终态写回、父关闭策略、子 Run 取消请求补发）不在这里处理，
// 由低频的全量补偿 DispatchPendingTasks 兜底。Run 截止时间、timer 与 Activity 超时在进入 lookahead 时由这里挂到
// 本进程，启动恢复不挂更晚到期的定时器。
func (s *Server) Reconcile(ctx context.Context, grace, lookahead time.Duration) (dispatch.WorkflowDispatchResult, OverdueTimerResult, error) {
	var dispatched dispatch.WorkflowDispatchResult
	var overdue OverdueTimerResult
	horizon := s.clock.Now().Add(lookahead)
	runIDs, err := s.mgr.ListRecoveryRunIDs(horizon)
	errs := []error{err}
	for _, runID := range runIDs {
		if err := ctx.Err(); err != nil {
			errs = append(errs, err)
			break
		}
		result, err := s.reconcileRun(ctx, runID, false, horizon)
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
