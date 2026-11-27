package executor

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"time"

	"github.com/donutnomad/tenuto/internal/runtimectx"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/sdk/activity"
	"github.com/donutnomad/tenuto/sdk/apperror"
	"github.com/google/uuid"
)

type ActivityRuntime interface {
	BeginActivityTask(context.Context, protocol.Task) (protocol.ActivityTaskBeginResult, error)
	RecordActivityHeartbeat(context.Context, protocol.Task, map[string]any) (protocol.ActivityHeartbeatResult, error)
	CompleteActivityTask(context.Context, protocol.Task, map[string]any, *protocol.Failure) error
	FailActivityTask(context.Context, protocol.Task, protocol.Failure) error
	// ReleaseActivityTask 交还一个已开始、因 Worker 停止而没有结果的 attempt：Server 以同一个
	// attempt 重新派发它，不记失败、不消耗重试次数。
	ReleaseActivityTask(context.Context, protocol.Task) error
}

// errActivityCancelRequested 是 Workflow 请求取消这个 Activity 时 Activity context 的取消原因。
var errActivityCancelRequested = errors.New("activity cancellation requested")

// errActivityAbandoned 是心跳得知结果不再有接收方（Run 已关闭、attempt 已过期或 Activity 已结束）时
// Activity context 的取消原因。
var errActivityAbandoned = errors.New("activity result abandoned")

const (
	// reportWindow 是 Activity 结束后上报结果的期限上限：Server 滚动重启期间结果仍能送达。
	reportWindow = time.Minute
	// reportMinimum 是上报期限的下限：attempt 截止时间已到或将到时仍发出上报，并容纳本地与 Server 的时钟偏差。
	reportMinimum = 5 * time.Second
)

// reportTimeout 是 Activity 在 now 结束后上报结果的期限：min(attempt 剩余时间, reportWindow)，不短于 reportMinimum。
// attempt 截止之后 Server 不再接受这次 attempt 的结果。deadline 为零表示 attempt 没有截止时间。
func reportTimeout(deadline, now time.Time) time.Duration {
	timeout := reportWindow
	if !deadline.IsZero() {
		timeout = min(timeout, deadline.Sub(now))
	}
	return max(timeout, reportMinimum)
}

// reportContext 是上报结果用的 context：Worker 停止取消 ctx 之后，已结束的 Activity 仍要把结果送到 Server。
func reportContext(ctx context.Context, deadline time.Time) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), reportTimeout(deadline, time.Now()))
}

// RunActivityTask executes one leased Activity and reports the result to the
// Run runtime. The caller owns queue ack/nack and task telemetry.
//
// started runs once the Server has recorded the attempt as started and before the
// Activity function runs. The caller acknowledges the queue lease there: from that
// point the attempt's liveness is governed by its StartToClose and heartbeat
// timeouts, and a redelivered copy of the task is told not to execute.
//
// ctx is the worker's execution context. When it is cancelled (the worker's graceful
// stop timed out), or the Activity returns activity.ErrWorkerStopping after the
// worker's stop channel closed, the attempt is released instead of reported.
// After a heartbeat delivered the Workflow's cancellation request, an Activity
// that returns a canceled error or context.Canceled reports the cancellation
// (Temporal RespondActivityTaskCanceled) as a failure of type CanceledError with
// the error's details; a result or another error is reported as usual. An
// Activity whose heartbeat said nobody awaits its result reports nothing.
//
// The Activity's ctx expires at the attempt deadline, the earlier of
// StartToClose and ScheduleToClose. An attempt that ends after its ctx expired
// reports nothing: the Server's timeout decides the attempt, so the Workflow
// receives a TimeoutError (Temporal Go SDK behaves the same).
//
// Heartbeats are throttled (see heartbeater). Results are reported within
// min(the attempt's remaining time, 60s), at least 5s, so a Server restart
// during that window does not drop them.
func RunActivityTask(ctx context.Context, task protocol.Task, runtime ActivityRuntime, activities *activity.Registry, started func()) (outcome string, executionErr, reportErr error) {
	if task.RequestID == "" {
		// 每份投递一个 Begin 请求标识：应答丢失后的重发由 Server 认作同一次开始。
		task.RequestID = uuid.NewString()
	}
	begin, err := runtime.BeginActivityTask(ctx, task)
	if err != nil {
		return "error", nil, err
	}
	if !begin.Execute {
		return "skipped", nil, nil
	}
	if started != nil {
		started()
	}
	info := activityInfo(task, begin.StartedTime)

	fn, err := activities.Get(task.ActivityType)
	if err != nil {
		reportCtx, cancelReport := reportContext(ctx, info.Deadline)
		defer cancelReport()
		return "failed", err, runtime.FailActivityTask(reportCtx, task, activityFailure(err))
	}

	activityCtx, cancel := activityExecutionContext(ctx, info.Deadline)
	defer cancel(nil)
	heartbeats := newHeartbeater(activityCtx, cancel, runtime, task, begin.PayloadSizeLimit)
	activityCtx = runtimectx.WithHeartbeat(activityCtx, heartbeats.record)
	if task.LastHeartbeatDetails != nil {
		activityCtx = runtimectx.WithLastHeartbeat(activityCtx, task.LastHeartbeatDetails)
	}
	activityCtx = runtimectx.WithActivityInfo(activityCtx, info)

	output, err, panicked := invokeActivity(activityCtx, fn, task)
	if errors.Is(activityCtx.Err(), context.DeadlineExceeded) && ctx.Err() == nil {
		// attempt 已到截止时间：不上报结果，由 Server 的超时计时写 ActivityTimedOut，Workflow 稳定收到超时错误
		// （对标 Temporal Go SDK 的 activityTaskHandlerImpl.Execute）。
		heartbeats.stop()
		return "timed_out", context.DeadlineExceeded, nil
	}
	reportCtx, cancelReport := reportContext(ctx, info.Deadline)
	defer cancelReport()
	if err == nil && panicked == nil {
		// 完成结果取代心跳 details，窗口内未发出的心跳不再发出。
		heartbeats.stop()
		if limitErr := protocol.CheckPayloadSize("activity result", output, begin.PayloadSizeLimit); limitErr != nil {
			// 结果超过 Server 的载荷上限：上报不可重试的失败，不发送这个结果。
			return "failed", limitErr, runtime.FailActivityTask(reportCtx, task, protocol.PayloadSizeLimitFailure(limitErr))
		}
		if err := runtime.CompleteActivityTask(reportCtx, task, output, nil); errors.Is(err, protocol.ErrInvalidRequest) {
			// Server 以不可重试的 4xx 拒绝这个结果：改报一次不可重试的失败，attempt 不必等到 StartToClose 超时后
			// 重跑同样的副作用。
			return "failed", err, runtime.FailActivityTask(reportCtx, task, protocol.Failure{
				Message: "activity result rejected by server: " + err.Error(), Type: protocol.FailureTypeActivityResultRejected, NonRetryable: true,
			})
		} else if err != nil {
			return "success", nil, err
		}
		return "success", nil, nil
	}
	cause := context.Cause(activityCtx)
	if errors.Is(cause, errActivityAbandoned) {
		heartbeats.stop()
	} else {
		// 下一次 attempt 从最后的心跳 details 恢复进度：窗口内未发出的心跳先发出。
		heartbeats.flush(reportCtx)
	}
	if panicked != nil {
		// panic 与返回 error 走同一条失败上报，重试策略照常生效。
		return "failed", errors.New(panicked.Message), runtime.FailActivityTask(reportCtx, task, *panicked)
	}
	if errors.Is(err, activity.ErrPending) {
		return "pending", nil, nil
	}
	if errors.Is(cause, errActivityCancelRequested) && (errors.Is(err, context.Canceled) || apperror.IsCanceled(err)) {
		return "cancelled", err, runtime.FailActivityTask(reportCtx, task, canceledFailure(err))
	} else if errors.Is(cause, errActivityAbandoned) {
		// Run 已关闭，或这次 attempt 已超时、被重置：结果不再有接收方。
		return "cancelled", err, nil
	}
	if stoppedByWorker(ctx, err) {
		return "released", err, runtime.ReleaseActivityTask(reportCtx, task)
	}
	// Activity 实现是原生 Go：返回 apperror 时结构化成 Failure（type/nonRetryable/details/cause），
	// 普通 error 只带 message 与 Go 类型名。
	return "failed", err, runtime.FailActivityTask(reportCtx, task, activityFailure(err))
}

// activityInfo 是交给 Activity 的本次 attempt 信息。Deadline 取 StartToCloseTimeout 与
// ScheduleToCloseTimeout 中先到的一个，两个时间点都按 Server 记录的时间计算。
func activityInfo(task protocol.Task, startedTime time.Time) runtimectx.ActivityInfo {
	info := runtimectx.ActivityInfo{
		Namespace:        protocol.NormalizeNamespace(task.Namespace),
		WorkflowID:       task.WorkflowID,
		WorkflowType:     task.WorkflowType,
		RunID:            task.RunID,
		ActivityID:       task.ActivityID,
		ActivityType:     task.ActivityType,
		TaskQueue:        task.TaskQueue,
		Attempt:          max(task.Attempt, 1),
		ScheduledTime:    task.ScheduledTime,
		StartedTime:      startedTime,
		HeartbeatTimeout: task.HeartbeatTimeout,
		TaskToken:        string(task.TaskToken),
	}
	if task.StartToCloseTimeout > 0 && !startedTime.IsZero() {
		info.Deadline = startedTime.Add(task.StartToCloseTimeout)
	}
	if task.ScheduleToCloseTimeout > 0 && !task.ScheduledTime.IsZero() {
		if deadline := task.ScheduledTime.Add(task.ScheduleToCloseTimeout); info.Deadline.IsZero() || deadline.Before(info.Deadline) {
			info.Deadline = deadline
		}
	}
	return info
}

// stoppedByWorker 判断一个返回 error 的 Activity 是否因 Worker 停止而结束：Worker 的执行 context
// 已取消，或者 Worker 已开始停止且 Activity 返回 activity.ErrWorkerStopping。
func stoppedByWorker(ctx context.Context, err error) bool {
	if ctx.Err() != nil {
		return true
	}
	stop := runtimectx.WorkerStopFrom(ctx)
	if stop == nil || !errors.Is(err, activity.ErrWorkerStopping) {
		return false
	}
	select {
	case <-stop:
		return true
	default:
		return false
	}
}

// invokeActivity 运行 Activity 实现。实现 panic 时返回 Type 为 PanicError 的 Failure：
// message 带 panic 值，details 带调用栈，worker 进程继续处理后续任务。
func invokeActivity(ctx context.Context, fn protocol.ActivityFunc, task protocol.Task) (output map[string]any, err error, panicked *protocol.Failure) {
	defer func() {
		recovered := recover()
		if recovered == nil {
			return
		}
		stack := string(debug.Stack())
		panicked = &protocol.Failure{Type: "PanicError", Message: fmt.Sprintf("activity panicked: %v", recovered)}
		if payload, encodeErr := protocol.NewJSONPayload(stack); encodeErr == nil {
			panicked.Details = protocol.Payloads{payload}
		}
	}()
	output, err = fn(ctx, task.Input)
	return output, err, nil
}

// activityExecutionContext 是 Activity 的 ctx：截止时间是 info.Deadline（StartToClose 与 ScheduleToClose 中先到的一个，
// 对标 Temporal Go SDK），没有截止时间时只能被取消。
func activityExecutionContext(ctx context.Context, deadline time.Time) (context.Context, context.CancelCauseFunc) {
	ctx, cancel := context.WithCancelCause(ctx)
	if deadline.IsZero() {
		return ctx, cancel
	}
	ctx, stop := context.WithDeadline(ctx, deadline)
	// 先取消外层：取消原因经父 context 传给 deadline context，context.Cause 才读得到它。
	return ctx, func(cause error) {
		cancel(cause)
		stop()
	}
}

// canceledFailure 是 Activity 确认取消的上报：取消错误带它的 details，context.Canceled 没有 details。
func canceledFailure(err error) protocol.Failure {
	if canceled := apperror.CanceledIn(err); canceled != nil {
		if failure, encodeErr := canceled.ToFailure(apperror.JSONEncoder); encodeErr == nil {
			return failure
		}
	}
	return protocol.Failure{Message: "canceled", Type: protocol.CanceledFailureType, NonRetryable: true}
}

// activityFailure 把 Activity 返回的原生 Go error 编码成共享 Failure。
func activityFailure(err error) protocol.Failure {
	failure, encodeErr := apperror.Encode(err, apperror.JSONEncoder)
	if encodeErr != nil {
		return protocol.Failure{Message: err.Error()}
	}
	return failure
}
