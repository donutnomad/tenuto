// Package activitytasks 是 Activity Worker 面向 Server 的任务用例：开始、心跳、取消检查、完成上报与交还。
package activitytasks

import (
	"context"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
)

type API interface {
	BeginActivityTask(ctx context.Context, task protocol.Task) (protocol.ActivityTaskBeginResult, error)
	RecordActivityHeartbeat(ctx context.Context, task protocol.Task, details map[string]any) (protocol.ActivityHeartbeatResult, error)
	CheckActivityTaskCancellation(ctx context.Context, task protocol.Task) (protocol.ActivityCancellationResult, error)
	// CompleteActivityTask 是异步完成的单入口：failure 为 nil 表示成功，非 nil 表示失败。
	CompleteActivityTask(ctx context.Context, task protocol.Task, output map[string]any, failure *protocol.Failure) error
	FailActivityTask(ctx context.Context, task protocol.Task, failure protocol.Failure) error
	// ReleaseActivityTask 交还一个已开始、因 Worker 停止而没有结果的 attempt，以同一个 attempt 重新派发。
	ReleaseActivityTask(ctx context.Context, task protocol.Task) error
}

// Backend 是用例依赖的 Server 能力。每个请求先经 TaskRun 读一次 Run 并校验 namespace，之后带 run 参数的方法用这一份
// Run，不再各读一次。
type Backend interface {
	GetWorkflowExecutionState(string) (*execution.WorkflowExecutionState, error)
	GetActivityState(string, int64) (*execution.ActivityState, bool, error)
	// TaskRun 读 task 寻址的 Run 并校验 task 的 namespace 属于它。
	TaskRun(protocol.Task) (*execution.Run, error)
	// StartActivityAttempt 记录 attempt started，返回 Server 记下的开始时间；只有写入了 started 的调用 started 为 true。
	// 调用方已确认 Run 未结束。
	StartActivityAttempt(task protocol.Task, run *execution.Run, startToCloseTimeout, heartbeatTimeout time.Duration) (time.Time, bool, error)
	CompleteActivityAttempt(ctx context.Context, run *execution.Run, scheduledEventID int64, attempt int, stamp int64, output map[string]any) error
	FailActivityAttempt(ctx context.Context, run *execution.Run, scheduledEventID int64, attempt int, stamp int64, failure protocol.Failure) error
	ReleaseActivityAttempt(ctx context.Context, run *execution.Run, scheduledEventID int64, attempt int, stamp int64) error
	HandleActivityHeartbeat(string, int64, int, int64, map[string]any) error
	// RefuseIncompatibleActivityClaim 对照 Run 当前 Program 的契约声明与领取 Worker 自己的上报：不兼容时不开始
	// 这次 attempt，按重派延迟重新投递任务，返回 true。
	RefuseIncompatibleActivityClaim(ctx context.Context, task protocol.Task, run *execution.Run) (bool, error)
	// PayloadSizeLimit 是 Activity 输出与心跳详情的字节上限，随开始结果告诉 Worker。
	PayloadSizeLimit() int
}

// ReasonWorkerContractMismatch 是领取 Activity 的 Worker 的契约与 Workflow 声明不兼容时 BeginActivityTask 的拒绝原因。
const ReasonWorkerContractMismatch = "worker_contract_mismatch"

type UseCase struct {
	backend Backend
}

func NewUseCase(backend Backend) *UseCase {
	return &UseCase{backend: backend}
}

func (s *UseCase) BeginActivityTask(ctx context.Context, task protocol.Task) (protocol.ActivityTaskBeginResult, error) {
	if err := validateRuntimeTask(task); err != nil {
		return protocol.ActivityTaskBeginResult{}, err
	}
	if err := execution.ValidateRequestID(task.RequestID); err != nil {
		return protocol.ActivityTaskBeginResult{}, err
	}
	run, err := s.backend.TaskRun(task)
	if err != nil {
		return protocol.ActivityTaskBeginResult{}, err
	}
	state, err := s.backend.GetWorkflowExecutionState(task.RunID)
	if err != nil {
		return protocol.ActivityTaskBeginResult{}, err
	}
	if state.IsTerminal() {
		return protocol.ActivityTaskBeginResult{Reason: "run_closed"}, nil
	}
	// Workflow 的取消请求不影响 Activity：Workflow 代码收到取消后可能还要调度清理 Activity，
	// 要取消的 Activity 由它的取消命令结束。

	// 可执行性按 ActivityState 判定。
	st, ok, err := s.backend.GetActivityState(task.RunID, task.ScheduledEventID)
	if err != nil {
		return protocol.ActivityTaskBeginResult{}, err
	}
	if !ok || st.IsTerminal() {
		return protocol.ActivityTaskBeginResult{Reason: "activity_closed"}, nil
	}
	if st.TargetWorkerIdentity != "" && st.TargetWorkerIdentity != task.WorkerIdentity {
		return protocol.ActivityTaskBeginResult{Reason: "worker_target_mismatch"}, protocol.InvalidRequestf("activity targets worker %q; caller is %q", st.TargetWorkerIdentity, task.WorkerIdentity)
	}
	if st.Stamp != task.Stamp || st.Attempt != history.NormalizeActivityAttempt(task.Attempt) {
		return protocol.ActivityTaskBeginResult{Reason: "stale_task"}, nil // stamp/attempt 不符 → 丢弃
	}
	// 已开始的 attempt 由 StartToClose 与 heartbeat 超时负责；队列重投的副本不再执行。开始它的那次 Begin 的重发
	// （应答在途中丢失）带着相同的 RequestID，得到原来的开始结果（Temporal recordActivityTaskStarted 的 RequestId）。
	if st.StartedEventID != execution.EmptyEventID {
		if task.RequestID != "" && task.RequestID == st.StartRequestID && st.StartedTime != nil {
			return protocol.ActivityTaskBeginResult{Execute: true, StartedTime: *st.StartedTime, PayloadSizeLimit: s.backend.PayloadSizeLimit()}, nil
		}
		return protocol.ActivityTaskBeginResult{Reason: "attempt_already_started"}, nil
	}
	// 任务不绑定 build，任何订阅队列的 Worker 都能领取：契约不兼容的 Worker 不开始 attempt，attempt 不消耗，
	// 这份投递由 Worker 确认丢弃，Server 稍后重新投递给其他 Worker。
	refused, err := s.backend.RefuseIncompatibleActivityClaim(ctx, task, run)
	if err != nil {
		return protocol.ActivityTaskBeginResult{}, err
	}
	if refused {
		return protocol.ActivityTaskBeginResult{Reason: ReasonWorkerContractMismatch}, nil
	}

	// Worker 本地持有执行 ctx；取消经心跳响应与 CheckActivityTaskCancellation 通知它。
	startedTime, started, err := s.backend.StartActivityAttempt(task, run, task.StartToCloseTimeout, task.HeartbeatTimeout)
	if err != nil {
		return protocol.ActivityTaskBeginResult{}, err
	}
	if !started {
		// 并发的另一份投递先记录了 started。
		return protocol.ActivityTaskBeginResult{Reason: "attempt_already_started"}, nil
	}
	// StartedTime 以 Server 记下的开始时间为准，Worker 据此算出本次 attempt 的截止时间。
	return protocol.ActivityTaskBeginResult{Execute: true, StartedTime: startedTime, PayloadSizeLimit: s.backend.PayloadSizeLimit()}, nil
}

func (s *UseCase) RecordActivityHeartbeat(ctx context.Context, task protocol.Task, details map[string]any) (protocol.ActivityHeartbeatResult, error) {
	if err := validateRuntimeTask(task); err != nil {
		return protocol.ActivityHeartbeatResult{}, err
	}
	if _, err := s.backend.TaskRun(task); err != nil {
		return protocol.ActivityHeartbeatResult{}, err
	}
	// 先记录心跳 details，再检查取消。即使 activity 即将被取消，最新的
	// heartbeat details 仍需持久化，以便重试时通过 LastHeartbeatDetails 恢复进度。
	if err := s.backend.HandleActivityHeartbeat(task.RunID, task.ScheduledEventID, task.Attempt, task.Stamp, details); err != nil {
		return protocol.ActivityHeartbeatResult{}, err
	}
	cancel, err := s.checkCancellation(task)
	if err != nil || cancel.CancelRequested {
		return protocol.ActivityHeartbeatResult(cancel), err
	}
	return protocol.ActivityHeartbeatResult{}, nil
}

func (s *UseCase) CheckActivityTaskCancellation(ctx context.Context, task protocol.Task) (protocol.ActivityCancellationResult, error) {
	if err := validateRuntimeTask(task); err != nil {
		return protocol.ActivityCancellationResult{}, err
	}
	if _, err := s.backend.TaskRun(task); err != nil {
		return protocol.ActivityCancellationResult{}, err
	}
	return s.checkCancellation(task)
}

// checkCancellation 按 execution state 与 Activity state 判定 task 的 attempt 是否应停止；调用方已校验 task。
func (s *UseCase) checkCancellation(task protocol.Task) (protocol.ActivityCancellationResult, error) {
	state, err := s.backend.GetWorkflowExecutionState(task.RunID)
	if err != nil {
		return protocol.ActivityCancellationResult{}, err
	}
	if state.IsTerminal() {
		reason := "run_closed"
		switch state.Status {
		case history.StatusCancelled:
			reason = "run_cancelled"
		case history.StatusTerminated:
			reason = "run_terminated"
		}
		return protocol.ActivityCancellationResult{CancelRequested: true, Reason: reason}, nil
	}
	// 取消按 ActivityState 判定：Workflow 的取消请求本身不取消 Activity，Workflow 代码取消它的作用域时
	// 才对其中的 Activity 发出取消命令。
	st, ok, err := s.backend.GetActivityState(task.RunID, task.ScheduledEventID)
	if err != nil {
		return protocol.ActivityCancellationResult{}, err
	}
	if !ok || st.IsTerminal() {
		return protocol.ActivityCancellationResult{CancelRequested: true, Reason: "activity_closed"}, nil
	}
	if st.Stamp != task.Stamp || st.Attempt != history.NormalizeActivityAttempt(task.Attempt) {
		return protocol.ActivityCancellationResult{CancelRequested: true, Reason: "stale_task"}, nil
	}
	if st.CancelRequested {
		return protocol.ActivityCancellationResult{CancelRequested: true, Reason: protocol.ActivityCancelRequestedReason}, nil
	}
	return protocol.ActivityCancellationResult{}, nil
}

// CompleteActivityTask 上报一次异步完成：failure 为 nil 表示成功，非 nil 表示失败。
// 失败是否成立只由 failure 决定——Message 为空串的失败仍是失败。
func (s *UseCase) CompleteActivityTask(ctx context.Context, task protocol.Task, output map[string]any, failure *protocol.Failure) error {
	if err := validateRuntimeTask(task); err != nil {
		return err
	}
	run, err := s.backend.TaskRun(task)
	if err != nil {
		return err
	}
	if failure == nil {
		return s.backend.CompleteActivityAttempt(ctx, run, task.ScheduledEventID, task.Attempt, task.Stamp, output)
	}
	return s.reportFailure(ctx, task, run, *failure)
}

func (s *UseCase) FailActivityTask(ctx context.Context, task protocol.Task, failure protocol.Failure) error {
	if err := validateRuntimeTask(task); err != nil {
		return err
	}
	run, err := s.backend.TaskRun(task)
	if err != nil {
		return err
	}
	return s.reportFailure(ctx, task, run, failure)
}

func (s *UseCase) ReleaseActivityTask(ctx context.Context, task protocol.Task) error {
	if err := validateRuntimeTask(task); err != nil {
		return err
	}
	run, err := s.backend.TaskRun(task)
	if err != nil {
		return err
	}
	return s.backend.ReleaseActivityAttempt(ctx, run, task.ScheduledEventID, task.Attempt, task.Stamp)
}

// reportFailure 是失败上报的唯一实现：入口校验后把完整 Failure 交给后端。
func (s *UseCase) reportFailure(ctx context.Context, task protocol.Task, run *execution.Run, failure protocol.Failure) error {
	// 入口校验复用既有 payload 上限（数量与字节）与 canonical 化：超限或不合法编码在这里明确失败。
	if err := failure.Validate(); err != nil {
		return protocol.InvalidRequestf("invalid activity failure: %w", err)
	}
	return s.backend.FailActivityAttempt(ctx, run, task.ScheduledEventID, task.Attempt, task.Stamp, failure)
}

func validateRuntimeTask(task protocol.Task) error {
	if task.RunID == "" {
		return protocol.InvalidRequestf("activity runtime task run id is empty")
	}
	if task.ScheduledEventID == 0 {
		return protocol.InvalidRequestf("activity runtime task scheduled event id is empty")
	}
	return nil
}
