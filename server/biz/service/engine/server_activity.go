package engine

import (
	"context"
	"log/slog"
	"slices"
	"time"

	"github.com/donutnomad/tenuto/protocol/history"
	tenutotel "github.com/donutnomad/tenuto/telemetry"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/server/biz/domain/dispatch"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"go.opentelemetry.io/otel/attribute"
)

// runBranch 是 Run 的 History 分支；err 是 branch token 解不出时的错误，由调用方在用到分支的位置报出。
type runBranch struct {
	treeID, branchID string
	err              error
}

func branchOf(run *execution.Run) runBranch {
	var branch runBranch
	branch.treeID, branch.branchID, branch.err = runBranchInfoFromRun(run)
	return branch
}

// TaskRun 读 task 寻址的 Run 并校验 task 的 namespace 属于它。一次 Activity 运行时请求只读一次 Run，交给本次请求
// 后续的处理：namespace 与 History 分支在 Run 创建后不变，分支在事务外取出再传入事务，取值与事务内一致；Run 当前
// Program 版本只用于本次请求的契约对照，与此前在同一请求里另读一次的时效相同。
func (s *Server) TaskRun(task protocol.Task) (*execution.Run, error) {
	run, err := s.mgr.Get(task.RunID)
	if err != nil {
		return nil, err
	}
	if err := dispatch.ValidateTaskNamespace(task, run.Namespace); err != nil {
		return nil, err
	}
	return run, nil
}

// HandleActivityStartedWithMetadata 把 task 对应的 attempt 记为 started。返回 true 表示本次调用写入了
// started；Run 已结束、attempt 已开始、已结束或 task 已过期时返回 false。
func (s *Server) HandleActivityStartedWithMetadata(task protocol.Task, startToCloseTimeout, heartbeatTimeout time.Duration) (bool, error) {
	run, err := s.TaskRun(task)
	if err != nil {
		return false, err
	}
	if terminated, err := s.workflowAlreadyTerminated(task.RunID); err != nil || terminated {
		return false, err
	}
	_, started, err := s.startActivityAttempt(task, branchOf(run), startToCloseTimeout, heartbeatTimeout)
	return started, err
}

// StartActivityAttempt 把 task 对应的 attempt 记为 started，返回 Server 记下的开始时间。run 来自本次请求的 TaskRun，
// 只取它的 History 分支；调用方已读过 execution state 确认 Run 未结束。attempt 已开始、已结束或 task 已过期时
// started 为 false。
func (s *Server) StartActivityAttempt(task protocol.Task, run *execution.Run, startToCloseTimeout, heartbeatTimeout time.Duration) (time.Time, bool, error) {
	return s.startActivityAttempt(task, branchOf(run), startToCloseTimeout, heartbeatTimeout)
}

func (s *Server) startActivityAttempt(task protocol.Task, branch runBranch, startToCloseTimeout, heartbeatTimeout time.Duration) (startedTime time.Time, started bool, err error) {
	runID := task.RunID
	scheduledEventID := task.ScheduledEventID
	attempt := history.NormalizeActivityAttempt(task.Attempt)
	if branch.err != nil {
		return time.Time{}, false, branch.err
	}
	now := s.clock.Now()
	tid, bid := branch.treeID, branch.branchID
	txCtx := s.telemetry.extract(context.Background(), task.TraceContext)
	if err := s.runInTx(txCtx, "start_activity", func(tx execution.ExecutionTx) error {
		var err error
		started, err = execution.StartActivity(tx, execution.ActivityTransition{
			RunID: runID, TreeID: tid, BranchID: bid,
			ScheduledEventID: scheduledEventID, Attempt: attempt, Stamp: task.Stamp, Now: now,
		}, task.WorkerIdentity, task.WorkerBuildID, task.RequestID)
		return err
	}); err != nil {
		return time.Time{}, false, err
	}
	if !started {
		return time.Time{}, false, nil // stale / redelivery：state 未变，不动 timer
	}

	// 进入 Started：停 schedule_to_start，按本次 start 重排 start_to_close 与 heartbeat。
	s.timers.Stop(activityTimerRef(timerActivityScheduleToStart, runID, scheduledEventID))
	for kind, timeout := range map[timerKind]time.Duration{
		timerActivityStartToClose: startToCloseTimeout,
		timerActivityHeartbeat:    heartbeatTimeout,
	} {
		ref := activityTimerRef(kind, runID, scheduledEventID)
		if timeout > 0 {
			s.timers.Arm(ref, now.Add(timeout))
		} else {
			s.timers.Stop(ref)
		}
	}
	return now, true, nil
}

func (s *Server) HandleActivityCompleted(ctx context.Context, runID string, scheduledEventID int64, attempt int, stamp int64, output map[string]any) error {
	treeID, branchID, err := s.runBranchInfo(runID)
	if err != nil {
		return err
	}
	branch := runBranch{treeID: treeID, branchID: branchID}
	return s.completeActivityAttempt(ctx, runID, branch, scheduledEventID, attempt, stamp, output)
}

// CompleteActivityAttempt 记录一次成功完成。run 来自本次请求的 TaskRun，只取它的 History 分支。
func (s *Server) CompleteActivityAttempt(ctx context.Context, run *execution.Run, scheduledEventID int64, attempt int, stamp int64, output map[string]any) error {
	return s.completeActivityAttempt(ctx, run.ID, branchOf(run), scheduledEventID, attempt, stamp, output)
}

func (s *Server) completeActivityAttempt(ctx context.Context, runID string, branch runBranch, scheduledEventID int64, attempt int, stamp int64, output map[string]any) (err error) {
	if limitErr := protocol.CheckPayloadSize("activity result", output, s.payloadSizeLimit); limitErr != nil {
		// 结果超过载荷上限：照 Temporal RespondActivityTaskCompleted，改记为不可重试的失败。
		return s.failActivityAttempt(ctx, runID, branch, scheduledEventID, attempt, stamp, protocol.PayloadSizeLimitFailure(limitErr))
	}
	ctx, span := s.telemetry.start(ctx, "CompleteActivity",
		attribute.String(tenutotel.AttrWorkflowRunID, runID),
		attribute.Int64(tenutotel.AttrScheduledEventID, scheduledEventID),
		attribute.Int(tenutotel.AttrAttempt, history.NormalizeActivityAttempt(attempt)),
	)
	defer func() { tenutotel.FinishSpan(span, err) }()
	if terminated, err := s.workflowAlreadyTerminated(runID); err != nil {
		return err
	} else if terminated {
		return nil
	}
	attempt = history.NormalizeActivityAttempt(attempt)
	now := s.clock.Now()
	completed := false
	// 分支在事务外取出（BranchToken 在 Run 创建后不变，取值与事务内一致）。
	tid, bid, err := branch.treeID, branch.branchID, branch.err
	if err != nil {
		return err
	}
	// 事务内读取并更新同一条 ActivityState，提交后才推进下一轮。
	if err := s.runInTx(ctx, "complete_activity", func(tx execution.ExecutionTx) error {
		var err error
		completed, err = execution.CompleteActivity(tx, execution.ActivityTransition{
			RunID: runID, TreeID: tid, BranchID: bid,
			ScheduledEventID: scheduledEventID, Attempt: attempt, Stamp: stamp, Now: now,
		}, output)
		return err
	}); err != nil {
		return err
	}
	if !completed {
		return nil
	}
	s.stopActivityTimers(runID, scheduledEventID) // 终态后停内存 timer（事务外，无锁交叉）
	postCommitCtx := context.WithoutCancel(ctx)
	return s.scheduleWorkflowTask(postCommitCtx, runID)
}

// ActivityTaskToken 是异步完成时外部系统交回的 Activity Task Token。
type ActivityTaskToken string

// CompleteActivity 上报一次异步完成（task token 路径）：failure 为 nil 表示成功，
// 非 nil 表示失败——presence 只看 failure，Message 为空串的失败仍是失败。
func (s *Server) CompleteActivity(ctx context.Context, token ActivityTaskToken, output map[string]any, failure *protocol.Failure) error {
	task, err := s.tokens.Decode(protocol.TaskToken(token))
	if err != nil {
		return err
	}
	if task.Type != protocol.TaskTypeActivity {
		return protocol.InvalidRequestf("task token type %s is invalid for activity task", task.Type)
	}
	return s.completeActivity(ctx, task.RunID, task.ScheduledEventID, task.Attempt, task.Stamp, output, failure)
}

// CompleteActivityByID 按 ActivityID 上报一次异步完成：failure 为 nil 表示成功，非 nil 表示失败。
func (s *Server) CompleteActivityByID(ctx context.Context, runID string, activityID string, output map[string]any, failure *protocol.Failure) error {
	if runID == "" || activityID == "" {
		return protocol.InvalidRequestf("run id and activity id are required")
	}
	status, err := s.GetRunStatus(runID)
	if err != nil {
		return err
	}
	if history.IsTerminalStatus(status) {
		return nil
	}
	states, err := s.mgr.ListActivityStates(runID)
	if err != nil {
		return err
	}
	// 同一 ActivityID 关闭后可以再用：取 scheduledEventID 最大的 state；没有或已终态时幂等返回。
	var st *execution.ActivityState
	for _, candidate := range states {
		if candidate.ActivityID == activityID && (st == nil || candidate.ScheduledEventID > st.ScheduledEventID) {
			st = candidate
		}
	}
	if st == nil || st.IsTerminal() {
		return nil
	}
	// 这条路径没有 task token，stamp 取 state 自身的值；两次调用之间发生 reset/retry 时旧 stamp 被丢弃。
	return s.completeActivity(ctx, runID, st.ScheduledEventID, st.Attempt, st.Stamp, output, failure)
}

// completeActivity 是两条异步完成路径共用的分发：failure 是否存在决定成功还是失败。
func (s *Server) completeActivity(ctx context.Context, runID string, scheduledEventID int64, attempt int, stamp int64, output map[string]any, failure *protocol.Failure) error {
	if failure != nil {
		return s.HandleActivityFailed(ctx, runID, scheduledEventID, attempt, stamp, *failure)
	}
	return s.HandleActivityCompleted(ctx, runID, scheduledEventID, attempt, stamp, output)
}

// HandleActivityFailed 处理 activity 失败上报：Failure 落库为完整结构化失败，
// 失败是否成立由调用方选择本入口决定（不看 Message 是否为空）。
func (s *Server) HandleActivityFailed(ctx context.Context, runID string, scheduledEventID int64, attempt int, stamp int64, failure protocol.Failure) error {
	treeID, branchID, err := s.runBranchInfo(runID)
	if err != nil {
		return err
	}
	branch := runBranch{treeID: treeID, branchID: branchID}
	return s.failActivityAttempt(ctx, runID, branch, scheduledEventID, attempt, stamp, failure)
}

// FailActivityAttempt 记录一次失败上报。run 来自本次请求的 TaskRun，只取它的 History 分支。
func (s *Server) FailActivityAttempt(ctx context.Context, run *execution.Run, scheduledEventID int64, attempt int, stamp int64, failure protocol.Failure) error {
	return s.failActivityAttempt(ctx, run.ID, branchOf(run), scheduledEventID, attempt, stamp, failure)
}

func (s *Server) failActivityAttempt(ctx context.Context, runID string, branch runBranch, scheduledEventID int64, attempt int, stamp int64, failure protocol.Failure) (err error) {
	ctx, span := s.telemetry.start(ctx, "FailActivity",
		attribute.String(tenutotel.AttrWorkflowRunID, runID),
		attribute.Int64(tenutotel.AttrScheduledEventID, scheduledEventID),
		attribute.Int(tenutotel.AttrAttempt, history.NormalizeActivityAttempt(attempt)),
	)
	defer func() { tenutotel.FinishSpan(span, err) }()
	if terminated, err := s.workflowAlreadyTerminated(runID); err != nil {
		return err
	} else if terminated {
		return nil
	}
	attempt = history.NormalizeActivityAttempt(attempt)
	now := s.clock.Now()

	// 分支在事务外取出（BranchToken 在 Run 创建后不变，取值与事务内一致）。
	tid, bid, err := branch.treeID, branch.branchID, branch.err
	if err != nil {
		return err
	}
	// 事务返回的新状态用于提交后安装内存 retry timer。
	var result execution.ActivityFailureResult
	if err := s.runInTx(ctx, "fail_activity", func(tx execution.ExecutionTx) error {
		var txErr error
		result, txErr = execution.FailActivity(tx, execution.ActivityTransition{
			RunID: runID, TreeID: tid, BranchID: bid,
			ScheduledEventID: scheduledEventID, Attempt: attempt, Stamp: stamp, Now: now,
		}, failure)
		return txErr
	}); err != nil {
		return err
	}

	return s.settleActivityFailure(ctx, runID, scheduledEventID, result)
}

func (s *Server) settleActivityFailure(ctx context.Context, runID string, scheduledEventID int64, failure execution.ActivityFailureResult) error {
	if failure.RetryState != nil {
		s.scheduleActivityRetryTimer(failure.RetryState)
		return nil
	}
	if failure.Terminal {
		s.stopActivityTimers(runID, scheduledEventID)
		postCommitCtx := context.WithoutCancel(ctx)
		return s.scheduleWorkflowTask(postCommitCtx, runID)
	}
	return nil
}

// ResetActivity 把一个 pending（Scheduled/Started/重试中）的 activity 的 attempt 重置回 1
// 并立即重新调度。Activity 按 ActivityID 寻址：同一 Run 里未关闭的 Activity 的 ID 互不相同。
// 仅当 workflow Running 且 activity 处于 pending 时有效；已终态的 activity 应使用 Reset Workflow。
// Run 按 execution.ResolveRun 的规则寻址。
func (s *Server) ResetActivity(namespace, runID, activityID string, opts execution.ResetActivityOptions) error {
	if activityID == "" {
		return protocol.InvalidRequestf("cannot reset activity: activity id is required")
	}
	if _, err := s.resolveRunRef(namespace, "", runID); err != nil {
		return err
	}
	status, err := s.GetRunStatus(runID)
	if err != nil {
		return err
	}
	if status != history.StatusRunning {
		return protocol.InvalidRequestf("cannot reset activity: workflow %s is not running (status=%s)", runID, status)
	}
	states, err := s.mgr.ListActivityStates(runID)
	if err != nil {
		return err
	}
	i := slices.IndexFunc(states, func(state *execution.ActivityState) bool {
		return state.ActivityID == activityID && !state.IsTerminal()
	})
	if i < 0 {
		return protocol.InvalidRequestf("cannot reset activity: run %s has no pending activity %q", runID, activityID)
	}
	scheduledEventID := states[i].ScheduledEventID

	now := s.clock.Now()
	var newState *execution.ActivityState // 事务内更新后的 state（出事务后重派用）
	// runBranchInfo 走非事务 Get，在 RunInTx 闭包内调用会与外层事务争抢唯一连接而死锁；
	// 在事务外先算好分支信息再传入（BranchToken 本次调用期间不变，取值与事务内一致）。
	tid, bid, err := s.runBranchInfo(runID)
	if err != nil {
		return err
	}
	// 状态校验、reset 事件和 stamp 更新在同一事务内完成。
	if err := s.runInTx(context.Background(), "reset_activity", func(tx execution.ExecutionTx) error {
		var err error
		newState, err = execution.ResetActivity(tx, execution.ActivityReset{
			ActivityTransition: execution.ActivityTransition{
				RunID: runID, TreeID: tid, BranchID: bid,
				ScheduledEventID: scheduledEventID, Now: now,
			},
			Identity: opts.Identity, Reason: opts.Reason, ResetHeartbeat: opts.ResetHeartbeat,
		})
		return err
	}); err != nil {
		return err
	}

	// 换上新 attempt（attempt=1, 新 stamp）的 timer，ScheduleToStart 从 reset 时刻起算，并立即重派。
	s.stopActivityAttemptTimers(runID, scheduledEventID)
	s.armActivityTimeouts(newState, true)
	_, err = s.redispatchActivityTask(context.Background(), s.activityTaskFromState(newState))
	return err
}

// HandleActivityReleased 处理 Worker 停止时交还的已开始 attempt：不记失败、不消耗重试次数，
// 以同一个 attempt 立即重新派发。attempt 已结束、已过期或尚未开始时什么也不做。
func (s *Server) HandleActivityReleased(ctx context.Context, runID string, scheduledEventID int64, attempt int, stamp int64) error {
	treeID, branchID, err := s.runBranchInfo(runID)
	if err != nil {
		return err
	}
	branch := runBranch{treeID: treeID, branchID: branchID}
	return s.releaseActivityAttempt(ctx, runID, branch, scheduledEventID, attempt, stamp)
}

// ReleaseActivityAttempt 同 HandleActivityReleased。run 来自本次请求的 TaskRun，只取它的 History 分支。
func (s *Server) ReleaseActivityAttempt(ctx context.Context, run *execution.Run, scheduledEventID int64, attempt int, stamp int64) error {
	return s.releaseActivityAttempt(ctx, run.ID, branchOf(run), scheduledEventID, attempt, stamp)
}

func (s *Server) releaseActivityAttempt(ctx context.Context, runID string, branch runBranch, scheduledEventID int64, attempt int, stamp int64) error {
	if terminated, err := s.workflowAlreadyTerminated(runID); err != nil {
		return err
	} else if terminated {
		return nil
	}
	tid, bid, err := branch.treeID, branch.branchID, branch.err
	if err != nil {
		return err
	}
	var release execution.ActivityRelease
	if err := s.runInTx(ctx, "release_activity", func(tx execution.ExecutionTx) error {
		var err error
		release, err = execution.ReleaseActivity(tx, execution.ActivityTransition{
			RunID: runID, TreeID: tid, BranchID: bid,
			ScheduledEventID: scheduledEventID, Attempt: history.NormalizeActivityAttempt(attempt), Stamp: stamp, Now: s.clock.Now(),
		})
		return err
	}); err != nil {
		return err
	}
	if release.Cancelled {
		s.stopActivityTimers(runID, scheduledEventID)
		return s.scheduleWorkflowTask(context.WithoutCancel(ctx), runID)
	}
	released := release.Redispatch
	if released == nil {
		return nil
	}
	// 换下 StartToClose 与心跳 timer，ScheduleToStart 从交还时刻起算，并立即重派。
	s.stopActivityAttemptTimers(runID, scheduledEventID)
	s.armActivityTimeouts(released, true)
	_, err = s.redispatchActivityTask(context.WithoutCancel(ctx), s.activityTaskFromState(released))
	return err
}

// RefuseIncompatibleActivityClaim 在 Worker 开始一次 attempt 之前校验它的契约。Activity 任务不绑定 build，队列上任何 Worker
// 都能领取；Run 当前 Program 为这个 Activity 类型声明了契约、而领取的 Worker 这个 build 的上报与声明不兼容时拒绝：
// attempt 不开始、不消耗，这份投递由 Worker 确认丢弃，释放派发记录，并在 Workflow Task 失败退避的首个间隔
// （activity_route_unavailable 等待 Worker 用的同一个退避）之后重新投递。只有不兼容的 Worker 在线时，每个间隔只投递一次。
// 进程重启丢掉这个 timer 时，启动恢复与周期补偿按已释放的派发记录重新投递。
//
// run 是本次请求经 TaskRun 读到的 Run，对照它当前采用的 Program 版本。
func (s *Server) RefuseIncompatibleActivityClaim(ctx context.Context, task protocol.Task, run *execution.Run) (bool, error) {
	if s.workerVersions == nil {
		return false, nil
	}
	// 读不到 Run 的配置时没有可对照的声明，不拦截。
	wf, err := s.workflowConfigs.GetWorkflowConfig(runConfigRef(run, ""))
	if err != nil || dispatch.DeclaredActivityContract(wf, task.ActivityType) == nil {
		return false, nil
	}
	namespace := protocol.NormalizeNamespace(task.Namespace)
	registrations, err := s.workerVersions.ListActivityWorkerVersionsOfWorker(namespace, task.WorkerIdentity)
	if err != nil {
		return false, err
	}
	mismatch := dispatch.CheckClaimingWorkerContract(wf, registrations, namespace, task.WorkerIdentity, task.WorkerBuildID,
		dispatch.NormalizeActivityTaskQueue(task.TaskQueue), task.ActivityType)
	if mismatch == nil {
		return false, nil
	}
	delay := execution.WorkflowTaskFailureBackoff(1, s.wftBackoffBase, s.wftBackoffCap)
	s.logger.Warn("activity claim refused: worker contract is incompatible with the workflow declaration; the task is redelivered",
		slog.String("run_id", task.RunID),
		slog.Int64("scheduled_event_id", task.ScheduledEventID),
		slog.String("activity_type", task.ActivityType),
		slog.String("worker_identity", task.WorkerIdentity),
		slog.String("worker_build_id", task.WorkerBuildID),
		slog.Duration("redelivery_delay", delay),
		slog.Any("err", mismatch),
	)
	if err := s.dispatches.ReleaseTaskDispatch(dispatch.NormalizeTask(task), "worker contract mismatch"); err != nil {
		return false, err
	}
	s.timers.Arm(activityTimerRef(timerActivityRetry, task.RunID, task.ScheduledEventID), s.clock.Now().Add(delay))
	return true, nil
}

func (s *Server) HandleActivityHeartbeat(runID string, scheduledEventID int64, attempt int, stamp int64, details map[string]any) error {
	if terminated, err := s.workflowAlreadyTerminated(runID); err != nil {
		return err
	} else if terminated {
		return nil
	}
	if limitErr := protocol.CheckPayloadSize("activity heartbeat details", details, s.payloadSizeLimit); limitErr != nil {
		// 心跳详情超过载荷上限：照 Temporal RecordActivityTaskHeartbeat，以不可重试的失败结束这次 attempt；
		// 之后的取消检查告诉 Worker 结果不再有接收方。
		return s.HandleActivityFailed(context.Background(), runID, scheduledEventID, attempt, stamp, protocol.PayloadSizeLimitFailure(limitErr))
	}
	now := s.clock.Now()
	var st *execution.ActivityState
	if err := s.runInTx(context.Background(), "record_activity_heartbeat", func(tx execution.ExecutionTx) error {
		var err error
		st, err = execution.RecordActivityHeartbeat(tx, execution.ActivityTransition{
			RunID: runID, ScheduledEventID: scheduledEventID,
			Attempt: history.NormalizeActivityAttempt(attempt), Stamp: stamp, Now: now,
		}, details)
		return err
	}); err != nil || st == nil {
		return err
	}
	// 心跳截止时间以持久化状态为准（LastHeartbeatAt + HeartbeatTimeout）。
	if deadline, ok := execution.ActivityTimeoutDeadline(st, activityTimeoutKinds[timerActivityHeartbeat]); ok {
		s.timers.Arm(activityTimerRef(timerActivityHeartbeat, runID, scheduledEventID), deadline)
	}
	return nil
}

// GetActivityState 返回 activity 的持久化状态（stamp、attempt 等）。
func (s *Server) GetActivityState(runID string, scheduledEventID int64) (*execution.ActivityState, bool, error) {
	return s.mgr.GetActivityState(runID, scheduledEventID)
}

// PayloadSizeLimit 是单个载荷的字节上限，见 WithPayloadSizeLimit。
func (s *Server) PayloadSizeLimit() int { return s.payloadSizeLimit }
