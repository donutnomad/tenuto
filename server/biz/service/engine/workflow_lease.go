package engine

import (
	"context"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
)

// LeaseAction 是 withWorkflowLease 中 fn 返回的"后续动作"声明（对标 Temporal UpdateWorkflowAction）。
// fn 只描述意图，由骨架在事务外、锁内统一执行——保持 fn 体纯业务。
type LeaseAction struct {
	ScheduleWFT bool // 需要调度一次 WorkflowTask（仅当 run 仍 Running）
	// AfterSignal 表示这次调度由 Signal 触发：重试 Run 仍在首个 Workflow Task 的退避期内时不调度，
	// 见 scheduleWorkflowTaskAfterSignal。
	AfterSignal bool
}

// resolveRun 加载请求寻址的 Run，规则见 execution.ResolveRun。它在事务之外读取，只供读路径使用；写路径经
// resolveRunRef 寻址，在事务内用 ExecutionTx.GetRunForUpdate 读取 Run。
func (s *Server) resolveRun(namespace, workflowID, runID string) (*execution.Run, error) {
	return execution.ResolveRun(s.mgr, namespace, workflowID, runID)
}

// runRef 是 Run 创建后不再改变的字段。事务之外的读取只用它们寻址、取 keyed lock 与派发；写事务依据的 Run 字段
// （状态、策略、配置错误、当前内容版本）在事务内用 ExecutionTx.GetRunForUpdate 读取。
type runRef struct {
	ID, Namespace, WorkflowID, WorkflowType string
	// branch 是 Run 的 History 分支，事务外取出后可以带进写事务。
	branch runBranch
}

func refOf(run *execution.Run) runRef {
	return runRef{ID: run.ID, Namespace: run.Namespace, WorkflowID: run.WorkflowID, WorkflowType: run.WorkflowType, branch: branchOf(run)}
}

// resolveRunRef 按 execution.ResolveRun 的规则解析请求寻址的 Run，返回它不变的字段。
func (s *Server) resolveRunRef(namespace, workflowID, runID string) (runRef, error) {
	run, err := s.resolveRun(namespace, workflowID, runID)
	if err != nil {
		return runRef{}, err
	}
	return refOf(run), nil
}

// runRefOf 读取 runID 不变的字段。
func (s *Server) runRefOf(runID string) (runRef, error) {
	run, err := s.mgr.Get(runID)
	if err != nil {
		return runRef{}, err
	}
	return refOf(run), nil
}

// lockRunWorkflow 拿 runID 所属 (namespace, workflowID) 的 keyed lock，err 为 nil 时调用方负责 release。
func (s *Server) lockRunWorkflow(ctx context.Context, runID string) (runRef, func(), error) {
	ref, err := s.runRefOf(runID)
	if err != nil {
		return runRef{}, nil, err
	}
	release, err := s.keyedMu.Lock(ctx, workflowLockKey(ref.Namespace, ref.WorkflowID))
	if err != nil {
		return runRef{}, nil, err
	}
	return ref, release, nil
}

// withWorkflowLease 是写操作的统一骨架（对标 Temporal GetAndUpdateWorkflowWithNew）。
//
// 流程：拿 per-(namespace, workflowID) 锁 → 锁内解析 current（runID=="" 时，防解析后漂移）→ 在 RunInTx 内用
// GetRunForUpdate 读取 run 并调 fn 原子写回 → 锁内、事务外按 LeaseAction 调度 WFT → defer release。fn 拿到的
// run 是持有 execution state 行锁时读到的，其它副本已提交的改动都在里面。
//
// ⚠️ key 用 workflowLockKey(namespace, workflowID)（per-(namespace, workflowID) 粒度）：
// 覆盖"解析 current → 操作"全窗口，且 reset 的 base+current 同属一个 (namespace, workflowID)，
// 一把锁全覆盖，天然无 ABBA 死锁；不同 namespace 下的同名 workflowID 互不阻塞。
//
// Run 按 resolveRun 的规则寻址：不属于请求 namespace（或给出的 workflowID）的 Run 与不存在一样。调用方给出
// runID 时，锁 key 取这个 Run 自己的 (namespace, workflowID)：只带 runID 的请求与同一 Workflow 的其它写操作
// 因此争同一把锁。
func (s *Server) withWorkflowLease(ctx context.Context, namespace, workflowID, runID string,
	fn func(tx execution.ExecutionTx, run *execution.Run) (LeaseAction, error)) error {
	if runID != "" {
		ref, err := s.resolveRunRef(namespace, workflowID, runID)
		if err != nil {
			return err
		}
		namespace, workflowID = ref.Namespace, ref.WorkflowID
	}
	release, err := s.keyedMu.Lock(ctx, workflowLockKey(namespace, workflowID)) // 拿锁
	if err != nil {
		return err
	}
	defer release()

	// 没给 runID 时在锁内解析 current，本进程内解析与写入之间 current 不会变。
	ref, err := s.resolveRunRef(namespace, workflowID, runID)
	if err != nil {
		return err
	}

	var act LeaseAction
	if txErr := s.telemetry.runInTx(ctx, "workflow_lease", s.mgr, func(tx execution.ExecutionTx) error {
		run, e := tx.GetRunForUpdate(ref.ID)
		if e != nil {
			return e
		}
		act, e = fn(tx, run)
		return e
	}); txErr != nil {
		return txErr
	}

	if act.ScheduleWFT { // 锁内、事务外调度
		postCommitCtx := context.WithoutCancel(ctx)
		if act.AfterSignal {
			return s.scheduleWorkflowTaskAfterSignal(postCommitCtx, ref.ID)
		}
		return s.scheduleWorkflowTaskIfRunning(postCommitCtx, ref.ID)
	}
	return nil
}

// ensureStartedThenAcceptUpdate 是 UpdateWithStart 的原子内核。在 per-(namespace, workflowID) keyed lock 内：
//   - 该 Workflow ID 没有进行中的 Run → 新 Run、current CAS、WorkflowStarted 与 UpdateAccepted 写入同一个事务；
//   - 已有进行中的 Run → 复用它，只写 UpdateAccepted（updateID 已记录时返回记录的结果）。
//
// updateID 已记录在刚结束的 current Run 上时，这次调用是一次重试：返回记录的结果，不启动新 Run。
// 队列调度和 timer 注册在提交完成后执行；long-poll 等待在锁外，由调用方负责。
func (s *Server) ensureStartedThenAcceptUpdate(
	ctx context.Context, namespace, workflowType, workflowID string,
	startInput map[string]any,
	updateName, updateID string,
	args protocol.Payloads,
	existing history.WorkflowUpdateRecord, hasExisting bool,
	result *protocol.WorkflowUpdateResult,
) (runID string, err error) {
	release, err := s.keyedMu.Lock(ctx, workflowLockKey(namespace, workflowID)) // 拿锁：覆盖 start + accept 全窗口
	if err != nil {
		return "", err
	}
	defer release()

	plan, currentID, err := s.planStart(namespace, workflowType, workflowID, startInput, execution.InitiatorEnums.Workflow)
	if err != nil {
		return "", err
	}
	if plan != nil {
		if hasExisting {
			*result = execution.RecordedUpdateResult(plan.expectedCurrent, updateID, existing)
			return plan.expectedCurrent, nil
		}
		if err := s.commitStart(ctx, plan, func(tx execution.ExecutionTx) error {
			accepted, _, e := execution.AcceptOrReuseUpdate(tx, plan.run, plan.treeID, plan.branchID, updateName, updateID, args, history.WorkflowUpdateRecord{}, false)
			*result = accepted
			return e
		}); err != nil {
			return "", err
		}
		return plan.run.ID, nil
	}

	var scheduled bool
	if txErr := s.telemetry.runInTx(ctx, "workflow_lease", s.mgr, func(tx execution.ExecutionTx) error {
		current, e := tx.GetRunForUpdate(currentID)
		if e != nil {
			return e
		}
		if e := requireWorkflowType(current, workflowType); e != nil {
			return e
		}
		treeID, branchID, e := runBranchInfoFromRun(current)
		if e != nil {
			return e
		}
		accepted, sched, e := execution.AcceptOrReuseUpdate(tx, current, treeID, branchID, updateName, updateID, args, existing, hasExisting)
		if e != nil {
			return e
		}
		*result = accepted
		scheduled = sched
		return nil
	}); txErr != nil {
		return "", txErr
	}
	if scheduled {
		if e := s.scheduleWorkflowTaskIfRunning(context.WithoutCancel(ctx), currentID); e != nil {
			return "", e
		}
	}
	return currentID, nil
}

// ensureStartedThenSignal 是 SignalWithStart 的原子内核。在 per-(namespace, workflowID) keyed lock 内：
//   - requestID 已经生效（见 signalWithStartByRequest）→ 返回第一次的 Run 与 Signal 事件 ID，不启动、不再记 Signal；
//   - 该 Workflow ID 没有进行中的 Run → 新 Run、current CAS、WorkflowStarted（记下 requestID）与 WorkflowSignaled
//     写入同一个事务；
//   - 已有进行中的 Run → 复用它，只写 WorkflowSignaled，并调度 Workflow Task（重试 Run 的退避期内也立即调度）。
//
// requestID 非空时是启动与 Signal 的幂等键（Signal 一侧见 execution.RecordSignal）；为空时调用方重试会多记一条
// WorkflowSignaled，Run 已结束时还会启动新 Run。
func (s *Server) ensureStartedThenSignal(
	ctx context.Context, namespace, workflowType, workflowID string,
	startInput map[string]any,
	signalName, requestID string, signalInput map[string]any,
) (runID string, eventID int64, err error) {
	release, err := s.keyedMu.Lock(ctx, workflowLockKey(namespace, workflowID)) // 拿锁：覆盖 start + signal 全窗口
	if err != nil {
		return "", 0, err
	}
	defer release()

	if runID, eventID, err := s.signalWithStartByRequest(namespace, workflowID, requestID); err != nil || runID != "" {
		return runID, eventID, err
	}
	plan, currentID, err := s.planStart(namespace, workflowType, workflowID, startInput, execution.InitiatorEnums.Workflow)
	if err != nil {
		return "", 0, err
	}
	if plan != nil {
		plan.started.WorkflowStarted.RequestID = requestID
		if err := s.commitStart(ctx, plan, func(tx execution.ExecutionTx) error {
			id, e := execution.RecordSignal(tx, plan.run, plan.treeID, plan.branchID, signalName, requestID, signalInput)
			eventID = id
			return e
		}); err != nil {
			return "", 0, err
		}
		return plan.run.ID, eventID, nil
	}

	if txErr := s.telemetry.runInTx(ctx, "signal_with_start", s.mgr, func(tx execution.ExecutionTx) error {
		current, e := tx.GetRunForUpdate(currentID)
		if e != nil {
			return e
		}
		if e := requireWorkflowType(current, workflowType); e != nil {
			return e
		}
		// runBranchInfoFromRun 是纯内存版（不走 mgr.Get），避免在 RunInTx 内重入存储导致死锁。
		treeID, branchID, e := runBranchInfoFromRun(current)
		if e != nil {
			return e
		}
		eventID, e = execution.RecordSignal(tx, current, treeID, branchID, signalName, requestID, signalInput)
		return e
	}); txErr != nil {
		return "", 0, txErr
	}
	// 锁内、事务外调度（幂等：已有 open WFT 则 no-op）。重试 Run 的退避期内也立即调度：同 Temporal
	// signalwithstartworkflow 在 EnableSignalWithStartWorkflowTaskBackoff 默认关闭时的行为，与普通 Signal 不同。
	if e := s.scheduleWorkflowTaskIfRunning(context.WithoutCancel(ctx), currentID); e != nil {
		return "", 0, e
	}
	return currentID, eventID, nil
}

// requireWorkflowType 要求复用的进行中 Run 与请求的 Workflow Type 一致。
func requireWorkflowType(run *execution.Run, workflowType string) error {
	if got := run.WorkflowType; got != workflowType {
		return protocol.InvalidRequestf("workflow %q is running type %q, requested %q", run.WorkflowID, got, workflowType)
	}
	return nil
}
