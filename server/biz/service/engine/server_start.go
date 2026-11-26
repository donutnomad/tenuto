package engine

import (
	"context"
	"errors"
	"maps"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	tenutotel "github.com/donutnomad/tenuto/telemetry"

	"github.com/donutnomad/tenuto/server/biz/domain/definition"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
)

// startPlan 是一次启动在提交前准备好的内容。
type startPlan struct {
	run *execution.Run
	// expectedCurrent 是 current 指针的 CAS 旧值：空表示该 Workflow ID 还没有 Run，
	// 非空是被接替的那一代（已结束的 current，或 cron 的上一代）。
	expectedCurrent  string
	treeID, branchID string
	started          history.Event
}

// planStart 是 Start、SignalWithStart、UpdateWithStart 共用的启动判定：
//   - 该 Workflow ID 的 current Run 仍在进行 → 返回它的 ID（existingID），不准备新 Run；调用方在事务内读取它；
//   - 没有 current Run，或 current Run 已结束 → 准备新 Run，提交时以 current CAS 接替 current；
//   - 新 Run 的 Program 取该 Workflow Type 的 active 配置。
func (s *Server) planStart(namespace, workflowType, workflowID string, input map[string]any, initiator string) (plan *startPlan, existingID string, err error) {
	expectedCurrent, err := s.mgr.GetCurrentRunID(namespace, workflowID)
	if err == nil {
		status, err := s.GetRunStatus(expectedCurrent)
		if err != nil {
			return nil, "", err
		}
		if !history.IsTerminalStatus(status) {
			return nil, expectedCurrent, nil
		}
	} else if !errors.Is(err, execution.ErrNoCurrentRun) {
		return nil, "", err
	}
	plan, err = s.planNewRun(namespace, workflowType, workflowID, input, initiator)
	if err != nil {
		return nil, "", err
	}
	plan.expectedCurrent = expectedCurrent
	return plan, "", nil
}

// planNewRun 按 Workflow Type 的 active 配置准备一个新 Run。
func (s *Server) planNewRun(namespace, workflowType, workflowID string, input map[string]any, initiator string) (*startPlan, error) {
	configRef, err := s.activeWorkflowRefInNamespace(namespace, workflowType)
	if err != nil {
		return nil, err
	}
	run, err := s.newWorkflowVersionRun(workflowID, configRef, input, initiator)
	if err != nil {
		return nil, err
	}
	plan := &startPlan{run: run}
	plan.treeID, plan.branchID, plan.started, err = s.prepareStartedRun(run)
	if err != nil {
		return nil, err
	}
	return plan, nil
}

// commitStart 在一个事务里写入 Run、current 指针和 WorkflowStarted，并执行 with 写入的
// 同事务内容（SignalWithStart 的 Signal、UpdateWithStart 的 UpdateAccepted）；提交后调度首个
// Workflow Task 并注册超时 timer。
func (s *Server) commitStart(ctx context.Context, plan *startPlan, with func(tx execution.ExecutionTx) error) (err error) {
	run := plan.run
	ctx, span := s.telemetry.start(ctx, "StartWorkflow", workflowSpanAttributes(run.Namespace, run.WorkflowID, run.ID, run.WorkflowType)...)
	defer func() { tenutotel.FinishSpan(span, err) }()
	// CreateRun + Append 同事务原子提交（崩溃在两步之间不再产生"有 run 无 started 事件"的不一致）。
	if err := s.telemetry.runInTx(ctx, "create_started_run", s.mgr, func(tx execution.ExecutionTx) error {
		if err := execution.CommitStartedRun(tx, run, plan.expectedCurrent, plan.treeID, plan.branchID, plan.started); err != nil {
			return err
		}
		if with != nil {
			return with(tx)
		}
		return nil
	}); err != nil {
		return err
	}
	return s.activateStartedRun(context.WithoutCancel(ctx), run)
}

// StartWorkflow 按 Workflow Type 启动一个 Run。
//
// workflowID 标识这次执行，同一个 Workflow ID 同时只有一个进行中的 Run：已有进行中的 Run 时返回
// ErrCurrentRunConflict；current Run 已结束时新 Run 接替它。workflowID 为空时服务端生成一个
// UUIDv7 文本，所以同一类型可以同时运行任意多个 Run。返回新建的 Run，它带着最终采用的 Workflow ID。
//
// requestID 非空时按它去重：Workflow ID 的 current Run（运行中或已结束）由同一个 requestID 启动时返回这个 Run，
// 不报冲突、不启动新 Run。调用方重发应答丢失的启动请求因此是安全的；requestID 记在 WorkflowStarted 事件里。
// requestID 为空时不去重。
func (s *Server) StartWorkflow(ctx context.Context, namespace, workflowType, workflowID, requestID string, input map[string]any) (*execution.Run, error) {
	if err := execution.ValidateRequestID(requestID); err != nil {
		return nil, err
	}
	if workflowID == "" {
		workflowID = execution.NewID()
	}
	return s.startWorkflow(ctx, startRequest{
		namespace: namespace, workflowType: workflowType, workflowID: workflowID, input: input,
		initiator: execution.InitiatorEnums.Workflow, requestID: requestID,
	})
}

// startRequest 是 startWorkflow 的参数。
type startRequest struct {
	namespace, workflowType, workflowID string
	input                               map[string]any
	initiator                           string
	// requestID 非空时按它去重，见 StartWorkflow。
	requestID string
	// searchAttributes 是新 Run 的启动搜索属性（Schedule 触发时的 Schedule ID 与名义时间）。
	searchAttributes map[string]string
}

// startWorkflow 按 planStart 启动一个 Run：workflowID 的 current Run 仍在进行时返回 ErrCurrentRunConflict。
func (s *Server) startWorkflow(ctx context.Context, req startRequest) (*execution.Run, error) {
	if req.workflowType == "" {
		return nil, protocol.InvalidRequestf("workflow type is empty")
	}
	namespace := protocol.NormalizeNamespace(req.namespace)
	if run, err := s.runStartedByRequest(namespace, req.workflowID, req.requestID); run != nil || err != nil {
		return run, err
	}
	plan, existingID, err := s.planStart(namespace, req.workflowType, req.workflowID, req.input, req.initiator)
	if err != nil {
		return nil, err
	}
	if existingID != "" {
		return nil, execution.ErrCurrentRunConflict
	}
	if req.searchAttributes != nil {
		plan.run.SearchAttributes = maps.Clone(req.searchAttributes)
		plan.started.WorkflowStarted.SearchAttributes = maps.Clone(req.searchAttributes)
	}
	plan.started.WorkflowStarted.RequestID = req.requestID
	if err := s.commitStart(ctx, plan, nil); err != nil {
		// 同一个请求的另一次发送先提交了：返回它启动的 Run。
		if errors.Is(err, execution.ErrCurrentRunConflict) {
			if run, lookupErr := s.runStartedByRequest(namespace, req.workflowID, req.requestID); run != nil && lookupErr == nil {
				return run, nil
			}
		}
		return nil, err
	}
	return plan.run, nil
}

// runStartedByRequest 在 requestID 非空、且 workflowID 的 current Run 属于它启动的 Run 链时返回这个 Run；否则返回 nil。
// 只读 current Run 的第一个事件（WorkflowStarted）：失败重试与 continue-as-new 派生的 Run 继承链首的 RequestID。
func (s *Server) runStartedByRequest(namespace, workflowID, requestID string) (*execution.Run, error) {
	if requestID == "" {
		return nil, nil
	}
	currentID, err := s.mgr.GetCurrentRunID(namespace, workflowID)
	if errors.Is(err, execution.ErrNoCurrentRun) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	events, err := s.mgr.GetEventsRange(currentID, 0, 1)
	if err != nil {
		return nil, err
	}
	if len(events) == 0 || events[0].WorkflowStarted == nil || events[0].WorkflowStarted.RequestID != requestID {
		return nil, nil
	}
	return s.mgr.Get(currentID)
}

// StartWorkflowVersion 按指定配置版本启动 Run 并返回它；workflowID 为空时服务端生成。
func (s *Server) StartWorkflowVersion(ctx context.Context, configRef definition.WorkflowConfigRef, workflowID string, input map[string]any) (*execution.Run, error) {
	configRef.Namespace = protocol.NormalizeNamespace(configRef.Namespace)
	return s.startWorkflowVersion(ctx, configRef, workflowID, input)
}

// StartWorkflowForCron 由 Schedule 触发，新 Run 标记 Initiator=Cron、带 searchAttributes（Schedule ID 与名义触发时间）。
// 启动规则与 StartWorkflow 相同：workflowID 的 current Run 仍在进行时返回 ErrCurrentRunConflict，触发不会接替运行中的 Run。
func (s *Server) StartWorkflowForCron(ctx context.Context, namespace, workflowType, workflowID string, input map[string]any, searchAttributes map[string]string) (string, error) {
	if workflowID == "" {
		return "", protocol.InvalidRequestf("workflow id is empty")
	}
	run, err := s.startWorkflow(ctx, startRequest{
		namespace: namespace, workflowType: workflowType, workflowID: workflowID, input: input,
		initiator: execution.InitiatorEnums.Cron, searchAttributes: searchAttributes,
	})
	if err != nil {
		return "", err
	}
	return run.ID, nil
}

// CurrentRunStatus 返回 runID 所在 Workflow ID 的 current Run 的状态：runID 经 continue-as-new、重试或 reset 接续时是
// 接续的那一代。runID 不存在时返回的错误满足 errors.Is(err, ErrRunNotFound)。
func (s *Server) CurrentRunStatus(runID string) (history.Status, error) {
	ref, err := s.runRefOf(runID)
	if err != nil {
		return "", err
	}
	current, err := s.mgr.GetCurrentRunID(ref.Namespace, ref.WorkflowID)
	if err != nil {
		return "", err
	}
	return s.GetRunStatus(current)
}

// startWorkflowVersion 用默认 Workflow initiator 创建并启动一个新 run；workflowID 为空时生成一个。
func (s *Server) startWorkflowVersion(ctx context.Context, configRef definition.WorkflowConfigRef, workflowID string, input map[string]any) (*execution.Run, error) {
	if workflowID == "" {
		workflowID = execution.NewID()
	}
	run, err := s.newWorkflowVersionRun(workflowID, configRef, input, execution.InitiatorEnums.Workflow)
	if err != nil {
		return nil, err
	}
	if err := s.createStartedRun(ctx, run, nil); err != nil {
		return nil, err
	}
	return run, nil
}

func (s *Server) newWorkflowVersionRun(workflowID string, configRef definition.WorkflowConfigRef, input map[string]any, initiator string) (*execution.Run, error) {
	return execution.NewWorkflowVersionRun(execution.WorkflowStartOptions{
		Namespace: configRef.Namespace, WorkflowID: workflowID,
		WorkflowType: configRef.WorkflowType, ProgramVersion: configRef.Version,
		Input: input, Initiator: initiator,
		Now: s.clock.Now(),
	})
}

// prepareContinuedRun 在事务外备好 continue-as-new 的接续 Run 与它的 WorkflowStarted。接续 Run 与旧 Run 的终态事件
// 同事务提交（execution.CommitContinuedRun），提交时在锁下重读旧 Run 取有效策略。
func (s *Server) prepareContinuedRun(oldRunID, newRunID string, input map[string]any, configRef definition.WorkflowConfigRef) (*startPlan, error) {
	if configRef.WorkflowType == "" || configRef.Version == "" {
		return nil, protocol.InvalidRequestf("continued workflow config ref is incomplete")
	}
	oldRun, err := s.mgr.Get(oldRunID)
	if err != nil {
		return nil, err
	}
	oldStart, err := s.mgr.GetEventsRange(oldRunID, 0, 1)
	if err != nil {
		return nil, err
	}
	run := execution.NewContinuedRun(oldRun, newRunID, configRef.Namespace, configRef.WorkflowType, configRef.Version, input, s.clock.Now())
	plan := &startPlan{run: run, expectedCurrent: oldRunID}
	if plan.treeID, plan.branchID, plan.started, err = s.prepareStartedRun(run); err != nil {
		return nil, err
	}
	plan.started.WorkflowStarted.RequestID = execution.StartRequestID(oldStart)
	return plan, nil
}

// createStartedRun 用于按配置版本启动与子 Workflow 首次启动：Workflow ID 的 current Run 已结束时以它为 CAS 旧值接替，
// 仍在进行时返回 ErrCurrentRunConflict。with 非空时在建 Run 的同一事务里执行（子 Run 的父侧 started 状态）。
func (s *Server) createStartedRun(ctx context.Context, run *execution.Run, with func(tx execution.ExecutionTx) error) error {
	var expected string
	current, err := s.mgr.GetCurrentRunID(run.Namespace, run.WorkflowID)
	if err == nil {
		status, err := s.GetRunStatus(current)
		if err != nil {
			return err
		}
		if !history.IsTerminalStatus(status) {
			return execution.ErrCurrentRunConflict
		}
		expected = current
	} else if !errors.Is(err, execution.ErrNoCurrentRun) {
		return err
	}
	plan := &startPlan{run: run, expectedCurrent: expected}
	plan.treeID, plan.branchID, plan.started, err = s.prepareStartedRun(run)
	if err != nil {
		return err
	}
	return s.commitStart(ctx, plan, with)
}

func (s *Server) prepareStartedRun(run *execution.Run) (string, string, history.Event, error) {
	if run.WorkflowType == "" || run.WorkflowConfigVersion == "" {
		return "", "", history.Event{}, protocol.InvalidRequestf("workflow config ref is incomplete")
	}
	cfg, err := s.workflowConfigs.GetWorkflowConfig(definition.WorkflowConfigRef{Namespace: run.Namespace, WorkflowType: run.WorkflowType, Version: run.WorkflowConfigVersion})
	if err != nil {
		return "", "", history.Event{}, err
	}
	if err := run.ApplyStartPolicy(cfg.Policy, cfg.TargetWorkerIdentity); err != nil {
		return "", "", history.Event{}, err
	}
	applyRunTimeouts(run, cfg, s.clock.Now())
	return execution.PrepareStartedRun(run, cfg.Name, execution.ExecutionTimeoutMs(cfg.ExecutionTimeout))
}

func (s *Server) activateStartedRun(ctx context.Context, run *execution.Run) error {
	// 事务外（本质非事务）：队列入队 + 内存 timer。截止时间已随 Run 写入。
	if err := s.scheduleWorkflowTask(ctx, run.ID); err != nil {
		return err
	}
	s.armRunTimers(run)
	return nil
}

func (s *Server) continuedWorkflowType(runID string, requestedType string) (string, error) {
	if requestedType != "" {
		return requestedType, nil
	}
	run, err := s.mgr.Get(runID)
	if err != nil {
		return "", err
	}
	return run.WorkflowType, nil
}
