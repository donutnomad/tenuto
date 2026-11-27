package engine

import (
	"context"
	"errors"
	"fmt"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/definition"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
)

type parentChildRunRequest struct {
	parentRunID      string
	childID          string
	childRunID       string
	scheduledEventID int64
	input            map[string]any
	configRef        definition.WorkflowConfigRef
}

func (s *Server) startChildWorkflowRunWithRef(ctx context.Context, req parentChildRunRequest) error {
	if req.childID == "" {
		return protocol.InvalidRequestf("child workflow child id is empty")
	}
	if req.configRef.WorkflowType == "" || req.configRef.Version == "" {
		return protocol.InvalidRequestf("child workflow config ref is incomplete")
	}
	child := execution.ChildStart{
		ParentRunID: req.parentRunID, ChildID: req.childID,
		ChildRunID: req.childRunID, ScheduledEventID: req.scheduledEventID,
		Namespace: req.configRef.Namespace, WorkflowType: req.configRef.WorkflowType,
		ConfigVersion: req.configRef.Version, Input: req.input,
	}
	now := s.clock.Now()
	run := execution.NewChildRun(child, now)
	return s.createStartedRun(ctx, run, func(tx execution.ExecutionTx) error {
		return execution.MarkChildStarted(tx, child, now)
	})
}

// propagateChildWorkflowClose 把子 Run childRunID 的终态写到父 Run parentRunID 的历史；parentRunID 为空时没有父 Run。
// 子 Run 在事务内用 GetRunForUpdate 读取，锁顺序先子后父。failure 是失败终态的唯一载荷（缺失即协议错误）；
// reason 只用于 Terminated 事件。
func (s *Server) propagateChildWorkflowClose(ctx context.Context, childRunID, parentRunID string, typ history.EventType, output map[string]any, reason string, failure *protocol.Failure) error {
	if parentRunID == "" {
		return nil
	}
	parentTreeID, parentBranchID, err := s.runBranchInfo(parentRunID)
	if err != nil {
		return err
	}
	now := s.clock.Now()
	propagated := false
	if txErr := s.runInTx(ctx, "propagate_child_close", func(tx execution.ExecutionTx) error {
		propagated = false // 死锁重放时上一次尝试的结果作废。
		child, err := tx.GetRunForUpdate(childRunID)
		if err != nil || child.ParentScheduledEventID == 0 {
			return err
		}
		propagated, err = execution.PropagateChildClose(tx, execution.ChildClose{
			Child: child, ParentTreeID: parentTreeID, ParentBranchID: parentBranchID,
			Type: typ, Output: output, Reason: reason, Failure: failure, Now: now,
		})
		return err
	}); txErr != nil {
		return txErr
	}
	if !propagated {
		return nil // 已终态，无新进展
	}
	postCommitCtx := context.WithoutCancel(ctx)
	return s.scheduleWorkflowTask(postCommitCtx, parentRunID)
}

// enforceParentClosePolicy 对已关闭的父 Run 仍在运行的子 Workflow 执行各自的父关闭策略。子关联读持久化的
// ChildWorkflowState；子 Workflow 的当前 Run 按它的 Workflow ID 解析，覆盖子 Workflow 的 continue-as-new、重试与 reset。
// 子 Run 尚未建好的子关联不处理：父 Run 已关闭，恢复不再补建它；之后才建好的子 Run 由它自己的 reconcileRun 处理。
func (s *Server) enforceParentClosePolicy(ctx context.Context, parentRunID string) error {
	states, err := s.mgr.ListChildWorkflowStates(parentRunID)
	if err != nil {
		return err
	}
	candidates := execution.ParentCloseCandidates(states)
	if len(candidates) == 0 {
		return nil
	}
	parent, err := s.mgr.GetWorkflowExecutionState(parentRunID)
	if err != nil {
		return err
	}
	var errs []error
	for _, link := range candidates {
		first, err := s.runRefOf(link.ChildRunID)
		if errors.Is(err, execution.ErrRunNotFound) {
			continue
		}
		if err != nil {
			errs = append(errs, err)
			continue
		}
		child, err := s.resolveRunRef(first.Namespace, first.WorkflowID, "")
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if _, err := s.applyParentClosePolicy(ctx, parent, link, child); err != nil {
			errs = append(errs, fmt.Errorf("child workflow %s: %w", link.ChildID, err))
		}
	}
	return errors.Join(errs...)
}

// applyParentClosePolicy 按 execution.ParentCloseActionFor 处理子 Workflow 的当前 Run child，terminated 报告是否终止了它。
// 两种处理都幂等：终止跳过已结束的 Run，请求取消跳过已请求过的 Run。
func (s *Server) applyParentClosePolicy(ctx context.Context, parent *execution.WorkflowExecutionState, link *execution.ChildWorkflowState, child runRef) (terminated bool, err error) {
	state, err := s.mgr.GetWorkflowExecutionState(child.ID)
	if err != nil {
		return false, err
	}
	switch execution.ParentCloseActionFor(parent, link, state) {
	case execution.ParentCloseActionTerminate:
		return true, s.terminateRun(ctx, child.Namespace, child.WorkflowID, child.ID, execution.ParentCloseReason, false)
	case execution.ParentCloseActionRequestCancel:
		return false, s.cancelRun(child.ID, execution.ParentCloseReason)
	}
	return false, nil
}

// ChildCancelReason 是父 Workflow 取消子 Workflow 时子 Run 取消请求记录的原因。
const ChildCancelReason = "by parent workflow"

// requestChildCancellation 向父 Run 上 scheduledEventID 这个子关联的子 Workflow 当前 Run 发出取消请求。
// 子 Workflow 的当前 Run 按它的 Workflow ID 解析，覆盖 continue-as-new、重试与 reset；子 Run 尚未建好、
// 已结束或已收到请求时不做任何事。
func (s *Server) requestChildCancellation(ctx context.Context, parentRunID string, scheduledEventID int64) error {
	link, found, err := s.mgr.GetChildWorkflowState(parentRunID, scheduledEventID)
	if err != nil || !found || link.ChildRunID == "" {
		return err
	}
	first, err := s.runRefOf(link.ChildRunID)
	if errors.Is(err, execution.ErrRunNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	child, err := s.resolveRunRef(first.Namespace, first.WorkflowID, "")
	if err != nil {
		return err
	}
	state, err := s.mgr.GetWorkflowExecutionState(child.ID)
	if err != nil || !execution.ChildCancelPending(link, state) {
		return err
	}
	return s.cancelRun(child.ID, ChildCancelReason)
}

// recoverParentClosePolicy 是父 Run 对子 Workflow 处理的恢复路径。父 Run 已关闭时补做父关闭策略
// （父 Run 关闭之后进程崩溃，或子 Run 在父 Run 关闭之后才建好），terminated 为 true 时子 Run 已被终止；
// 父 Run 仍在运行且已请求取消这个子 Workflow 时补发子 Run 的取消请求。
// 父 Run 已被保留期删除时子关联随之删除，不再处理。
func (s *Server) recoverParentClosePolicy(ctx context.Context, run *execution.Run) (terminated bool, err error) {
	if run.ParentRunID == "" || run.ParentScheduledEventID == 0 {
		return false, nil
	}
	parent, err := s.mgr.GetWorkflowExecutionState(run.ParentRunID)
	if errors.Is(err, execution.ErrRunNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	link, found, err := s.mgr.GetChildWorkflowState(run.ParentRunID, run.ParentScheduledEventID)
	if err != nil || !found {
		return false, err
	}
	var state *execution.WorkflowExecutionState
	if !parent.IsTerminal() && link.CancelRequested {
		if state, err = s.mgr.GetWorkflowExecutionState(run.ID); err != nil {
			return false, err
		}
	}
	return s.recoverParentLink(ctx, run, state, parent, link)
}

// recoverParentLink 按父 Run 的 execution state 与子关联补做子 Run run 的父侧处理，state 是 run 自己的 execution state
// （父 Run 仍在运行且 link.CancelRequested 时才用到）。逐个恢复与全量补偿的批量读取共用它。
func (s *Server) recoverParentLink(ctx context.Context, run *execution.Run, state, parent *execution.WorkflowExecutionState,
	link *execution.ChildWorkflowState) (terminated bool, err error) {
	if parent.IsTerminal() {
		return s.applyParentClosePolicy(ctx, parent, link, refOf(run))
	}
	if execution.ChildCancelPending(link, state) {
		return false, s.cancelRun(run.ID, ChildCancelReason)
	}
	return false, nil
}
