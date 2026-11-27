package engine

import (
	"context"
	"errors"
	"fmt"

	"github.com/donutnomad/tenuto/server/biz/domain/definition"

	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
)

func (s *Server) ResetWorkflow(ctx context.Context, req execution.ResetWorkflowRequest) (string, error) {
	if err := req.Validate(); err != nil {
		return "", err
	}
	base, err := s.resolveRunRef(req.Namespace, "", req.RunID)
	if err != nil {
		return "", err
	}

	// reset 涉及 base run + current run，但二者同属一个 (namespace, workflowID)（reset 不跨 workflow，
	// 也不跨 namespace），故一把 per-(namespace, workflowID) keyed lock 全覆盖——无 Temporal 的 ABBA
	// 双锁问题。锁在最外层拿，覆盖整个 reset 流程（GetCurrentRunID / fork / 终止被取代的 run 并
	// CAS 切代），解析 current → CAS 全在锁内，无漂移窗口：reset 进行中，同 namespace 同 workflowID 的
	// signal/cancel/terminate 被本锁挡住，等 reset 完成后才解析到新 current（不会打到正被 reset 的旧 run）；
	// 别的 namespace 下的同名 workflowID 不受影响（锁 key 已含 namespace）。跨副本的互斥由事务内的
	// execution state 行锁负责（见 execution.CommitResetRun）。
	// 被调函数（GetEvents/ForkBranch/RunInTx/scheduleWorkflowTask）均不拿 keyed lock，确认无重入。
	release, err := s.keyedMu.Lock(ctx, workflowLockKey(base.Namespace, base.WorkflowID))
	if err != nil {
		return "", err
	}
	defer release()

	// 带 RequestID 的 reset 在事务里比较 base 的 ResetRunID：另一个副本在准备与提交之间提交了 reset 时重来一次，
	// 重来时按 RequestID 认出同一个请求的那次 reset 并返回它。
	for attempt := 0; ; attempt++ {
		newRunID, err := s.resetOnce(ctx, req)
		if errors.Is(err, execution.ErrCurrentRunConflict) && req.RequestID != "" && attempt == 0 {
			continue
		}
		return newRunID, err
	}
}

// resetOnce 执行一次 reset。调用方持有 workflow 的 keyed lock。
func (s *Server) resetOnce(ctx context.Context, req execution.ResetWorkflowRequest) (string, error) {
	commit, duplicateOf, err := s.prepareReset(req)
	if err != nil || duplicateOf != "" {
		return duplicateOf, err
	}
	// 核心原子化：终止被取代的 run + 建新 run + 切 current 到新 run + 改旧 run reset_run_id + 落 reset marker +
	// 分叉点上未结束的 Activity / Timer（关闭被打断的 Activity，在新 run 上建保留项的 state）+
	// 搬运 reset 点之后的外部输入（Signal / Update-Accepted，ReapplyExcludeTypes 排除指定类型）+
	// 把父 run 的子关联改指新 run，在同一事务内，任一步失败整体回滚。scheduleWorkflowTask（含队列入队）
	// 留事务外。对标 startRetryRun。
	if txErr := s.runInTx(ctx, "reset_workflow", func(tx execution.ExecutionTx) error {
		return execution.CommitResetRun(tx, commit)
	}); txErr != nil {
		return "", txErr
	}
	// 被取代的 run 已终止：停掉它们在本进程的 timer。
	for _, replaced := range commit.Terminate {
		s.runClosed(replaced.RunID)
	}

	s.armRunTimers(commit.NewRun)
	s.startResetPendingWork(ctx, commit.Pending)

	postCommitCtx := context.WithoutCancel(ctx)
	if err := s.scheduleWorkflowTask(postCommitCtx, commit.NewRun.ID); err != nil {
		return "", err
	}

	return commit.NewRun.ID, nil
}

// resetDuplicateOf 返回 base 最近一次 reset 出的 Run，当且仅当那次 reset 来自同一个 RequestID 的请求；否则返回空。
// 去重键写在 reset 出的 Run 的 reset 标记上（见 execution.ResetRequestDedupeKey），只在这个 Run 自己的 branch 上
// 查找：reset 出的 Run 再被 reset 时，新 Run fork 来的祖先前缀里带着上一代的 reset 标记，不能算作新 Run 的创建请求
// （对齐 Temporal 只比较 current Run 的 CreateRequestId）。
func (s *Server) resetDuplicateOf(base *execution.Run, requestID string) (string, error) {
	if requestID == "" || base.ResetRunID == "" {
		return "", nil
	}
	reset, err := s.mgr.Get(base.ResetRunID)
	if err != nil {
		return "", err
	}
	_, branchID, err := runBranchInfoFromRun(reset)
	if err != nil {
		return "", err
	}
	_, found, err := s.mgr.GetEventIDByDedupeKey(reset.ID, branchID, execution.ResetRequestDedupeKey(requestID))
	if err != nil || !found {
		return "", err
	}
	return reset.ID, nil
}

// prepareReset 在事务之外完成 reset 的全部校验与 History 分支的 fork，构造新 Run 与提交内容：被拒绝的 reset
// 不改动原 run。新 Run 按事务外读到的 base run 构造；CommitResetRun 在锁下重读 base 与被取代的 run。
// base 已被同一个 RequestID 的请求 reset 过时不 fork，duplicateOf 返回那次 reset 出的 Run。
func (s *Server) prepareReset(req execution.ResetWorkflowRequest) (commit execution.ResetRunCommit, duplicateOf string, err error) {
	oldRun, err := s.mgr.Get(req.RunID)
	if err != nil {
		return execution.ResetRunCommit{}, "", err
	}
	if duplicateOf, err := s.resetDuplicateOf(oldRun, req.RequestID); err != nil || duplicateOf != "" {
		return execution.ResetRunCommit{}, duplicateOf, err
	}
	events, err := s.mgr.GetEvents(req.RunID)
	if err != nil {
		return execution.ResetRunCommit{}, "", err
	}
	point, err := execution.PlanReset(events, req)
	if err != nil {
		return execution.ResetRunCommit{}, "", err
	}
	resetNodeID := point.NodeID
	status, err := s.GetRunStatus(oldRun.ID)
	if err != nil {
		return execution.ResetRunCommit{}, "", err
	}

	newRunID := execution.NewID()
	newBranchID := newRunID
	now := s.clock.Now()
	// 新 run 的搜索属性与分叉点上未结束的 Activity / Timer 按 History 求出（base run 的 state 行只提供心跳详情），
	// 在 fork 之前完成：读取或校验失败时不留下孤儿分支。
	baseActivities, err := s.mgr.ListActivityStates(req.RunID)
	if err != nil {
		return execution.ResetRunCommit{}, "", err
	}
	searchAttributes, err := execution.SearchAttributesAtEvent(events, resetNodeID)
	if err != nil {
		return execution.ResetRunCommit{}, "", err
	}
	pending := execution.PlanResetPendingWork(execution.ResetPendingInput{
		Events: events, Cutoff: resetNodeID, BaseActivities: baseActivities,
		NewRunID: newRunID, TargetWorkerIdentity: oldRun.TargetWorkerIdentity, Reason: req.Reason, Now: now,
	})

	forkResp, err := s.mgr.ForkBranch(execution.ForkBranchRequest{
		SourceRunID: req.RunID,
		ForkNodeID:  resetNodeID,
		NewRunID:    newRunID,
		NewBranchID: newBranchID,
		Info:        oldRun.WorkflowID,
	})
	if err != nil {
		return execution.ResetRunCommit{}, "", fmt.Errorf("fork branch: %w", err)
	}

	newRun := execution.NewResetRun(oldRun, newRunID, req.RunID, forkResp.BranchToken, now)
	newRun.SearchAttributes = searchAttributes
	// 新 run 继承 execution deadline，run deadline 从 reset 起算，随新 run 写入。配置读不到时 reset 照常
	// 进行，新 run 不设截止时间。
	if wf, err := s.workflowConfigs.GetWorkflowConfig(definition.WorkflowConfigRef{Namespace: newRun.Namespace, WorkflowType: newRun.WorkflowType, Version: newRun.WorkflowConfigVersion}); err == nil {
		applyRunTimeouts(newRun, wf, now)
	} else {
		s.logger.Warn("workflow config unavailable for reset run; run deadlines not set", "run_id", newRunID, "err", err)
	}
	// 进事务前构造好 newTreeID/newBranchIDVal/wftFailedEvent（gorm 单连接：事务内不能再开非事务读，会死锁）。
	newTreeID, newBranchIDVal, err := runBranchInfoFromRun(newRun)
	if err != nil {
		return execution.ResetRunCommit{}, "", err
	}
	wftFailedEvent := history.NewWorkflowTaskFailedEvent(history.WorkflowTaskFailedEvent{
		ScheduledEventID: point.ScheduledEventID,
		StartedEventID:   resetNodeID,
		Cause:            execution.WorkflowTaskFailedCauseResetWorkflow,
		BaseRunID:        req.RunID,
		ResetRunID:       newRunID,
		RequestID:        req.RequestID,
		Reason:           req.Reason,
	})

	// 被取代的 Run 与新 Run 在同一事务里终止：reset 失败时它们保持运行。
	var terminate []execution.ResetTermination
	if status == history.StatusRunning {
		replaced, err := resetTermination(oldRun)
		if err != nil {
			return execution.ResetRunCommit{}, "", err
		}
		terminate = append(terminate, replaced)
	}
	expectedCurrent, replacedCurrent, err := s.takeOverCurrentForReset(refOf(oldRun))
	if err != nil {
		return execution.ResetRunCommit{}, "", err
	}
	if replacedCurrent != nil {
		terminate = append(terminate, *replacedCurrent)
	}
	return execution.ResetRunCommit{
		NewRun: newRun, ExpectedCurrent: expectedCurrent, BaseRunID: req.RunID, ExpectedBaseResetRunID: oldRun.ResetRunID,
		TreeID: newTreeID, BranchID: newBranchIDVal, FailedEvent: wftFailedEvent, Pending: pending,
		ReapplyEvents: execution.ResetReapplyEvents(events, resetNodeID, req.ReapplyExcludeTypes),
		Terminate:     terminate,
		Now:           now,
	}, "", nil
}

// takeOverCurrentForReset 返回 reset 切代时 current CAS 的 expectedOld，以及需要随 reset 一起终止的实际 current。
//
// reset 把 current 切到新 run：expectedOld 取当前 current（不一定是 base——reset 一个历史代时
// current 仍指向最新代），与 Temporal reset 一致（新 reset run 始终成为 current）。查不到 current
// 则按首次设置（expectedOld=""）。必须在事务前调用（gorm 单连接：事务内不能再开非事务读）。
//
// reset 历史代时（base != 实际 current），实际 current 可能仍在 running，CAS 切代后会变成
// 「不再是 current 的 running run」——孤儿/僵尸代，故对标 Temporal workflow_resetter 由 reset 的事务终止它。
// base == current 时 base 已由调用方处理，这里不重复终止。
func (s *Server) takeOverCurrentForReset(base runRef) (string, *execution.ResetTermination, error) {
	current, err := s.mgr.GetCurrentRunID(base.Namespace, base.WorkflowID)
	if errors.Is(err, execution.ErrNoCurrentRun) {
		return "", nil, nil
	}
	if err != nil {
		return "", nil, err
	}
	if current == "" || current == base.ID {
		return current, nil, nil
	}
	status, err := s.GetRunStatus(current)
	if err != nil {
		return "", nil, err
	}
	if status != history.StatusRunning {
		return current, nil, nil
	}
	treeID, branchID, err := s.runBranchInfo(current)
	if err != nil {
		return "", nil, err
	}
	return current, &execution.ResetTermination{RunID: current, TreeID: treeID, BranchID: branchID}, nil
}

// resetTermination 在事务外解出被取代 run 的 History 分支；事务内 CommitResetRun 在锁下读取并终止它。
func resetTermination(run *execution.Run) (execution.ResetTermination, error) {
	treeID, branchID, err := runBranchInfoFromRun(run)
	if err != nil {
		return execution.ResetTermination{}, err
	}
	return execution.ResetTermination{RunID: run.ID, TreeID: treeID, BranchID: branchID}, nil
}

// startResetPendingWork 在 reset 提交之后派发新 Run 保留的 Activity、挂上超时与 Timer。Activity 的
// Schedule-To-Start 与 Schedule-To-Close 从 reset 时刻起算；已过期的 Timer 立即触发。派发失败时 state 已落库，
// 由 reconciler 重投，只记日志。
func (s *Server) startResetPendingWork(ctx context.Context, pending execution.ResetPendingWork) {
	postCommitCtx := context.WithoutCancel(ctx)
	for _, st := range pending.Activities {
		s.armActivityTimeouts(st, true)
		if _, err := s.dispatchActivityTask(postCommitCtx, s.activityTaskFromState(st)); err != nil {
			s.logger.Error("post-commit reset activity dispatch failed; reconciler will redispatch",
				"run_id", st.RunID, "scheduled_event_id", st.ScheduledEventID, "err", err)
		}
	}
	for _, st := range pending.Timers {
		s.startDurableTimer(st.RunID, st.ScheduledEventID, st.Deadline)
	}
}
