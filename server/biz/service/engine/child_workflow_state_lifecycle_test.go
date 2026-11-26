package engine

import (
	"context"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/program/programtest"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
)

// startChildWorkflow 注册 parent+child 配置、建 parent run + open WFT，提交一个 CmdStartChildWorkflow，
// 返回 parentRunID + childScheduledEventID（父 ChildWorkflowStarted 事件 ID = child state 生命周期 key）
// + childRunID。
func startChildWorkflow(t *testing.T, srv *Server, mgr execution.ExecutionManager, parentRunID string) (string, int64, string) {
	t.Helper()
	wftID, childWorkflowType := openParentForChildWorkflow(t, srv, mgr, parentRunID)
	submitStartChildWorkflow(t, srv, parentRunID, wftID, childWorkflowType)
	states, _ := mgr.ListChildWorkflowStates(parentRunID)
	if len(states) != 1 {
		t.Fatalf("want 1 child state after schedule, got %d", len(states))
	}
	return parentRunID, states[0].ScheduledEventID, states[0].ChildRunID
}

// openParentForChildWorkflow 注册 parent+child 配置、建 parent run 并打开一个 WFT，返回 WFT 与子 Workflow ID。
func openParentForChildWorkflow(t *testing.T, srv *Server, mgr execution.ExecutionManager, parentRunID string) (int64, string) {
	t.Helper()
	parentRef, err := srv.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{Type: "parent-flow-" + parentRunID, Program: programtest.Noop()})
	if err != nil {
		t.Fatal(err)
	}
	childRef, err := srv.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{Type: "child-flow-" + parentRunID, Program: programtest.Noop()})
	if err != nil {
		t.Fatal(err)
	}
	parent := &execution.Run{ID: parentRunID, WorkflowID: parentRef.WorkflowType, WorkflowType: parentRef.WorkflowType, WorkflowConfigVersion: parentRef.Version, Status: history.StatusRunning, CreatedAt: time.Now()}
	createStartedRunForTest(t, mgr, parent)
	return appendOpenWorkflowTaskForTest(t, mgr, parentRunID), childRef.WorkflowType
}

// submitStartChildWorkflow 以一个 CmdStartChildWorkflow 完成父 Run 的 WFT。
func submitStartChildWorkflow(t *testing.T, srv *Server, parentRunID string, wftID int64, childWorkflowType string) {
	t.Helper()
	if err := srv.HandleWorkflowTaskCompleted(srv.WorkflowTaskContext(context.Background(), parentRunID, wftID), parentRunID, wftID, []history.Command{{
		Type:         history.CmdStartChildWorkflow,
		ChildID:      "child_1",
		WorkflowType: childWorkflowType,
		Input:        map[string]any{"order_id": "o-1"},
	}}, ""); err != nil {
		t.Fatal(err)
	}
}

// Happy Path: 子完成后，父侧 ChildWorkflowState=completed（带 Output），父 history 有 1 个
// ChildWorkflowCompleted，父被唤醒（多一个 WFT scheduled）。
func TestChildCompletePropagatesState(t *testing.T) {
	srv, mgr := newServerForTest(t)
	parentRunID, schedID, childRunID := startChildWorkflow(t, srv, mgr, "run-parent-complete")

	if err := srv.completeWorkflow(childRunID, map[string]any{"ok": true}); err != nil {
		t.Fatal(err)
	}

	st, _, _ := mgr.GetChildWorkflowState(parentRunID, schedID)
	if st.Status != execution.ChildWorkflowStatusEnums.Completed || !st.IsTerminal() {
		t.Fatalf("expected completed terminal, got %+v", st)
	}
	if st.Output["ok"] != true {
		t.Fatalf("output not propagated: %+v", st)
	}
	parentHistory, _ := mgr.GetEvents(parentRunID)
	if countEngineEvents(parentHistory, history.EventChildWorkflowCompleted) != 1 {
		t.Fatalf("expected 1 ChildWorkflowCompleted, got %d", countEngineEvents(parentHistory, history.EventChildWorkflowCompleted))
	}
	if countEngineEvents(parentHistory, history.EventWorkflowTaskScheduled) != 2 {
		t.Fatalf("expected child completion to wake parent (2nd WFT), got %+v", parentHistory)
	}
}

// Happy Path: 子失败后，父侧 ChildWorkflowState=failed（带 Error），父 history 有 1 个 ChildWorkflowFailed。
func TestChildFailPropagatesState(t *testing.T) {
	srv, mgr := newServerForTest(t)
	parentRunID, schedID, childRunID := startChildWorkflow(t, srv, mgr, "run-parent-fail")

	if err := srv.failWorkflow(childRunID, protocol.Failure{Message: "boom"}, ""); err != nil {
		t.Fatal(err)
	}

	st, _, _ := mgr.GetChildWorkflowState(parentRunID, schedID)
	if st.Status != execution.ChildWorkflowStatusEnums.Failed || st.Error != "boom" {
		t.Fatalf("expected failed with error, got %+v", st)
	}
	parentHistory, _ := mgr.GetEvents(parentRunID)
	if countEngineEvents(parentHistory, history.EventChildWorkflowFailed) != 1 {
		t.Fatalf("expected 1 ChildWorkflowFailed, got %d", countEngineEvents(parentHistory, history.EventChildWorkflowFailed))
	}
}

// Edge Case（幂等，对标 timer 重复 fire）: 重复 propagate close 被 Status 终态拦截——不写第二个父 close 事件
// （替代 childWorkflowAlreadyClosed 历史扫描）。
func TestChildClosePropagateIdempotent(t *testing.T) {
	srv, mgr := newServerForTest(t)
	parentRunID, _, childRunID := startChildWorkflow(t, srv, mgr, "run-parent-dup")
	child, _ := mgr.Get(childRunID)

	if err := srv.completeWorkflow(childRunID, map[string]any{"ok": true}); err != nil {
		t.Fatal(err)
	}
	// 直接再调一次 propagate（模拟重投 / recovery 补发）——应被父 state 终态拦截。
	if err := srv.propagateChildWorkflowClose(context.Background(), child.ID, child.ParentRunID, history.EventChildWorkflowCompleted, map[string]any{"ok": true}, "", nil); err != nil {
		t.Fatal(err)
	}

	parentHistory, _ := mgr.GetEvents(parentRunID)
	if got := countEngineEvents(parentHistory, history.EventChildWorkflowCompleted); got != 1 {
		t.Fatalf("repeat propagate must be idempotent, got %d ChildWorkflowCompleted", got)
	}
}

// Happy Path（recovery 路径 A：子 Run 不存在 → 重建）: ChildWorkflowState=initiated 且子 Run 未建
// （崩溃在 pass-2 建子 Run 前）。recovery 读 state 重建子 Run（不扫历史、不需父 history close）。
func TestRecoverChildRebuildsMissingChildRun(t *testing.T) {
	srv, mgr := newFailingServerForTest(t)
	const parentRunID = "run-parent-recover-rebuild"
	wftID, childWorkflowType := openParentForChildWorkflow(t, srv, mgr, parentRunID)
	// 模拟"崩溃在建子 Run 前"：子 Run 的 WorkflowStarted 写入失败，父侧 state 停在 initiated。
	mgr.failOn(history.EventWorkflowStarted)
	submitStartChildWorkflow(t, srv, parentRunID, wftID, childWorkflowType)
	mgr.failOn("")
	states, _ := mgr.ListChildWorkflowStates(parentRunID)
	if len(states) != 1 || states[0].Status != execution.ChildWorkflowStatusEnums.Initiated || states[0].ChildRunID == "" || srv.runExists(states[0].ChildRunID) {
		t.Fatalf("want one initiated child state without a child run, got %+v", states)
	}
	schedID, childRunID := states[0].ScheduledEventID, states[0].ChildRunID

	if err := srv.recoverChildWorkflows(context.Background(), parentRunID); err != nil {
		t.Fatal(err)
	}
	child, err := mgr.Get(childRunID)
	if err != nil || child.ParentRunID != parentRunID || child.ParentScheduledEventID != schedID {
		t.Fatalf("child run not rebuilt from state: %+v, %v", child, err)
	}
	childHistory, _ := mgr.GetEvents(childRunID)
	if countEngineEvents(childHistory, history.EventWorkflowStarted) != 1 {
		t.Fatalf("rebuilt child missing WorkflowStarted: %+v", childHistory)
	}
}

// Happy Path（recovery 路径 B：子 Run 已存在终态 → 补 propagate）: 子 Run 已完成但父 ChildWorkflowState
// 仍 started（崩溃在 propagate 前）。recovery 读 state 求 open child，发现子 Run 已终态 → 补 propagate。
func TestRecoverChildPropagatesClosedChild(t *testing.T) {
	srv, mgr := newServerForTest(t)
	parentRunID, schedID, childRunID := startChildWorkflow(t, srv, mgr, "run-parent-recover-propagate")
	// 子 Run 直接置 completed（不经 completeWorkflow 的 propagate），父 state 留 started。
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error {
		child, err := tx.GetRunForUpdate(childRunID)
		if err != nil {
			return err
		}
		output := map[string]any{"ok": true}
		if _, err := tx.Append(childRunID, childRunID, childRunID, history.NewWorkflowCompletedEvent(output)); err != nil {
			return err
		}
		if err := child.MarkTerminal(history.StatusCompleted, output); err != nil {
			return err
		}
		return tx.UpdateRun(child)
	}); err != nil {
		t.Fatal(err)
	}

	if err := srv.recoverChildWorkflows(context.Background(), parentRunID); err != nil {
		t.Fatal(err)
	}
	st, _, _ := mgr.GetChildWorkflowState(parentRunID, schedID)
	if st.Status != execution.ChildWorkflowStatusEnums.Completed {
		t.Fatalf("recovery did not propagate child close to parent state, got %q", st.Status)
	}
	parentHistory, _ := mgr.GetEvents(parentRunID)
	if countEngineEvents(parentHistory, history.EventChildWorkflowCompleted) != 1 {
		t.Fatalf("recovery did not append child close to parent history, got %d", countEngineEvents(parentHistory, history.EventChildWorkflowCompleted))
	}
}

// Edge Case: 已终态的 ChildWorkflowState 在 recovery 时被跳过（不重建子 Run、不重复 propagate）。
func TestRecoverChildSkipsTerminal(t *testing.T) {
	srv, mgr := newServerForTest(t)
	parentRunID, schedID, childRunID := startChildWorkflow(t, srv, mgr, "run-parent-recover-skip")
	if err := srv.completeWorkflow(childRunID, map[string]any{"ok": true}); err != nil {
		t.Fatal(err)
	}
	parentBefore, _ := mgr.GetEvents(parentRunID)
	completedBefore := countEngineEvents(parentBefore, history.EventChildWorkflowCompleted)

	if err := srv.recoverChildWorkflows(context.Background(), parentRunID); err != nil {
		t.Fatal(err)
	}
	parentAfter, _ := mgr.GetEvents(parentRunID)
	if got := countEngineEvents(parentAfter, history.EventChildWorkflowCompleted) - completedBefore; got != 0 {
		t.Fatalf("terminal child must not be re-propagated, got %d extra close", got)
	}
	st, _, _ := mgr.GetChildWorkflowState(parentRunID, schedID)
	if st.Status != execution.ChildWorkflowStatusEnums.Completed {
		t.Fatalf("terminal state changed by recovery: %q", st.Status)
	}
}

// Edge Case（读 state 区分性）: 父 history 无 ChildWorkflowStarted 事件但 ChildWorkflowState=initiated
// （state 是唯一权威）时，recovery 仍据 state 重建子 Run（证明读 state 而非扫父 history）。
func TestRecoverChildReadsStateNotHistory(t *testing.T) {
	srv, mgr := newServerForTest(t)
	parentRef, _ := srv.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{Type: "parent-only-state", Program: programtest.Noop()})
	childRef, _ := srv.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{Type: "child-only-state", Program: programtest.Noop()})
	parent := &execution.Run{ID: "run-parent-only-state", WorkflowID: parentRef.WorkflowType, WorkflowType: parentRef.WorkflowType, WorkflowConfigVersion: parentRef.Version, Status: history.StatusRunning, CreatedAt: time.Now()}
	_ = mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(parent)) })
	// 只写 ChildWorkflowState（initiated），父 history 无任何 child 事件。
	_ = mgr.UpsertChildWorkflowState(&execution.ChildWorkflowState{
		RunID: parent.ID, ScheduledEventID: 99, ChildID: "child_1", ChildRunID: "child-run-only-state",
		WorkflowType: childRef.WorkflowType, WorkflowConfigVersion: childRef.Version,
		Status: execution.ChildWorkflowStatusEnums.Initiated, Input: map[string]any{"x": 1},
	})

	if err := srv.recoverChildWorkflows(context.Background(), parent.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.Get("child-run-only-state"); err != nil {
		t.Fatal("recovery must rebuild child run from state alone (no history)")
	}
}

// Happy Path: schedule 子后，父侧 ChildWorkflowState 应存在，Status=started（建子 Run 成功后），
// ChildRunID 已填、ChildID/WorkflowID/Input 正确，生命周期 key = 父 ChildWorkflowStarted 事件 ID。
func TestStartChildWorkflowCreatesState(t *testing.T) {
	srv, mgr := newServerForTest(t)
	parentRunID, schedID, childRunID := startChildWorkflow(t, srv, mgr, "run-parent-start")

	st, ok, _ := mgr.GetChildWorkflowState(parentRunID, schedID)
	if !ok {
		t.Fatal("child state not created")
	}
	if st.ChildID != "child_1" || st.Input["order_id"] != "o-1" {
		t.Fatalf("bad child state: %+v", st)
	}
	if st.Status != execution.ChildWorkflowStatusEnums.Started {
		t.Fatalf("after child run created expected started, got %q", st.Status)
	}
	if st.ChildRunID == "" || st.ChildRunID != childRunID {
		t.Fatalf("childRunID not filled: %+v", st)
	}
	// 生命周期 key = 父 ChildWorkflowStarted 事件 ID
	parentHistory, _ := mgr.GetEvents(parentRunID)
	for i := range parentHistory {
		if parentHistory[i].ChildWorkflowStarted != nil && parentHistory[i].ID != schedID {
			t.Fatalf("child state key %d must equal ChildWorkflowStarted event ID %d", schedID, parentHistory[i].ID)
		}
	}
	// 子 Run 的 ParentScheduledEventID 与 state 主键一致
	child, _ := mgr.Get(childRunID)
	if child.ParentScheduledEventID != schedID || child.ParentRunID != parentRunID {
		t.Fatalf("child run parent linkage mismatch: %+v", child)
	}
}

// completeWorkflow 是测试辅助：直接落 WorkflowCompleted 终态并走完成后的传播，
// 不经过 WFT 提交路径。
func (s *Server) completeWorkflow(runID string, output map[string]any) error {
	run, inserted, err := s.commitTerminalForTest(runID, history.NewWorkflowCompletedEvent(output), history.StatusCompleted, output)
	if err != nil || !inserted {
		return err
	}
	return s.workflowCompletedAfterCommit(run, output)
}

// commitTerminalForTest 在一个事务里落终态事件与 Run 的终态。inserted 为 false 表示 History 已有终态，Run 不变。
func (s *Server) commitTerminalForTest(runID string, event history.Event, status history.Status, output map[string]any) (*execution.Run, bool, error) {
	run, err := s.mgr.Get(runID)
	if err != nil {
		return nil, false, err
	}
	treeID, branchID, err := runBranchInfoFromRun(run)
	if err != nil {
		return nil, false, err
	}
	inserted := false
	err = s.mgr.RunInTx(func(tx execution.ExecutionTx) error {
		var err error
		if _, inserted, err = tx.AppendIdempotent(runID, treeID, branchID, event); err != nil || !inserted {
			return err
		}
		if err := run.MarkTerminal(status, output); err != nil {
			return err
		}
		return tx.UpdateRun(run)
	})
	return run, inserted, err
}
