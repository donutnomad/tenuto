package engine

import (
	"context"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/program/programtest"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

// ============================================
// 父关闭策略
// ============================================
// 测试策略:
// - Happy Path: 每种策略 × 父 Run 的每种关闭方式（完成、失败、超时、取消、终止、continue-as-new）
// - Happy Path: 多层树：被终止的子 Workflow 按自己子关联的策略处理孙 Workflow
// - Happy Path: 子 Workflow continue-as-new 之后，策略作用在它的当前 Run 上
// - Edge Case: 已结束的子 Workflow 不被处理；Abandon 的子 Workflow 结束时不再写父 History
// - Recovery: 父 Run 关闭之后、处理子 Workflow 之前崩溃，恢复时由子 Run 补做
// ============================================

// policyTree 是一棵用于父关闭策略测试的 Workflow 树：父 Run 已启动，父、子、孙三种类型已注册。
type policyTree struct {
	srv                                   *Server
	mgr                                   *memory.ExecutionManager
	parentRunID                           string
	parentType, childType, grandchildType string
}

func newPolicyTree(t *testing.T, parentRunID string) *policyTree {
	t.Helper()
	srv, mgr := newServerForTest(t)
	register := func(name string) string {
		ref, err := srv.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{Type: name + "-flow-" + parentRunID, Program: programtest.Noop()})
		if err != nil {
			t.Fatal(err)
		}
		return ref.WorkflowType
	}
	tree := &policyTree{
		srv: srv, mgr: mgr, parentRunID: parentRunID,
		parentType: register("parent"), childType: register("child"), grandchildType: register("grandchild"),
	}
	createStartedRunForTest(t, mgr, &execution.Run{
		ID: parentRunID, Namespace: protocol.DefaultNamespace, WorkflowID: tree.parentType, WorkflowType: tree.parentType,
		Status: history.StatusRunning, CreatedAt: time.Now(),
	})
	// continue-as-new 按 current 指针 CAS 切代。
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error {
		_, err := tx.SetCurrentRunCAS(protocol.DefaultNamespace, tree.parentType, "", parentRunID)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	return tree
}

// completeTask 在 runID 上打开一个 WFT 并以 commands 完成它。
func (tree *policyTree) completeTask(t *testing.T, runID string, commands ...history.Command) {
	t.Helper()
	wftID := appendOpenWorkflowTaskForTest(t, tree.mgr, runID)
	if err := tree.srv.HandleWorkflowTaskCompleted(tree.srv.WorkflowTaskContext(context.Background(), runID, wftID), runID, wftID, commands, ""); err != nil {
		t.Fatal(err)
	}
}

// startChild 由 runID 以 policy 启动类型为 typ 的子 Workflow，返回子 Run ID。
func (tree *policyTree) startChild(t *testing.T, runID, typ, childID string, policy protocol.ParentClosePolicy) string {
	t.Helper()
	tree.completeTask(t, runID, history.Command{
		Type: history.CmdStartChildWorkflow, ChildID: childID, WorkflowType: typ,
		Input: map[string]any{"order_id": "o-1"}, ParentClosePolicy: policy,
	})
	return tree.link(t, runID, childID).ChildRunID
}

func (tree *policyTree) link(t *testing.T, runID, childID string) *execution.ChildWorkflowState {
	t.Helper()
	states, err := tree.mgr.ListChildWorkflowStates(runID)
	if err != nil {
		t.Fatal(err)
	}
	for _, state := range states {
		if state.ChildID == childID {
			return state
		}
	}
	t.Fatalf("run %s has no child %s", runID, childID)
	return nil
}

// closeRun 以 kind 指定的方式关闭 runID，全部走生产的关闭路径。
func (tree *policyTree) closeRun(t *testing.T, runID, typ, kind string) {
	t.Helper()
	switch kind {
	case "completed":
		tree.completeTask(t, runID, history.Command{Type: history.CmdCompleteWorkflow, Output: map[string]any{"ok": true}})
	case "failed":
		tree.completeTask(t, runID, history.Command{Type: history.CmdFailWorkflow, Error: "boom", WorkflowFailure: &protocol.Failure{Message: "boom"}})
	case "cancelled":
		tree.completeTask(t, runID, history.Command{Type: history.CmdCancelWorkflow})
	case "continued_as_new":
		tree.completeTask(t, runID, history.Command{Type: history.CmdContinueAsNew, WorkflowType: typ, Input: map[string]any{}})
	case "terminated":
		if err := tree.srv.TerminateWorkflow(context.Background(), "", "", runID, "stop"); err != nil {
			t.Fatal(err)
		}
	case "timed_out":
		tree.srv.handleWorkflowTimeout(runID)
	default:
		t.Fatalf("unknown close kind %q", kind)
	}
	state, err := tree.mgr.GetWorkflowExecutionState(runID)
	if err != nil || !state.IsTerminal() {
		t.Fatalf("run %s not closed by %s: %+v, %v", runID, kind, state, err)
	}
}

func (tree *policyTree) state(t *testing.T, runID string) *execution.WorkflowExecutionState {
	t.Helper()
	state, err := tree.mgr.GetWorkflowExecutionState(runID)
	if err != nil {
		t.Fatal(err)
	}
	return state
}

// assertTerminatedByPolicy 断言 runID 被父关闭策略终止。
func (tree *policyTree) assertTerminatedByPolicy(t *testing.T, runID string) {
	t.Helper()
	if state := tree.state(t, runID); state.Status != history.StatusTerminated {
		t.Fatalf("run %s status = %s, want terminated", runID, state.Status)
	}
	events, err := tree.mgr.GetEvents(runID)
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range events {
		if event.WorkflowTerminated != nil {
			if event.WorkflowTerminated.Reason != execution.ParentCloseReason {
				t.Fatalf("run %s terminated with reason %q, want %q", runID, event.WorkflowTerminated.Reason, execution.ParentCloseReason)
			}
			return
		}
	}
	t.Fatalf("run %s has no WorkflowTerminated event", runID)
}

func (tree *policyTree) assertRunning(t *testing.T, runID string, cancelRequested bool) {
	t.Helper()
	state := tree.state(t, runID)
	if state.Status != history.StatusRunning || state.CancelRequested != cancelRequested {
		t.Fatalf("run %s = %s (cancel requested %t), want running (cancel requested %t)", runID, state.Status, state.CancelRequested, cancelRequested)
	}
}

// 每种策略在父 Run 以每种方式关闭时生效，包括 continue-as-new：子 Workflow 属于关闭的那个 Run。
func TestParentClosePolicyAppliesOnEveryParentClose(t *testing.T) {
	kinds := []string{"completed", "failed", "timed_out", "cancelled", "terminated", "continued_as_new"}
	policies := []protocol.ParentClosePolicy{"", protocol.ParentClosePolicyTerminate, protocol.ParentClosePolicyRequestCancel, protocol.ParentClosePolicyAbandon}
	for _, kind := range kinds {
		for _, policy := range policies {
			name := kind + "/" + string(policy)
			if policy == "" {
				name = kind + "/default"
			}
			t.Run(name, func(t *testing.T) {
				tree := newPolicyTree(t, "run-policy-"+kind+"-"+string(policy))
				childRunID := tree.startChild(t, tree.parentRunID, tree.childType, "child_1", policy)
				tree.assertRunning(t, childRunID, false)

				tree.closeRun(t, tree.parentRunID, tree.parentType, kind)

				switch policy {
				case "", protocol.ParentClosePolicyTerminate:
					tree.assertTerminatedByPolicy(t, childRunID)
					if link := tree.link(t, tree.parentRunID, "child_1"); link.Status != execution.ChildWorkflowStatusEnums.Terminated {
						t.Fatalf("child link = %s, want terminated", link.Status)
					}
				case protocol.ParentClosePolicyRequestCancel:
					tree.assertRunning(t, childRunID, true)
				case protocol.ParentClosePolicyAbandon:
					tree.assertRunning(t, childRunID, false)
				}
			})
		}
	}
}

// 策略记录在父 Run 的 ChildWorkflowStarted 事件与子关联上；未指定时记为 Terminate。
func TestParentClosePolicyRecordedOnInitiation(t *testing.T) {
	tree := newPolicyTree(t, "run-policy-recorded")
	tree.startChild(t, tree.parentRunID, tree.childType, "abandoned", protocol.ParentClosePolicyAbandon)
	tree.startChild(t, tree.parentRunID, tree.childType, "defaulted", "")
	events, err := tree.mgr.GetEvents(tree.parentRunID)
	if err != nil {
		t.Fatal(err)
	}
	recorded := map[string]protocol.ParentClosePolicy{}
	for _, event := range events {
		if started := event.ChildWorkflowStarted; started != nil {
			recorded[started.ChildID] = started.ParentClosePolicy
		}
	}
	want := map[string]protocol.ParentClosePolicy{"abandoned": protocol.ParentClosePolicyAbandon, "defaulted": protocol.ParentClosePolicyTerminate}
	for childID, policy := range want {
		if recorded[childID] != policy {
			t.Fatalf("ChildWorkflowStarted(%s).ParentClosePolicy = %q, want %q", childID, recorded[childID], policy)
		}
		if link := tree.link(t, tree.parentRunID, childID); link.ParentClosePolicy != policy {
			t.Fatalf("child link %s policy = %q, want %q", childID, link.ParentClosePolicy, policy)
		}
	}
}

// 未知的策略让 Workflow Task 失败，不启动子 Workflow。
func TestParentClosePolicyRejectsUnknownValue(t *testing.T) {
	tree := newPolicyTree(t, "run-policy-unknown")
	wftID := appendOpenWorkflowTaskForTest(t, tree.mgr, tree.parentRunID)
	err := tree.srv.HandleWorkflowTaskCompleted(tree.srv.WorkflowTaskContext(context.Background(), tree.parentRunID, wftID), tree.parentRunID, wftID, []history.Command{{
		Type: history.CmdStartChildWorkflow, ChildID: "child_1", WorkflowType: tree.childType, ParentClosePolicy: "detach",
	}}, "")
	if err == nil {
		t.Fatal("unknown parent close policy accepted")
	}
	if states, _ := tree.mgr.ListChildWorkflowStates(tree.parentRunID); len(states) != 0 {
		t.Fatalf("child started despite unknown policy: %+v", states)
	}
}

// 被终止的子 Workflow 按它自己子关联的策略处理孙 Workflow。
func TestParentClosePolicyPropagatesThroughTree(t *testing.T) {
	tree := newPolicyTree(t, "run-policy-tree")
	childRunID := tree.startChild(t, tree.parentRunID, tree.childType, "child_1", "")
	terminatedGrandchild := tree.startChild(t, childRunID, tree.grandchildType, "grand_terminate", protocol.ParentClosePolicyTerminate)
	cancelledGrandchild := tree.startChild(t, childRunID, tree.grandchildType, "grand_cancel", protocol.ParentClosePolicyRequestCancel)
	abandonedGrandchild := tree.startChild(t, childRunID, tree.grandchildType, "grand_abandon", protocol.ParentClosePolicyAbandon)

	tree.closeRun(t, tree.parentRunID, tree.parentType, "terminated")

	tree.assertTerminatedByPolicy(t, childRunID)
	tree.assertTerminatedByPolicy(t, terminatedGrandchild)
	tree.assertRunning(t, cancelledGrandchild, true)
	tree.assertRunning(t, abandonedGrandchild, false)
}

// 子 Workflow continue-as-new 之后，策略作用在它的当前 Run 上。
func TestParentClosePolicyFollowsChildContinuation(t *testing.T) {
	tree := newPolicyTree(t, "run-policy-child-can")
	firstChildRunID := tree.startChild(t, tree.parentRunID, tree.childType, "child_1", "")
	tree.closeRun(t, firstChildRunID, tree.childType, "continued_as_new")
	first, err := tree.mgr.Get(firstChildRunID)
	if err != nil {
		t.Fatal(err)
	}
	currentChildRunID, err := tree.mgr.GetCurrentRunID(first.Namespace, first.WorkflowID)
	if err != nil || currentChildRunID == firstChildRunID {
		t.Fatalf("child continuation missing: %s, %v", currentChildRunID, err)
	}

	tree.closeRun(t, tree.parentRunID, tree.parentType, "completed")

	tree.assertTerminatedByPolicy(t, currentChildRunID)
}

// 已结束的子 Workflow 不被处理；Abandon 的子 Workflow 在父 Run 关闭之后结束时，父 History 不再追加事件。
func TestParentClosePolicyLeavesClosedChildrenAlone(t *testing.T) {
	tree := newPolicyTree(t, "run-policy-closed-child")
	doneRunID := tree.startChild(t, tree.parentRunID, tree.childType, "done", "")
	abandonedRunID := tree.startChild(t, tree.parentRunID, tree.childType, "abandoned", protocol.ParentClosePolicyAbandon)
	tree.closeRun(t, doneRunID, tree.childType, "completed")

	tree.closeRun(t, tree.parentRunID, tree.parentType, "terminated")

	if state := tree.state(t, doneRunID); state.Status != history.StatusCompleted {
		t.Fatalf("closed child status = %s, want completed", state.Status)
	}
	if link := tree.link(t, tree.parentRunID, "done"); link.Status != execution.ChildWorkflowStatusEnums.Completed {
		t.Fatalf("closed child link = %s, want completed", link.Status)
	}
	before, _ := tree.mgr.GetEvents(tree.parentRunID)
	tree.closeRun(t, abandonedRunID, tree.childType, "completed")
	after, _ := tree.mgr.GetEvents(tree.parentRunID)
	if len(after) != len(before) {
		t.Fatalf("abandoned child close appended to the closed parent's history: %d -> %d events", len(before), len(after))
	}
	if link := tree.link(t, tree.parentRunID, "abandoned"); link.Status != execution.ChildWorkflowStatusEnums.Completed {
		t.Fatalf("abandoned child link = %s, want completed", link.Status)
	}
}

// 父 Run 的终态提交之后、处理子 Workflow 之前进程崩溃：恢复扫描到仍在运行的子 Run，补做父关闭策略。
func TestParentClosePolicyRecoveredAfterCrash(t *testing.T) {
	tree := newPolicyTree(t, "run-policy-crash")
	terminateRunID := tree.startChild(t, tree.parentRunID, tree.childType, "terminate", "")
	cancelRunID := tree.startChild(t, tree.parentRunID, tree.childType, "cancel", protocol.ParentClosePolicyRequestCancel)
	abandonRunID := tree.startChild(t, tree.parentRunID, tree.childType, "abandon", protocol.ParentClosePolicyAbandon)
	// 只提交父 Run 的终态，不执行提交之后的收尾（runClosed），模拟两步之间崩溃。
	if _, inserted, err := tree.srv.commitTerminalForTest(tree.parentRunID, history.NewWorkflowCompletedEvent(nil), history.StatusCompleted, nil); err != nil || !inserted {
		t.Fatalf("commit parent close: inserted=%t err=%v", inserted, err)
	}
	tree.assertRunning(t, terminateRunID, false)
	tree.assertRunning(t, cancelRunID, false)

	if err := tree.srv.RecoverRunningWorkflows(context.Background()); err != nil {
		t.Fatal(err)
	}

	tree.assertTerminatedByPolicy(t, terminateRunID)
	tree.assertRunning(t, cancelRunID, true)
	tree.assertRunning(t, abandonRunID, false)
	// 补做幂等：再扫一轮不追加取消请求。
	if err := tree.srv.RecoverRunningWorkflows(context.Background()); err != nil {
		t.Fatal(err)
	}
	events, _ := tree.mgr.GetEvents(cancelRunID)
	if got := countEngineEvents(events, history.EventWorkflowCancellationRequested); got != 1 {
		t.Fatalf("cancel requested %d times, want 1", got)
	}
}
