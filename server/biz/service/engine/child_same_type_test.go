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
)

// startChildrenOfType 让 parentRunID 在一个 Workflow Task 里启动 childIDs 对应的子 Workflow，
// 类型都是 childType，返回按 childIDs 顺序排列的子 Run ID。
func startChildrenOfType(t *testing.T, srv *Server, mgr execution.ExecutionManager, parentRunID, childType string, childIDs ...string) []string {
	t.Helper()
	wftID := openWorkflowTaskForTest(t, mgr, parentRunID)
	commands := make([]history.Command, 0, len(childIDs))
	for _, childID := range childIDs {
		commands = append(commands, history.Command{Type: history.CmdStartChildWorkflow, ChildID: childID, WorkflowID: childType})
	}
	if err := srv.HandleWorkflowTaskCompleted(srv.WorkflowTaskContext(context.Background(), parentRunID, wftID), parentRunID, wftID, commands, ""); err != nil {
		t.Fatal(err)
	}
	states, err := mgr.ListChildWorkflowStates(parentRunID)
	if err != nil {
		t.Fatal(err)
	}
	byChildID := map[string]string{}
	for _, state := range states {
		byChildID[state.ChildID] = state.ChildRunID
	}
	runIDs := make([]string, 0, len(childIDs))
	for _, childID := range childIDs {
		runID := byChildID[childID]
		if runID == "" || !srv.runExists(runID) {
			t.Fatalf("child %s of parent %s has no run (states %+v)", childID, parentRunID, states)
		}
		runIDs = append(runIDs, runID)
	}
	return runIDs
}

func completeChildrenAndExpectPropagation(t *testing.T, srv *Server, mgr execution.ExecutionManager, parentRunID string, childRunIDs []string) {
	t.Helper()
	for _, childRunID := range childRunIDs {
		if err := srv.completeWorkflow(childRunID, map[string]any{"ok": true}); err != nil {
			t.Fatal(err)
		}
	}
	parentHistory, err := mgr.GetEvents(parentRunID)
	if err != nil {
		t.Fatal(err)
	}
	if got := countEngineEvents(parentHistory, history.EventChildWorkflowCompleted); got != len(childRunIDs) {
		t.Fatalf("parent %s: want %d ChildWorkflowCompleted, got %d", parentRunID, len(childRunIDs), got)
	}
}

// 一个父 Run 启动两个同类型的子 Workflow，两个都启动并完成。
func TestSameTypeChildren_OneParentTwoChildrenBothComplete(t *testing.T) {
	srv, mgr := newServerForTest(t)
	childRef, err := srv.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{Type: "same-type-child", Program: programtest.Noop()})
	if err != nil {
		t.Fatal(err)
	}
	parentRunID := startReconcileTestRun(t, srv, &program.WorkflowConfig{Type: "same-type-parent", Program: programtest.Noop()})
	children := startChildrenOfType(t, srv, mgr, parentRunID, childRef.WorkflowID, "child_1", "child_2")

	first, _ := mgr.Get(children[0])
	second, _ := mgr.Get(children[1])
	if first.WorkflowID == second.WorkflowID {
		t.Fatalf("same-type children must have distinct workflow ids, both are %q", first.WorkflowID)
	}
	for _, child := range []*execution.Run{first, second} {
		if child.WorkflowType != childRef.WorkflowID {
			t.Fatalf("child %s: want workflow type %q, got %q", child.ID, childRef.WorkflowID, child.WorkflowType)
		}
		if child.WorkflowID != execution.ChildWorkflowID(parentRunID, child.ParentChildID) {
			t.Fatalf("child %s: workflow id %q is not derived from parent run and child id", child.ID, child.WorkflowID)
		}
	}
	completeChildrenAndExpectPropagation(t, srv, mgr, parentRunID, children)
}

// 两个父 Run 各启动一个同类型的子 Workflow。
func TestSameTypeChildren_TwoParentsOneChildEach(t *testing.T) {
	srv, mgr := newServerForTest(t)
	childRef, err := srv.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{Type: "same-type-child-2p", Program: programtest.Noop()})
	if err != nil {
		t.Fatal(err)
	}
	parentA := startReconcileTestRun(t, srv, &program.WorkflowConfig{Type: "same-type-parent-a", Program: programtest.Noop()})
	parentB := startReconcileTestRun(t, srv, &program.WorkflowConfig{Type: "same-type-parent-b", Program: programtest.Noop()})
	childrenA := startChildrenOfType(t, srv, mgr, parentA, childRef.WorkflowID, "child_1")
	childrenB := startChildrenOfType(t, srv, mgr, parentB, childRef.WorkflowID, "child_1")
	completeChildrenAndExpectPropagation(t, srv, mgr, parentA, childrenA)
	completeChildrenAndExpectPropagation(t, srv, mgr, parentB, childrenB)
}

// 子 Workflow 的类型同时有一个正在运行的顶层 Run：子 Run 照常启动，顶层 Run 仍是该 WorkflowID 的 current。
func TestSameTypeChildren_TypeHasRunningTopLevelRun(t *testing.T) {
	srv, mgr := newServerForTest(t)
	topLevelRunID := startReconcileTestRun(t, srv, &program.WorkflowConfig{Type: "same-type-child-top", Program: programtest.Noop()})
	parentRunID := startReconcileTestRun(t, srv, &program.WorkflowConfig{Type: "same-type-parent-top", Program: programtest.Noop()})
	children := startChildrenOfType(t, srv, mgr, parentRunID, "same-type-child-top", "child_1")

	current, err := mgr.GetCurrentRunID("default", "same-type-child-top")
	if err != nil || current != topLevelRunID {
		t.Fatalf("top-level run must stay current for its workflow id, got %q err=%v", current, err)
	}
	completeChildrenAndExpectPropagation(t, srv, mgr, parentRunID, children)
	if status, _ := srv.GetRunStatus(topLevelRunID); status != history.StatusRunning {
		t.Fatalf("top-level run must stay running, got %s", status)
	}
}

// 恢复时子 Run 启动失败：错误上报，同一父 Run 的 durable timer 仍被恢复。
func TestReconcileRun_ChildStartFailureKeepsRemainingRecoverySteps(t *testing.T) {
	ctx := context.Background()
	srv, mgr := newFailingServerForTest(t)
	childRef, err := srv.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{Type: "recover-child-start-error", Program: programtest.Noop()})
	if err != nil {
		t.Fatal(err)
	}
	runID := startReconcileTestRun(t, srv, &program.WorkflowConfig{Type: "recover-parent-child-start-error", Program: programtest.Noop()})
	wftID := openWorkflowTaskForTest(t, mgr, runID)

	mgr.failOn(history.EventWorkflowStarted)
	defer mgr.failOn("")
	if err := srv.HandleWorkflowTaskCompleted(srv.WorkflowTaskContext(ctx, runID, wftID), runID, wftID, []history.Command{
		{Type: history.CmdStartChildWorkflow, ChildID: "child_1", WorkflowID: childRef.WorkflowID},
		{Type: history.CmdScheduleTimer, TimerID: "sleep-1", Delay: time.Hour},
	}, ""); err != nil {
		t.Fatal(err)
	}
	srv.timers.StopAll()
	if got := timerCount(srv, timerDurable); got != 0 {
		t.Fatalf("precondition: want no armed durable timer, got %d", got)
	}

	_, err = srv.reconcileRun(ctx, runID, true)
	if err == nil {
		t.Fatal("expected the child start failure to be reported")
	}
	if got := timerCount(srv, timerDurable); got != 1 {
		t.Fatalf("durable timer must be recovered despite the child start failure (%v), got %d timers", err, got)
	}
}
