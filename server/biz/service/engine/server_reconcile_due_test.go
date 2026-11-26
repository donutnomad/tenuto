package engine

import (
	"context"
	"testing"
	"time"

	historypkg "github.com/donutnomad/tenuto/protocol/history"
)

// 周期补偿只处理有到期或待处理事项的 Run：远期 timer 的 Run 不被选中，定时器不挂到本副本；
// 进入 lookahead 后下一轮才挂上，仍按原截止时间触发。
func TestReconcileArmsOnlyTimersDueWithinLookahead(t *testing.T) {
	srv, mgr, clk := newOverdueServer(t)
	runID, schedID := scheduleDurableTimer(t, srv, mgr, "run-reconcile-lookahead", time.Hour)
	survivor := loseReplica(t, srv, mgr, clk)
	ref := timerRef{Kind: timerDurable, RunID: runID, ScheduledEventID: schedID}
	const lookahead = 10 * time.Minute

	if _, _, err := survivor.Reconcile(context.Background(), overdueTestGrace, lookahead); err != nil {
		t.Fatal(err)
	}
	if timerArmed(survivor, ref) {
		t.Fatal("timer due in an hour must not be armed by a pass with a 10m lookahead")
	}

	clk.Advance(55 * time.Minute)
	if _, _, err := survivor.Reconcile(context.Background(), overdueTestGrace, lookahead); err != nil {
		t.Fatal(err)
	}
	deadline, armed := survivor.timers.Deadline(ref)
	st, _, err := mgr.GetTimerState(runID, schedID)
	if err != nil {
		t.Fatal(err)
	}
	if !armed || !deadline.Equal(st.Deadline) {
		t.Fatalf("timer within lookahead must be armed at its persisted deadline: armed=%v deadline=%s want %s", armed, deadline, st.Deadline)
	}
}

// 新事实已提交、WFT 调度失败的 Run（NeedsWorkflowTask）由周期补偿补调度，不等全量补偿。
func TestReconcileSchedulesLostWorkflowActivation(t *testing.T) {
	srv, mgr, clk := newOverdueServer(t)
	runID, _ := scheduleDurableTimer(t, srv, mgr, "run-reconcile-activation", time.Hour)
	survivor := loseReplica(t, srv, mgr, clk)
	run, err := mgr.Get(runID)
	if err != nil {
		t.Fatal(err)
	}
	treeID, branchID, err := runBranchInfoFromRun(run)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.Append(runID, treeID, branchID, historypkg.NewWorkflowSignaledEvent("poke", nil)); err != nil {
		t.Fatal(err)
	}
	if state, _ := mgr.GetWorkflowExecutionState(runID); !state.NeedsWorkflowTask() {
		t.Fatal("signal without a scheduled WFT must leave the run needing a workflow task")
	}

	if _, _, err := survivor.Reconcile(context.Background(), overdueTestGrace, 0); err != nil {
		t.Fatal(err)
	}
	state, err := mgr.GetWorkflowExecutionState(runID)
	if err != nil {
		t.Fatal(err)
	}
	if state.OpenWorkflowTaskEventID == 0 {
		t.Fatal("periodic reconcile must schedule the missing workflow task")
	}
}
