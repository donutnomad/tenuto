package engine

import (
	"context"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/program/programtest"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

// 启动恢复只挂 lookahead 内到期的定时器：空闲 Running Run 的执行截止时间晚于 lookahead 时，重启的副本启动时不挂它，
// 截止时间进入 lookahead 后由周期补偿挂上，并按原截止时间触发。
func TestPeriodicReconcileArmsRunDeadlineEnteringLookahead(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	srv, clk := newFakeClockServer(mgr, newTestTaskQueue(), newTestTaskQueue())
	config := &program.WorkflowConfig{Program: programtest.Noop(), Type: "wf-idle-deadline", ExecutionTimeout: "10m"}
	if _, err := srv.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, config); err != nil {
		t.Fatal(err)
	}
	runID, err := startedRunID(srv.StartWorkflow(ctx, "", config.Type, config.Type, "", nil))
	if err != nil {
		t.Fatal(err)
	}
	// 首个 WFT 不发命令：Run 保持运行，没有任何待处理事项。
	wftID := currentWFTScheduledID(t, srv, runID)
	if err := srv.HandleWorkflowTaskCompleted(srv.WorkflowTaskContext(ctx, runID, wftID), runID, wftID, nil, ""); err != nil {
		t.Fatal(err)
	}
	run, err := mgr.Get(runID)
	if err != nil || run.DeadlineAt == nil {
		t.Fatalf("run deadline = %v, err = %v", run, err)
	}
	deadline := *run.DeadlineAt
	srv.Stop()

	restarted := assembleServerWithClock(mgr, newTestTaskQueue(), newTestTaskQueue(), clk)
	t.Cleanup(restarted.Stop)
	if _, err := restarted.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, config); err != nil {
		t.Fatal(err)
	}
	ref := timerRef{Kind: timerWorkflowExecution, RunID: runID}
	if err := restarted.RecoverRunningWorkflows(ctx, DefaultReconcileLookahead); err != nil {
		t.Fatal(err)
	}
	if timerArmed(restarted, ref) {
		t.Fatal("startup recovery armed a run deadline beyond the lookahead")
	}

	advanceTo(clk, deadline.Add(-DefaultReconcileLookahead/2))
	if _, _, err := restarted.Reconcile(ctx, DefaultOverdueTimerGrace, DefaultReconcileLookahead); err != nil {
		t.Fatal(err)
	}
	if armed, ok := restarted.timers.Deadline(ref); !ok || !armed.Equal(deadline) {
		t.Fatalf("run deadline timer = %v (armed %v), want %v", armed, ok, deadline)
	}
	advanceTo(clk, deadline.Add(-time.Millisecond))
	if status, _ := restarted.GetRunStatus(runID); status != history.StatusRunning {
		t.Fatalf("status before the deadline = %s, want running", status)
	}
	clk.Advance(time.Millisecond)
	waitForEngineEventCount(t, mgr, runID, history.EventWorkflowFailed, 1, 3*time.Second)
	if got := failedTimeoutType(t, mgr, runID); got != TimeoutTypeExecution {
		t.Fatalf("TimeoutType = %q, want %q", got, TimeoutTypeExecution)
	}
}
