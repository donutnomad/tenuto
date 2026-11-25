package engine

import (
	"context"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/program/programtest"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

// TestRecoverFailedAutoUpgradeAttemptRetriesLatest 固定重启后的重试语义：
// Run 带 UpgradeError、尾部 WFT Failed 且没有 open WFT 时，启动恢复入口仍会调度新 WFT；
// 新 WFT 重新选择 latest，而不是被上一次失败锁定或暂停。
func TestRecoverFailedAutoUpgradeAttemptRetriesLatest(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	server := assembleServer(mgr, newTestTaskQueue(), newTestTaskQueue())
	server.wftBackoffBase = time.Millisecond

	refA, err := server.Definitions().RegisterWorkflowInNamespace("default", &program.WorkflowConfig{Type: "recover-upgrade-flow", Name: "A", Program: programtest.Noop()})
	if err != nil {
		t.Fatal(err)
	}
	refB, err := server.Definitions().RegisterWorkflowInNamespace("default", &program.WorkflowConfig{Type: "recover-upgrade-flow", Name: "B", Program: programtest.Noop()})
	if err != nil {
		t.Fatal(err)
	}
	run := &execution.Run{
		ID: "recover-upgrade-run", Namespace: "default", WorkflowID: refA.WorkflowID, WorkflowType: refA.WorkflowID,
		WorkflowConfigVersion: refA.Version, OriginalWorkflowConfigVersion: refA.Version,
		Policy: execution.RunPolicyAutoUpgrade, Status: history.StatusRunning, CreatedAt: time.Now(),
		UpgradeError: "auto upgrade to " + refB.Version + " failed; retrying with backoff",
	}
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(run)) }); err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.Append(run.ID, run.ID, run.ID, history.NewWorkflowStartedEvent(run.Input, "", "", run.WorkflowID, "", 0)); err != nil {
		t.Fatal(err)
	}
	scheduledEventID, err := mgr.Append(run.ID, run.ID, run.ID, history.NewWorkflowTaskScheduledEvent(0))
	if err != nil {
		t.Fatal(err)
	}
	started := history.NewWorkflowTaskStartedEvent(scheduledEventID, "worker", "build", 0)
	started.WorkflowTaskStarted.ProgramVersion = refB.Version
	if _, err := mgr.Append(run.ID, run.ID, run.ID, started); err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.Append(run.ID, run.ID, run.ID, history.NewWorkflowTaskFailedEvent(history.WorkflowTaskFailedEvent{
		ScheduledEventID: scheduledEventID,
		Cause:            "non_deterministic_error",
		Reason:           "replay command mismatch",
	})); err != nil {
		t.Fatal(err)
	}

	// 启动恢复入口：模拟进程重启后 app 调用 RecoverRunWorkspaces。
	if err := server.RecoverRun(ctx, run.ID); err != nil {
		t.Fatal(err)
	}
	waitForEngineEventCount(t, mgr, run.ID, history.EventWorkflowTaskScheduled, 2, time.Second)

	recovered, err := mgr.Get(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if recovered.UpgradeError == "" {
		t.Fatal("recovery must keep the failure diagnostic until a successful workflow task")
	}
	selected, err := chooseProgramFor(server, recovered)
	if err != nil {
		t.Fatal(err)
	}
	if selected.Version != refB.Version {
		t.Fatalf("recovered retry selected %q, want latest %q", selected.Version, refB.Version)
	}
}
