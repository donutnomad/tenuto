package engine

import (
	"context"
	"testing"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/program/programtest"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
)

// 这些用例模拟另一个副本：在本副本按事务外读到的 Run 准备好决定之后、提交决定的事务之前，另一个事务直接经
// ExecutionTx 改了同一个 Run（不经本进程的 keyed lock）。提交的事务按锁下读到的 Run 判定，另一个事务的改动生效。

// commitOnRun 以另一个副本的身份在一个事务里锁下读取 Run 并改它。
func commitOnRun(t *testing.T, server *Server, runID string, change func(tx execution.ExecutionTx, run *execution.Run) error) {
	t.Helper()
	if err := server.mgr.RunInTx(func(tx execution.ExecutionTx) error {
		run, err := tx.GetRunForUpdate(runID)
		if err != nil {
			return err
		}
		return change(tx, run)
	}); err != nil {
		t.Fatal(err)
	}
}

func workerContext(identity string) context.Context {
	return protocol.WithWorkerInfo(context.Background(), protocol.WorkerInfo{Identity: identity, BuildID: "build-1", WorkflowCapabilities: program.CurrentWLangCapabilities()})
}

// startedProgramVersions 返回 Run 的 History 里每个 WorkflowTaskStarted 记录的 Program 版本。
func startedProgramVersions(t *testing.T, server *Server, runID string) []string {
	t.Helper()
	var versions []string
	for _, event := range engineMgrEvents(t, server, runID) {
		if event.WorkflowTaskStarted != nil {
			versions = append(versions, event.WorkflowTaskStarted.ProgramVersion)
		}
	}
	return versions
}

// Integration：poll 按 auto_upgrade 的 Run 为 Workflow Task 准备了 latest；领取的事务提交之前，另一个副本把策略改成
// pinned。领取的事务按锁下读到的 pinned 策略选版，不在 latest 上开始，这份投递放回队列；重新领取时在当前版本上开始。
func TestWorkflowTaskStartHonoursPolicySetByAnotherTransaction(t *testing.T) {
	server := newTestServer()
	refA, err := server.Definitions().RegisterWorkflowInNamespace("default", &program.WorkflowConfig{ID: "policy-race", Name: "A", Program: programtest.Noop()})
	if err != nil {
		t.Fatal(err)
	}
	runID, err := startedRunID(server.StartWorkflow(context.Background(), "", "policy-race", "policy-race", "", nil))
	if err != nil {
		t.Fatal(err)
	}
	refB, err := server.Definitions().RegisterWorkflowInNamespace("default", &program.WorkflowConfig{ID: "policy-race", Name: "B", Program: programtest.Noop()})
	if err != nil {
		t.Fatal(err)
	}
	queued := server.wfQueue.(*testTaskQueue).snapshot()
	if len(queued) != 1 {
		t.Fatalf("queued workflow tasks = %d, want 1", len(queued))
	}

	ctx := workerContext("worker-a")
	polled, matched, err := server.pollWorkflowTaskOnce(ctx)
	if err != nil || !matched {
		t.Fatalf("poll: matched=%v err=%v", matched, err)
	}
	if polled.lease.Task.ProgramVersion != refB.Version {
		t.Fatalf("prepared program %q, want latest %q", polled.lease.Task.ProgramVersion, refB.Version)
	}
	commitOnRun(t, server, runID, func(tx execution.ExecutionTx, run *execution.Run) error {
		treeID, branchID, err := runBranchInfoFromRun(run)
		if err != nil {
			return err
		}
		return execution.OverrideRunPolicy(tx, run, execution.RunPolicyPinned, treeID, branchID, nil)
	})

	if _, started, err := server.startWorkflowTask(ctx, polled); err != nil || started {
		t.Fatalf("start after the policy change: started=%v err=%v, want the delivery returned to the queue", started, err)
	}
	if got := startedProgramVersions(t, server, runID); len(got) != 0 {
		t.Fatalf("workflow task started on %v after the run was pinned", got)
	}

	// 队列重新投递同一个任务。
	if err := server.wfQueue.Enqueue(context.Background(), queued[0].Task); err != nil {
		t.Fatal(err)
	}
	lease, err := server.PollWorkflowTask(capableWorkerContext(ctx))
	if err != nil {
		t.Fatal(err)
	}
	if lease.Task.ProgramVersion != refA.Version {
		t.Fatalf("redelivered task prepared %q, want the pinned version %q", lease.Task.ProgramVersion, refA.Version)
	}
	if got := startedProgramVersions(t, server, runID); len(got) != 1 || got[0] != refA.Version {
		t.Fatalf("started program versions = %v, want [%s]", got, refA.Version)
	}
}

// Integration：poll 按未暂停的 Run 准备好 Workflow Task；领取的事务提交之前，另一个副本以 ConfigError 暂停了 Run。
// 领取的事务读到暂停，不开始这个 WFT，投递被确认丢弃。
func TestWorkflowTaskStartHonoursPauseByAnotherTransaction(t *testing.T) {
	server := newTestServer()
	if _, err := server.Definitions().RegisterWorkflowInNamespace("default", &program.WorkflowConfig{ID: "pause-race", Name: "A", Program: programtest.Noop()}); err != nil {
		t.Fatal(err)
	}
	runID, err := startedRunID(server.StartWorkflow(context.Background(), "", "pause-race", "pause-race", "", nil))
	if err != nil {
		t.Fatal(err)
	}
	ctx := workerContext("worker-a")
	polled, matched, err := server.pollWorkflowTaskOnce(ctx)
	if err != nil || !matched {
		t.Fatalf("poll: matched=%v err=%v", matched, err)
	}
	commitOnRun(t, server, runID, func(tx execution.ExecutionTx, run *execution.Run) error {
		return execution.PauseRun(tx, run, "activity contract mismatch")
	})

	if _, started, err := server.startWorkflowTask(ctx, polled); err != nil || started {
		t.Fatalf("start after the pause: started=%v err=%v, want the delivery dropped", started, err)
	}
	if got := startedProgramVersions(t, server, runID); len(got) != 0 {
		t.Fatalf("workflow task started on %v after the run was paused", got)
	}
	run, err := server.mgr.Get(runID)
	if err != nil {
		t.Fatal(err)
	}
	if run.ConfigError != "activity contract mismatch" {
		t.Fatalf("config error = %q, want the pause from the other transaction", run.ConfigError)
	}
}

// Integration：Worker 完成 Workflow Task 之前的预检看到 Run 仍在运行；提交完成的事务之前，另一个副本终止了 Run。
// 提交的事务在锁下读到终态，不写入完成，Run 保持 Terminated。
func TestWorkflowTaskCompletionHonoursTerminationByAnotherTransaction(t *testing.T) {
	server := newTestServer()
	if _, err := server.Definitions().RegisterWorkflowInNamespace("default", &program.WorkflowConfig{ID: "terminate-race", Name: "A", Program: programtest.Noop()}); err != nil {
		t.Fatal(err)
	}
	runID, err := startedRunID(server.StartWorkflow(context.Background(), "", "terminate-race", "terminate-race", "", nil))
	if err != nil {
		t.Fatal(err)
	}
	lease, err := server.PollWorkflowTask(workerContext("worker-a"))
	if err != nil {
		t.Fatal(err)
	}
	commitOnRun(t, server, runID, func(tx execution.ExecutionTx, run *execution.Run) error {
		treeID, branchID, err := runBranchInfoFromRun(run)
		if err != nil {
			return err
		}
		_, err = execution.Terminate(tx, run, treeID, branchID, "operator")
		return err
	})

	// 预检之后的部分：本副本已持有 keyed lock、确认 WFT open，提交按锁下读到的 Run 判定。
	complete := []history.Command{{Type: history.CmdCompleteWorkflow, Output: map[string]any{"ok": true}}}
	preRun, err := server.mgr.Get(runID)
	if err != nil {
		t.Fatal(err)
	}
	preState, err := server.mgr.GetWorkflowExecutionState(runID)
	if err != nil {
		t.Fatal(err)
	}
	treeID, branchID, err := runBranchInfoFromRun(preRun)
	if err != nil {
		t.Fatal(err)
	}
	open := openWorkflowTask{ref: refOf(preRun), branch: runBranch{treeID: treeID, branchID: branchID}, state: preState, scheduledEventID: lease.Task.ScheduledEventID}
	if err := server.completeWorkflowTaskWithStore(context.Background(), open, complete, "worker-a"); err != nil {
		t.Fatal(err)
	}
	run, err := server.mgr.Get(runID)
	if err != nil {
		t.Fatal(err)
	}
	if run.Status != history.StatusTerminated || run.Output != nil {
		t.Fatalf("run = %s output=%v, want the termination to stand", run.Status, run.Output)
	}
	events := engineMgrEvents(t, server, runID)
	if got := countEngineEvents(events, history.EventWorkflowCompleted); got != 0 {
		t.Fatalf("WorkflowCompleted events = %d after termination, want 0", got)
	}
	if got := countEngineEvents(events, history.EventWorkflowTaskCompleted); got != 0 {
		t.Fatalf("WorkflowTaskCompleted events = %d after termination, want 0", got)
	}
}
