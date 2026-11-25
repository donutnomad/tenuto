package engine

import (
	"context"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/sdk/activity"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

// Activity 契约在调度边界上的行为矩阵。
//
// 源码事实：调度时（server_definition.go resolveActivityCommand）确定队列，并确认声明的契约能被在线 Worker 满足；
// ActivityState 只保存队列与 Run 的目标 Worker，不保存 build。每次投递都只按队列路由，Worker 开始 attempt 时
// （RefuseIncompatibleActivityClaim）按领取者自己的上报再对照一次契约。

func registerChargeWorkerVersion(t testing.TB, server *Server, identity, buildID, input, output string) {
	t.Helper()
	if err := server.RegisterActivityWorkerVersion(context.Background(), protocol.ActivityWorkerVersion{
		Identity:      identity,
		BuildID:       buildID,
		TaskQueues:    []string{"payments"},
		ActivityTypes: []string{"payments.charge"},
		Contracts: []activity.Contract{{
			Type: "payments.charge", Input: input, Output: output,
		}},
	}); err != nil {
		t.Fatal(err)
	}
}

// chargeWorkflowDeclaring 声明指定输入输出指纹的本地 Activity 契约。
func chargeWorkflowDeclaring(id, input, output string) *program.WorkflowConfig {
	return contractWorkflowConfig(id, map[string]program.ActivityRef{
		"charge": {Type: "payments.charge", TaskQueue: "payments", Input: input, Output: output},
	})
}

func scheduleChargeCommand() []history.Command {
	return []history.Command{{
		StartToCloseTimeout: time.Minute,
		Type:                history.CmdScheduleActivity,
		ActivityID:          "charge",
		ActivityType:        "payments.charge",
		RetryPolicy: &protocol.ParsedRetryPolicy{
			MaxAttempts:        3,
			InitialInterval:    30 * time.Millisecond,
			BackoffCoefficient: 1,
		},
	}}
}

func waitForQueuedActivity(t *testing.T, queue *testTaskQueue, match func(protocol.Task) bool) protocol.Task {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for _, lease := range queue.snapshot() {
			if match(lease.Task) {
				return lease.Task
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("no matching activity task queued within 5s (queued %d)", len(queue.snapshot()))
	return protocol.Task{}
}

// Activity 已 scheduled、尚未 started 时部署契约 v2 worker：
// 已入队任务与失败后的 retry 都只带队列，不带 build；Run 不因在途的契约变化暂停。
func TestActivityContractScheduledBoundary(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	activityTasks := newTestTaskQueue()
	server := assembleServer(mgr, newTestTaskQueue(), activityTasks)
	defer server.Stop()

	registerChargeWorkerVersion(t, server, "pay-worker-v1", "pay@v1", "ChargeInput.v1", "Receipt.v1")
	runID, wftID := startContractRun(t, mgr, server, chargeWorkflowDeclaring("contract-scheduled-v1", "ChargeInput.v1", "Receipt.v1"))
	if err := server.HandleWorkflowTaskCompleted(server.WorkflowTaskContext(ctx, runID, wftID), runID, wftID, scheduleChargeCommand(), ""); err != nil {
		t.Fatal(err)
	}

	first := waitForQueuedActivity(t, activityTasks, func(task protocol.Task) bool { return task.Attempt == 1 })
	if first.TaskQueue != "payments" || first.TargetWorkerIdentity != "" {
		t.Fatalf("first delivery route = %q/%q, want the payments queue", first.TaskQueue, first.TargetWorkerIdentity)
	}

	// 部署 v2：已 scheduled 的任务不受影响。
	registerChargeWorkerVersion(t, server, "pay-worker-v2", "pay@v2", "ChargeInput.v2", "Receipt.v2")

	if _, err := server.HandleActivityStartedWithMetadata(first, 0, 0); err != nil {
		t.Fatal(err)
	}
	if err := server.HandleActivityFailed(ctx, runID, first.ScheduledEventID, first.Attempt, first.Stamp, protocol.Failure{Message: "temporary"}); err != nil {
		t.Fatal(err)
	}

	retry := waitForQueuedActivity(t, activityTasks, func(task protocol.Task) bool { return task.Attempt == 2 })
	if retry.TaskQueue != "payments" || retry.TargetWorkerIdentity != "" {
		t.Fatalf("retry route = %q/%q, want the payments queue", retry.TaskQueue, retry.TargetWorkerIdentity)
	}
	state, ok, err := server.GetActivityState(runID, retry.ScheduledEventID)
	if err != nil || !ok {
		t.Fatalf("activity state missing: ok=%v err=%v", ok, err)
	}
	if state.TaskQueue != "payments" {
		t.Fatalf("retry state queue = %q, want payments", state.TaskQueue)
	}
	run, err := mgr.Get(runID)
	if err != nil {
		t.Fatal(err)
	}
	if run.ConfigError != "" {
		t.Fatalf("in-flight contract change must not pause the run: %q", run.ConfigError)
	}
}

// Activity 已 started 时部署 v2 worker：v1 attempt 的完成照常入账。
func TestActivityContractStartedBoundary(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	activityTasks := newTestTaskQueue()
	workflowTasks := newTestTaskQueue()
	server := assembleServer(mgr, workflowTasks, activityTasks)
	defer server.Stop()

	registerChargeWorkerVersion(t, server, "pay-worker-v1", "pay@v1", "ChargeInput.v1", "Receipt.v1")
	runID, wftID := startContractRun(t, mgr, server, chargeWorkflowDeclaring("contract-started-v1", "ChargeInput.v1", "Receipt.v1"))
	if err := server.HandleWorkflowTaskCompleted(server.WorkflowTaskContext(ctx, runID, wftID), runID, wftID, scheduleChargeCommand(), ""); err != nil {
		t.Fatal(err)
	}
	task := waitForQueuedActivity(t, activityTasks, func(task protocol.Task) bool { return task.Attempt == 1 })
	if _, err := server.HandleActivityStartedWithMetadata(task, 0, 0); err != nil {
		t.Fatal(err)
	}

	registerChargeWorkerVersion(t, server, "pay-worker-v2", "pay@v2", "ChargeInput.v2", "Receipt.v2")

	if err := server.HandleActivityCompleted(ctx, task.RunID, task.ScheduledEventID, task.Attempt, task.Stamp, map[string]any{"receipt": "r-1"}); err != nil {
		t.Fatalf("completion from started v1 attempt must be accepted: %v", err)
	}
	state, ok, err := server.GetActivityState(runID, task.ScheduledEventID)
	if err != nil || !ok {
		t.Fatalf("activity state missing: ok=%v err=%v", ok, err)
	}
	if state.Status != execution.ActivityStatusEnums.Completed {
		t.Fatalf("activity status = %s, want completed", state.Status)
	}
	if got := len(workflowTasks.snapshot()); got != 1 {
		t.Fatalf("completion must schedule the next workflow task, got %d", got)
	}
}

// Activity 已 completed、随后部署 v2：结果复用；新 Run 声明 v2 契约，调度时由在线的 v2 满足。
func TestActivityContractCompletedBoundary(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	activityTasks := newTestTaskQueue()
	server := assembleServer(mgr, newTestTaskQueue(), activityTasks)
	defer server.Stop()

	registerChargeWorkerVersion(t, server, "pay-worker-v1", "pay@v1", "ChargeInput.v1", "Receipt.v1")
	runID, wftID := startContractRun(t, mgr, server, chargeWorkflowDeclaring("contract-completed-v1", "ChargeInput.v1", "Receipt.v1"))
	if err := server.HandleWorkflowTaskCompleted(server.WorkflowTaskContext(ctx, runID, wftID), runID, wftID, scheduleChargeCommand(), ""); err != nil {
		t.Fatal(err)
	}
	task := waitForQueuedActivity(t, activityTasks, func(task protocol.Task) bool { return task.Attempt == 1 })
	if _, err := server.HandleActivityStartedWithMetadata(task, 0, 0); err != nil {
		t.Fatal(err)
	}
	if err := server.HandleActivityCompleted(ctx, task.RunID, task.ScheduledEventID, task.Attempt, task.Stamp, map[string]any{"receipt": "r-1"}); err != nil {
		t.Fatal(err)
	}

	// 部署 v2 并把 Workflow 内容声明升级到 v2；新 Run 走 v2 路由。
	registerChargeWorkerVersion(t, server, "pay-worker-v2", "pay@v2", "ChargeInput.v2", "Receipt.v2")
	newRunID, newWFTID := startContractRun(t, mgr, server, chargeWorkflowDeclaring("contract-completed-v2", "ChargeInput.v2", "Receipt.v2"))
	if err := server.HandleWorkflowTaskCompleted(server.WorkflowTaskContext(ctx, newRunID, newWFTID), newRunID, newWFTID, scheduleChargeCommand(), ""); err != nil {
		t.Fatal(err)
	}
	next := waitForQueuedActivity(t, activityTasks, func(candidate protocol.Task) bool {
		return candidate.RunID == newRunID && candidate.Attempt == 1
	})
	if next.TaskQueue != "payments" {
		t.Fatalf("new run routed to queue %q, want payments", next.TaskQueue)
	}
}

// 对照拒绝时 Run 记录 config_error、不派发任务；修复上报后同一条 WFT 重试即可恢复。
func TestActivityContractMismatchPausesRun(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	activityTasks := newTestTaskQueue()
	server := assembleServer(mgr, newTestTaskQueue(), activityTasks)
	defer server.Stop()

	registerChargeWorkerVersion(t, server, "pay-worker-v1", "pay@v1", "ChargeInput.v1", "Receipt.v1")
	runID, wftID := startContractRun(t, mgr, server, chargeWorkflowDeclaring("contract-paused", "ChargeInput.v2", "Receipt.v2"))
	if err := server.HandleWorkflowTaskCompleted(server.WorkflowTaskContext(ctx, runID, wftID), runID, wftID, scheduleChargeCommand(), ""); err == nil {
		t.Fatal("expected contract mismatch error")
	}
	run, err := mgr.Get(runID)
	if err != nil {
		t.Fatal(err)
	}
	if run.ConfigError == "" {
		t.Fatal("contract mismatch must record config_error")
	}
	if got := len(activityTasks.snapshot()); got != 0 {
		t.Fatalf("mismatched activity must not be dispatched, got %d tasks", got)
	}

	registerChargeWorkerVersion(t, server, "pay-worker-v2", "pay@v2", "ChargeInput.v2", "Receipt.v2")
	if err := server.HandleWorkflowTaskCompleted(server.WorkflowTaskContext(ctx, runID, wftID), runID, wftID, scheduleChargeCommand(), ""); err != nil {
		t.Fatalf("recovered contract must schedule: %v", err)
	}
	if got := len(activityTasks.snapshot()); got != 1 {
		t.Fatalf("expected one activity task after recovery, got %d", got)
	}
}
