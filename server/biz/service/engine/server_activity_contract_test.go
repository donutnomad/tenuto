package engine

import (
	"context"
	"strings"
	"testing"
	"time"

	executor2 "github.com/donutnomad/tenuto/internal/executor"
	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/program/programtest"
	"github.com/donutnomad/tenuto/protocol"
	historypkg "github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/sdk/activity"
	"github.com/donutnomad/tenuto/server/biz/domain/dispatch"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

func wlangProgramForTest() program.WLangProgram {
	return program.WLangProgram{
		Schema:           program.WLangProgramSchemaV2,
		Lang:             program.WLangLanguageV1,
		Program:          []any{map[string]any{"return": nil}},
		HandlerABISchema: program.WLangHandlerABISchemaV1,
	}
}

// contractWorkflowConfig 用 WLang Activities 映射声明 Activity 契约。
func contractWorkflowConfig(id string, refs map[string]program.ActivityRef) *program.WorkflowConfig {
	return &program.WorkflowConfig{
		Type:       id,
		Program:    wlangProgramForTest(),
		Activities: refs,
	}
}

func startContractRun(t *testing.T, mgr execution.ExecutionManager, server *Server, wf *program.WorkflowConfig) (string, int64) {
	t.Helper()
	return startContractRunWithTarget(t, mgr, server, wf, "")
}

// startContractRunWithTarget 与 startContractRun 相同，Run 固定路由到 target Worker。
func startContractRunWithTarget(t testing.TB, mgr execution.ExecutionManager, server *Server, wf *program.WorkflowConfig, target string) (string, int64) {
	t.Helper()
	ref, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, wf)
	if err != nil {
		t.Fatal(err)
	}
	runID := "run-" + wf.Type
	run := &execution.Run{
		ID:                    runID,
		Namespace:             protocol.DefaultNamespace,
		WorkflowID:            ref.WorkflowID,
		WorkflowType:          ref.WorkflowID,
		WorkflowConfigVersion: ref.Version,
		TargetWorkerIdentity:  target,
		Status:                historypkg.StatusRunning,
		CreatedAt:             time.Now(),
	}
	createStartedRunForTest(t, mgr, run)
	return runID, appendOpenWorkflowTaskForTest(t, mgr, runID)
}

func scheduleChargeTask() []historypkg.Command {
	return []historypkg.Command{{
		StartToCloseTimeout: time.Minute,
		Type:                historypkg.CmdScheduleActivity,
		ActivityID:          "charge",
		ActivityType:        "payments.charge",
	}}
}

func chargeContractWorkflow(id string) *program.WorkflowConfig {
	return contractWorkflowConfig(id, map[string]program.ActivityRef{
		"charge": {Type: "payments.charge", TaskQueue: "payments", Version: "v2", Input: "ChargeInput.v2", Output: "Receipt.v2"},
	})
}

// 升级边界：auto_upgrade 的 Run 在选中 B 的 WFT 里，命令按 B 的声明选路。
func TestUpgradeBoundaryRoutesByWorkflowTaskVersion(t *testing.T) {
	const workflowID = "contract-upgrade-boundary"
	register := func(t *testing.T, server *Server, build, version string) {
		t.Helper()
		if err := server.RegisterActivityWorkerVersion(context.Background(), protocol.ActivityWorkerVersion{
			Identity: "pay-" + build, BuildID: build,
			TaskQueues:    []string{"payments"},
			ActivityTypes: []string{"payments.charge"},
			Contracts: []activity.Contract{{
				Type: "payments.charge", Version: version, Input: "ChargeInput." + version, Output: "Receipt." + version,
			}},
		}); err != nil {
			t.Fatal(err)
		}
	}
	workflowFor := func(version string) *program.WorkflowConfig {
		return contractWorkflowConfig(workflowID, map[string]program.ActivityRef{
			"charge": {Type: "payments.charge", TaskQueue: "payments", Version: version, Input: "ChargeInput." + version, Output: "Receipt." + version},
		})
	}
	for _, tc := range []struct {
		name string
		old  bool
	}{{name: "only new worker"}, {name: "old and new workers", old: true}} {
		t.Run(tc.name, func(t *testing.T) {
			mgr := memory.NewExecutionManager()
			activityTasks := newTestTaskQueue()
			server := assembleServer(mgr, newTestTaskQueue(), activityTasks)
			defer server.Stop()
			register(t, server, "pay-new", "v2")
			if tc.old {
				register(t, server, "pay-old", "v1")
			}
			runID, wftID := startContractRun(t, mgr, server, workflowFor("v1"))
			refB, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, workflowFor("v2"))
			if err != nil {
				t.Fatal(err)
			}
			started := historypkg.NewWorkflowTaskStartedEvent(wftID, "wf-worker", "wf-build", 1)
			started.WorkflowTaskStarted.ProgramVersion = refB.Version
			if _, err := mgr.Append(runID, runID, runID, started); err != nil {
				t.Fatal(err)
			}

			if err := server.HandleWorkflowTaskCompleted(server.WorkflowTaskContext(context.Background(), runID, wftID), runID, wftID, scheduleChargeTask(), ""); err != nil {
				t.Fatalf("command from selected version must schedule: %v", err)
			}
			if len(activityTasks.tasks) != 1 || activityTasks.tasks[0].Task.TaskQueue != "payments" {
				t.Fatalf("activity tasks = %+v, want one task on the payments queue", activityTasks.tasks)
			}
		})
	}
}

// Worker 上报的契约与 Workflow 声明一致 → 正常调度。
func TestScheduleActivityContractMatchDispatches(t *testing.T) {
	mgr := memory.NewExecutionManager()
	activityTasks := newTestTaskQueue()
	server := assembleServer(mgr, newTestTaskQueue(), activityTasks)
	defer server.Stop()
	if err := server.RegisterActivityWorkerVersion(context.Background(), protocol.ActivityWorkerVersion{
		Identity: "pay-worker-1", BuildID: "pay@v1",
		TaskQueues:    []string{"payments"},
		ActivityTypes: []string{"payments.charge"},
		Contracts: []activity.Contract{{
			Type: "payments.charge", Version: "v2", Input: "ChargeInput.v2", Output: "Receipt.v2",
		}},
	}); err != nil {
		t.Fatal(err)
	}
	runID, wftID := startContractRun(t, mgr, server, chargeContractWorkflow("contract-match"))

	if err := server.HandleWorkflowTaskCompleted(server.WorkflowTaskContext(context.Background(), runID, wftID), runID, wftID, scheduleChargeTask(), ""); err != nil {
		t.Fatalf("matching contract must schedule: %v", err)
	}
	if len(activityTasks.tasks) != 1 {
		t.Fatalf("expected one activity task, got %d", len(activityTasks.tasks))
	}
	task := activityTasks.tasks[0].Task
	if task.TaskQueue != "payments" {
		t.Fatalf("unexpected route: %+v", task)
	}
}

// Worker 登记时没有带契约 → 声明的每个维度都不匹配，任务不派发。
func TestScheduleActivityRejectsWorkerWithoutContract(t *testing.T) {
	mgr := memory.NewExecutionManager()
	activityTasks := newTestTaskQueue()
	server := assembleServer(mgr, newTestTaskQueue(), activityTasks)
	defer server.Stop()
	if err := server.RegisterActivityWorkerVersion(context.Background(), protocol.ActivityWorkerVersion{
		Identity: "bare-worker", BuildID: "pay@v1",
		TaskQueues:    []string{"payments"},
		ActivityTypes: []string{"payments.charge"},
	}); err != nil {
		t.Fatal(err)
	}
	runID, wftID := startContractRun(t, mgr, server, chargeContractWorkflow("contract-unreported"))

	err := server.HandleWorkflowTaskCompleted(server.WorkflowTaskContext(context.Background(), runID, wftID), runID, wftID, scheduleChargeTask(), "")
	if err == nil {
		t.Fatal("a worker without a contract must not receive a contract-declared activity")
	}
	for _, want := range []string{`"payments.charge"`, `"bare-worker"`, `version expected "v2" actual ""`} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error %q does not contain %q", err.Error(), want)
		}
	}
	if len(activityTasks.tasks) != 0 {
		t.Fatalf("activity must not be dispatched, got %d tasks", len(activityTasks.tasks))
	}
}

// 上报契约与声明不兼容 → 明确错误、任务不派发、现场保留；修复上报后可恢复调度。
func TestScheduleActivityContractMismatchBlocksDispatch(t *testing.T) {
	mgr := memory.NewExecutionManager()
	activityTasks := newTestTaskQueue()
	server := assembleServer(mgr, newTestTaskQueue(), activityTasks)
	defer server.Stop()
	registerWorker := func(version, input, output string) {
		t.Helper()
		if err := server.RegisterActivityWorkerVersion(context.Background(), protocol.ActivityWorkerVersion{
			Identity: "pay-worker-1", BuildID: "pay@v1",
			TaskQueues:    []string{"payments"},
			ActivityTypes: []string{"payments.charge"},
			Contracts: []activity.Contract{{
				Type: "payments.charge", Version: version, Input: input, Output: output,
			}},
		}); err != nil {
			t.Fatal(err)
		}
	}
	registerWorker("v1", "ChargeInput.v1", "Receipt.v1")
	runID, wftID := startContractRun(t, mgr, server, chargeContractWorkflow("contract-mismatch"))

	err := server.HandleWorkflowTaskCompleted(server.WorkflowTaskContext(context.Background(), runID, wftID), runID, wftID, scheduleChargeTask(), "")
	if err == nil {
		t.Fatal("expected contract mismatch error")
	}
	for _, want := range []string{
		`"payments.charge"`,
		`"pay-worker-1"`,
		`version expected "v2" actual "v1"`,
		`input expected "ChargeInput.v2" actual "ChargeInput.v1"`,
		`output expected "Receipt.v2" actual "Receipt.v1"`,
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error %q does not contain %q", err.Error(), want)
		}
	}
	history, histErr := mgr.GetEvents(runID)
	if histErr != nil {
		t.Fatal(histErr)
	}
	if countEngineEvents(history, historypkg.EventActivityScheduled) != 0 {
		t.Fatal("mismatched activity must not be scheduled into history")
	}
	if countEngineEvents(history, historypkg.EventActivityFailed) != 0 {
		t.Fatal("mismatched activity must not produce a failure event")
	}
	if len(activityTasks.tasks) != 0 {
		t.Fatalf("mismatched activity must not be dispatched, got %d tasks", len(activityTasks.tasks))
	}
	run, runErr := mgr.Get(runID)
	if runErr != nil {
		t.Fatal(runErr)
	}
	if run.Status != historypkg.StatusRunning {
		t.Fatalf("run must stay running for recovery, got %s", run.Status)
	}

	// 修复上报后用同一条命令重试：WFT 尚未提交，重新执行即可恢复。
	registerWorker("v2", "ChargeInput.v2", "Receipt.v2")
	if err := server.HandleWorkflowTaskCompleted(server.WorkflowTaskContext(context.Background(), runID, wftID), runID, wftID, scheduleChargeTask(), ""); err != nil {
		t.Fatalf("recovered contract must schedule: %v", err)
	}
	if len(activityTasks.tasks) != 1 {
		t.Fatalf("expected one activity task after recovery, got %d", len(activityTasks.tasks))
	}
}

// 设置 target 必须覆盖全部静态引用，包括未选中的 GetVersion 分支。
func TestSetWorkflowTargetRequiresAllStaticActivities(t *testing.T) {
	server := newTestServer()
	defer server.Stop()
	if err := server.RegisterActivityWorkerVersion(context.Background(), protocol.ActivityWorkerVersion{
		Identity: "pay-worker", BuildID: "pay@v1",
		TaskQueues:    []string{"payments"},
		ActivityTypes: []string{"payments.charge", "payments.refund", "payments.settle"},
	}); err != nil {
		t.Fatal(err)
	}
	wf := contractWorkflowConfig("target-coverage", map[string]program.ActivityRef{
		// 两支都会写进 Program.Activities；未选中的分支同样要求 target 具备能力。
		"charge.v1": {Type: "payments.charge", TaskQueue: "payments", Version: "v1"},
		"charge.v2": {Type: "payments.charge", TaskQueue: "payments", Version: "v2"},
		"reserve":   {Type: "inventory.reserve", TaskQueue: "inventory"},
		"refund":    {Type: "payments.refund", TaskQueue: "payments"},
	})
	if _, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, wf); err != nil {
		t.Fatal(err)
	}
	// pay-worker 覆盖支付类 Activity，但缺库存队列能力 → 必须拒绝。
	_, err := server.Definitions().SetWorkflowWorkerTarget(protocol.DefaultNamespace, wf.Type, "pay-worker")
	if err == nil {
		t.Fatal("expected capability error for uncovered inventory activity")
	}
	if !strings.Contains(err.Error(), `"inventory.reserve"`) {
		t.Fatalf("error must name the uncovered activity: %v", err)
	}
	if !strings.Contains(err.Error(), "cannot handle activity") {
		t.Fatalf("unexpected error: %v", err)
	}
	// 请求被拒绝后原配置保持无 target。
	ref, err := server.activeWorkflowRefInNamespace(protocol.DefaultNamespace, wf.Type)
	if err != nil {
		t.Fatal(err)
	}
	active, err := server.workflowConfigs.GetWorkflowConfig(ref)
	if err != nil {
		t.Fatal(err)
	}
	if active.TargetWorkerIdentity != "" {
		t.Fatalf("rejected target must not change active config: %q", active.TargetWorkerIdentity)
	}

	// 覆盖全部引用的 worker 可以让设置通过。
	if err := server.RegisterActivityWorkerVersion(context.Background(), protocol.ActivityWorkerVersion{
		Identity: "all-worker", BuildID: "all@v1",
		TaskQueues:    []string{"payments", "inventory"},
		ActivityTypes: []string{"payments.charge", "inventory.reserve", "payments.refund"},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := server.Definitions().SetWorkflowWorkerTarget(protocol.DefaultNamespace, wf.Type, "all-worker"); err != nil {
		t.Fatalf("all-capable target must be accepted: %v", err)
	}
}

// 离线 identity 不能设置 target，且原 active 配置与已有 Run 保持。
func TestSetWorkflowTargetRejectsOfflineIdentity(t *testing.T) {
	server := newTestServer()
	defer server.Stop()
	if err := server.RegisterActivityWorkerVersion(context.Background(), protocol.ActivityWorkerVersion{
		Identity: "pay-worker", BuildID: "pay@v1",
		TaskQueues:    []string{"payments"},
		ActivityTypes: []string{"payments.charge"},
	}); err != nil {
		t.Fatal(err)
	}
	wf := chargeContractWorkflow("target-offline")
	if _, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, wf); err != nil {
		t.Fatal(err)
	}
	_, err := server.Definitions().SetWorkflowWorkerTarget(protocol.DefaultNamespace, wf.Type, "ghost-worker")
	if err == nil || !strings.Contains(err.Error(), "offline or unknown") {
		t.Fatalf("unexpected error: %v", err)
	}
	ref, err := server.activeWorkflowRefInNamespace(protocol.DefaultNamespace, wf.Type)
	if err != nil {
		t.Fatal(err)
	}
	active, err := server.workflowConfigs.GetWorkflowConfig(ref)
	if err != nil {
		t.Fatal(err)
	}
	if active.TargetWorkerIdentity != "" {
		t.Fatalf("offline target must not be published: %q", active.TargetWorkerIdentity)
	}
}

// retry 沿用 ActivityState 保存的队列与 Run 的目标 Worker，不重新解析队列；任务不带 build。
func TestActivityRetryKeepsRoutingSnapshot(t *testing.T) {
	mgr := memory.NewExecutionManager()
	server := assembleServer(mgr, newTestTaskQueue(), newTestTaskQueue())
	defer server.Stop()
	run := &execution.Run{ID: "run-retry-snapshot", Namespace: protocol.DefaultNamespace, WorkflowID: "wf", WorkflowType: "wf", Status: historypkg.StatusRunning, CreatedAt: time.Now()}
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(run)) }); err != nil {
		t.Fatal(err)
	}
	state := &execution.ActivityState{
		RunID: run.ID, ScheduledEventID: 7, ActivityID: "charge", ActivityType: "payments.charge",
		TaskQueue: "payments", TargetWorkerIdentity: "pay-worker-1",
		Attempt: 2, Stamp: 2, Status: execution.ActivityStatusEnums.Scheduled, ScheduledTime: time.Now(),
	}
	if err := mgr.UpsertActivityState(state); err != nil {
		t.Fatal(err)
	}
	// 路由目录随后变化（新 build、新队列），retry 仍然用快照。
	if err := server.RegisterActivityWorkerVersion(context.Background(), protocol.ActivityWorkerVersion{
		Identity: "pay-worker-1", BuildID: "pay@v9",
		TaskQueues:    []string{"payments-v9"},
		ActivityTypes: []string{"payments.charge"},
	}); err != nil {
		t.Fatal(err)
	}
	task := server.activityTaskFromState(state)
	if task.TaskQueue != "payments" || task.TargetWorkerIdentity != "pay-worker-1" {
		t.Fatalf("retry lost routing snapshot: %+v", task)
	}
	if task.Attempt != 2 || task.Stamp != 2 {
		t.Fatalf("retry lost attempt/stamp: %+v", task)
	}
}

// 两阶段故障：队列无消费者（Scheduled 等待）与领取后缺 ActivityType 分开处理。
func TestActivityQueueWithoutConsumerVersusMissingActivityType(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	router := memory.NewActivityQueueRouter(8)
	server := assembleServerWithActivityRouter(mgr, newTestTaskQueue(), router)
	defer server.Stop()

	ref, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{
		Type:    "inventory-two-stage",
		Program: programtest.Noop(),
		Activities: map[string]program.ActivityRef{
			"reserve": {Symbol: "reserve", Type: "inventory.reserve.v2", TaskQueue: "inventory"},
		},
	})

	if err != nil {
		t.Fatal(err)
	}
	runID := "run-inventory-two-stage"
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error {
		return tx.CreateRun(WithRootBranch(&execution.Run{
			ID: runID, Namespace: protocol.DefaultNamespace, WorkflowID: ref.WorkflowID, WorkflowType: ref.WorkflowID,
			WorkflowConfigVersion: ref.Version, Status: historypkg.StatusRunning, CreatedAt: time.Now(),
		}))
	}); err != nil {
		t.Fatal(err)
	}
	wftID := appendOpenWorkflowTaskForTest(t, mgr, runID)
	if err := server.HandleWorkflowTaskCompleted(server.WorkflowTaskContext(ctx, runID, wftID), runID, wftID, []historypkg.Command{{
		StartToCloseTimeout: time.Minute,
		Type:                historypkg.CmdScheduleActivity, ActivityID: "reserve", ActivityType: "inventory.reserve.v2", TaskQueue: "inventory",
	}}, ""); err != nil {
		t.Fatalf("explicit queue scheduling must succeed without a consumer: %v", err)
	}

	// 阶段一：队列有任务但没有消费者 → 保持 Scheduled 等待，不产生失败事件。
	states, err := mgr.ListActivityStates(runID)
	if err != nil {
		t.Fatal(err)
	}
	if len(states) != 1 {
		t.Fatalf("expected one activity state, got %d", len(states))
	}
	state := states[0]
	if state.Status != execution.ActivityStatusEnums.Scheduled {
		t.Fatalf("activity must stay scheduled while no consumer exists, got %s", state.Status)
	}
	// 目录里没有任何活跃 worker 承接 (inventory, inventory.reserve.v2)：任务留在队列等待，
	// 其他队列的消费者不会收到它。
	otherWorker := protocol.WithWorkerInfo(protocol.WithNamespace(ctx, protocol.DefaultNamespace), protocol.WorkerInfo{Namespace: protocol.DefaultNamespace, Identity: "payments-worker", BuildID: "pay@v1"})
	pollCtx, cancel := context.WithTimeout(otherWorker, 20*time.Millisecond)
	defer cancel()
	if _, err := server.PollActivityTask(pollCtx, "payments"); err != context.DeadlineExceeded {
		t.Fatalf("expected poll timeout without consumer, got %v", err)
	}
	history, err := mgr.GetEvents(runID)
	if err != nil {
		t.Fatal(err)
	}
	if countEngineEvents(history, historypkg.EventActivityFailed) != 0 {
		t.Fatal("waiting for a consumer must not fail the activity")
	}

	// 阶段二：消费者上线，但本地没有注册该 ActivityType → 领取后报 not found。
	if err := server.RegisterActivityWorkerVersion(context.Background(), protocol.ActivityWorkerVersion{
		Identity: "inventory-worker", BuildID: "inv@v1",
		TaskQueues:    []string{"inventory"},
		ActivityTypes: []string{"inventory.reserve"}, // 只注册 v1，没有 v2
	}); err != nil {
		t.Fatal(err)
	}
	workerCtx := protocol.WithWorkerInfo(protocol.WithNamespace(ctx, protocol.DefaultNamespace), protocol.WorkerInfo{Namespace: protocol.DefaultNamespace, Identity: "inventory-worker", BuildID: "inv@v1"})
	leaseCtx, leaseCancel := context.WithTimeout(workerCtx, time.Second)
	defer leaseCancel()
	lease, err := server.PollActivityTask(leaseCtx, "inventory")
	if err != nil {
		t.Fatalf("consumer must receive the queued task: %v", err)
	}
	lease.Task.WorkerIdentity = "inventory-worker"
	lease.Task.WorkerBuildID = "inv@v1"
	outcome, execErr, reportErr := executor2.RunActivityTask(leaseCtx, lease.Task, NewActivityRuntimeService(server), activity.NewRegistry(), nil)
	if outcome != "failed" {
		t.Fatalf("outcome=%q want failed", outcome)
	}
	if execErr == nil || !strings.Contains(execErr.Error(), "activity inventory.reserve.v2 not found") {
		t.Fatalf("unexpected execution error: %v", execErr)
	}
	if reportErr != nil {
		t.Fatalf("fail report: %v", reportErr)
	}
	history, err = mgr.GetEvents(runID)
	if err != nil {
		t.Fatal(err)
	}
	if countEngineEvents(history, historypkg.EventActivityFailed) == 0 && countEngineEvents(history, historypkg.EventActivityAttemptFailed) == 0 {
		t.Fatal("missing activity type must be recorded in history")
	}
}

// TestActivityContractRoutingPrefersCompatibleWorker 固定滚动发布场景的选路语义：
// 同一 (namespace, type, queue) 下同时有 v1/v2 两个活跃 build 时，Workflow 声明 v2
// 必须选中上报 v2 契约的 worker，而不是按 build 排序取第一个，然后误判为确定性冲突。
func TestActivityContractRoutingPrefersCompatibleWorker(t *testing.T) {
	mgr := memory.NewExecutionManager()
	activityTasks := newTestTaskQueue()
	server := assembleServer(mgr, newTestTaskQueue(), activityTasks)
	defer server.Stop()
	for _, worker := range []struct {
		identity string
		build    string
		version  string
		input    string
		output   string
	}{
		{identity: "pay-worker-old", build: "pay@v1", version: "v1", input: "ChargeInput.v1", output: "Receipt.v1"},
		{identity: "pay-worker-new", build: "pay@v2", version: "v2", input: "ChargeInput.v2", output: "Receipt.v2"},
	} {
		if err := server.RegisterActivityWorkerVersion(context.Background(), protocol.ActivityWorkerVersion{
			Identity: worker.identity, BuildID: worker.build,
			TaskQueues:    []string{"payments"},
			ActivityTypes: []string{"payments.charge"},
			Contracts: []activity.Contract{{
				Type: "payments.charge", Version: worker.version, Input: worker.input, Output: worker.output,
			}},
		}); err != nil {
			t.Fatal(err)
		}
	}
	runID, wftID := startContractRun(t, mgr, server, chargeContractWorkflow("contract-routing"))

	if err := server.HandleWorkflowTaskCompleted(server.WorkflowTaskContext(context.Background(), runID, wftID), runID, wftID, scheduleChargeTask(), ""); err != nil {
		t.Fatalf("compatible worker exists, scheduling must succeed: %v", err)
	}
	if len(activityTasks.tasks) != 1 {
		t.Fatalf("expected one activity task, got %d", len(activityTasks.tasks))
	}
	if got := activityTasks.tasks[0].Task.TaskQueue; got != "payments" {
		t.Fatalf("scheduled to queue %q, want payments", got)
	}
	run, err := mgr.Get(runID)
	if err != nil {
		t.Fatal(err)
	}
	if run.ConfigError != "" {
		t.Fatalf("routing to a compatible worker must not pause the run: %q", run.ConfigError)
	}
}

// [高熵密钥] 固定“全部候选都不兼容”的语义：
// 不能静默挑一个 build 派发；返回带期望/实际值的明确错误并写 ConfigError 暂停，任务不入队。
func TestActivityContractRoutingRejectsAllIncompatibleWorkers(t *testing.T) {
	mgr := memory.NewExecutionManager()
	activityTasks := newTestTaskQueue()
	server := assembleServer(mgr, newTestTaskQueue(), activityTasks)
	defer server.Stop()
	for _, build := range []struct {
		identity string
		buildID  string
		version  string
	}{
		{identity: "pay-worker-a", buildID: "pay@a", version: "v1"},
		{identity: "pay-worker-b", buildID: "pay@b", version: "v3"},
	} {
		if err := server.RegisterActivityWorkerVersion(context.Background(), protocol.ActivityWorkerVersion{
			Identity: build.identity, BuildID: build.buildID,
			TaskQueues:    []string{"payments"},
			ActivityTypes: []string{"payments.charge"},
			Contracts: []activity.Contract{{
				Type: "payments.charge", Version: build.version, Input: "ChargeInput." + build.version, Output: "Receipt." + build.version,
			}},
		}); err != nil {
			t.Fatal(err)
		}
	}
	runID, wftID := startContractRun(t, mgr, server, chargeContractWorkflow("contract-all-incompatible"))

	err := server.HandleWorkflowTaskCompleted(server.WorkflowTaskContext(context.Background(), runID, wftID), runID, wftID, scheduleChargeTask(), "")
	if err == nil {
		t.Fatal("all incompatible workers must fail explicitly")
	}
	if !strings.Contains(err.Error(), `version expected "v2"`) {
		t.Fatalf("error must report expected/actual contract: %v", err)
	}
	if len(activityTasks.tasks) != 0 {
		t.Fatalf("incompatible activity must not be dispatched, got %d tasks", len(activityTasks.tasks))
	}
	run, runErr := mgr.Get(runID)
	if runErr != nil {
		t.Fatal(runErr)
	}
	if run.ConfigError == "" {
		t.Fatal("all incompatible workers must pause the run with ConfigError")
	}
}

// [高熵密钥] 固定 target 优先级：
// Run 固定 target 后，契约过滤只能在 target 自身的 route 内选择，不能漂移到同 type/queue 的其它 worker。
func TestActivityContractRoutingKeepsTargetIdentity(t *testing.T) {
	mgr := memory.NewExecutionManager()
	activityTasks := newTestTaskQueue()
	server := assembleServer(mgr, newTestTaskQueue(), activityTasks)
	defer server.Stop()
	for _, worker := range []struct {
		identity string
		build    string
		version  string
	}{
		{identity: "pay-worker-old", build: "pay@v1", version: "v1"},
		{identity: "pay-worker-new", build: "pay@v2", version: "v2"},
	} {
		if err := server.RegisterActivityWorkerVersion(context.Background(), protocol.ActivityWorkerVersion{
			Identity: worker.identity, BuildID: worker.build,
			TaskQueues:    []string{"payments"},
			ActivityTypes: []string{"payments.charge"},
			Contracts: []activity.Contract{{
				Type: "payments.charge", Version: worker.version, Input: "ChargeInput." + worker.version, Output: "Receipt." + worker.version,
			}},
		}); err != nil {
			t.Fatal(err)
		}
	}
	runID, wftID := startContractRunWithTarget(t, mgr, server, chargeContractWorkflow("contract-target-precedence"), "pay-worker-old")

	err := server.HandleWorkflowTaskCompleted(server.WorkflowTaskContext(context.Background(), runID, wftID), runID, wftID, scheduleChargeTask(), "")
	if err == nil {
		t.Fatal("target worker with incompatible contract must fail")
	}
	if !strings.Contains(err.Error(), `"pay-worker-old"`) {
		t.Fatalf("error must name the pinned target worker: %v", err)
	}
	if len(activityTasks.tasks) != 0 {
		t.Fatalf("contract filter must not drift off the pinned target, got %d tasks", len(activityTasks.tasks))
	}
}

// [高熵密钥] target 复活路径必须和目录注册使用同一套队列归一化（worker.App 也是队列），
// 并且仍受 target 限制：route 要保留 target 的 BuildID/Contracts，不能漂移到其它 worker。
func TestAppQueueTargetRouteKeepsBuildAndContracts(t *testing.T) {
	mgr := memory.NewExecutionManager()
	activityTasks := newTestTaskQueue()
	server := assembleServer(mgr, newTestTaskQueue(), activityTasks)
	defer server.Stop()
	register := func(identity, build, version string, contractVersion string) {
		t.Helper()
		if err := server.RegisterActivityWorkerVersion(context.Background(), protocol.ActivityWorkerVersion{
			Identity: identity, BuildID: build, App: "payments",
			ActivityTypes: []string{"payments.charge"},
			Contracts: []activity.Contract{{
				Type: "payments.charge", Version: contractVersion,
				Input: "ChargeInput." + contractVersion, Output: "Receipt." + contractVersion,
			}},
		}); err != nil {
			t.Fatal(err)
		}
	}
	// target 的契约落后一个版本；另一个 worker 已经有兼容契约，选路不能漂移过去。
	register("pay-worker", "pay@v1", "v1", "v1")
	register("other-worker", "pay@v9", "v1", "v2")
	runID, wftID := startContractRunWithTarget(t, mgr, server, chargeContractWorkflow("contract-app-target"), "pay-worker")

	err := server.HandleWorkflowTaskCompleted(server.WorkflowTaskContext(context.Background(), runID, wftID), runID, wftID, []historypkg.Command{{Type: historypkg.CmdScheduleActivity, StartToCloseTimeout: time.Minute, ActivityID: "charge", ActivityType: "payments.charge", TaskQueue: "payments"}}, "")
	if err == nil {
		t.Fatal("incompatible target contract must be detected on the App-only target route")
	}
	if !strings.Contains(err.Error(), "contract mismatch") {
		t.Fatalf("error must be the contract mismatch, not a routing fallback: %v", err)
	}
	if !strings.Contains(err.Error(), `"pay-worker"`) {
		t.Fatalf("error must name the pinned target, not another worker: %v", err)
	}
	if len(activityTasks.tasks) != 0 {
		t.Fatalf("incompatible activity must not be dispatched, got %d tasks", len(activityTasks.tasks))
	}

	// target 升级契约后 route 解析必须保留 App 队列、target build 与上报契约，且不漂移到 other-worker。
	register("pay-worker", "pay@v1", "v1", "v2")
	route, routeErr := server.resolveActivityRoute(dispatch.ActivityRouteRef{
		Namespace: protocol.DefaultNamespace, TargetWorkerIdentity: "pay-worker",
		ActivityID: "charge", Type: "payments.charge", TaskQueue: "payments",
		Version: "v2", Input: "ChargeInput.v2", Output: "Receipt.v2",
	})
	if routeErr != nil {
		t.Fatalf("resolve target route: %v", routeErr)
	}
	if route.TaskQueue != "payments" || route.BuildID != "pay@v1" || route.Identity != "pay-worker" {
		t.Fatalf("route must stay on the pinned target with App queue/build, got %+v", route)
	}
	if len(route.Contracts) != 1 || route.Contracts[0].Version != "v2" {
		t.Fatalf("target route must retain reported contracts, got %+v", route.Contracts)
	}
}

// 显式队列上没有在线 Worker，而 Workflow 声明了契约：无法对照契约，WFT 记 activity_route_unavailable 退避等待，
// Run 不写 ConfigError；任务不入队。
func TestExplicitQueueWithoutWorkerWaitsInsteadOfPausing(t *testing.T) {
	mgr := memory.NewExecutionManager()
	activityTasks := newTestTaskQueue()
	server := assembleServer(mgr, newTestTaskQueue(), activityTasks)
	defer server.Stop()
	// 让 Server 持有路由目录：另一个队列上的 Worker。
	if err := server.RegisterActivityWorkerVersion(context.Background(), protocol.ActivityWorkerVersion{
		Identity: "other-worker", BuildID: "other@v1", TaskQueues: []string{"other"}, ActivityTypes: []string{"other.Ping"},
	}); err != nil {
		t.Fatal(err)
	}
	runID, wftID := startContractRun(t, mgr, server, chargeWorkflowDeclaring("explicit-queue-offline", "v1", "ChargeInput.v1", "Receipt.v1"))
	command := scheduleChargeCommand()
	command[0].TaskQueue = "payments"
	if err := server.HandleWorkflowTaskCompleted(server.WorkflowTaskContext(context.Background(), runID, wftID), runID, wftID, command, ""); err != nil {
		t.Fatal(err)
	}

	run, err := mgr.Get(runID)
	if err != nil {
		t.Fatal(err)
	}
	if run.ConfigError != "" {
		t.Fatalf("an offline worker must not pause the run: %q", run.ConfigError)
	}
	state, err := server.GetWorkflowExecutionState(runID)
	if err != nil {
		t.Fatal(err)
	}
	if blocked := state.BlockedInfo(); !blocked.Blocked || blocked.Cause != WorkflowTaskFailedCauseActivityRouteUnavailable {
		t.Fatalf("blocked info = %+v", blocked)
	}
	if len(activityTasks.snapshot()) != 0 {
		t.Fatalf("activity must wait for a worker, got %d queued tasks", len(activityTasks.snapshot()))
	}
}
