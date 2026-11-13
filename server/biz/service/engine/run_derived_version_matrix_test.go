package engine_test

import (
	"context"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/compiler/sdkgen"
	"github.com/donutnomad/tenuto/internal/worker"
	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/app/assembly"
	"github.com/donutnomad/tenuto/server/biz/domain/definition"
	"github.com/donutnomad/tenuto/server/biz/domain/dispatch"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/biz/service/engine"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

// 本文件实测派生 Run（Reset / Continue-As-New / RunTimeout retry）的选版与继承：
//   - 内容版本：Reset/CAN 采用 active latest；retry 沿用失败 Run 的 current。
//   - original 始终保留 lineage 起点；Policy/TargetWorkerIdentity 继承。
//   - execution deadline 由 Reset/CAN/retry 继承；run deadline 每代重置。
//   - 已完成 Activity 的结果复用（不重新调度），未完成 Activity 在新 Run 上的重新派发行为按实测记录。

func registerGeneratedWorkflowTuned(t *testing.T, server *engine.Server, workflowID, source, policy, name string, tune func(*program.WorkflowConfig)) definition.WorkflowConfigRef {
	t.Helper()
	generated, err := sdkgen.GenerateFromSource([]byte(source), sdkgen.Options{
		WorkflowID: workflowID,
		Name:       name,
		FuncName:   "Flow",
		Filename:   "flow.go",
	})
	if err != nil {
		t.Fatal(err)
	}
	wf := generated.Workflow
	wf.Policy = policy
	if tune != nil {
		tune(wf)
	}
	ref, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, wf)
	if err != nil {
		t.Fatal(err)
	}
	return ref
}

// registerTargetWorker 登记固定路由快照引用的 worker（默认队列 + 全部 Step 能力）。
func registerTargetWorker(t *testing.T, server *engine.Server, identity, buildID string, includeStep5 bool) {
	t.Helper()
	types := []string{"activities.Step1", "activities.Step2", "activities.Step3", "activities.Step4"}
	if includeStep5 {
		types = append(types, "activities.Step5")
	}
	if err := server.RegisterActivityWorkerVersion(context.Background(), protocol.ActivityWorkerVersion{
		Identity: identity, BuildID: buildID,
		TaskQueues:    []string{protocol.DefaultActivityTaskQueue},
		ActivityTypes: types,
	}); err != nil {
		t.Fatal(err)
	}
}

// replayServerControl 提供可显式启动/停止 WorkflowWorker 的夹具：
// 用于在“无消费者”窗口内断言派生 Run 的创建快照，再启动消费者断言首个 WFT 后的 current。
type replayServerControl struct {
	Server     *engine.Server
	Mgr        *memory.ExecutionManager
	Activities *memory.ActivityQueueRouter
	worker     *worker.Worker
	cancel     context.CancelFunc
}

func newControlledReplayServer(t *testing.T) *replayServerControl {
	t.Helper()
	mgr := memory.NewExecutionManager()
	activities := memory.NewActivityQueueRouter(32)
	server := assembly.NewServer(assembly.Options{Stores: assembly.Stores{Executions: mgr}, WorkflowQueue: memory.NewTaskQueue(32), ActivityRouter: activities})
	registerE2EActivities(t, server)
	c := &replayServerControl{Server: server, Mgr: mgr, Activities: activities}
	t.Cleanup(func() {
		c.StopWorker()
		server.Stop()
	})
	return c
}

func (c *replayServerControl) StartWorker(t *testing.T) {
	t.Helper()
	if c.worker != nil {
		return
	}
	c.worker = worker.NewWorkflowWorker(c.Server, worker.WorkerOptions{Identity: "upgrade-replay-worker", BuildID: "build-v1"})
	ctx, cancel := context.WithCancel(context.Background())
	c.cancel = cancel
	c.worker.Start(ctx)
	readyCtx, readyCancel := context.WithTimeout(context.Background(), time.Second)
	defer readyCancel()
	if err := c.worker.WaitReady(readyCtx); err != nil {
		t.Fatal(err)
	}
}

func (c *replayServerControl) StopWorker() {
	if c.worker == nil {
		return
	}
	if c.cancel != nil {
		c.cancel()
	}
	c.worker.Stop()
	c.worker = nil
	c.cancel = nil
}

// dequeueActivityForRun 以 target worker 身份消费定向队列，跳过其它 Run
// （含 Reset 前的旧 Run）遗留的排队任务。无身份 poll 取不到 target 任务，
// 与生产路由限制一致。
func dequeueActivityForRun(t *testing.T, queue dispatch.ActivityTaskQueueRouter, runID, activityType string) protocol.Task {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(
			protocol.WithWorkerRoutingInfo(protocol.WithNamespace(context.Background(), protocol.DefaultNamespace), protocol.WorkerRoutingInfo{
				Namespace: protocol.DefaultNamespace, Identity: "upgrade-replay-worker", BuildID: "build-v1",
			}), time.Second)
		lease, err := queue.Dequeue(ctx, protocol.DefaultActivityTaskQueue)
		cancel()
		if err != nil {
			continue
		}
		if lease.Task.RunID != runID {
			// 旧 Run/旧代遗留的租约必须 Ack，避免悬挂在定向队列里。
			if err := queue.Ack(ctx, lease); err != nil {
				t.Fatalf("ack stale lease: %v", err)
			}
			continue
		}
		if lease.Task.ActivityType != activityType {
			t.Fatalf("activity type = %q, want %q (run %s)", lease.Task.ActivityType, activityType, runID)
		}
		return lease.Task
	}
	t.Fatalf("no %s task for run %s within 10s", activityType, runID)
	return protocol.Task{}
}

func completeActivityForRun(t *testing.T, server *engine.Server, queue dispatch.ActivityTaskQueueRouter, runID, activityType string) {
	t.Helper()
	task := dequeueActivityForRun(t, queue, runID, activityType)
	if err := server.HandleActivityCompleted(context.Background(), task.RunID, task.ScheduledEventID, task.Attempt, task.Stamp, map[string]any{}); err != nil {
		t.Fatal(err)
	}
}

// 真实边界 Reset（ResetEventID）：新建 Run 引用旧 current（A），frok 复用边界前 history；
// 首个 WFT 边界 AutoUpgrade 选 latest B / Pinned 保持 A。
func TestBoundaryResetReusesPrefixUnderLatestContent(t *testing.T) {
	for _, tc := range []struct {
		policy    string
		wantAfter string
	}{
		{policy: execution.RunPolicyAutoUpgrade, wantAfter: "B"},
		{policy: execution.RunPolicyPinned, wantAfter: "A"},
	} {
		t.Run(tc.policy, func(t *testing.T) {
			ctx := context.Background()
			controlled := newControlledReplayServer(t)
			server, mgr, activities := controlled.Server, controlled.Mgr, controlled.Activities
			controlled.StartWorker(t)
			workflowID := "derived-boundary-reset-" + tc.policy

			refA := registerGeneratedWorkflowNamed(t, server, workflowID, upgradeSourceA, tc.policy, "A")
			runID, err := startedRunID(server.StartWorkflow(ctx, "", workflowID, workflowID, "", nil))
			if err != nil {
				t.Fatal(err)
			}
			completeActivityForRun(t, server, activities, runID, "activities.Step1")
			waitForScheduledType(t, mgr, runID, "activities.Step2", 1)

			boundary := int64(0)
			seenCompleted := false
			for _, event := range mgrEvents(t, mgr, runID) {
				if event.ActivityCompleted != nil && event.ActivityCompleted.ActivityType == "activities.Step1" {
					seenCompleted = true
					continue
				}
				if seenCompleted && event.WorkflowTaskCompleted != nil {
					boundary = event.ID
					break
				}
			}
			if boundary == 0 {
				t.Fatal("missing reset boundary after Step1")
			}

			refB := registerGeneratedWorkflowNamed(t, server, workflowID, upgradeSourceAppendE, tc.policy, "B")
			// 暂停消费者后 Reset：创建快照必然仍是旧 current A。
			controlled.StopWorker()
			newRunID, err := server.ResetWorkflow(ctx, execution.ResetWorkflowRequest{RunID: runID, ResetEventID: boundary, Reason: "boundary-reset"})
			if err != nil {
				t.Fatal(err)
			}
			created, err := mgr.Get(newRunID)
			if err != nil {
				t.Fatal(err)
			}
			if created.WorkflowConfigVersion != refA.Version {
				t.Fatalf("boundary reset created with content %q, want old current %q", created.WorkflowConfigVersion, refA.Version)
			}
			controlled.StartWorker(t)
			waitForScheduledType(t, mgr, newRunID, "activities.Step2", 1)

			steps := []string{"activities.Step2", "activities.Step3", "activities.Step4"}
			if tc.wantAfter == "B" {
				steps = append(steps, "activities.Step5")
			}
			for _, activityType := range steps {
				completeActivityForRun(t, server, activities, newRunID, activityType)
			}
			waitForRunStatusWithin(t, server, newRunID, history.StatusCompleted, 20*time.Second)

			wantVersion := refB.Version
			if tc.wantAfter == "A" {
				wantVersion = refA.Version
			}
			assertRunVersion(t, mgr, newRunID, wantVersion, refA.Version, "boundary reset")
			history := mgrEvents(t, mgr, newRunID)
			if got := countScheduledType(history, "activities.Step1"); got != 1 {
				t.Fatalf("Step1 scheduled %d times on boundary reset run, want 1 (copied prefix, no re-run)", got)
			}
			for _, activityType := range steps {
				if got := countScheduledType(history, activityType); got != 1 {
					t.Fatalf("%s scheduled %d times on boundary reset run, want 1", activityType, got)
				}
			}
			for _, event := range history {
				if event.WorkflowTaskFailed != nil && event.WorkflowTaskFailed.Cause != "reset_workflow" {
					t.Fatalf("boundary reset run recorded replay failure: cause=%s reason=%s", event.WorkflowTaskFailed.Cause, event.WorkflowTaskFailed.Reason)
				}
			}
		})
	}
}

// RunTimeout retry 派生 Run：创建时引用失败 Run 的 current（A）；随后的 AutoUpgrade
// 首个 WFT 可升级到 latest B，Pinned 保持 A。input/target/policy/original/execution deadline 继承。
func TestRunTimeoutRetryKeepsRunContentAndInheritance(t *testing.T) {
	for _, tc := range []struct {
		policy    string
		wantAfter string // 首个成功 WFT 后的 current："A" 或 "B"
	}{
		{policy: execution.RunPolicyAutoUpgrade, wantAfter: "B"},
		{policy: execution.RunPolicyPinned, wantAfter: "A"},
	} {
		t.Run(tc.policy, func(t *testing.T) {
			ctx := context.Background()
			controlled := newControlledReplayServer(t)
			mgr := controlled.Mgr
			server := controlled.Server

			workflowID := "derived-retry-content-" + tc.policy
			registerTargetWorker(t, server, "upgrade-replay-worker", "build-v1", true)
			refA := registerGeneratedWorkflowTuned(t, server, workflowID, upgradeSourceA, tc.policy, "A", func(wf *program.WorkflowConfig) {
				wf.RunTimeout = "200ms"
				wf.ExecutionTimeout = "5s"
				wf.TargetWorkerIdentity = "upgrade-replay-worker"
				wf.RetryPolicy = &program.RetryPolicy{MaxAttempts: 2, InitialInterval: "10ms", BackoffCoefficient: 1}
			})
			run1ID, err := startedRunID(server.StartWorkflow(ctx, "", workflowID, workflowID, "", map[string]any{"seed": "one"}))
			if err != nil {
				t.Fatal(err)
			}

			refB := registerGeneratedWorkflowNamed(t, server, workflowID, upgradeSourceAppendE, execution.RunPolicyAutoUpgrade, "B")
			if refB.Version == refA.Version {
				t.Fatal("B content version must differ from A")
			}

			waitForRunStatusWithin(t, server, run1ID, history.StatusFailed, 5*time.Second)
			failed, err := mgr.Get(run1ID)
			if err != nil {
				t.Fatal(err)
			}
			retry := waitForNewCurrentRunForTest(t, mgr, protocol.DefaultNamespace, workflowID, run1ID, 5*time.Second)
			// 创建时的引用是失败 Run 的 current（A），不是 latest B。
			if retry.WorkflowConfigVersion != refA.Version {
				t.Fatalf("retry created with content %q, want failed run current %q", retry.WorkflowConfigVersion, refA.Version)
			}
			if retry.OriginalWorkflowConfigVersion != refA.Version {
				t.Fatalf("retry original = %q, want %q", retry.OriginalWorkflowConfigVersion, refA.Version)
			}
			if retry.Policy != tc.policy {
				t.Fatalf("retry policy = %q, want inherited %q", retry.Policy, tc.policy)
			}
			if retry.TargetWorkerIdentity != "upgrade-replay-worker" {
				t.Fatalf("retry target = %q, want inherited upgrade-replay-worker", retry.TargetWorkerIdentity)
			}
			if retry.Input["seed"] != "one" {
				t.Fatalf("retry input = %#v, want inherited seed", retry.Input)
			}
			if retry.DeadlineAt == nil || failed.DeadlineAt == nil || !retry.DeadlineAt.Equal(*failed.DeadlineAt) {
				t.Fatalf("retry must inherit execution deadline: failed=%v retry=%v", failed.DeadlineAt, retry.DeadlineAt)
			}
			if retry.Attempt != 2 || retry.Initiator != execution.InitiatorEnums.Retry {
				t.Fatalf("retry attempt/initiator = %d/%q, want 2/retry", retry.Attempt, retry.Initiator)
			}

			// 启动消费者：首个成功 WFT 边界 AutoUpgrade 升级 latest B，Pinned 留在 A。
			controlled.StartWorker(t)
			waitForScheduledType(t, mgr, retry.ID, "activities.Step1", 1)
			after, err := mgr.Get(retry.ID)
			if err != nil {
				t.Fatal(err)
			}
			wantAfter := refB.Version
			if tc.wantAfter == "A" {
				wantAfter = refA.Version
			}
			assertRunVersion(t, mgr, retry.ID, wantAfter, refA.Version, "retry after first workflow task")
			if after.WorkflowConfigVersion != wantAfter {
				t.Fatalf("retry current after first WFT = %q, want %q", after.WorkflowConfigVersion, wantAfter)
			}
		})
	}
}

// 目标 worker 的候选内容冲突：AutoUpgrade 选版期发现候选契约/能力不满足当前 Run 的 target 时，
// 暂停并写 ConfigError；保留原 target；人工修复（登记兼容 worker）后同一 WFT 重试恢复。
func TestAutoUpgradeTargetConflictThenManualRecovery(t *testing.T) {
	ctx := context.Background()
	controlled := newControlledReplayServer(t)
	controlled.StartWorker(t)
	server, mgr, activities := controlled.Server, controlled.Mgr, controlled.Activities
	workflowID := "derived-target-conflict"

	// target worker 只具备 Step1/2/3/4，不具备 B 引入的 Step5；先登记再启动 Run。
	registerTargetWorker(t, server, "upgrade-replay-worker", "build-v1", false)
	refA := registerGeneratedWorkflowTuned(t, server, workflowID, upgradeSourceA, execution.RunPolicyAutoUpgrade, "A", func(wf *program.WorkflowConfig) {
		wf.TargetWorkerIdentity = "upgrade-replay-worker"
	})
	runID, err := startedRunID(server.StartWorkflow(ctx, "", workflowID, workflowID, "", nil))
	if err != nil {
		t.Fatal(err)
	}
	completeActivityForRun(t, server, activities, runID, "activities.Step1")
	waitForScheduledType(t, mgr, runID, "activities.Step2", 1)

	// B 引入的 Step5 由另一个 worker 服务，保证内容可发布；
	// Run 的 target 快照仍是只具备 Step1-4 的 upgrade-replay-worker。
	registerTargetWorker(t, server, "other-worker", "build-v1", true)
	refB := registerGeneratedWorkflowNamed(t, server, workflowID, upgradeSourceAppendE, execution.RunPolicyAutoUpgrade, "B")
	completeActivityForRun(t, server, activities, runID, "activities.Step2")

	waitForRunConfigError(t, mgr, runID)
	paused, err := mgr.Get(runID)
	if err != nil {
		t.Fatal(err)
	}
	assertRunVersion(t, mgr, runID, refA.Version, refA.Version, "target conflict keeps current/original")
	if paused.TargetWorkerIdentity != "upgrade-replay-worker" {
		t.Fatalf("target clock changed on conflict: %q", paused.TargetWorkerIdentity)
	}
	if len(paused.ConfigError) == 0 {
		t.Fatal("ConfigError must explain the target conflict")
	}

	// 人工恢复：登记具备 Step5 的同一 target worker，再走显式 set-policy 入口重排 WFT。
	registerTargetWorker(t, server, "upgrade-replay-worker", "build-v1", true)
	if _, err := server.SetRunPolicy(context.Background(), protocol.DefaultNamespace, runID, execution.RunPolicyAutoUpgrade); err != nil {
		t.Fatal(err)
	}
	completeActivityForRun(t, server, activities, runID, "activities.Step3")
	completeActivityForRun(t, server, activities, runID, "activities.Step4")
	completeActivityForRun(t, server, activities, runID, "activities.Step5")
	waitForRunStatusWithin(t, server, runID, history.StatusCompleted, 20*time.Second)
	assertRunVersion(t, mgr, runID, refB.Version, refA.Version, "recovered run")
}

func waitForNewCurrentRunForTest(t *testing.T, mgr *memory.ExecutionManager, namespace, workflowID, oldRunID string, timeout time.Duration) *execution.Run {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		current, err := mgr.GetCurrentRunID(namespace, workflowID)
		if err == nil && current != "" && current != oldRunID {
			run, err := mgr.Get(current)
			if err != nil {
				t.Fatal(err)
			}
			return run
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("no new current run for %s within %s", workflowID, timeout)
	return nil
}

func waitForRunConfigError(t *testing.T, mgr *memory.ExecutionManager, runID string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		run, err := mgr.Get(runID)
		if err == nil && run.ConfigError != "" {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	run, _ := mgr.Get(runID)
	t.Fatalf("run %s ConfigError stayed empty (status=%s, upgrade_error=%q)", runID, run.Status, run.UpgradeError)
}
