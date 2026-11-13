package engine_test

import (
	"context"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/compiler/sdkgen"
	"github.com/donutnomad/tenuto/internal/worker"
	"github.com/donutnomad/tenuto/protocol"
	historypkg "github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/app/assembly"
	"github.com/donutnomad/tenuto/server/biz/domain/definition"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/biz/service/engine"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

const upgradeSourceA = `package flows

import (
	"time"
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/app/activities"
)

func Flow(ctx workflow.Context) error {
	var one map[string]any
	if err := workflow.ExecuteActivity(ctx, activities.Step1, nil, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &one); err != nil {
		return err
	}
	var two map[string]any
	if err := workflow.ExecuteActivity(ctx, activities.Step2, nil, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &two); err != nil {
		return err
	}
	var three map[string]any
	if err := workflow.ExecuteActivity(ctx, activities.Step3, nil, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &three); err != nil {
		return err
	}
	var four map[string]any
	if err := workflow.ExecuteActivity(ctx, activities.Step4, nil, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &four); err != nil {
		return err
	}
	return nil
}
`

const upgradeSourceB = `package flows

import (
	"time"
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/app/activities"
)

func Flow(ctx workflow.Context) error {
	var one map[string]any
	if err := workflow.ExecuteActivity(ctx, activities.Step1, nil, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &one); err != nil {
		return err
	}
	var two map[string]any
	if err := workflow.ExecuteActivity(ctx, activities.Step2, nil, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &two); err != nil {
		return err
	}
	if workflow.GetVersion(ctx, "step-3-to-6", -1, 1) == -1 {
		var three map[string]any
		if err := workflow.ExecuteActivity(ctx, activities.Step3, nil, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &three); err != nil {
			return err
		}
	} else {
		var six map[string]any
		if err := workflow.ExecuteActivity(ctx, activities.Step6, nil, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &six); err != nil {
			return err
		}
	}
	var four map[string]any
	if err := workflow.ExecuteActivity(ctx, activities.Step4, nil, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &four); err != nil {
		return err
	}
	return nil
}
`

// TestAutoUpgradeReplaysOldHistoryWithNewProgram 是核心验收：
// A 的旧 History 在发布 B 后由 B 完整 replay，旧步骤 3 的结果复用而不是重跑，
// GetVersion 缺 marker 时走旧分支 3，再推进 4；新 Run 在 B 上记录 marker 并执行 6。
func TestAutoUpgradeReplaysOldHistoryWithNewProgram(t *testing.T) {
	server, mgr, activities := newUpgradeReplayServer(t)
	ctx := context.Background()
	workflowID := "upgrade-replay"

	refA := registerGeneratedWorkflow(t, server, workflowID, upgradeSourceA, execution.RunPolicyAutoUpgrade)
	runID, err := startedRunID(server.StartWorkflow(ctx, "", workflowID, workflowID, "", nil))
	if err != nil {
		t.Fatal(err)
	}
	completeByType(t, server, activities, "activities.Step1")
	completeByType(t, server, activities, "activities.Step2")
	completeByType(t, server, activities, "activities.Step3")

	refB := registerGeneratedWorkflow(t, server, workflowID, upgradeSourceB, execution.RunPolicyAutoUpgrade)
	if refB.Version == refA.Version {
		t.Fatal("B content version must differ from A")
	}
	completeByType(t, server, activities, "activities.Step4")
	waitForRunStatus(t, server, runID, historypkg.StatusCompleted)

	current, err := mgr.Get(runID)
	if err != nil {
		t.Fatal(err)
	}
	if current.WorkflowConfigVersion != refB.Version {
		t.Fatalf("old run current = %q, want B %q", current.WorkflowConfigVersion, refB.Version)
	}
	if current.OriginalWorkflowConfigVersion != refA.Version {
		t.Fatalf("old run original = %q, want A %q", current.OriginalWorkflowConfigVersion, refA.Version)
	}
	history, err := mgr.GetEvents(runID)
	if err != nil {
		t.Fatal(err)
	}
	if got := countScheduledType(history, "activities.Step3"); got != 1 {
		t.Fatalf("step3 scheduled %d times, want 1 (completed result reused in replay)", got)
	}
	if got := countScheduledType(history, "activities.Step6"); got != 0 {
		t.Fatalf("old run must keep branch 3, got %d step6 schedules", got)
	}

	// 新 Run 走 B：GetVersion 写入 marker=1，执行 6 而不是 3。
	newRunID, err := startedRunID(server.StartWorkflow(ctx, "", workflowID, workflowID, "", nil))
	if err != nil {
		t.Fatal(err)
	}
	completeByType(t, server, activities, "activities.Step1")
	completeByType(t, server, activities, "activities.Step2")
	completeByType(t, server, activities, "activities.Step6")
	completeByType(t, server, activities, "activities.Step4")
	waitForRunStatus(t, server, newRunID, historypkg.StatusCompleted)
	newHistory, err := mgr.GetEvents(newRunID)
	if err != nil {
		t.Fatal(err)
	}
	if got := countScheduledType(newHistory, "activities.Step3"); got != 0 {
		t.Fatalf("new run must take branch 6, got %d step3 schedules", got)
	}
	if got := countScheduledType(newHistory, "activities.Step6"); got != 1 {
		t.Fatalf("new run step6 schedules = %d, want 1", got)
	}
}

// TestPinnedRunReplaysOriginalProgram 证明 Pinned 发布 B 后仍走 A 的 3→4。
func TestPinnedRunReplaysOriginalProgram(t *testing.T) {
	server, mgr, activities := newUpgradeReplayServer(t)
	ctx := context.Background()
	workflowID := "pinned-replay"

	refA := registerGeneratedWorkflow(t, server, workflowID, upgradeSourceA, execution.RunPolicyPinned)
	runID, err := startedRunID(server.StartWorkflow(ctx, "", workflowID, workflowID, "", nil))
	if err != nil {
		t.Fatal(err)
	}
	completeByType(t, server, activities, "activities.Step1")
	completeByType(t, server, activities, "activities.Step2")
	completeByType(t, server, activities, "activities.Step3")
	refB := registerGeneratedWorkflow(t, server, workflowID, upgradeSourceB, execution.RunPolicyPinned)
	if refB.Version == refA.Version {
		t.Fatal("B content version must differ from A")
	}
	refC := registerGeneratedWorkflowNamed(t, server, workflowID, upgradeSourceA, execution.RunPolicyPinned, "compatible C")
	if refC.Version == refA.Version || refC.Version == refB.Version {
		t.Fatal("C content version must differ from A and B")
	}
	completeByType(t, server, activities, "activities.Step4")
	waitForRunStatus(t, server, runID, historypkg.StatusCompleted)

	current, err := mgr.Get(runID)
	if err != nil {
		t.Fatal(err)
	}
	if current.WorkflowConfigVersion != refA.Version {
		t.Fatalf("pinned run current = %q, want A %q", current.WorkflowConfigVersion, refA.Version)
	}
	if current.OriginalWorkflowConfigVersion != refA.Version {
		t.Fatalf("pinned run original = %q, want A %q", current.OriginalWorkflowConfigVersion, refA.Version)
	}
	history, err := mgr.GetEvents(runID)
	if err != nil {
		t.Fatal(err)
	}
	if got := countScheduledType(history, "activities.Step6"); got != 0 {
		t.Fatalf("pinned run scheduled step6 %d times", got)
	}
	if got := countScheduledType(history, "activities.Step3"); got != 1 {
		t.Fatalf("pinned run scheduled step3 %d times, want 1", got)
	}
}

// registerE2EActivities 让本包 e2e 测试调度的 Activity 类型有路由。
func registerE2EActivities(t *testing.T, server *engine.Server) {
	t.Helper()
	engine.RegisterTestActivityWorker(t, server, "activities.Step1", "activities.Step2", "activities.Step3",
		"activities.Step4", "activities.Step5", "activities.Step6", "activities.StepExtra", "activity.first", "activity.second")
}

func newUpgradeReplayServer(t *testing.T) (*engine.Server, *memory.ExecutionManager, *memory.TaskQueue) {
	t.Helper()
	mgr := memory.NewExecutionManager()
	activityTasks := memory.NewTaskQueue(16)
	server := assembly.NewServer(assembly.Options{Stores: assembly.Stores{Executions: mgr}, WorkflowQueue: memory.NewTaskQueue(16), ActivityRouter: engine.QueueActivityRouter{Queue: activityTasks}})
	registerE2EActivities(t, server)
	worker := worker.NewWorkflowWorker(server, worker.WorkerOptions{
		Identity: "upgrade-replay-worker",
		BuildID:  "upgrade-replay-build",
	})
	ctx, cancel := context.WithCancel(context.Background())
	worker.Start(ctx)
	t.Cleanup(func() {
		cancel()
		worker.Stop()
	})
	readyCtx, readyCancel := context.WithTimeout(ctx, time.Second)
	defer readyCancel()
	if err := worker.WaitReady(readyCtx); err != nil {
		t.Fatal(err)
	}
	return server, mgr, activityTasks
}

func completeByType(t *testing.T, server *engine.Server, queue *memory.TaskQueue, activityType string) {
	t.Helper()
	task := dequeueActivityTask(t, queue)
	if task.ActivityType != activityType {
		t.Fatalf("activity type = %q, want %q (step %s)", task.ActivityType, activityType, task.ActivityID)
	}
	completeActivityTask(t, server, task, map[string]any{})
}

func countScheduledType(events []historypkg.Event, activityType string) int {
	count := 0
	for _, event := range events {
		if event.ActivityScheduled != nil && event.ActivityScheduled.ActivityType == activityType {
			count++
		}
	}
	return count
}

func registerGeneratedWorkflow(t *testing.T, server *engine.Server, workflowID, source, policy string) definition.WorkflowConfigRef {
	t.Helper()
	return registerGeneratedWorkflowNamed(t, server, workflowID, source, policy, workflowID)
}

func registerGeneratedWorkflowNamed(t *testing.T, server *engine.Server, workflowID, source, policy, name string) definition.WorkflowConfigRef {
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
	workflow := generated.Workflow
	workflow.Policy = policy
	ref, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, workflow)
	if err != nil {
		t.Fatal(err)
	}
	return ref
}

// broken upgrade attempt deletes Step1, so replaying A's history hits a command mismatch.
const upgradeSourceBroken = `package flows

import (
	"time"
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/app/activities"
)

func Flow(ctx workflow.Context) error {
	var two map[string]any
	if err := workflow.ExecuteActivity(ctx, activities.Step2, nil, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &two); err != nil {
		return err
	}
	var three map[string]any
	if err := workflow.ExecuteActivity(ctx, activities.Step3, nil, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &three); err != nil {
		return err
	}
	var four map[string]any
	if err := workflow.ExecuteActivity(ctx, activities.Step4, nil, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &four); err != nil {
		return err
	}
	return nil
}
`

// TestAutoUpgradeReplayFailureRetriesAndCompletes 是重放冲突的验收：
// A 的 history 发布不兼容 B 后，WFT 失败并自动退避重试；重试不重复业务 Activity，
// current/original/target 保持 A；发布兼容 C 后仅凭正常重试完成，无需 set-policy/signal/额外 start。
func TestAutoUpgradeReplayFailureRetriesAndCompletes(t *testing.T) {
	server, mgr, activities := newUpgradeReplayServer(t)
	ctx := context.Background()
	workflowID := "upgrade-retry-recover"

	refA := registerGeneratedWorkflow(t, server, workflowID, upgradeSourceA, execution.RunPolicyAutoUpgrade)
	runID, err := startedRunID(server.StartWorkflow(ctx, "", workflowID, workflowID, "", nil))
	if err != nil {
		t.Fatal(err)
	}
	completeByType(t, server, activities, "activities.Step1")
	completeByType(t, server, activities, "activities.Step2")
	third := dequeueActivityTask(t, activities)
	if third.ActivityType != "activities.Step3" {
		t.Fatalf("third activity = %q", third.ActivityType)
	}
	// 发布坏版本后再完成 Step3，保证下一个 WFT 边界选中坏 B。
	registerGeneratedWorkflow(t, server, workflowID, upgradeSourceBroken, execution.RunPolicyAutoUpgrade)
	completeActivityTask(t, server, third, map[string]any{})

	waitForWorkflowTaskFailures(t, mgr, runID, 1)
	current, err := mgr.Get(runID)
	if err != nil {
		t.Fatal(err)
	}
	if current.WorkflowConfigVersion != refA.Version {
		t.Fatalf("failed upgrade changed current to %q, want A %q", current.WorkflowConfigVersion, refA.Version)
	}
	if current.OriginalWorkflowConfigVersion != refA.Version {
		t.Fatalf("original = %q, want A %q", current.OriginalWorkflowConfigVersion, refA.Version)
	}
	if current.UpgradeError == "" {
		t.Fatal("failed upgrade must leave a queryable upgrade_error diagnostic")
	}
	targetAtFailure := current.TargetWorkerIdentity

	// 重复投递同一个失败 WFT：只关闭一次，不追加失败事件、不产生业务 Activity。
	eventsAtFailure := mgrEvents(t, mgr, runID)
	firstFailedScheduledID := firstFailedScheduledEventID(eventsAtFailure)
	if firstFailedScheduledID == 0 {
		t.Fatal("failed workflow task event missing scheduled event id")
	}
	if err := server.HandleWorkflowTaskFailed(server.WorkflowTaskContext(ctx, runID, firstFailedScheduledID), runID, firstFailedScheduledID, historypkg.WorkflowTaskFailedCauseNonDeterministic, "duplicate delivery"); err != nil {
		t.Fatal(err)
	}
	if got := countEventType(mgrEvents(t, mgr, runID), "WorkflowTaskFailed"); got != 1 {
		t.Fatalf("duplicate delivery produced %d failure events, want 1", got)
	}

	// 自动重试事实：不调用任何恢复入口，第二次 WFT 仍按 backoff 发起并再次失败。
	waitForWorkflowTaskFailures(t, mgr, runID, 2)
	eventsAfterRetry := mgrEvents(t, mgr, runID)
	for _, activityType := range []string{"activities.Step1", "activities.Step2", "activities.Step3"} {
		if got := countScheduledType(eventsAfterRetry, activityType); got != 1 {
			t.Fatalf("%s scheduled %d times after retries, want 1", activityType, got)
		}
	}
	if got := countScheduledType(eventsAfterRetry, "activities.Step4"); got != 0 {
		t.Fatalf("failed retries scheduled %d step4 activities", got)
	}
	current, err = mgr.Get(runID)
	if err != nil {
		t.Fatal(err)
	}
	if current.WorkflowConfigVersion != refA.Version || current.OriginalWorkflowConfigVersion != refA.Version {
		t.Fatalf("retrying run moved current/original to %q/%q, want A", current.WorkflowConfigVersion, current.OriginalWorkflowConfigVersion)
	}
	if current.TargetWorkerIdentity != targetAtFailure {
		t.Fatalf("retrying run changed target %q -> %q", targetAtFailure, current.TargetWorkerIdentity)
	}

	// 发布兼容 C；下一次正常重试选中 C，replay 通过后调度 Step4。
	refC := registerGeneratedWorkflowNamed(t, server, workflowID, upgradeSourceA, execution.RunPolicyAutoUpgrade, "compatible C")
	if refC.Version == refA.Version || refC.Version == "" {
		t.Fatal("compatible C must have different content from A")
	}
	stepFour := dequeueActivityTaskWithin(t, activities, "activities.Step4", 15*time.Second)
	completeActivityTask(t, server, stepFour, map[string]any{})
	waitForRunStatusWithin(t, server, runID, historypkg.StatusCompleted, 15*time.Second)

	history, err := mgr.GetEvents(runID)
	if err != nil {
		t.Fatal(err)
	}
	if got := countEventType(history, "WorkflowTaskFailed"); got < 2 {
		t.Fatalf("workflow task failures = %d, want automatic retry evidence (>=2)", got)
	}
	if got := countScheduledType(history, "activities.Step4"); got != 1 {
		t.Fatalf("step4 scheduled %d times, want 1", got)
	}
	current, err = mgr.Get(runID)
	if err != nil {
		t.Fatal(err)
	}
	if current.WorkflowConfigVersion != refC.Version {
		t.Fatalf("recovered run current = %q, want compatible C %q", current.WorkflowConfigVersion, refC.Version)
	}
	if current.OriginalWorkflowConfigVersion != refA.Version {
		t.Fatalf("recovered run original = %q, want A %q", current.OriginalWorkflowConfigVersion, refA.Version)
	}
	if current.UpgradeError != "" {
		t.Fatalf("successful retry did not clear upgrade_error %q", current.UpgradeError)
	}
}

func firstFailedScheduledEventID(events []historypkg.Event) int64 {
	for _, event := range events {
		if event.WorkflowTaskFailed != nil {
			return event.WorkflowTaskFailed.ScheduledEventID
		}
	}
	return 0
}

func dequeueActivityTaskWithin(t *testing.T, queue *memory.TaskQueue, activityType string, timeout time.Duration) protocol.Task {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Until(deadline))
		lease, err := queue.Dequeue(ctx)
		cancel()
		if err != nil {
			break
		}
		if lease.Task.ActivityType == activityType {
			return lease.Task
		}
		t.Fatalf("dequeued activity %q, want %q", lease.Task.ActivityType, activityType)
	}
	t.Fatalf("activity %q was not scheduled within %s", activityType, timeout)
	return protocol.Task{}
}

func waitForRunStatusWithin(t *testing.T, server *engine.Server, runID string, expected historypkg.Status, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		status, err := server.GetRunStatus(runID)
		if err == nil && status == expected {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	status, err := server.GetRunStatus(runID)
	t.Fatalf("run status = %s, err=%v, want %s within %s", status, err, expected, timeout)
}

func mgrEvents(t *testing.T, mgr *memory.ExecutionManager, runID string) []historypkg.Event {
	t.Helper()
	events, err := mgr.GetEvents(runID)
	if err != nil {
		t.Fatal(err)
	}
	return events
}

func waitForWorkflowTaskFailures(t *testing.T, mgr *memory.ExecutionManager, runID string, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		events, err := mgr.GetEvents(runID)
		if err == nil && countEventType(events, "WorkflowTaskFailed") >= want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	events, _ := mgr.GetEvents(runID)
	types := make([]string, 0, len(events))
	for _, event := range events {
		types = append(types, string(event.EventType()))
	}
	t.Fatalf("workflow task failures = %d, want %d (events %d: %v)", countEventType(events, "WorkflowTaskFailed"), want, len(events), types)
}

func countEventType(events []historypkg.Event, eventType string) int {
	count := 0
	for _, event := range events {
		if string(event.EventType()) == eventType {
			count++
		}
	}
	return count
}

// TestGetVersionPublishesSearchableChangeVersion 验证 live 首见写入的版本可按列表成员查询，
// 带版本索引事件的 History 后续重放保持确定性，并跑到完成。
func TestGetVersionPublishesSearchableChangeVersion(t *testing.T) {
	server, mgr, activities := newUpgradeReplayServer(t)
	ctx := context.Background()
	workflowID := "change-version-search"
	registerGeneratedWorkflow(t, server, workflowID, upgradeSourceB, execution.RunPolicyAutoUpgrade)

	runID, err := startedRunID(server.StartWorkflow(ctx, "", workflowID, workflowID, "", nil))
	if err != nil {
		t.Fatal(err)
	}
	completeByType(t, server, activities, "activities.Step1")
	completeByType(t, server, activities, "activities.Step2")
	completeByType(t, server, activities, "activities.Step6")
	completeByType(t, server, activities, "activities.Step4")
	waitForRunStatus(t, server, runID, historypkg.StatusCompleted)

	current, err := mgr.Get(runID)
	if err != nil {
		t.Fatal(err)
	}
	if got := current.SearchAttributes["TemporalChangeVersion"]; got != `["step-3-to-6-1"]` {
		t.Fatalf("run search attribute = %q", got)
	}
	for value, want := range map[string]int{"step-3-to-6-1": 1, "step-3-to-6--1": 0} {
		result, err := server.View().ListRuns(execution.RunListFilter{
			WorkflowID:       workflowID,
			IncludeTerminal:  true,
			SearchAttributes: map[string]string{"TemporalChangeVersion": value},
		})
		if err != nil {
			t.Fatal(err)
		}
		if len(result.Items) != want {
			t.Fatalf("filter %q matched %d runs, want %d", value, len(result.Items), want)
		}
	}
}

// startedRunID 取 Server 启动结果里的 Run ID。
func startedRunID(run *execution.Run, err error) (string, error) {
	if err != nil {
		return "", err
	}
	return run.ID, nil
}
