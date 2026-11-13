package engine_test

import (
	"context"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/biz/service/engine"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

// 本文件是 Workflow 内容版本 A→B 的 replay 边界矩阵。
//
// 所有用例只用内存 ExecutionManager + 内存 TaskQueue + 真实 WorkflowWorker，
// 不启动外部服务；Activity 由测试显式 dequeue/start/complete 驱动，
// 因此没有真实 sleep 窗口：每一步都以前一个已提交事件为同步点。
//
// A = a(Step1), b(Step2), c(Step3), d(Step4)。
// 每个用例都在“已推进到某个位置”后发布 B，验证 replay 兼容性、current/original/target、
// 业务 Activity 是否被重复调度。

const upgradeSourceAppendE = `package flows

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
	var five map[string]any
	if err := workflow.ExecuteActivity(ctx, activities.Step5, nil, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &five); err != nil {
		return err
	}
	return nil
}
`

// 尾部删除：d 尚未调度时兼容；d 已调度后发布则 replay 冲突。
const upgradeSourceDropD = `package flows

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
	return nil
}
`

// 在 b 与 c 之间插入新步骤：只有 c 尚未调度时兼容。
const upgradeSourceInsertX = `package flows

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
	var extra map[string]any
	if err := workflow.ExecuteActivity(ctx, activities.StepExtra, nil, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &extra); err != nil {
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

// 重排 a 与 b：history 已有 a scheduled 时不兼容。
const upgradeSourceReorderBA = `package flows

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
	var one map[string]any
	if err := workflow.ExecuteActivity(ctx, activities.Step1, nil, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &one); err != nil {
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

// 删除首步 a：任何已有 history 都不兼容。
const upgradeSourceDropA = `package flows

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

// GetVersion 分支：老 history 缺 marker 走 Step3，新 Run 走 Step5。
const upgradeSourceGetVersion = `package flows

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
	if workflow.GetVersion(ctx, "c-to-e", -1, 1) == -1 {
		var three map[string]any
		if err := workflow.ExecuteActivity(ctx, activities.Step3, nil, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &three); err != nil {
			return err
		}
	} else {
		var five map[string]any
		if err := workflow.ExecuteActivity(ctx, activities.Step5, nil, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &five); err != nil {
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

var linearSteps = []string{"activities.Step1", "activities.Step2", "activities.Step3", "activities.Step4"}

type contentUpgradeCase struct {
	name string
	// policy 是 A/B 注册时使用的策略（auto_upgrade / pinned）。
	policy string
	// sourceB 是发布的新内容。
	sourceB string
	// advance 是发布前已完成的 A 步骤数（0..4）。advance=4 表示 Run 已完成，单独测试。
	advance int
	// trigger 是发布后第一个完成的 Activity：它的完成事件触发边界 WFT。
	// inFlight=true 时该步骤在发布前已经 started；否则发布时它已 scheduled。
	trigger string
	// inFlight=true 表示 trigger 在发布时已经 started 但未完成。
	inFlight bool
	// rest 是 trigger 完成、边界确定后还需要按顺序完成的 Activity 类型。
	rest []string
	// wantFailure=true 表示预期 B replay 冲突：WFT 失败、current/original 保持 A，
	// 发布行为等价的 C 后由既有 WFT 重试自动恢复。
	wantFailure bool
	// wantCurrent 是完成后的内容版本归属："A"、"B"、"C"。
	wantCurrent string
	// wantScheduled 是完成后各 Activity 类型的累计调度次数断言。
	wantScheduled map[string]int
}

func TestUpgradeMatrix1(t *testing.T) {
	abcd := map[string]int{
		"activities.Step1": 1, "activities.Step2": 1, "activities.Step3": 1, "activities.Step4": 1,
	}
	abcde := map[string]int{
		"activities.Step1": 1, "activities.Step2": 1, "activities.Step3": 1,
		"activities.Step4": 1, "activities.Step5": 1,
	}
	cases := []contentUpgradeCase{
		{
			name: "compatible-append-before-a", policy: execution.RunPolicyAutoUpgrade,
			sourceB: upgradeSourceAppendE, advance: 0, trigger: "activities.Step1",
			rest:        []string{"activities.Step2", "activities.Step3", "activities.Step4", "activities.Step5"},
			wantCurrent: "B", wantScheduled: scheduledCounts(abcde),
		},
		{
			name: "compatible-append-after-a", policy: execution.RunPolicyAutoUpgrade,
			sourceB: upgradeSourceAppendE, advance: 1, trigger: "activities.Step2", inFlight: true,
			rest:        []string{"activities.Step3", "activities.Step4", "activities.Step5"},
			wantCurrent: "B", wantScheduled: scheduledCounts(abcde),
		},
		{
			name: "compatible-append-after-b", policy: execution.RunPolicyAutoUpgrade,
			sourceB: upgradeSourceAppendE, advance: 2, trigger: "activities.Step3", inFlight: true,
			rest:        []string{"activities.Step4", "activities.Step5"},
			wantCurrent: "B", wantScheduled: scheduledCounts(abcde),
		},
		{
			name: "compatible-append-after-c", policy: execution.RunPolicyAutoUpgrade,
			sourceB: upgradeSourceAppendE, advance: 3, trigger: "activities.Step4", inFlight: true,
			rest:        []string{"activities.Step5"},
			wantCurrent: "B", wantScheduled: scheduledCounts(abcde),
		},
		{
			name: "compatible-append-d-already-scheduled", policy: execution.RunPolicyAutoUpgrade,
			sourceB: upgradeSourceAppendE, advance: 3, trigger: "activities.Step4",
			rest:        []string{"activities.Step5"},
			wantCurrent: "B", wantScheduled: scheduledCounts(abcde),
		},
		{
			name: "compatible-tail-delete-while-c-in-flight", policy: execution.RunPolicyAutoUpgrade,
			sourceB: upgradeSourceDropD, advance: 2, trigger: "activities.Step3", inFlight: true,
			rest: nil, wantCurrent: "B",
			wantScheduled: scheduledCounts(map[string]int{
				"activities.Step1": 1, "activities.Step2": 1, "activities.Step3": 1, "activities.Step4": 0,
			}),
		},
		{
			name: "compatible-insert-while-b-in-flight", policy: execution.RunPolicyAutoUpgrade,
			sourceB: upgradeSourceInsertX, advance: 1, trigger: "activities.Step2", inFlight: true,
			rest:        []string{"activities.StepExtra", "activities.Step3", "activities.Step4"},
			wantCurrent: "B",
			wantScheduled: scheduledCounts(map[string]int{
				"activities.Step1": 1, "activities.Step2": 1, "activities.StepExtra": 1,
				"activities.Step3": 1, "activities.Step4": 1,
			}),
		},
		{
			name: "incompatible-insert-after-c-completed", policy: execution.RunPolicyAutoUpgrade,
			sourceB: upgradeSourceInsertX, advance: 3, trigger: "activities.Step4", wantFailure: true,
			rest: nil, wantCurrent: "C",
			wantScheduled: scheduledCounts(map[string]int{
				"activities.Step1": 1, "activities.Step2": 1, "activities.Step3": 1,
				"activities.StepExtra": 0, "activities.Step4": 1,
			}),
		},
		{
			name: "incompatible-tail-delete-while-d-in-flight", policy: execution.RunPolicyAutoUpgrade,
			sourceB: upgradeSourceDropD, advance: 3, trigger: "activities.Step4", inFlight: true, wantFailure: true,
			rest: nil, wantCurrent: "C", wantScheduled: scheduledCounts(abcd),
		},
		{
			name: "incompatible-reorder-while-b-in-flight", policy: execution.RunPolicyAutoUpgrade,
			sourceB: upgradeSourceReorderBA, advance: 1, trigger: "activities.Step2", inFlight: true, wantFailure: true,
			rest:        []string{"activities.Step3", "activities.Step4"},
			wantCurrent: "C", wantScheduled: scheduledCounts(abcd),
		},
		{
			name: "incompatible-drop-a-before-a-completed", policy: execution.RunPolicyAutoUpgrade,
			sourceB: upgradeSourceDropA, advance: 0, trigger: "activities.Step1", wantFailure: true,
			rest:        []string{"activities.Step2", "activities.Step3", "activities.Step4"},
			wantCurrent: "C", wantScheduled: scheduledCounts(abcd),
		},
		{
			name: "getversion-old-history", policy: execution.RunPolicyAutoUpgrade,
			sourceB: upgradeSourceGetVersion, advance: 2, trigger: "activities.Step3", inFlight: true,
			rest:        []string{"activities.Step4"},
			wantCurrent: "B",
			wantScheduled: scheduledCounts(map[string]int{
				"activities.Step1": 1, "activities.Step2": 1, "activities.Step3": 1,
				"activities.Step4": 1, "activities.Step5": 0,
			}),
		},
		{
			name: "pinned-append-after-b-keeps-a", policy: execution.RunPolicyPinned,
			sourceB: upgradeSourceAppendE, advance: 2, trigger: "activities.Step3", inFlight: true,
			rest:        []string{"activities.Step4"},
			wantCurrent: "A",
			wantScheduled: scheduledCounts(map[string]int{
				"activities.Step1": 1, "activities.Step2": 1, "activities.Step3": 1,
				"activities.Step4": 1, "activities.Step5": 0,
			}),
		},
		{
			name: "pinned-incompatible-after-b-does-not-fail", policy: execution.RunPolicyPinned,
			sourceB: upgradeSourceDropA, advance: 2, trigger: "activities.Step3", inFlight: true,
			rest:        []string{"activities.Step4"},
			wantCurrent: "A", wantScheduled: scheduledCounts(abcd),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			server, mgr, activities := newUpgradeReplayServer(t)
			workflowID := "upgrade-matrix-" + tc.name
			t.Logf("case %s: advance=%d trigger=%s inFlight=%v", tc.name, tc.advance, tc.trigger, tc.inFlight)

			refA := registerGeneratedWorkflowNamed(t, server, workflowID, upgradeSourceA, tc.policy, "A "+tc.name)
			runID, err := startedRunID(server.StartWorkflow(ctx, "", workflowID, workflowID, "", nil))
			if err != nil {
				t.Fatal(err)
			}
			waitForScheduledType(t, mgr, runID, linearSteps[0], 1)

			for i := 0; i < tc.advance; i++ {
				completeByType(t, server, activities, linearSteps[i])
				waitForScheduledType(t, mgr, runID, linearSteps[i+1], 1)
			}
			waitForScheduledType(t, mgr, runID, tc.trigger, 1)

			var started *protocol.Task
			if tc.inFlight {
				task := dequeueActivityTaskWithin(t, activities, tc.trigger, 10*time.Second)
				if _, err := server.HandleActivityStartedWithMetadata(task, time.Minute, 0); err != nil {
					t.Fatal(err)
				}
				started = &task
			}

			// 发布本身不是 WFT 边界：没有新事件时 current 必须保持 A。
			refB := registerGeneratedWorkflowNamed(t, server, workflowID, tc.sourceB, tc.policy, "B "+tc.name)
			if refB.Version == refA.Version {
				t.Fatal("B content version must differ from A")
			}
			assertRunVersion(t, mgr, runID, refA.Version, refA.Version, "publish must not move current")

			// 完成 trigger：inFlight 用已 started 的任务；否则从队列取。
			if started != nil {
				completeStarted(t, server, started)
			} else {
				completeByType(t, server, activities, tc.trigger)
			}

			if tc.wantFailure {
				waitForWorkflowTaskFailures(t, mgr, runID, 1)
				run, err := mgr.Get(runID)
				if err != nil {
					t.Fatal(err)
				}
				if run.UpgradeError == "" {
					t.Fatal("failed replay must record upgrade_error")
				}
				assertRunVersion(t, mgr, runID, refA.Version, refA.Version, "failed replay must keep current/original")

				// 发布行为等价的 C（同 A 源码、不同 name），让既有 WFT 重试自动恢复。
				refC := registerGeneratedWorkflowNamed(t, server, workflowID, upgradeSourceA, tc.policy, "C "+tc.name)
				for _, activityType := range tc.rest {
					completeByType(t, server, activities, activityType)
				}
				waitForRunStatusWithin(t, server, runID, history.StatusCompleted, 20*time.Second)
				wantVersion := refC.Version
				if tc.wantCurrent == "A" {
					wantVersion = refA.Version
				}
				assertRunVersion(t, mgr, runID, wantVersion, refA.Version, "recovered run")
			} else {
				for _, activityType := range tc.rest {
					completeByType(t, server, activities, activityType)
				}
				waitForRunStatusWithin(t, server, runID, history.StatusCompleted, 20*time.Second)
				wantVersion := refB.Version
				if tc.wantCurrent == "A" {
					wantVersion = refA.Version
				}
				assertRunVersion(t, mgr, runID, wantVersion, refA.Version, "completed run")
			}

			history := mgrEvents(t, mgr, runID)
			for activityType, want := range tc.wantScheduled {
				if got := countScheduledType(history, activityType); got != want {
					t.Fatalf("%s scheduled %d times, want %d (history %d events)", activityType, got, want, len(history))
				}
			}
		})
	}
}

// 发布发生在 Run 完成之后：完成状态与 current 内容都不再变化。
func TestUpgradeMatrix2(t *testing.T) {
	ctx := context.Background()
	server, mgr, activities := newUpgradeReplayServer(t)
	workflowID := "upgrade-after-completed"

	refA := registerGeneratedWorkflowNamed(t, server, workflowID, upgradeSourceA, execution.RunPolicyAutoUpgrade, "A")
	runID, err := startedRunID(server.StartWorkflow(ctx, "", workflowID, workflowID, "", nil))
	if err != nil {
		t.Fatal(err)
	}
	for _, activityType := range linearSteps {
		completeByType(t, server, activities, activityType)
	}
	waitForRunStatusWithin(t, server, runID, history.StatusCompleted, 10*time.Second)
	eventsBefore := len(mgrEvents(t, mgr, runID))

	refB := registerGeneratedWorkflowNamed(t, server, workflowID, upgradeSourceAppendE, execution.RunPolicyAutoUpgrade, "B")
	if refB.Version == refA.Version {
		t.Fatal("B content version must differ from A")
	}
	assertRunVersion(t, mgr, runID, refA.Version, refA.Version, "completed run keeps its content")

	if got := len(mgrEvents(t, mgr, runID)); got != eventsBefore {
		t.Fatalf("completed run history changed after publish: %d -> %d events", eventsBefore, got)
	}
	if got := countScheduledType(mgrEvents(t, mgr, runID), "activities.Step5"); got != 0 {
		t.Fatalf("completed run scheduled %d step5 activities", got)
	}
}

// 新 Run 直接走 B：GetVersion 记录 marker 并选新分支，Step3 不再调度。
func TestUpgradeMatrix3(t *testing.T) {
	ctx := context.Background()
	server, mgr, activities := newUpgradeReplayServer(t)
	workflowID := "upgrade-getversion-new-run"

	refB := registerGeneratedWorkflowNamed(t, server, workflowID, upgradeSourceGetVersion, execution.RunPolicyAutoUpgrade, "B")
	runID, err := startedRunID(server.StartWorkflow(ctx, "", workflowID, workflowID, "", nil))
	if err != nil {
		t.Fatal(err)
	}
	for _, activityType := range []string{"activities.Step1", "activities.Step2", "activities.Step5", "activities.Step4"} {
		completeByType(t, server, activities, activityType)
	}
	waitForRunStatusWithin(t, server, runID, history.StatusCompleted, 10*time.Second)

	history := mgrEvents(t, mgr, runID)
	if got := countScheduledType(history, "activities.Step3"); got != 0 {
		t.Fatalf("new run on B scheduled %d step3 activities, want 0", got)
	}
	if got := countScheduledType(history, "activities.Step5"); got != 1 {
		t.Fatalf("new run scheduled %d step5 activities, want 1", got)
	}
	if got := countEventType(history, "MarkerRecorded"); got == 0 {
		t.Fatal("new run must record a GetVersion marker")
	}
	assertRunVersion(t, mgr, runID, refB.Version, refB.Version, "new run original")
}

func waitForScheduledType(t *testing.T, mgr *memory.ExecutionManager, runID, activityType string, want int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		events, err := mgr.GetEvents(runID)
		if err == nil && countScheduledType(events, activityType) >= want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	events, _ := mgr.GetEvents(runID)
	t.Fatalf("%s was not scheduled %d time(s) within 10s (got %d)", activityType, want, countScheduledType(events, activityType))
}

func completeStarted(t *testing.T, server *engine.Server, task *protocol.Task) {
	t.Helper()
	if task == nil {
		return
	}
	if err := server.HandleActivityCompleted(context.Background(), task.RunID, task.ScheduledEventID, task.Attempt, task.Stamp, map[string]any{}); err != nil {
		t.Fatal(err)
	}
}

func assertRunVersion(t *testing.T, mgr *memory.ExecutionManager, runID, wantCurrent, wantOriginal, message string) {
	t.Helper()
	run, err := mgr.Get(runID)
	if err != nil {
		t.Fatal(err)
	}
	if run.WorkflowConfigVersion != wantCurrent {
		t.Fatalf("%s: current = %q, want %q", message, run.WorkflowConfigVersion, wantCurrent)
	}
	if run.OriginalWorkflowConfigVersion != wantOriginal {
		t.Fatalf("%s: original = %q, want %q", message, run.OriginalWorkflowConfigVersion, wantOriginal)
	}
}

func scheduledCounts(counts map[string]int) map[string]int { return counts }
