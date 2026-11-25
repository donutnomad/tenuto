package engine

import (
	"context"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/program/programtest"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

// Happy Path: StartWorkflowForCron 通过真 Server 创建的 run，
// 断言 Initiator/Attempt/FirstRunID/SearchAttributes 符合预期（安全网）。
func TestCronRun_InitiatorAttemptFirstRunIDSearchAttributes(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	server := assembleServer(mgr, newTestTaskQueue(), newTestTaskQueue())
	defer server.Stop()

	ref, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{Type: "wf-cron-safety", Program: programtest.Noop()})
	if err != nil {
		t.Fatal(err)
	}

	runID, err := server.StartWorkflowForCron(ctx, "", ref.WorkflowType, ref.WorkflowType, map[string]any{"key": "val"}, nil)
	if err != nil {
		t.Fatal(err)
	}

	run, err := mgr.Get(runID)
	if err != nil || run == nil {
		t.Fatalf("get cron run: %v", err)
	}
	if run.Initiator != execution.InitiatorEnums.Cron {
		t.Errorf("Initiator: want %q got %q", execution.InitiatorEnums.Cron, run.Initiator)
	}
	if run.Attempt != 1 {
		t.Errorf("Attempt: want 1 got %d", run.Attempt)
	}
	if run.FirstRunID != run.ID {
		t.Errorf("FirstRunID: want %q (self) got %q", run.ID, run.FirstRunID)
	}
	if len(run.SearchAttributes) != 0 {
		t.Errorf("SearchAttributes: want empty (cron does not inherit), got %v", run.SearchAttributes)
	}
}

// waitRetryRun 等待 oldRunID 失败后创建的 retry 新 run。
func waitRetryRun(t *testing.T, mgr execution.ExecutionManager, workflowID, oldRunID string) *execution.Run {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		runs, _ := runsByStatus(mgr, history.StatusRunning)
		for _, r := range runs {
			if r.ID != oldRunID && r.WorkflowID == workflowID {
				return r
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("expected retry run to be created")
	return nil
}

// 照 Temporal retry.go 的 startAttr.SearchAttributes：retry 新 run 只继承旧 run 启动时的搜索属性，
// 运行期写入的业务键与 GetVersion 版本都不带。
func TestRetryRun_InheritsStartSearchAttributesOnly(t *testing.T) {
	mgr := memory.NewExecutionManager()
	server := assembleServer(mgr, newTestTaskQueue(), newTestTaskQueue())
	defer server.Stop()

	ref, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{
		Program:     programtest.Noop(),
		Type:        "wf-retry-start-sa",
		RetryPolicy: &program.RetryPolicy{MaxAttempts: 3, InitialInterval: "100ms", BackoffCoefficient: 2.0, MaxInterval: "1s"},
	})

	if err != nil {
		t.Fatal(err)
	}
	const run1ID = "run-retry-start-sa-1"
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error {
		return tx.CreateRun(WithRootBranch(&execution.Run{
			ID: run1ID, WorkflowID: ref.WorkflowType, WorkflowType: ref.WorkflowType, WorkflowConfigVersion: ref.Version,
			FirstRunID: run1ID, Status: history.StatusRunning, Attempt: 1, Initiator: execution.InitiatorEnums.Workflow,
			SearchAttributes: map[string]string{"env": "prod", "biz": "x", "TemporalChangeVersion": `["foo-1"]`},
			IsCurrent:        true, CreatedAt: time.Now(),
		}))
	}); err != nil {
		t.Fatal(err)
	}
	started := history.NewWorkflowStartedEvent(nil, run1ID, "", ref.WorkflowType, ref.WorkflowType, 0)
	started.WorkflowStarted.SearchAttributes = map[string]string{"env": "prod"}
	if _, err := mgr.Append(run1ID, run1ID, run1ID, started); err != nil {
		t.Fatal(err)
	}

	if err := server.failWorkflow(run1ID, protocol.Failure{Message: "transient error"}, ""); err != nil {
		t.Fatal(err)
	}
	newRun := waitRetryRun(t, mgr, ref.WorkflowType, run1ID)
	if len(newRun.SearchAttributes) != 1 || newRun.SearchAttributes["env"] != "prod" {
		t.Fatalf("retry run search attributes = %v, want only start-time env=prod", newRun.SearchAttributes)
	}
	events, err := mgr.GetEvents(newRun.ID)
	if err != nil || len(events) == 0 || events[0].WorkflowStarted == nil || events[0].WorkflowStarted.SearchAttributes["env"] != "prod" {
		t.Fatalf("retry started event = %+v err=%v", events, err)
	}
}

// Happy Path: 旧 History 的启动事件没有记录搜索属性时，retry 新 run 继承旧 run 当前的 SearchAttributes。
func TestRetryRun_SearchAttributesInherited(t *testing.T) {
	mgr := memory.NewExecutionManager()
	server := assembleServer(mgr, newTestTaskQueue(), newTestTaskQueue())
	defer server.Stop()

	ref, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{
		Program: programtest.Noop(),
		Type:    "wf-retry-sa",
		RetryPolicy: &program.RetryPolicy{
			MaxAttempts:        3,
			InitialInterval:    "100ms",
			BackoffCoefficient: 2.0,
			MaxInterval:        "1s",
		},
	})

	if err != nil {
		t.Fatal(err)
	}

	run1ID := "run-retry-sa-1"
	run1 := &execution.Run{
		ID:                    run1ID,
		WorkflowID:            ref.WorkflowType,
		WorkflowType:          ref.WorkflowType,
		WorkflowConfigVersion: ref.Version,
		FirstRunID:            run1ID,
		Status:                history.StatusRunning,
		SearchAttributes:      map[string]string{"env": "prod", "tier": "gold"},
		Attempt:               1,
		Initiator:             execution.InitiatorEnums.Workflow,
		IsCurrent:             true, // 旧 run 是 current（retry 切代 expectedOld=oldRun 需其为 current）
		CreatedAt:             time.Now(),
	}
	createStartedRunForTest(t, mgr, run1)

	if err := server.failWorkflow(run1ID, protocol.Failure{Message: "transient error"}, ""); err != nil {
		t.Fatal(err)
	}

	newRun := waitRetryRun(t, mgr, ref.WorkflowType, run1ID)
	if newRun.SearchAttributes["env"] != "prod" || newRun.SearchAttributes["tier"] != "gold" {
		t.Errorf("SearchAttributes not inherited: got %v", newRun.SearchAttributes)
	}
	if newRun.ContinuedRunID != run1ID {
		t.Errorf("ContinuedRunID: want %q got %q", run1ID, newRun.ContinuedRunID)
	}
}

// Happy Path: retry 新 run 的 ContinuedRunID 指向旧 run（安全网）。
func TestRetryRun_ContinuedRunIDSet(t *testing.T) {
	mgr := memory.NewExecutionManager()
	server := assembleServer(mgr, newTestTaskQueue(), newTestTaskQueue())
	defer server.Stop()

	ref, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{
		Program: programtest.Noop(),
		Type:    "wf-retry-contid",
		RetryPolicy: &program.RetryPolicy{
			MaxAttempts:        2,
			InitialInterval:    "50ms",
			BackoffCoefficient: 1.0,
			MaxInterval:        "200ms",
		},
	})

	if err != nil {
		t.Fatal(err)
	}

	oldRunID := "run-retry-contid-1"
	oldRun := &execution.Run{
		ID:                    oldRunID,
		WorkflowID:            ref.WorkflowType,
		WorkflowType:          ref.WorkflowType,
		WorkflowConfigVersion: ref.Version,
		FirstRunID:            oldRunID,
		Status:                history.StatusRunning,
		Attempt:               1,
		Initiator:             execution.InitiatorEnums.Workflow,
		IsCurrent:             true, // 旧 run 是 current（retry 切代 expectedOld=oldRun 需其为 current）
		CreatedAt:             time.Now(),
	}
	createStartedRunForTest(t, mgr, oldRun)

	if err := server.failWorkflow(oldRunID, protocol.Failure{Message: "err"}, ""); err != nil {
		t.Fatal(err)
	}

	var newRun *execution.Run
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		runs, _ := runsByStatus(mgr, history.StatusRunning)
		for _, r := range runs {
			if r.ID != oldRunID && r.WorkflowID == ref.WorkflowType {
				newRun = r
				break
			}
		}
		if newRun != nil {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if newRun == nil {
		t.Fatal("expected retry run to be created")
	}
	if newRun.ContinuedRunID != oldRunID {
		t.Errorf("ContinuedRunID: want %q got %q", oldRunID, newRun.ContinuedRunID)
	}
}
