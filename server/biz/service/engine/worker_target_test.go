package engine

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/program/programtest"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/definition"
	"github.com/donutnomad/tenuto/server/biz/domain/dispatch"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

func TestWorkerTargetVersionAndOfflineRoute(t *testing.T) {
	s := newTestServer()
	defer s.Stop()
	for _, id := range []string{"A", "B"} {
		if err := s.RegisterActivityWorkerVersion(context.Background(), protocol.ActivityWorkerVersion{Identity: id, BuildID: "same", TaskQueues: []string{"payments"}, ActivityTypes: []string{"charge"}}); err != nil {
			t.Fatal(err)
		}
	}
	original, err := s.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{Type: "targeted", Program: programtest.Noop(),
		Activities: map[string]program.ActivityRef{
			"charge": {Symbol: "charge", Type: "charge"},
		}})
	if err != nil {
		t.Fatal(err)
	}
	first, err := s.Definitions().SetWorkflowWorkerTarget("default", "targeted", "A")
	if err != nil {
		t.Fatal(err)
	}
	second, err := s.Definitions().SetWorkflowWorkerTarget("default", "targeted", "B")
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		ref    definition.WorkflowConfigRef
		target string
	}{{original, ""}, {first, "A"}, {second, "B"}} {
		wf, err := s.workflowConfigs.GetWorkflowConfig(tc.ref)
		if err != nil {
			t.Fatal(err)
		}
		if wf.TargetWorkerIdentity != tc.target {
			t.Fatalf("version %s target=%q want %q", tc.ref.Version, wf.TargetWorkerIdentity, tc.target)
		}
	}
	if _, err := s.Definitions().SetWorkflowWorkerTarget("default", "targeted", "missing"); err == nil || !strings.Contains(err.Error(), "offline or unknown") {
		t.Fatalf("unknown target error=%v", err)
	}
	// Expire A while preserving its registered queue, then resolve an existing run's target.
	now := time.Now().Add(-time.Minute)
	if _, err := s.workerVersions.UpsertActivityWorkerVersionInNamespace(protocol.DefaultNamespace, protocol.ActivityWorkerVersion{Identity: "A", BuildID: "same", TaskQueues: []string{"payments"}, ActivityTypes: []string{"charge"}}, now, time.Second); err != nil {
		t.Fatal(err)
	}
	route, err := s.resolveActivityRoute(dispatch.ActivityRouteRef{Namespace: "default", Type: "charge", TargetWorkerIdentity: "A"})
	if err != nil {
		t.Fatal(err)
	}
	if route.TaskQueue != "payments" || route.Identity != "A" {
		t.Fatalf("offline route=%+v", route)
	}
	if _, err := s.Definitions().SetWorkflowWorkerTarget("default", "targeted", ""); err != nil {
		t.Fatal(err)
	}
}

func TestTargetCase1(t *testing.T) {
	mgr := memory.NewExecutionManager()
	s := assembleServer(mgr, newTestTaskQueue(), newTestTaskQueue())
	defer s.Stop()
	run := &execution.Run{ID: "guard-target", WorkflowID: "wf", WorkflowType: "wf", Status: history.StatusRunning, CreatedAt: time.Now()}
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(run)) }); err != nil {
		t.Fatal(err)
	}
	id, err := mgr.Append(run.ID, run.ID, run.ID, history.NewActivityScheduledEvent(history.ActivityScheduledEvent{ActivityID: "charge", ActivityType: "charge"}))
	if err != nil {
		t.Fatal(err)
	}
	state := &execution.ActivityState{RunID: run.ID, ScheduledEventID: id, ActivityID: "charge", ActivityType: "charge", TargetWorkerIdentity: "A", Attempt: 1, Stamp: 1, Status: execution.ActivityStatusEnums.Scheduled, ScheduledTime: time.Now()}
	if err := mgr.UpsertActivityState(state); err != nil {
		t.Fatal(err)
	}
	task := protocol.Task{Type: protocol.TaskTypeActivity, RunID: run.ID, ScheduledEventID: id, Attempt: 1, Stamp: 1, WorkerIdentity: "B"}
	result, err := NewActivityRuntimeService(s).BeginActivityTask(context.Background(), task)
	if err == nil || result.Execute || result.Reason != "worker_target_mismatch" {
		t.Fatalf("wrong worker: %+v %v", result, err)
	}
	events, err := mgr.GetEvents(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if countEngineEvents(events, history.EventActivityStarted) != 0 {
		t.Fatal("non-target worker created execution history")
	}
	task.WorkerIdentity = "A"
	result, err = NewActivityRuntimeService(s).BeginActivityTask(context.Background(), task)
	if err != nil || !result.Execute {
		t.Fatalf("target worker: %+v %v", result, err)
	}
}

func TestFirstTargetDelivery(t *testing.T) {
	mgr := memory.NewExecutionManager()
	queue := memory.NewActivityQueueRouter(8)
	server := assembleServerWithActivityRouter(mgr, newTestTaskQueue(), queue)
	defer server.Stop()
	for _, id := range []string{"A", "B"} {
		if err := server.RegisterActivityWorkerVersion(context.Background(), protocol.ActivityWorkerVersion{Identity: id, BuildID: "v1", TaskQueues: []string{"payments"}, ActivityTypes: []string{"charge"}}); err != nil {
			t.Fatal(err)
		}
	}
	ref, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{Type: "first-target", TargetWorkerIdentity: "B", Program: programtest.Noop(),
		Activities: map[string]program.ActivityRef{
			"charge": {Symbol: "charge", Type: "charge"},
		}})
	if err != nil {
		t.Fatal(err)
	}
	run := &execution.Run{ID: "first-delivery", Namespace: "default", WorkflowID: ref.WorkflowID, WorkflowType: ref.WorkflowID, WorkflowConfigVersion: ref.Version, TargetWorkerIdentity: "B", Status: history.StatusRunning, CreatedAt: time.Now()}
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(run)) }); err != nil {
		t.Fatal(err)
	}
	scheduled := appendOpenWorkflowTaskForTest(t, mgr, run.ID)
	if err := server.HandleWorkflowTaskCompleted(server.WorkflowTaskContext(context.Background(), run.ID, scheduled), run.ID, scheduled, []history.Command{{Type: history.CmdScheduleActivity, StartToCloseTimeout: time.Minute, ActivityID: "charge", ActivityType: "charge",
		// 只执行一次的 Activity 开始即写 ActivityStarted，下面按事件核对执行者。
		RetryPolicy: &protocol.ParsedRetryPolicy{MaxAttempts: 1}}}, ""); err != nil {
		t.Fatal(err)
	}
	a := protocol.WithWorkerInfo(protocol.WithNamespace(context.Background(), "default"), protocol.WorkerInfo{Namespace: "default", Identity: "A", BuildID: "v1"})
	ctx, cancel := context.WithTimeout(a, 20*time.Millisecond)
	if _, err := server.PollActivityTask(ctx, "payments"); err != context.DeadlineExceeded {
		t.Fatalf("A received first delivery: %v", err)
	}
	cancel()
	b := protocol.WithWorkerInfo(protocol.WithNamespace(context.Background(), "default"), protocol.WorkerInfo{Namespace: "default", Identity: "B", BuildID: "v1"})
	ctxB, cancelB := context.WithTimeout(b, time.Second)
	defer cancelB()
	lease, err := server.PollActivityTask(ctxB, "payments")
	if err != nil {
		t.Fatal(err)
	}
	if lease.Task.TargetWorkerIdentity != "B" {
		t.Fatalf("first delivery lost target: %+v", lease.Task)
	}
	lease.Task.WorkerIdentity = "B"
	lease.Task.WorkerBuildID = "v1"
	result, err := NewActivityRuntimeService(server).BeginActivityTask(ctxB, lease.Task)
	if err != nil || !result.Execute {
		t.Fatalf("B begin: %+v %v", result, err)
	}
	events, err := mgr.GetEvents(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if countEngineEvents(events, history.EventActivityStarted) != 1 {
		t.Fatal("expected exactly one B start event")
	}
	for _, event := range events {
		if event.ActivityStarted != nil && event.ActivityStarted.WorkerIdentity != "B" {
			t.Fatalf("wrong executor: %+v", event)
		}
	}
}

func TestCompletedRestart(t *testing.T) {
	server := newTestServer()
	defer server.Stop()
	if _, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{Type: "repeat", Program: programtest.Noop()}); err != nil {
		t.Fatal(err)
	}
	first, err := startedRunID(server.StartWorkflow(context.Background(), "", "repeat", "repeat", "", nil))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := startedRunID(server.StartWorkflow(context.Background(), "", "repeat", "repeat", "", nil)); err == nil {
		t.Fatal("expected live current conflict")
	}
	run, err := server.mgr.Get(first)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := server.commitTerminalForTest(run.ID, history.NewWorkflowCompletedEvent(nil), history.StatusCompleted, nil); err != nil {
		t.Fatal(err)
	}
	second, err := startedRunID(server.StartWorkflow(context.Background(), "", "repeat", "repeat", "", nil))
	if err != nil {
		t.Fatal(err)
	}
	if first == second {
		t.Fatal("new execution reused run id")
	}
}

func TestTargetNewRegistration(t *testing.T) {
	server := newTestServer()
	defer server.Stop()
	old := protocol.ActivityWorkerVersion{Identity: "A", BuildID: "v1", TaskQueues: []string{"old"}, ActivityTypes: []string{"charge"}, Default: true}
	if _, err := server.workerVersions.UpsertActivityWorkerVersionInNamespace(protocol.DefaultNamespace, old, time.Now().Add(-time.Minute), time.Second); err != nil {
		t.Fatal(err)
	}
	if err := server.RegisterActivityWorkerVersion(context.Background(), protocol.ActivityWorkerVersion{Identity: "A", BuildID: "v2", TaskQueues: []string{"new"}, ActivityTypes: []string{"charge"}, Default: true}); err != nil {
		t.Fatal(err)
	}
	route, err := server.resolveActivityRoute(dispatch.ActivityRouteRef{Namespace: "default", Type: "charge", TargetWorkerIdentity: "A"})
	if err != nil {
		t.Fatal(err)
	}
	if route.BuildID != "v2" || route.TaskQueue != "new" {
		t.Fatalf("selected old registration: %+v", route)
	}
}
