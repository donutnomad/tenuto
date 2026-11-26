package engine

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/program/programtest"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
)

// TestConfigErrorPausesRun 固定确定性配置错误的暂停语义：记录 ConfigError、停止 WFT 推进，
// 人工 set-policy 后清空并重新调度。
func TestConfigErrorPausesRun(t *testing.T) {
	server := newTestServer()
	current := &execution.Run{
		ID: "config-error-run", Namespace: "default", WorkflowID: "config-error-flow", WorkflowType: "config-error-flow",
		WorkflowConfigVersion: "v1", OriginalWorkflowConfigVersion: "v1",
		Policy: execution.RunPolicyAutoUpgrade, Status: history.StatusRunning, CreatedAt: time.Now(),
	}
	if err := server.mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(current)) }); err != nil {
		t.Fatal(err)
	}
	if err := server.pauseRun(current.ID, "activity contract mismatch: payments.charge.v1 expects order_id"); err != nil {
		t.Fatal(err)
	}
	paused, err := server.mgr.Get(current.ID)
	if err != nil {
		t.Fatal(err)
	}
	if paused.ConfigError == "" {
		t.Fatal("config error was not recorded")
	}
	if paused.WorkflowConfigVersion != "v1" {
		t.Fatalf("pause changed current content to %q", paused.WorkflowConfigVersion)
	}
	before := countEvents(t, server, current.ID)
	if err := server.scheduleWorkflowTask(testContext(), current.ID); err != nil {
		t.Fatal(err)
	}
	if after := countEvents(t, server, current.ID); after != before {
		t.Fatalf("paused run scheduled %d new events", after-before)
	}
	if _, err := server.SetRunPolicy(context.Background(), "default", current.ID, execution.RunPolicyPinned); err != nil {
		t.Fatal(err)
	}
	resumed, err := server.mgr.Get(current.ID)
	if err != nil {
		t.Fatal(err)
	}
	if resumed.ConfigError != "" {
		t.Fatalf("set-policy did not clear config error: %q", resumed.ConfigError)
	}
	if countEvents(t, server, current.ID) <= before {
		t.Fatal("explicit set-policy must reschedule a workflow task")
	}
}

// TestAutoUpgradeTargetConflictPauses 固定 Run target 与新内容不兼容时的最小失败语义：
// 不派发新内容、不改 target、记录 ConfigError；兼容内容仍可被选中。
func TestAutoUpgradeTargetConflictPauses(t *testing.T) {
	server := newTestServer()
	if err := server.RegisterActivityWorkerVersion(context.Background(), protocol.ActivityWorkerVersion{
		Namespace: "default", Identity: "pay-worker", BuildID: "b1",
		TaskQueues: []string{"payments"}, ActivityTypes: []string{"payments.charge"},
	}); err != nil {
		t.Fatal(err)
	}
	refA, err := server.Definitions().RegisterWorkflowInNamespace("default", &program.WorkflowConfig{
		Type: "target-conflict", Name: "A",
		Program: programtest.Noop(),
		Activities: map[string]program.ActivityRef{
			"charge": {Symbol: "charge", Type: "payments.charge", TaskQueue: "payments"},
		},
	})

	if err != nil {
		t.Fatal(err)
	}
	current := &execution.Run{
		ID: "target-conflict-run", Namespace: "default", WorkflowID: "target-conflict", WorkflowType: "target-conflict",
		WorkflowConfigVersion: refA.Version, OriginalWorkflowConfigVersion: refA.Version,
		TargetWorkerIdentity: "pay-worker", Policy: execution.RunPolicyAutoUpgrade, Status: history.StatusRunning, CreatedAt: time.Now(),
	}
	if err := server.mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(current)) }); err != nil {
		t.Fatal(err)
	}
	// 新内容引入库存 Activity，固定 target 只承接支付，不能自动换 worker。
	if _, err := server.Definitions().RegisterWorkflowInNamespace("default", &program.WorkflowConfig{
		Type: "target-conflict", Name: "B-with-inventory",
		Program: programtest.Noop(),
		Activities: map[string]program.ActivityRef{
			"charge":  {Symbol: "charge", Type: "payments.charge", TaskQueue: "payments"},
			"reserve": {Symbol: "reserve", Type: "inventory.reserve", TaskQueue: "inventory"},
		},
	}); err != nil {
		t.Fatal(err)
	}
	candidate := server.upgradeCandidate(current)
	if candidate.TargetConflict == nil {
		t.Fatalf("candidate %s must conflict with the run target", candidate.Version)
	}
	selected, err := chooseProgramFor(server, current)
	if err != nil {
		t.Fatal(err)
	}
	if selected.Version != refA.Version {
		t.Fatalf("selected %q on target conflict, want current %q", selected.Version, refA.Version)
	}
	paused, err := server.mgr.Get(current.ID)
	if err != nil {
		t.Fatal(err)
	}
	if paused.ConfigError == "" {
		t.Fatal("target conflict must be recorded as config error")
	}
	if paused.TargetWorkerIdentity != "pay-worker" {
		t.Fatalf("target snapshot changed to %q", paused.TargetWorkerIdentity)
	}

	refC, err := server.Definitions().RegisterWorkflowInNamespace("default", &program.WorkflowConfig{
		Type: "target-conflict", Name: "C-compatible",
		Program: programtest.Noop(),
		Activities: map[string]program.ActivityRef{
			"charge": {Symbol: "charge", Type: "payments.charge", TaskQueue: "payments"},
		},
	})

	if err != nil {
		t.Fatal(err)
	}
	if _, err := server.SetRunPolicy(context.Background(), "default", current.ID, execution.RunPolicyAutoUpgrade); err != nil {
		t.Fatal(err)
	}
	reloaded, err := server.mgr.Get(current.ID)
	if err != nil {
		t.Fatal(err)
	}
	selected, err = chooseProgramFor(server, reloaded)
	if err != nil {
		t.Fatal(err)
	}
	if selected.Version != refC.Version {
		t.Fatalf("compatible latest selected %q, want C %q", selected.Version, refC.Version)
	}
}

func countEvents(t *testing.T, server *Server, runID string) int {
	t.Helper()
	events, err := server.mgr.GetEvents(runID)
	if err != nil {
		t.Fatal(err)
	}
	return len(events)
}

func testContext() context.Context { return context.Background() }

// [高熵密钥] 选版期发现 target 冲突时，本次 lease 必须被丢弃，
// 不能用旧内容派发一个 WFT。
func TestPollDropsWorkflowTaskWhenSelectionPauses(t *testing.T) {
	server := newTestServer()
	if err := server.RegisterActivityWorkerVersion(context.Background(), protocol.ActivityWorkerVersion{
		Namespace: "default", Identity: "pay-worker", BuildID: "b1",
		TaskQueues: []string{"payments"}, ActivityTypes: []string{"payments.charge"},
	}); err != nil {
		t.Fatal(err)
	}
	refA, err := server.Definitions().RegisterWorkflowInNamespace("default", &program.WorkflowConfig{
		Type: "poll-conflict", Name: "A",
		Program: programtest.Noop(),
		Activities: map[string]program.ActivityRef{
			"charge": {Symbol: "charge", Type: "payments.charge", TaskQueue: "payments"},
		},
	})

	if err != nil {
		t.Fatal(err)
	}
	current := &execution.Run{
		ID: "poll-conflict-run", Namespace: "default", WorkflowID: "poll-conflict", WorkflowType: "poll-conflict",
		WorkflowConfigVersion: refA.Version, OriginalWorkflowConfigVersion: refA.Version,
		TargetWorkerIdentity: "pay-worker", Policy: execution.RunPolicyAutoUpgrade, Status: history.StatusRunning, CreatedAt: time.Now(),
	}
	if err := server.mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(current)) }); err != nil {
		t.Fatal(err)
	}
	if _, err := server.Definitions().RegisterWorkflowInNamespace("default", &program.WorkflowConfig{
		Type: "poll-conflict", Name: "B-inventory",
		Program: programtest.Noop(),
		Activities: map[string]program.ActivityRef{
			"charge":  {Symbol: "charge", Type: "payments.charge", TaskQueue: "payments"},
			"reserve": {Symbol: "reserve", Type: "inventory.reserve", TaskQueue: "inventory"},
		},
	}); err != nil {
		t.Fatal(err)
	}
	if err := server.scheduleWorkflowTask(context.Background(), current.ID); err != nil {
		t.Fatal(err)
	}
	_, matched, err := server.pollWorkflowTaskOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if matched {
		t.Fatal("paused run must not receive a workflow task")
	}
	events := engineMgrEvents(t, server, current.ID)
	if got := countEngineEventType(events, "WorkflowTaskStarted"); got != 0 {
		t.Fatalf("workflow tasks started = %d, want 0", got)
	}
	paused, err := server.mgr.Get(current.ID)
	if err != nil {
		t.Fatal(err)
	}
	if paused.ConfigError == "" {
		t.Fatal("target conflict must be recorded")
	}
}

func engineMgrEvents(t *testing.T, server *Server, runID string) []history.Event {
	t.Helper()
	events, err := server.mgr.GetEvents(runID)
	if err != nil {
		t.Fatal(err)
	}
	return events
}

func countEngineEventType(events []history.Event, eventType string) int {
	count := 0
	for _, event := range events {
		if string(event.EventType()) == eventType {
			count++
		}
	}
	return count
}

var errConfigErrorRecoverProbe = errors.New("config error recovery probe")

// TestConfigErrorAckReleasesDispatchForSetPolicyRecovery 固定人工恢复语义：
// ConfigError 期间 poll ack 丢弃 lease 后必须释放 dispatch 记录，否则 set-policy
// 复用同一 open WFT 时会被 Dispatched 去重门控挡住，恢复变成静默 no-op。
func TestConfigErrorAckReleasesDispatchForSetPolicyRecovery(t *testing.T) {
	server := newTestServer()
	if _, err := server.Definitions().RegisterWorkflowInNamespace("default", &program.WorkflowConfig{Type: "config-error-recover-flow", Name: "A", Program: programtest.Noop()}); err != nil {
		t.Fatal(err)
	}
	runID, err := startedRunID(server.StartWorkflow(testContext(), "", "config-error-recover-flow", "config-error-recover-flow", "", nil))
	if err != nil {
		t.Fatal(err)
	}
	queue := server.wfQueue.(*testTaskQueue)
	if got := len(queue.snapshot()); got != 1 {
		t.Fatalf("initial workflow task queue size = %d, want 1", got)
	}
	if err := server.pauseRun(runID, errConfigErrorRecoverProbe.Error()); err != nil {
		t.Fatal(err)
	}
	_, matched, err := server.pollWorkflowTaskOnce(testContext())
	if err != nil {
		t.Fatal(err)
	}
	if matched {
		t.Fatal("paused run must not match a workflow task")
	}
	if got := len(queue.snapshot()); got != 0 {
		t.Fatalf("paused lease was not acked: queue size = %d", got)
	}

	if _, err := server.SetRunPolicy(context.Background(), "default", runID, execution.RunPolicyPinned); err != nil {
		t.Fatal(err)
	}
	if got := len(queue.snapshot()); got != 1 {
		t.Fatalf("set-policy recovery requeued %d workflow tasks, want 1 (dispatch lease was not released)", got)
	}
	events, err := server.mgr.GetEvents(runID)
	if err != nil {
		t.Fatal(err)
	}
	if got := countEngineEvents(events, history.EventWorkflowTaskScheduled); got != 1 {
		t.Fatalf("scheduled events = %d, want 1 (recovery must reuse the open task)", got)
	}
}
