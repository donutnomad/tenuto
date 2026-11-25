package engine

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/program/programtest"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
)

// 同一个 Workflow Type 可以同时运行多个 Run：不给 Workflow ID 时服务端每次生成一个。
func TestStartRunsTwoRunsOfOneWorkflowType(t *testing.T) {
	env := newUpdateWithStartTestEnv(t, "wf-two-runs")
	ctx := context.Background()
	first, err := startedRunID(env.server.StartWorkflow(ctx, "", "wf-two-runs", "", "", nil))
	if err != nil {
		t.Fatal(err)
	}
	second, err := startedRunID(env.server.StartWorkflow(ctx, "", "wf-two-runs", "", "", nil))
	if err != nil {
		t.Fatalf("second start of the same workflow type: %v", err)
	}
	firstRun, err := env.server.mgr.Get(first)
	if err != nil {
		t.Fatal(err)
	}
	secondRun, err := env.server.mgr.Get(second)
	if err != nil {
		t.Fatal(err)
	}
	if firstRun.WorkflowID == secondRun.WorkflowID {
		t.Fatalf("generated workflow ids collide: %q", firstRun.WorkflowID)
	}
	for _, run := range []*execution.Run{firstRun, secondRun} {
		if _, err := uuid.Parse(run.WorkflowID); err != nil {
			t.Fatalf("generated workflow id %q is not a UUID: %v", run.WorkflowID, err)
		}
		if run.WorkflowType != "wf-two-runs" {
			t.Fatalf("workflow type = %q", run.WorkflowType)
		}
	}
}

// 显式 Workflow ID：进行中时再次 Start 冲突；结束后 Start 启动新 Run 接替 current。
func TestStartWithExplicitWorkflowID(t *testing.T) {
	env := newUpdateWithStartTestEnv(t, "wf-explicit")
	ctx := context.Background()
	first, err := startedRunID(env.server.StartWorkflow(ctx, "", "wf-explicit", "order-1", "", nil))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := startedRunID(env.server.StartWorkflow(ctx, "", "wf-explicit", "order-1", "", nil)); !errors.Is(err, execution.ErrCurrentRunConflict) {
		t.Fatalf("start on a running workflow id: %v, want ErrCurrentRunConflict", err)
	}
	if _, err := startedRunID(env.server.StartWorkflow(ctx, "", "wf-explicit", "order-2", "", nil)); err != nil {
		t.Fatalf("start with another workflow id: %v", err)
	}
	if err := env.runtime.TerminateWorkflow(ctx, "", "order-1", first, "done"); err != nil {
		t.Fatal(err)
	}
	next, err := startedRunID(env.server.StartWorkflow(ctx, "", "wf-explicit", "order-1", "", nil))
	if err != nil {
		t.Fatalf("start after the current run ended: %v", err)
	}
	current, err := env.server.mgr.GetCurrentRunID(protocol.DefaultNamespace, "order-1")
	if err != nil {
		t.Fatal(err)
	}
	if next == first || current != next {
		t.Fatalf("current = %q, first = %q, next = %q", current, first, next)
	}
	if _, err := startedRunID(env.server.StartWorkflow(ctx, "", "", "order-3", "", nil)); err == nil {
		t.Fatal("start without a workflow type must fail")
	}
}

// current Run 已结束时，SignalWithStart 启动新 Run 并把 Signal 记在新 Run 上；Start 与 Signal 同一个事务。
func TestSignalWithStartStartsNewRunAfterTerminalCurrent(t *testing.T) {
	env := newUpdateWithStartTestEnv(t, "wf-sws-terminal")
	seedRun(t, env.mgr, "run-terminal", env.workflowID, history.StatusCompleted, true)
	runID, eventID, err := env.runtime.SignalWithStart(context.Background(), "", env.workflowID, env.workflowID,
		nil, "notify", "", map[string]any{"msg": "hello"})
	if err != nil {
		t.Fatalf("signal-with-start after a terminal current run: %v", err)
	}
	if runID == "run-terminal" || eventID == 0 {
		t.Fatalf("runID = %q eventID = %d", runID, eventID)
	}
	if started, signaled := countStartedAndSignaled(t, env.server, runID); started != 1 || signaled != 1 {
		t.Fatalf("new run started=%d signaled=%d, want 1/1", started, signaled)
	}
	if _, signaled := countStartedAndSignaled(t, env.server, "run-terminal"); signaled != 0 {
		t.Fatalf("terminal run received %d signals", signaled)
	}
}

// 复用进行中的 Run 时，请求的 Workflow Type 必须与它一致。
func TestSignalWithStartRejectsOtherWorkflowType(t *testing.T) {
	env := newUpdateWithStartTestEnv(t, "wf-sws-type-a")
	if _, err := env.server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{Type: "wf-sws-type-b", Program: programtest.Noop()}); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, _, err := env.runtime.SignalWithStart(ctx, "", "wf-sws-type-a", "shared-id", nil, "notify", "", nil); err != nil {
		t.Fatal(err)
	}
	if _, _, err := env.runtime.SignalWithStart(ctx, "", "wf-sws-type-b", "shared-id", nil, "notify", "", nil); err == nil {
		t.Fatal("signal-with-start with another workflow type reused the running run")
	}
}

func TestUpdateWithStartStartsNewRunAfterTerminalCurrent(t *testing.T) {
	env := newUpdateWithStartTestEnv(t, "wf-uws-terminal")
	seedRun(t, env.mgr, "run-terminal", env.workflowID, history.StatusCompleted, true)
	runID, _, err := env.runtime.UpdateWithStart(context.Background(), "", env.workflowID, env.workflowID, nil,
		"u", "update-after-terminal", nil, protocol.UpdateWaitStageEnums.Accepted)
	if err != nil {
		t.Fatalf("update-with-start after a terminal current run: %v", err)
	}
	if runID == "run-terminal" {
		t.Fatal("update landed on the terminal run")
	}
	if started, accepted := countStartedEvents(t, env.server, runID); started != 1 || accepted != 1 {
		t.Fatalf("new run started=%d accepted=%d, want 1/1", started, accepted)
	}
}

// updateID 已记录在刚结束的 current Run 上：重试返回记录的结果，不启动新 Run。
func TestUpdateWithStartRetryAfterRunEndedReturnsRecordedUpdate(t *testing.T) {
	env := newUpdateWithStartTestEnv(t, "wf-uws-retry")
	ctx := context.Background()
	runID, first, err := env.runtime.UpdateWithStart(ctx, "", env.workflowID, "exec-retry", nil,
		"u", "update-retry", nil, protocol.UpdateWaitStageEnums.Accepted)
	if err != nil {
		t.Fatal(err)
	}
	if err := env.runtime.TerminateWorkflow(ctx, "", "exec-retry", runID, "done"); err != nil {
		t.Fatal(err)
	}
	retryRunID, retry, err := env.runtime.UpdateWithStart(ctx, "", env.workflowID, "exec-retry", nil,
		"u", "update-retry", nil, protocol.UpdateWaitStageEnums.Accepted)
	if err != nil {
		t.Fatal(err)
	}
	if retryRunID != runID || retry.AcceptedEventID != first.AcceptedEventID {
		t.Fatalf("retry = run %q %+v, want run %q %+v", retryRunID, retry, runID, first)
	}
}

func TestWithStartUsesActiveProgram(t *testing.T) {
	env := newUpdateWithStartTestEnv(t, "wf-active-program")
	ctx := context.Background()
	active, err := env.server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{Type: "wf-active-program", Name: "Active", Program: programtest.Noop()})
	if err != nil {
		t.Fatal(err)
	}
	runID, _, err := env.runtime.UpdateWithStart(ctx, "", "wf-active-program", "exec-1", nil,
		"u", "update-1", nil, protocol.UpdateWaitStageEnums.Accepted)
	if err != nil {
		t.Fatal(err)
	}
	run, err := env.server.mgr.Get(runID)
	if err != nil {
		t.Fatal(err)
	}
	if run.WorkflowConfigVersion != active.Version {
		t.Fatalf("update-with-start program = %q, want the active config %q", run.WorkflowConfigVersion, active.Version)
	}
	signalRunID, _, err := env.runtime.SignalWithStart(ctx, "", "wf-active-program", "exec-2", nil, "notify", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	startRunID, err := startedRunID(env.server.StartWorkflow(ctx, "", "wf-active-program", "exec-3", "", nil))
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{signalRunID, startRunID} {
		other, err := env.server.mgr.Get(id)
		if err != nil {
			t.Fatal(err)
		}
		if other.WorkflowConfigVersion != run.WorkflowConfigVersion {
			t.Fatalf("run %s program = %q, want %q", id, other.WorkflowConfigVersion, run.WorkflowConfigVersion)
		}
	}
}
