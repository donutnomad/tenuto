package integration

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	historypkg "github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
)

// 实体 Workflow：每代处理 2 条 Signal，等全部 Update handler 结束（inflight==0）后 continue-as-new。
const signalStormEntitySrc = `package flows

import (
	"time"

	workflow "github.com/donutnomad/tenuto/sdk/workflow"
)

type State struct {
	Gen   int ` + "`json:\"gen\"`" + `
	Total int ` + "`json:\"total\"`" + `
}

type Inc struct {
	V int ` + "`json:\"v\"`" + `
}

func W(ctx workflow.Context, s State) (State, error) {
	inflight := 0
	if err := workflow.SetUpdateHandler(ctx, "add", func(ctx workflow.Context, v int) (int, error) {
		inflight++
		if err := workflow.Sleep(ctx, 5*time.Millisecond); err != nil {
			inflight--
			return 0, err
		}
		s.Total += v
		inflight--
		return s.Total, nil
	}); err != nil {
		return s, err
	}
	for i := 0; i < 2; i++ {
		var in Inc
		if err := workflow.WaitSignal(ctx, "tick").Get(ctx, &in); err != nil {
			return s, err
		}
	}
	if err := workflow.Await(ctx, func() bool { return inflight == 0 }); err != nil {
		return s, err
	}
	s.Gen++
	return s, workflow.ContinueAsNew(ctx, "", s)
}
`

// Signal 每 2ms 一条、Update 串行到达时执行 continue-as-new。带 ContinueAsNew 的 Workflow Task 被新事实作废后，
// Run 进入正在关闭状态：重做的 Workflow Task started 期间新的 Signal 与 Update 以 ErrWorkflowClosing 拒绝，
// 重做的 continue-as-new 一定能提交。接续链持续推进，每代 History 都很短，没有 Run 卡在 running。
func TestContinueAsNewCommitsUnderSignalAndUpdateStorm(t *testing.T) {
	h := newGoWorkerHarness(t, 2)
	h.register("can-storm", signalStormEntitySrc)
	const wfID = "can-storm-1"
	first := h.start("can-storm", wfID, map[string]any{})

	var signalsAccepted, signalsClosing atomic.Int64
	var wg sync.WaitGroup
	stop := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			_, err := h.server.SignalWorkflow(context.Background(), "", wfID, "", "tick", "", map[string]any{"v": 1})
			switch {
			case err == nil:
				signalsAccepted.Add(1)
			case errors.Is(err, execution.ErrWorkflowClosing):
				signalsClosing.Add(1)
			}
			time.Sleep(2 * time.Millisecond)
		}
	}()

	outcomes := map[string]int{}
	var failures []string
	deadline := time.Now().Add(10 * time.Second)
	for i := 0; time.Now().Before(deadline); i++ {
		var r protocol.WorkflowUpdateResult
		var err error
		// 与 SDK 一样，正在关闭时按 Workflow ID 重发：关闭命令提交后落到接续 Run。
		for {
			r, err = h.server.UpdateWorkflow(context.Background(), "", wfID, "", "add", fmt.Sprintf("u-%d", i), protocol.UpdateWaitStageEnums.Completed, jsonPayloads(t, 1))
			if !errors.Is(err, execution.ErrWorkflowClosing) {
				break
			}
			outcomes["closing_retry"]++
			time.Sleep(5 * time.Millisecond)
		}
		switch {
		case err != nil:
			outcomes["error"]++
			failures = append(failures, err.Error())
		case r.Outcome != nil && r.Outcome.Failure != nil:
			outcomes[r.Outcome.Failure.Type]++
		case r.Outcome != nil:
			outcomes["ok"]++
		default:
			outcomes[string(r.Stage)]++
		}
		time.Sleep(3 * time.Millisecond)
	}
	close(stop)
	wg.Wait()
	t.Logf("signals accepted=%d rejected as closing=%d; update outcomes: %v; errors: %v", signalsAccepted.Load(), signalsClosing.Load(), outcomes, failures)

	q, err := h.server.QueryWorkflow(context.Background(), protocol.DefaultNamespace, wfID, "", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	events := h.events(q.Run.ID)
	t.Logf("first run=%s current run=%s status=%s history=%d events", first, q.Run.ID, q.Run.Status, len(events))
	if q.Run.ID == first {
		t.Fatalf("continue-as-new never committed: the first run has %d events (%d failed workflow tasks)", len(events), countEvent(events, historypkg.EventWorkflowTaskFailed))
	}
	if q.Run.Status != historypkg.StatusRunning {
		t.Errorf("current run status %s, want running", q.Run.Status)
	}
	if len(events) > 2000 {
		t.Errorf("current run grew to %d events: continue-as-new keeps failing", len(events))
	}
	if outcomes["error"] > 0 || outcomes["AcceptedUpdateCompletedWorkflow"] > 0 {
		t.Errorf("updates failed although the workflow waits for its handlers before continue-as-new: %v %v", outcomes, failures)
	}
	if outcomes["ok"] == 0 {
		t.Errorf("no update completed: %v", outcomes)
	}

	// Signal 停止后，当前一代处理完已到达的 Signal 继续等待，不停在没有 Workflow Task 的 running。
	time.Sleep(2 * time.Second)
	q2, err := h.server.QueryWorkflow(context.Background(), protocol.DefaultNamespace, wfID, "", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	state, err := h.server.GetWorkflowExecutionState(q2.Run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if q2.Run.Status != historypkg.StatusRunning || state.OpenWorkflowTaskEventID != 0 || state.WorkflowTask.ActivationPending {
		t.Errorf("quiescent current run=%s status=%s open wft=%d pending=%v, want running and idle: %s",
			q2.Run.ID, q2.Run.Status, state.OpenWorkflowTaskEventID, state.WorkflowTask.ActivationPending, eventTypes(h.events(q2.Run.ID)))
	}
}

// 每代处理 2 条 Signal 后 continue-as-new，Signal 每 2ms 一条、没有 Update。
const signalStormCounterSrc = `package flows

import (
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
)

type State struct {
	Gen int ` + "`json:\"gen\"`" + `
}

type Tick struct {
	V int ` + "`json:\"v\"`" + `
}

func W(ctx workflow.Context, s State) (State, error) {
	for i := 0; i < 2; i++ {
		var in Tick
		if err := workflow.WaitSignal(ctx, "tick").Get(ctx, &in); err != nil {
			return s, err
		}
	}
	s.Gen++
	return s, workflow.ContinueAsNew(ctx, "", s)
}
`

// 只有 Signal 的风暴：接续链持续推进，任何一代都不会因反复作废而越来越长。
func TestContinueAsNewCommitsUnderSignalStorm(t *testing.T) {
	h := newGoWorkerHarness(t, 2)
	h.register("can-signal-storm", signalStormCounterSrc)
	const wfID = "can-signal-storm-1"
	first := h.start("can-signal-storm", wfID, map[string]any{})
	stop := make(chan struct{})
	done := make(chan struct{})
	var accepted, closing atomic.Int64
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
			}
			_, err := h.server.SignalWorkflow(context.Background(), "", wfID, "", "tick", "", map[string]any{"v": 1})
			switch {
			case err == nil:
				accepted.Add(1)
			case errors.Is(err, execution.ErrWorkflowClosing):
				closing.Add(1)
			}
			time.Sleep(2 * time.Millisecond)
		}
	}()
	time.Sleep(8 * time.Second)
	close(stop)
	<-done
	q, err := h.server.QueryWorkflow(context.Background(), protocol.DefaultNamespace, wfID, "", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	events := h.events(q.Run.ID)
	t.Logf("signals accepted=%d rejected as closing=%d; current run=%s status=%s history=%d", accepted.Load(), closing.Load(), q.Run.ID, q.Run.Status, len(events))
	if q.Run.ID == first {
		for _, e := range events {
			if e.WorkflowFailed != nil {
				t.Logf("failed: %+v %+v", e.WorkflowFailed, e.WorkflowFailed.Failure)
			}
			if e.WorkflowTaskFailed != nil {
				t.Logf("wft failed: %+v", e.WorkflowTaskFailed)
			}
		}
		t.Fatalf("continue-as-new never committed: the first run has %d events", len(events))
	}
	if q.Run.Status != historypkg.StatusRunning || len(events) > 2000 {
		t.Errorf("current run status=%s history=%d, want a short running generation", q.Run.Status, len(events))
	}
}
