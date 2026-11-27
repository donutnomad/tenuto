package engine

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/program/programtest"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

// historyReadCountingManager 记录读整份 History 的次数与按范围读到的事件数。
type historyReadCountingManager struct {
	*memory.ExecutionManager
	fullReads   atomic.Int64
	rangeEvents atomic.Int64
	stateReads  atomic.Int64
}

func (m *historyReadCountingManager) GetWorkflowExecutionState(runID string) (*execution.WorkflowExecutionState, error) {
	m.stateReads.Add(1)
	return m.ExecutionManager.GetWorkflowExecutionState(runID)
}

func (m *historyReadCountingManager) GetEvents(runID string) ([]history.Event, error) {
	m.fullReads.Add(1)
	return m.ExecutionManager.GetEvents(runID)
}

func (m *historyReadCountingManager) GetEventsRange(runID string, afterExclusive, throughInclusive int64) ([]history.Event, error) {
	events, err := m.ExecutionManager.GetEventsRange(runID, afterExclusive, throughInclusive)
	m.rangeEvents.Add(int64(len(events)))
	return events, err
}

// Update 的去重与结果查找按 update ID 的去重键读取，等待循环只读新增的事件，都不读整份 History。
func TestUpdateLookupAndWaitDoNotReadWholeHistory(t *testing.T) {
	ctx := context.Background()
	mgr := &historyReadCountingManager{ExecutionManager: memory.NewExecutionManager()}
	server, _ := newFakeClockServer(mgr, newTestTaskQueue(), newTestTaskQueue())
	run := &execution.Run{ID: "run-update-index", WorkflowID: "wf-update-index", WorkflowType: "wf-update-index", Namespace: protocol.DefaultNamespace, Status: history.StatusRunning}
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(run)) }); err != nil {
		t.Fatal(err)
	}
	// 一段较长的 History。
	for i := 0; i < 500; i++ {
		if _, err := mgr.Append(run.ID, run.ID, run.ID, history.NewWorkflowSignaledEvent("noise", nil)); err != nil {
			t.Fatal(err)
		}
	}

	accepted, err := server.UpdateWorkflow(ctx, "", "", run.ID, "place", "up-1", protocol.UpdateWaitStageEnums.Accepted, nil)
	if err != nil {
		t.Fatal(err)
	}
	again, err := server.UpdateWorkflow(ctx, "", "", run.ID, "place", "up-1", protocol.UpdateWaitStageEnums.Accepted, nil)
	if err != nil {
		t.Fatal(err)
	}
	if again.AcceptedEventID != accepted.AcceptedEventID {
		t.Fatalf("retried update accepted at %d, first at %d", again.AcceptedEventID, accepted.AcceptedEventID)
	}

	type outcome struct {
		result protocol.WorkflowUpdateResult
		err    error
	}
	waiter := make(chan outcome, 1)
	go func() {
		result, err := server.UpdateWorkflow(ctx, "", "", run.ID, "place", "up-1", protocol.UpdateWaitStageEnums.Completed, nil)
		waiter <- outcome{result, err}
	}()
	// 等待期间 Run 上陆续有新事件，每一条都唤醒等待方。
	const wakeups = 5
	for i := 0; i < wakeups; i++ {
		time.Sleep(10 * time.Millisecond)
		if _, err := mgr.Append(run.ID, run.ID, run.ID, history.NewWorkflowSignaledEvent("tick", nil)); err != nil {
			t.Fatal(err)
		}
		server.runWatch.Notify(run.ID)
	}
	payload, err := protocol.NewJSONPayload(map[string]any{"ok": true})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := mgr.AppendIdempotent(run.ID, run.ID, run.ID, history.Event{WorkflowUpdateCompleted: &history.WorkflowUpdateCompletedEvent{
		Name: "place", UpdateID: "up-1", Outcome: &protocol.UpdateOutcome{Success: &payload},
	}}); err != nil {
		t.Fatal(err)
	}
	server.runWatch.Notify(run.ID)
	select {
	case got := <-waiter:
		if got.err != nil || got.result.Stage != protocol.UpdateReachedStageEnums.Completed || got.result.Outcome == nil || got.result.Outcome.Success == nil {
			t.Fatalf("update result = %+v, err = %v", got.result, got.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("update waiter did not see the completion")
	}
	if n := mgr.fullReads.Load(); n != 0 {
		t.Fatalf("update lookup and wait read the whole history %d times", n)
	}
	if n := mgr.rangeEvents.Load(); n > wakeups+1 {
		t.Fatalf("update wait read %d events, want at most the %d appended while waiting", n, wakeups+1)
	}

	// Run 结束之后，已接受、未完成的 Update 按去重键查到，以 Run 关闭的失败结束。
	if _, err := server.UpdateWorkflow(ctx, "", "", run.ID, "place", "up-2", protocol.UpdateWaitStageEnums.Accepted, nil); err != nil {
		t.Fatal(err)
	}
	closedID, err := mgr.Append(run.ID, run.ID, run.ID, history.NewWorkflowCompletedEvent(nil))
	if err != nil {
		t.Fatal(err)
	}
	rec, ok := server.lookupUpdate("", "", run.ID, "up-2")
	if !ok || rec.CompletedEventID != closedID || rec.Outcome == nil || rec.Outcome.Failure == nil {
		t.Fatalf("update closed by the run = %+v (found %v), want failure at %d", rec, ok, closedID)
	}
	if n := mgr.fullReads.Load(); n != 0 {
		t.Fatalf("update lookup read the whole history %d times", n)
	}
}

// afterDedupeReadManager 在按去重键读完之后执行一次 afterRead，模拟读取之后、调用方下一步之前落库的提交。
type afterDedupeReadManager struct {
	*memory.ExecutionManager
	afterRead atomic.Pointer[func()]
}

func (m *afterDedupeReadManager) GetEventsByDedupeKeys(runID string, keys []string) ([]history.Event, error) {
	events, err := m.ExecutionManager.GetEventsByDedupeKeys(runID, keys)
	if hook := m.afterRead.Swap(nil); hook != nil {
		(*hook)()
	}
	return events, err
}

// Update handler 与 main 在同一个 Workflow Task 里返回：WorkflowUpdateCompleted 与 WorkflowCompleted 同批提交，
// 正好落在等待方按去重键读取之后。等待方拿到的必须是 handler 的结果，不能是「Run 结束了这个 Update」的失败。
func TestUpdateWaitSeesCompletionCommittedWithRunClose(t *testing.T) {
	ctx := context.Background()
	mgr := &afterDedupeReadManager{ExecutionManager: memory.NewExecutionManager()}
	server, _ := newFakeClockServer(mgr, newTestTaskQueue(), newTestTaskQueue())
	run := &execution.Run{ID: "run-update-close", WorkflowID: "wf-update-close", WorkflowType: "wf-update-close", Namespace: protocol.DefaultNamespace, Status: history.StatusRunning}
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(run)) }); err != nil {
		t.Fatal(err)
	}
	if _, err := server.UpdateWorkflow(ctx, "", "", run.ID, "place", "up-1", protocol.UpdateWaitStageEnums.Accepted, nil); err != nil {
		t.Fatal(err)
	}
	payload, err := protocol.NewJSONPayload(map[string]any{"ok": true})
	if err != nil {
		t.Fatal(err)
	}
	commit := func() {
		if err := mgr.RunInTx(func(tx execution.ExecutionTx) error {
			for _, event := range []history.Event{
				{WorkflowUpdateCompleted: &history.WorkflowUpdateCompletedEvent{Name: "place", UpdateID: "up-1", Outcome: &protocol.UpdateOutcome{Success: &payload}}},
				history.NewWorkflowCompletedEvent(nil),
			} {
				if _, _, err := tx.AppendIdempotent(run.ID, run.ID, run.ID, event); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			t.Error(err)
		}
		server.runWatch.Notify(run.ID)
	}
	mgr.afterRead.Store(&commit)

	result, err := server.UpdateWorkflow(ctx, "", "", run.ID, "place", "up-1", protocol.UpdateWaitStageEnums.Completed, nil)
	if err != nil {
		t.Fatal(err)
	}
	if mgr.afterRead.Load() != nil {
		t.Fatal("the commit hook did not run")
	}
	if state, err := mgr.GetWorkflowExecutionState(run.ID); err != nil || !state.IsTerminal() {
		t.Fatalf("run state after the commit = %+v, err = %v, want terminal", state, err)
	}
	if result.Stage != protocol.UpdateReachedStageEnums.Completed || result.Outcome == nil || result.Outcome.Success == nil {
		t.Fatalf("update result = %+v, want the handler's success", result)
	}
	// 幂等重试与 UpdateWithStart 共用的查找同样从同一次读取里得到结果。
	rec, ok := server.lookupUpdate("", "", run.ID, "up-1")
	if !ok || rec.Outcome == nil || rec.Outcome.Success == nil {
		t.Fatalf("lookup after close = %+v (found %v), want the handler's success", rec, ok)
	}
}

// blockingQueryEvaluator 在求值时停住，直到 release 关闭。
type blockingQueryEvaluator struct {
	entered chan struct{}
	release chan struct{}
	calls   atomic.Int64
}

func (e *blockingQueryEvaluator) EvaluateQuery(context.Context, *program.WorkflowConfig, []history.Event, string, map[string]any) (map[string]any, bool, string) {
	e.calls.Add(1)
	e.entered <- struct{}{}
	<-e.release
	return map[string]any{"ok": true}, false, ""
}

// Server 侧重放 Query 受并发上限约束，超出的请求立即以 ResourceExhausted 失败；每个 Query 只读一次 History。
func TestQueryReplayIsLimitedAndReadsHistoryOnce(t *testing.T) {
	ctx := context.Background()
	mgr := &historyReadCountingManager{ExecutionManager: memory.NewExecutionManager()}
	evaluator := &blockingQueryEvaluator{entered: make(chan struct{}, 4), release: make(chan struct{})}
	deps := assembleServerDeps(mgr, newTestTaskQueue(), QueueActivityRouter{Queue: newTestTaskQueue()})
	deps.QueryEvaluator = evaluator
	srv := NewServer(deps, WithQueryConcurrency(1))
	t.Cleanup(srv.Stop)
	runID := startReconcileTestRun(t, srv, &program.WorkflowConfig{Type: "wf-query-limit", Program: programtest.Noop()})

	first := make(chan error, 1)
	go func() {
		_, err := srv.QueryWorkflow(ctx, "", "", runID, "state", nil)
		first <- err
	}()
	<-evaluator.entered

	// 名额被占满：第二个重放 Query 不排队，立即以 ResourceExhausted 失败，没有读 execution state 与 History，也没有重放。
	reads, stateReads := mgr.fullReads.Load(), mgr.stateReads.Load()
	if _, err := srv.QueryWorkflow(ctx, "", "", runID, "state", nil); !errors.Is(err, protocol.ErrResourceExhausted) {
		t.Fatalf("query over the limit returned %v, want resource exhausted", err)
	}
	if mgr.fullReads.Load() != reads || mgr.stateReads.Load() != stateReads || evaluator.calls.Load() != 1 {
		t.Fatalf("query over the limit read history %d times, state %d times and replayed %d times",
			mgr.fullReads.Load()-reads, mgr.stateReads.Load()-stateReads, evaluator.calls.Load()-1)
	}
	// 不重放的 __stack_trace 不占名额。
	if _, err := srv.QueryWorkflow(ctx, "", "", runID, stackTraceQueryType, nil); err != nil {
		t.Fatalf("stack trace query while replay slots are full: %v", err)
	}

	close(evaluator.release)
	if err := <-first; err != nil {
		t.Fatal(err)
	}
	reads = mgr.fullReads.Load()
	result, err := srv.QueryWorkflow(ctx, "", "", runID, "state", nil)
	<-evaluator.entered
	if err != nil || result.QueryRejected || result.QueryResult["ok"] != true {
		t.Fatalf("query after the slot freed = %+v, err = %v", result, err)
	}
	if n := mgr.fullReads.Load() - reads; n != 1 {
		t.Fatalf("query read the history %d times, want 1", n)
	}
}
