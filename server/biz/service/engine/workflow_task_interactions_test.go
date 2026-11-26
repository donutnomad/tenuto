package engine

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/program/programtest"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/batch"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/biz/usecase/batches"
	"github.com/donutnomad/tenuto/server/infra/memory"
	"github.com/donutnomad/tenuto/server/infra/systemclock"
)

func registerNoopWorkflow(t *testing.T, server *Server, workflowType string) string {
	t.Helper()
	if _, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{
		Program: programtest.Noop(), Type: workflowType,
	}); err != nil {
		t.Fatal(err)
	}
	runID, err := startedRunID(server.StartWorkflow(context.Background(), "", workflowType, workflowType, "", nil))
	if err != nil {
		t.Fatal(err)
	}
	return runID
}

func openWorkflowTaskID(t *testing.T, mgr execution.ExecutionManager, runID string) int64 {
	t.Helper()
	state, err := mgr.GetWorkflowExecutionState(runID)
	if err != nil {
		t.Fatal(err)
	}
	return state.OpenWorkflowTaskEventID
}

// Blocked 的 Run 在退避期内收到的 Signal、Update 与取消请求只写入 History、记 ActivationPending，不调度 WFT；
// 周期补偿在退避期内也不调度。退避到期时由一次重试一并处理（对标 Temporal 的 transient WFT）。
func TestFactsDuringBlockedBackoffWaitForRetry(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	server, clk := newFakeClockServer(mgr, newTestTaskQueue(), newTestTaskQueue())
	defer server.Stop()
	const workflowType = "wf-blocked-backoff-facts"
	runID := registerNoopWorkflow(t, server, workflowType)

	open := openWorkflowTaskID(t, mgr, runID)
	if err := server.HandleWorkflowTaskFailed(server.WorkflowTaskContext(ctx, runID, open), runID, open,
		history.WorkflowTaskFailedCauseWorkflowPanic, "index out of range"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 30; i++ {
		if _, err := server.SignalWorkflow(ctx, "", workflowType, "", "poke", "", nil); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := server.UpdateWorkflow(ctx, "", workflowType, "", "approve", "update-1", protocol.UpdateWaitStageEnums.Accepted, nil); err != nil {
		t.Fatal(err)
	}
	if err := server.CancelWorkflow(ctx, "", workflowType, "", "stop"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := server.Reconcile(ctx, 0, time.Minute); err != nil {
		t.Fatal(err)
	}

	state, err := mgr.GetWorkflowExecutionState(runID)
	if err != nil {
		t.Fatal(err)
	}
	events := mustEvents(t, mgr, runID)
	if scheduled := countEngineEvents(events, history.EventWorkflowTaskScheduled); scheduled != 1 || state.OpenWorkflowTaskEventID != 0 {
		t.Fatalf("within the backoff: WorkflowTaskScheduled=%d open_wft=%d, want only the first task and no open task", scheduled, state.OpenWorkflowTaskEventID)
	}
	if !state.Blocked.Blocked || !state.WorkflowTask.ActivationPending {
		t.Fatalf("within the backoff: blocked=%v activation_pending=%v, want the facts pending on a blocked run", state.Blocked.Blocked, state.WorkflowTask.ActivationPending)
	}

	// 1s 退避到期：重试 timer 调度一个 WFT，它看到退避期内到达的全部事实。
	clk.Advance(time.Second)
	events = mustEvents(t, mgr, runID)
	if scheduled := countEngineEvents(events, history.EventWorkflowTaskScheduled); scheduled != 2 || openWorkflowTaskID(t, mgr, runID) == 0 {
		t.Fatalf("after the backoff: WorkflowTaskScheduled=%d, want one retry task", scheduled)
	}
	if last := events[len(events)-1]; last.WorkflowTaskScheduled == nil {
		t.Fatalf("history ends with %s, want the retry WorkflowTaskScheduled after every fact", last.EventType())
	}
}

// 启动恢复遇到退避期内收到事实的 Blocked Run：不提前调度，重挂退避 timer，到期后调度。
func TestRecoveryOfBlockedRunWithPendingFactsKeepsBackoff(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	server, clk := newFakeClockServer(mgr, newTestTaskQueue(), newTestTaskQueue())
	const workflowType = "wf-blocked-backoff-recovery"
	runID := registerNoopWorkflow(t, server, workflowType)
	open := openWorkflowTaskID(t, mgr, runID)
	if err := server.HandleWorkflowTaskFailed(server.WorkflowTaskContext(ctx, runID, open), runID, open,
		history.WorkflowTaskFailedCauseWorkflowPanic, "boom"); err != nil {
		t.Fatal(err)
	}
	if _, err := server.SignalWorkflow(ctx, "", workflowType, "", "poke", "", nil); err != nil {
		t.Fatal(err)
	}
	server.Stop()

	restarted := assembleServerWithClock(mgr, newTestTaskQueue(), newTestTaskQueue(), clk)
	defer restarted.Stop()
	if err := restarted.RecoverRunningWorkflows(ctx, time.Minute); err != nil {
		t.Fatal(err)
	}
	if open := openWorkflowTaskID(t, mgr, runID); open != 0 {
		t.Fatalf("startup recovery scheduled workflow task %d inside the backoff", open)
	}
	clk.Advance(time.Second)
	if openWorkflowTaskID(t, mgr, runID) == 0 {
		t.Fatal("no workflow task after the backoff re-armed by startup recovery elapsed")
	}
}

// WorkflowTaskTimedOut 提交之后、下一个 WFT 调度之前进程退出：超时让 ActivationPending 保持为 true，
// 启动恢复与周期补偿都按 NeedsWorkflowTask 补上调度，WFT 进行期间到达的 Signal 得到处理。
func TestWorkflowTaskTimeoutCrashWindowIsRecovered(t *testing.T) {
	for name, recover := range map[string]func(ctx context.Context, server *Server) error{
		"startup recovery": func(ctx context.Context, server *Server) error {
			return server.RecoverRunningWorkflows(ctx, time.Minute)
		},
		"periodic reconcile": func(ctx context.Context, server *Server) error {
			_, _, err := server.Reconcile(ctx, 30*time.Second, time.Minute)
			return err
		},
	} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			mgr := memory.NewExecutionManager()
			server, clk := newFakeClockServer(mgr, newTestTaskQueue(), newTestTaskQueue())
			const workflowType = "wf-timeout-crash-window"
			runID := registerNoopWorkflow(t, server, workflowType)
			open := openWorkflowTaskID(t, mgr, runID)
			_ = server.WorkflowTaskContext(ctx, runID, open) // Worker 领取后崩溃
			if _, err := server.SignalWorkflow(ctx, "", workflowType, "", "order-paid", "", nil); err != nil {
				t.Fatal(err)
			}
			server.Stop()

			// timeoutWorkflowTask 的事务提交，紧接着的调度因进程退出没有执行。
			clk.Advance(protocol.DefaultWorkflowTaskTimeout + time.Second)
			run, err := mgr.Get(runID)
			if err != nil {
				t.Fatal(err)
			}
			treeID, branchID, err := runBranchInfoFromRun(run)
			if err != nil {
				t.Fatal(err)
			}
			if err := mgr.RunInTx(func(tx execution.ExecutionTx) error {
				_, err := execution.TimeoutWorkflowTask(tx, execution.WorkflowTaskTimeout{
					RunID: runID, TreeID: treeID, BranchID: branchID, ScheduledEventID: open,
					Message: workflowTaskTimeoutMessage, Now: clk.Now(),
				})
				return err
			}); err != nil {
				t.Fatal(err)
			}
			state, err := mgr.GetWorkflowExecutionState(runID)
			if err != nil || !state.NeedsWorkflowTask() {
				t.Fatalf("after the timeout: needs_workflow_task=%v err=%v, want the run to need a workflow task", state.NeedsWorkflowTask(), err)
			}

			restarted := assembleServerWithClock(mgr, newTestTaskQueue(), newTestTaskQueue(), clk)
			defer restarted.Stop()
			if err := recover(ctx, restarted); err != nil {
				t.Fatal(err)
			}
			if next := openWorkflowTaskID(t, mgr, runID); next == 0 || next == open {
				t.Fatalf("open workflow task after %s = %d, want a new task after timed-out task %d", name, next, open)
			}
		})
	}
}

// 走全量路径完成的 WFT 清除 sticky 绑定（对标 Temporal 完成请求不带 StickyAttributes 时的 ClearStickyTaskQueue）：
// 之后的 WFT 不再先投给上一个增量 Worker。
func TestFullHistoryCompletionClearsStickyBinding(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	server, clk := newFakeClockServer(mgr, newTestTaskQueue(), newTestTaskQueue())
	defer server.Stop()
	const workflowType = "wf-sticky-full-completion"
	runID := registerNoopWorkflow(t, server, workflowType)
	completeFull := func(worker string) {
		t.Helper()
		open := openWorkflowTaskID(t, mgr, runID)
		if open == 0 {
			t.Fatal("no open workflow task")
		}
		if err := server.HandleWorkflowTaskCompleted(server.WorkflowTaskContext(ctx, runID, open), runID, open, nil, worker); err != nil {
			t.Fatal(err)
		}
	}
	completeFull("worker-1")
	state, err := mgr.GetWorkflowExecutionState(runID)
	if err != nil {
		t.Fatal(err)
	}
	// worker-1 经增量路径完成时回执分配的 sticky。
	if _, ok, err := mgr.AssignStickyExecution(runID, state.ExecutionGeneration, state.StickyGeneration, "worker-1", "build-1"); err != nil || !ok {
		t.Fatalf("assign sticky: ok=%v err=%v", ok, err)
	}
	clk.Advance(10 * time.Minute)
	if _, err := server.SignalWorkflow(ctx, "", workflowType, "", "tick", "", nil); err != nil {
		t.Fatal(err)
	}
	completeFull("worker-2")
	if _, err := server.SignalWorkflow(ctx, "", workflowType, "", "tick", "", nil); err != nil {
		t.Fatal(err)
	}
	state, err = mgr.GetWorkflowExecutionState(runID)
	if err != nil {
		t.Fatal(err)
	}
	if state.StickyWorkerIdentity != "" || state.StickyExpiresAt != nil {
		t.Fatalf("after a full-history completion by worker-2: sticky=%q expires=%v, want the binding cleared", state.StickyWorkerIdentity, state.StickyExpiresAt)
	}
}

// 同一个 RequestID 的 Reset 只生效一次：重复的请求返回第一次 reset 出的 Run；不同的 RequestID 照常 reset。
func TestResetRequestIDReturnsFirstResult(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	server := assembleServer(mgr, newTestTaskQueue(), newTestTaskQueue())
	defer server.Stop()
	runID := batchResetRun(t, ctx, server, mgr, "wf-reset-request-id")
	reset := func(requestID string) string {
		t.Helper()
		newRunID, err := server.ResetWorkflow(ctx, execution.ResetWorkflowRequest{
			RunID: runID, ResetType: execution.ResetTypeEnums.LastWorkflowTask, Reason: "fix", RequestID: requestID,
		})
		if err != nil {
			t.Fatal(err)
		}
		return newRunID
	}
	first := reset("request-1")
	if again := reset("request-1"); again != first {
		t.Fatalf("repeated reset request returned %s, want the first reset run %s", again, first)
	}
	if status, err := server.GetRunStatus(first); err != nil || status != history.StatusRunning {
		t.Fatalf("first reset run status=%s err=%v, want Running (a repeated request must not terminate it)", status, err)
	}
	if other := reset("request-2"); other == first {
		t.Fatal("a reset with another request id returned the first reset run")
	}
}

// RequestID 去重只看 reset 出的 Run 自己 branch 上的 reset 标记：A 以 K reset 出 B，B 在它的 reset 标记之后以 K2
// reset 出 C（C 的祖先前缀里带着 B 的 K 标记），再以 K reset B 时照常 reset，不返回 C。
func TestResetRequestIDIgnoresInheritedResetMarker(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	server := assembleServer(mgr, newTestTaskQueue(), newTestTaskQueue())
	defer server.Stop()
	runA := batchResetRun(t, ctx, server, mgr, "wf-reset-request-id-inherited")
	reset := func(runID, requestID string) string {
		t.Helper()
		newRunID, err := server.ResetWorkflow(ctx, execution.ResetWorkflowRequest{
			RunID: runID, ResetType: execution.ResetTypeEnums.LastWorkflowTask, Reason: "fix", RequestID: requestID,
		})
		if err != nil {
			t.Fatal(err)
		}
		return newRunID
	}
	runB := reset(runA, "K")
	// B 的首个 WFT 完成：B 的最后一个 Workflow Task 边界在它的 reset 标记之后。
	open := openWorkflowTaskID(t, mgr, runB)
	if err := server.HandleWorkflowTaskCompleted(server.WorkflowTaskContext(ctx, runB, open), runB, open, nil, "worker-1"); err != nil {
		t.Fatal(err)
	}
	runC := reset(runB, "K2")
	inherited, err := mgr.GetEventsByDedupeKeys(runC, []string{execution.ResetRequestDedupeKey("K")})
	if err != nil || len(inherited) != 1 {
		t.Fatalf("C's visible history holds %d K reset markers (err %v), want B's marker inherited through the fork", len(inherited), err)
	}

	runD := reset(runB, "K")
	if runD == runC || runD == runB {
		t.Fatalf("reset of B with K returned %s (B=%s, C=%s): the K marker C inherited from B was taken as C's own request", runD, runB, runC)
	}
	if again := reset(runA, "K"); again != runB {
		t.Fatalf("repeated reset of A with K returned %s, want B %s", again, runB)
	}
}

// barrierRuns 让两个驱动都先读完同一页、再各自执行动作：模拟租约切换瞬间的两个 leader 同时处理同一页。
type barrierRuns struct {
	execution.ExecutionManager
	listed  atomic.Int32
	arrived sync.WaitGroup
}

func (b *barrierRuns) List(filter execution.StoreListFilter) (execution.StoreListResult, error) {
	page, err := b.ExecutionManager.List(filter)
	if len(page.Runs) > 0 && b.listed.Add(1) <= 2 {
		b.arrived.Done()
		b.arrived.Wait()
	}
	return page, err
}

// 两个重叠的批量驱动处理同一页：批量 Reset 用 (batch ID, run ID) 的确定性 RequestID，同一个 Run 只 reset 一次。
func TestBatchResetByOverlappingDriversResetsOnce(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	server := assembleServer(mgr, newTestTaskQueue(), newTestTaskQueue())
	defer server.Stop()
	const workflowID = "wf-batch-reset-overlap"
	runID := batchResetRun(t, ctx, server, mgr, workflowID)

	repo := memory.NewBatchRepository()
	runs := &barrierRuns{ExecutionManager: mgr}
	runs.arrived.Add(2)
	driverA := batches.NewUseCase(repo, runs, server, systemclock.Clock{}, slog.Default())
	driverB := batches.NewUseCase(repo, runs, server, systemclock.Clock{}, slog.Default())
	if _, err := driverA.Start(batches.StartRequest{
		Namespace: protocol.DefaultNamespace, Type: batch.TypeEnums.Reset,
		Filter:    execution.RunListFilter{RunIDs: []string{runID}, IncludeTerminal: true},
		ResetType: execution.ResetTypeEnums.LastWorkflowTask, Reason: "overlap",
	}); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for _, driver := range []*batches.UseCase{driverA, driverB} {
		wg.Go(func() {
			for range 3 {
				driver.ProcessActive(ctx)
			}
		})
	}
	wg.Wait()

	listed, err := mgr.List(execution.StoreListFilter{Namespace: protocol.DefaultNamespace, WorkflowID: workflowID, IncludeTerminal: true, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	var resetRuns []*execution.Run
	for _, run := range listed.Runs {
		if run.ID != runID {
			resetRuns = append(resetRuns, run)
		}
	}
	if len(resetRuns) != 1 || resetRuns[0].Status != history.StatusRunning {
		for _, run := range resetRuns {
			t.Logf("reset run %s status=%s", run.ID, run.Status)
		}
		t.Fatalf("overlapping drivers produced %d reset runs, want exactly one running reset run", len(resetRuns))
	}
}
