package engine

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	programpkg "github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/program/programtest"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

// failingEventManager 在内存执行存储外加一层故障注入：failOn 指定的事件类型写入时返回错误，模拟数据库写失败；
// 事务内命中时整个事务回滚。未指定类型时调用原样转给内存存储。
type failingEventManager struct {
	execution.ExecutionManager
	failType atomic.Value // history.EventType
}

var errAppendEventFailed = errors.New("append event failed")

func newFailingEventManager() *failingEventManager {
	return &failingEventManager{ExecutionManager: memory.NewExecutionManager()}
}

// failOn 让之后写入 eventType 事件的追加失败；传空字符串恢复正常写入。
func (m *failingEventManager) failOn(eventType history.EventType) {
	m.failType.Store(eventType)
}

func (m *failingEventManager) fails(events ...history.Event) bool {
	failType, _ := m.failType.Load().(history.EventType)
	if failType == "" {
		return false
	}
	for _, event := range events {
		if event.EventType() == failType {
			return true
		}
	}
	return false
}

func (m *failingEventManager) Append(runID, treeID, branchID string, event history.Event) (int64, error) {
	if m.fails(event) {
		return 0, errAppendEventFailed
	}
	return m.ExecutionManager.Append(runID, treeID, branchID, event)
}

func (m *failingEventManager) AppendIdempotent(runID, treeID, branchID string, event history.Event) (int64, bool, error) {
	if m.fails(event) {
		return 0, false, errAppendEventFailed
	}
	return m.ExecutionManager.AppendIdempotent(runID, treeID, branchID, event)
}

func (m *failingEventManager) RunInTx(fn func(tx execution.ExecutionTx) error) error {
	return m.ExecutionManager.RunInTx(func(tx execution.ExecutionTx) error {
		return fn(&failingEventTx{ExecutionTx: tx, m: m})
	})
}

type failingEventTx struct {
	execution.ExecutionTx
	m *failingEventManager
}

func (t *failingEventTx) Append(runID, treeID, branchID string, event history.Event) (int64, error) {
	if t.m.fails(event) {
		return 0, errAppendEventFailed
	}
	return t.ExecutionTx.Append(runID, treeID, branchID, event)
}

func (t *failingEventTx) AppendIdempotent(runID, treeID, branchID string, event history.Event) (int64, bool, error) {
	if t.m.fails(event) {
		return 0, false, errAppendEventFailed
	}
	return t.ExecutionTx.AppendIdempotent(runID, treeID, branchID, event)
}

func (t *failingEventTx) AppendWorkflowTaskClosed(runID, treeID, branchID string, event history.Event) (int64, bool, error) {
	if t.m.fails(event) {
		return 0, false, errAppendEventFailed
	}
	return t.ExecutionTx.AppendWorkflowTaskClosed(runID, treeID, branchID, event)
}

func (t *failingEventTx) AppendWorkflowTaskCompleted(runID, treeID, branchID string, closeEvent history.Event, commandEvents []history.Event) (execution.WorkflowTaskCompletionResult, error) {
	if t.m.fails(append([]history.Event{closeEvent}, commandEvents...)...) {
		return execution.WorkflowTaskCompletionResult{}, errAppendEventFailed
	}
	return t.ExecutionTx.AppendWorkflowTaskCompleted(runID, treeID, branchID, closeEvent, commandEvents)
}

// newServerForTest 用内存执行存储构造测试 Server。
func newServerForTest(tb testing.TB) (*Server, *memory.ExecutionManager) {
	mgr := memory.NewExecutionManager()
	srv := assembleServer(mgr, newTestTaskQueue(), newTestTaskQueue())
	tb.Cleanup(srv.Stop)
	return srv, mgr
}

// newFailingServerForTest 用带故障注入的内存执行存储构造测试 Server。
func newFailingServerForTest(tb testing.TB) (*Server, *failingEventManager) {
	mgr := newFailingEventManager()
	srv := assembleServer(mgr, newTestTaskQueue(), newTestTaskQueue())
	tb.Cleanup(srv.Stop)
	return srv, mgr
}

// seedScheduledActivityState 写入一条 Scheduled 状态的 ActivityState（Attempt=1/Stamp=1），
// 供"手动注入事件 + 直接调 start/heartbeat"的旧测试在 mutable-state 架构下读取。
// retry 为 nil 表示无重试（非 transient，start 会写真实 ActivityStarted 进 history）。
func seedScheduledActivityState(tb testing.TB, mgr execution.ExecutionManager, runID string, scheduledEventID int64, stepID string, retry *history.RetryPolicy) {
	tb.Helper()
	if err := mgr.UpsertActivityState(&execution.ActivityState{
		RunID:            runID,
		ScheduledEventID: scheduledEventID,
		ActivityID:       stepID,
		Attempt:          1,
		Stamp:            1,
		StartedEventID:   execution.EmptyEventID,
		Status:           execution.ActivityStatusEnums.Scheduled,
		RetryPolicy:      retry,
	}); err != nil {
		tb.Fatal(err)
	}
}

// seedStartedActivityStateWithRetry 写入一条 Started 状态的 ActivityState（Attempt=1/Stamp=1/真实 StartedEventID），
// 供"手动注入 ActivityScheduled+ActivityStarted 事件 + 直接 fail"的旧测试在 mutable-state 架构下读取并触发重试。
func seedStartedActivityStateWithRetry(tb testing.TB, mgr execution.ExecutionManager, runID string, scheduledEventID int64, stepID string, retry *history.RetryPolicy) {
	tb.Helper()
	if err := mgr.UpsertActivityState(&execution.ActivityState{
		RunID:            runID,
		ScheduledEventID: scheduledEventID,
		ActivityID:       stepID,
		Attempt:          1,
		Stamp:            1,
		StartedEventID:   scheduledEventID, // 已写 history 的真实 started（非 transient）
		Status:           execution.ActivityStatusEnums.Started,
		RetryPolicy:      retry,
	}); err != nil {
		tb.Fatal(err)
	}
}

// computeBlockedInfo 用 execution state 的折叠计算一段 History 的 blocked 描述。
func computeBlockedInfo(events []history.Event) execution.BlockedInfo {
	state, err := execution.BuildWorkflowExecutionState(&execution.Run{ID: "run", Status: history.StatusRunning}, events)
	if err != nil {
		panic(err)
	}
	return state.BlockedInfo()
}

// workerAPIExecutionState 为 Worker 测试替身提供 execution state：状态取替身的 GetRunStatus，其余由替身的事件折叠得到。
func workerAPIExecutionState(api interface {
	GetRunStatus(string) (history.Status, error)
	GetEvents(string) ([]history.Event, error)
}, runID string) (*execution.WorkflowExecutionState, error) {
	status, err := api.GetRunStatus(runID)
	if err != nil {
		return nil, err
	}
	events, err := api.GetEvents(runID)
	if err != nil {
		return nil, err
	}
	state, err := execution.BuildWorkflowExecutionState(&execution.Run{ID: runID, Status: status}, events)
	if err != nil {
		return nil, err
	}
	state.Status = status
	return state, nil
}

// workflowRunsForTest 列出 workflowID 下的全部 Run，包括已结束的。
func workflowRunsForTest(t testing.TB, mgr execution.ExecutionManager, workflowID string) []*execution.Run {
	t.Helper()
	result, err := mgr.List(execution.StoreListFilter{WorkflowID: workflowID, IncludeTerminal: true, Limit: 1000})
	if err != nil {
		t.Fatal(err)
	}
	return result.Runs
}

// runCountForTest 返回存储里 Run 的总数，包括已结束的。
func runCountForTest(t testing.TB, mgr execution.ExecutionManager) int {
	t.Helper()
	result, err := mgr.List(execution.StoreListFilter{IncludeTerminal: true})
	if err != nil {
		t.Fatal(err)
	}
	return result.Total
}

// runsByStatus 列出处于 status 的 Run。
func runsByStatus(store interface {
	List(execution.StoreListFilter) (execution.StoreListResult, error)
}, status history.Status) ([]*execution.Run, error) {
	result, err := store.List(execution.StoreListFilter{Statuses: []history.Status{status}, IncludeTerminal: true, Limit: 1000})
	return result.Runs, err
}

// HandleActivityStarted 按 run 与 scheduledEventID 登记一次 Activity 开始，测试用它代替 Worker 的 Begin 请求。
func (s *Server) HandleActivityStarted(runID string, scheduledEventID int64, attempt int, stamp int64, startToCloseTimeout, heartbeatTimeout time.Duration) error {
	_, err := s.HandleActivityStartedWithMetadata(protocol.Task{Namespace: s.runNamespace(runID), RunID: runID, ScheduledEventID: scheduledEventID, Attempt: attempt, Stamp: stamp}, startToCloseTimeout, heartbeatTimeout)
	return err
}

// RecoverRun 对单个 run 做一次启动恢复级别的补偿。
func (s *Server) RecoverRun(ctx context.Context, runID string) error {
	_, err := s.reconcileRun(ctx, runID, true)
	return err
}

// WorkflowTaskContext 代替 Worker 的 poll：给 run 当前打开的 Workflow Task 补记 WorkflowTaskStarted，
// 返回携带它的 token 的 ctx，测试用这个 ctx 直接提交 Workflow Task。run 或 execution state 读不到时原样返回 ctx。
func (s *Server) WorkflowTaskContext(ctx context.Context, runID string, scheduledEventID int64) context.Context {
	state, err := s.mgr.GetWorkflowExecutionState(runID)
	if err != nil || state == nil {
		return ctx
	}
	if state.WorkflowTaskOpen(scheduledEventID) && state.WorkflowTask.StartedScheduledEventID != scheduledEventID {
		run, err := s.mgr.Get(runID)
		if err != nil {
			return ctx
		}
		treeID, branchID, err := s.runBranchInfo(runID)
		if err != nil {
			return ctx
		}
		if err := s.mgr.RunInTx(func(tx execution.ExecutionTx) error {
			_, err := execution.StartWorkflowTask(tx, execution.WorkflowTaskStart{
				RunID: runID, TreeID: treeID, BranchID: branchID, ScheduledEventID: scheduledEventID,
				ExecutionGeneration: state.ExecutionGeneration, StickyGeneration: state.StickyGeneration,
				ProgramVersion: run.WorkflowConfigVersion, Now: s.clock.Now(),
			})
			return err
		}); err != nil {
			return ctx
		}
	}
	token, err := s.tokens.Encode(protocol.Task{
		Type: protocol.TaskTypeWorkflow, Namespace: s.runNamespace(runID), RunID: runID, ScheduledEventID: scheduledEventID,
		ExecutionGeneration: state.ExecutionGeneration, StickyGeneration: state.StickyGeneration,
	})
	if err != nil {
		return ctx
	}
	return protocol.WithWorkflowTaskToken(ctx, token)
}

// completeOpenWorkflowTask 按 Worker 的顺序给 run 追加一个完成的 Workflow Task：没有打开的任务时先追加
// WorkflowTaskScheduled，再追加 WorkflowTaskStarted 与 WorkflowTaskCompleted，返回 Completed 的事件 ID。
func completeOpenWorkflowTask(t testing.TB, mgr execution.ExecutionManager, runID string) int64 {
	t.Helper()
	appendEvent := func(event history.Event) int64 {
		id, err := mgr.Append(runID, runID, runID, event)
		if err != nil {
			t.Fatal(err)
		}
		return id
	}
	events, err := mgr.GetEvents(runID)
	if err != nil {
		t.Fatal(err)
	}
	scheduled := openWorkflowTaskScheduledIDInEvents(events)
	if scheduled == 0 {
		scheduled = appendEvent(history.NewWorkflowTaskScheduledEvent(0))
	}
	started := false
	for _, event := range events {
		if event.WorkflowTaskStarted != nil && event.WorkflowTaskStarted.ScheduledEventID == scheduled {
			started = true
		}
	}
	if !started {
		appendEvent(history.NewWorkflowTaskStartedEvent(scheduled, "", "", 0))
	}
	return appendEvent(history.NewWorkflowTaskCompletedEvent(scheduled, ""))
}

// OpenWorkflowTaskID 返回 run 当前打开的 Workflow Task 的 scheduled 事件 ID，没有时为 0。
func (s *Server) OpenWorkflowTaskID(runID string) int64 {
	state, err := s.mgr.GetWorkflowExecutionState(runID)
	if err != nil || state == nil {
		return 0
	}
	return state.CurrentWorkflowTaskScheduledEventID()
}

// createStartedRunForTest 建 Run 并写入它的 WorkflowStarted：Run 与 Started 在生产路径上同事务落库。
func createStartedRunForTest(t testing.TB, mgr execution.ExecutionManager, run *execution.Run) {
	t.Helper()
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(run)) }); err != nil {
		t.Fatal(err)
	}
	started := history.NewWorkflowStartedEvent(run.Input, run.FirstRunID, run.ContinuedRunID, run.WorkflowID, run.WorkflowType, 0)
	started.WorkflowStarted.SearchAttributes = run.SearchAttributes
	if _, err := mgr.Append(run.ID, run.ID, run.ID, started); err != nil {
		t.Fatal(err)
	}
}

// registerRunWorkflowForTest 登记 run 的 Workflow 类型的一份空配置，并让 run 使用这一版。
func registerRunWorkflowForTest(t testing.TB, server *Server, run *execution.Run) {
	t.Helper()
	ref, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &programpkg.WorkflowConfig{ID: run.WorkflowType, Program: programtest.Noop()})
	if err != nil {
		t.Fatal(err)
	}
	run.WorkflowConfigVersion = ref.Version
}

// WithRootBranch 给没有 branch token 的测试 Run 补上 root branch token（tree 与 branch 都取 Run ID），
// 与 execution.NewRun 生成的 token 一致。已有 token 的 Run 原样返回。
func WithRootBranch(run *execution.Run) *execution.Run {
	if len(run.BranchToken) == 0 {
		run.BranchToken, _ = execution.SerializeBranchToken(&execution.HistoryBranch{TreeID: run.ID, BranchID: run.ID})
	}
	return run
}
