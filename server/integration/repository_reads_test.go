package integration

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/internal/executor"
	"github.com/donutnomad/tenuto/internal/worker"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	wfsdk "github.com/donutnomad/tenuto/sdk/activity"
	"github.com/donutnomad/tenuto/server/app/assembly"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

// ============================================
// 每个 Workflow 在事务之外读 Run 与 execution state 的次数（内存后端，不需要 MySQL）
// ============================================
// 负载与 TestStatementBudgetPerWorkflow 相同（3 个顺序 Activity 加一个 100ms Timer），逐个执行。MySQL 上每次读取是一条
// SELECT；这里统计 ExecutionManager 的事务外读取与事务次数，按入口（调用栈上最外层的 engine.(*Server) 导出方法）分组
// 写进测试日志：
//
//	go test ./server/integration -run TestRepositoryReadsPerWorkflow -count=1 -v
//
// 2026-11-12 实测每个 Workflow 读 Run 26 次（此前 39）、读 execution state 35 次（此前 38）：一次 Activity 运行时请求
// 只经 TaskRun 读一次 Run，Timer 与 Activity 超时复用取锁时读到的 Run，WFT 提交后派发 Activity 用事务里读到的 Run。
// 上界留出机器繁忙时偶发的超时与补偿读取。超出说明热路径又多读了，先看日志里的分组。
// ============================================

const (
	runReadsPerWorkflow   = 28
	stateReadsPerWorkflow = 37
)

type repositoryCall struct{ site, method string }

// repositoryCallCounter 记下每次事务外读取与事务的入口。
type repositoryCallCounter struct {
	mu      sync.Mutex
	counts  map[repositoryCall]int
	enabled atomic.Bool
}

func (c *repositoryCallCounter) add(method string) {
	if !c.enabled.Load() {
		return
	}
	site, harness := statementSite()
	if harness {
		return
	}
	c.mu.Lock()
	c.counts[repositoryCall{site, method}]++
	c.mu.Unlock()
}

func (c *repositoryCallCounter) total(method string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	total := 0
	for call, n := range c.counts {
		if call.method == method {
			total += n
		}
	}
	return total
}

// countingExecutions 包住内存 ExecutionManager，统计事务外的读取与事务次数。
type countingExecutions struct {
	execution.ExecutionManager
	calls *repositoryCallCounter
}

func (m countingExecutions) Get(runID string) (*execution.Run, error) {
	m.calls.add("Get")
	return m.ExecutionManager.Get(runID)
}

func (m countingExecutions) GetWorkflowExecutionState(runID string) (*execution.WorkflowExecutionState, error) {
	m.calls.add("GetWorkflowExecutionState")
	return m.ExecutionManager.GetWorkflowExecutionState(runID)
}

func (m countingExecutions) GetActivityState(runID string, scheduledEventID int64) (*execution.ActivityState, bool, error) {
	m.calls.add("GetActivityState")
	return m.ExecutionManager.GetActivityState(runID, scheduledEventID)
}

func (m countingExecutions) GetEventsRange(runID string, afterExclusive, throughInclusive int64) ([]history.Event, error) {
	m.calls.add("GetEventsRange")
	return m.ExecutionManager.GetEventsRange(runID, afterExclusive, throughInclusive)
}

func (m countingExecutions) AppendWorkflowTaskScheduled(runID, treeID, branchID string, event history.Event) (int64, bool, error) {
	m.calls.add("tx")
	return m.ExecutionManager.AppendWorkflowTaskScheduled(runID, treeID, branchID, event)
}

func (m countingExecutions) AssignStickyExecution(runID string, executionGeneration, expectedStickyGeneration int64, workerIdentity, workerBuildID string, expiresAt time.Time) (int64, bool, error) {
	m.calls.add("tx")
	return m.ExecutionManager.AssignStickyExecution(runID, executionGeneration, expectedStickyGeneration, workerIdentity, workerBuildID, expiresAt)
}

func (m countingExecutions) RunInTx(fn func(tx execution.ExecutionTx) error) error {
	m.calls.add("tx")
	return m.ExecutionManager.RunInTx(fn)
}

func TestRepositoryReadsPerWorkflow(t *testing.T) {
	calls := &repositoryCallCounter{counts: make(map[repositoryCall]int)}
	mem := memory.NewExecutionManager()
	server := assembly.NewServer(assembly.Options{
		Stores:         assembly.Stores{Executions: countingExecutions{mem, calls}, Configs: mem},
		WorkflowQueue:  memory.NewTaskQueue(64),
		ActivityRouter: memory.NewActivityQueueRouter(64),
	})
	defer server.Stop()
	registry := wfsdk.NewRegistry()
	registry.Register("budget.step", func(ctx context.Context, input m) (m, error) { return m{"ok": true}, nil })
	cache, err := executor.NewExecutionCache(executor.ExecutionCacheConfig{MaxEntries: 1000, MaxEstimatedBytes: 64 << 20, IdleTTL: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	w := assembly.NewWorker(server, registry, worker.WorkerOptions{
		WorkflowConcurrency: 8, ActivityConcurrency: 8, Identity: "budget-worker", ExecutionCache: cache,
	})
	w.Start(ctx)
	defer w.Stop()
	if _, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, budgetWorkflow(t)); err != nil {
		t.Fatal(err)
	}

	calls.enabled.Store(true)
	for i := range budgetWorkflows {
		runID, err := startedRunID(server.StartWorkflow(ctx, "", "budget-steps", fmt.Sprintf("budget-%d", i), "", nil))
		if err != nil {
			t.Fatal(err)
		}
		waitForServerRunStatus(t, server, runID, history.StatusCompleted, 30*time.Second)
	}
	calls.enabled.Store(false)

	calls.mu.Lock()
	keys := slices.SortedFunc(func(yield func(repositoryCall) bool) {
		for call := range calls.counts {
			if !yield(call) {
				return
			}
		}
	}, func(a, b repositoryCall) int { return strings.Compare(a.method+a.site, b.method+b.site) })
	var b strings.Builder
	b.WriteString("per workflow:")
	for _, call := range keys {
		fmt.Fprintf(&b, "\n  %6.2f  %-28s %s", float64(calls.counts[call])/budgetWorkflows, call.method, call.site)
	}
	calls.mu.Unlock()
	t.Log(b.String())

	if got := float64(calls.total("Get")) / budgetWorkflows; got > runReadsPerWorkflow {
		t.Errorf("run reads per workflow = %.2f, budget %d", got, runReadsPerWorkflow)
	}
	if got := float64(calls.total("GetWorkflowExecutionState")) / budgetWorkflows; got > stateReadsPerWorkflow {
		t.Errorf("execution state reads per workflow = %.2f, budget %d", got, stateReadsPerWorkflow)
	}
}
