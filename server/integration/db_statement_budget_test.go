package integration

import (
	"context"
	"fmt"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/compiler/sdkgen"
	"github.com/donutnomad/tenuto/internal/executor"
	"github.com/donutnomad/tenuto/internal/worker"
	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/protocol"
	historypkg "github.com/donutnomad/tenuto/protocol/history"
	wfsdk "github.com/donutnomad/tenuto/sdk/activity"
	"github.com/donutnomad/tenuto/server/app/assembly"
	"github.com/donutnomad/tenuto/server/infra/persistence"
	"github.com/donutnomad/tenuto/server/infra/persistence/mysqltest"
	"github.com/donutnomad/tenuto/server/infra/taskqueue/taskq"
	"gorm.io/gorm"
)

// ============================================
// 每个 Workflow 的 SQL 语句数（MySQL + 真实 TaskQ，TENUTO_TEST_MYSQL=1）
// ============================================
// 负载与 tenuto-cluster bench 相同：3 个顺序的零耗时 Activity 加一个 100ms Timer，闭环并发 5，单副本内嵌 Workflow
// Worker 与 Activity Worker。GORM 回调统计 Workflow 库与 TaskQ 库上的每条语句（不含 BEGIN/COMMIT），按操作、表与
// 发起语句的入口（调用栈上最外层的 engine.(*Server) 导出方法或 TaskQ 组件）分组写进测试日志；测试自己轮询 Run
// 状态的语句单独计数、不计入预算。之后空闲 3 秒，统计空闲时每秒的语句数。
//
//	TENUTO_TEST_MYSQL=1 go test ./server/integration -run TestStatementBudgetPerWorkflow -count=1 -v
//
// 预算是回归护栏：超出说明热路径又多读或多写了，先看日志里的分组找到新增的入口。
// ============================================

const (
	budgetWorkflows   = 20
	budgetConcurrency = 5
	budgetIdle        = 3 * time.Second
	// statementBudgetPerWorkflow 是按代码推算的值加约 15% 余量。2026-11-12 实测 345.1 条（SELECT 213.0、INSERT 58.0、
	// UPDATE 73.4、DELETE 0.7，含运行期间的后台心跳）；之后每个事务只加锁读、写回一次 execution state，Activity 运行时
	// 请求只读一次 Run，已加锁读出的 Activity 与 Timer state 只 UPDATE 不再先 INSERT，推算少 86 条（SELECT 65、
	// UPDATE 14、INSERT 7），约 259 条。
	statementBudgetPerWorkflow = 300
	// budgetIdlePartitions 是测试里载入的 TaskQ 分区：workflow-tasks 与 activity-tasks 各 4 个，Activity 路由的缺省
	// 物理队列按全局缺省 4 个。
	budgetIdlePartitions = 12
	// idleStatementBudgetPerSecond 是空闲时每秒的语句数上界。每个分区：分区锁续约 1 条/秒（UPDATE，锁 TTL 3s、心跳 1s，
	// 决定易主速度）、锁成员行刷新 1/5 条/秒（INSERT ... ON DUPLICATE KEY UPDATE）与清理 1/15 条/秒（DELETE）、GC 的
	// ack level CAS 2/5 条/秒（UPDATE，未推进时再 SELECT 一次）；Server 侧（sticky 期限扫描、Worker 登记心跳等）约
	// 3 条/秒。12 个分区合计约 23 条/秒，加约 15% 余量。修改前实测 41–45 条/秒，其中锁成员行的 INSERT 与 DELETE 各约
	// 12 条/秒。
	idleStatementBudgetPerSecond = budgetIdlePartitions*(1+1.0/5+1.0/15+2.0/5)*1.15 + 3
)

type statementKey struct {
	op, table, site string
}

// statementCounter 记下每条语句的操作、表与发起入口。
type statementCounter struct {
	mu      sync.Mutex
	counts  map[statementKey]int
	harness atomic.Int64
	enabled atomic.Bool
}

func newStatementCounter() *statementCounter {
	return &statementCounter{counts: make(map[statementKey]int)}
}

func (c *statementCounter) attach(t *testing.T, name string, db *gorm.DB) {
	t.Helper()
	record := func(op string) func(*gorm.DB) {
		return func(tx *gorm.DB) {
			if !c.enabled.Load() {
				return
			}
			site, harness := statementSite()
			if harness {
				c.harness.Add(1)
				return
			}
			table := tx.Statement.Table
			if op == "raw" {
				op, table = rawStatementShape(tx.Statement.SQL.String())
			}
			c.mu.Lock()
			c.counts[statementKey{op: op, table: table, site: site}]++
			c.mu.Unlock()
		}
	}
	callbacks := db.Callback()
	register := []error{
		callbacks.Create().After("gorm:create").Register("budget:"+name+":create", record("INSERT")),
		callbacks.Query().After("gorm:query").Register("budget:"+name+":query", record("SELECT")),
		callbacks.Update().After("gorm:update").Register("budget:"+name+":update", record("UPDATE")),
		callbacks.Delete().After("gorm:delete").Register("budget:"+name+":delete", record("DELETE")),
		callbacks.Row().After("gorm:row").Register("budget:"+name+":row", record("raw")),
		callbacks.Raw().After("gorm:raw").Register("budget:"+name+":raw", record("raw")),
	}
	for _, err := range register {
		if err != nil {
			t.Fatal(err)
		}
	}
}

// rawStatementShape 从 Raw/Exec/Row 的 SQL 里取出操作与表名。
func rawStatementShape(sql string) (string, string) {
	fields := strings.Fields(sql)
	if len(fields) == 0 {
		return "OTHER", ""
	}
	op := strings.ToUpper(fields[0])
	for i, field := range fields {
		upper := strings.ToUpper(field)
		if (upper == "FROM" || upper == "INTO" || upper == "UPDATE") && i+1 < len(fields) {
			return op, strings.Trim(fields[i+1], "`")
		}
	}
	return op, ""
}

// statementSite 返回发起语句的入口：调用栈上最外层的 engine.(*Server) 导出方法；没有时取最外层的 TaskQ 组件函数。
// harness 为 true 表示语句来自测试自己的轮询。
func statementSite() (site string, harness bool) {
	pcs := make([]uintptr, 64)
	n := runtime.Callers(3, pcs)
	frames := runtime.CallersFrames(pcs[:n])
	var engineSite, taskqSite string
	for {
		frame, more := frames.Next()
		name := frame.Function
		switch {
		case strings.Contains(name, "/server/integration.waitForServerRunStatus"):
			return "", true
		case strings.Contains(name, "/server/biz/service/engine.(*Server)."):
			method := name[strings.LastIndex(name, ".")+1:]
			if method != "" && method[0] >= 'A' && method[0] <= 'Z' {
				engineSite = "engine." + method
			}
		case strings.Contains(name, "donutnomad/taskq/"):
			taskqSite = "taskq." + name[strings.LastIndex(name, "/")+1:]
		}
		if !more {
			break
		}
	}
	switch {
	case engineSite != "":
		return engineSite, false
	case taskqSite != "":
		return taskqSite, false
	default:
		return "other", false
	}
}

// reset 清零并返回之前的计数。
func (c *statementCounter) reset() (map[statementKey]int, int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	counts := c.counts
	c.counts = make(map[statementKey]int)
	return counts, c.harness.Swap(0)
}

func totalStatements(counts map[statementKey]int) int {
	total := 0
	for _, n := range counts {
		total += n
	}
	return total
}

// logStatementBreakdown 按操作汇总，再按 (入口, 操作, 表) 列出，降序。
func logStatementBreakdown(t *testing.T, title string, counts map[statementKey]int, divisor float64) {
	t.Helper()
	byOp := make(map[string]int)
	for key, n := range counts {
		byOp[key.op] += n
	}
	var b strings.Builder
	fmt.Fprintf(&b, "%s: total %.1f", title, float64(totalStatements(counts))/divisor)
	for _, op := range []string{"SELECT", "INSERT", "UPDATE", "DELETE"} {
		fmt.Fprintf(&b, " %s %.1f", op, float64(byOp[op])/divisor)
	}
	keys := make([]statementKey, 0, len(counts))
	for key := range counts {
		keys = append(keys, key)
	}
	slices.SortFunc(keys, func(a, b statementKey) int { return counts[b] - counts[a] })
	for _, key := range keys {
		fmt.Fprintf(&b, "\n  %7.2f  %-45s %-6s %s", float64(counts[key])/divisor, key.site, key.op, key.table)
	}
	t.Log(b.String())
}

func budgetWorkflow(t *testing.T) *program.WorkflowConfig {
	t.Helper()
	source := `package flows

import (
	"time"

	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/activities"
)

func W(ctx workflow.Context, input map[string]any) error {
	for i := 0; i < 3; i++ {
		var out map[string]any
		if err := workflow.ExecuteActivity(ctx, activities.Step, input, workflow.ActivityOptions{StartToCloseTimeout: 10 * time.Second}).Get(ctx, &out); err != nil {
			return err
		}
	}
	return workflow.Sleep(ctx, 100*time.Millisecond)
}
`
	generated, err := sdkgen.GenerateFromSource([]byte(source), sdkgen.Options{WorkflowID: "budget-steps", FuncName: "W", Filename: "w.go"})
	if err != nil {
		t.Fatal(err)
	}
	ref := generated.Workflow.Activities["activities.Step"]
	ref.Type = "budget.step"
	generated.Workflow.Activities["activities.Step"] = ref
	return &program.WorkflowConfig{ID: "budget-steps", Program: generated.Workflow.Program, Activities: generated.Workflow.Activities}
}

func TestStatementBudgetPerWorkflow(t *testing.T) {
	counter := newStatementCounter()
	h := newTaskQHarness(t, map[string]int{"workflow-tasks": 4, "activity-tasks": 4})
	defer h.close()
	counter.attach(t, "taskq", h.store.DB())
	workflowDB := mysqltest.NewDB(t)
	counter.attach(t, "workflow", workflowDB)
	store := openWorkflowStore(t, workflowDB, persistence.Config{})

	server := assembly.NewServer(assembly.Options{
		Stores:         assembly.Persistent(store),
		WorkflowQueue:  taskq.NewQueue(h.frontend(t, "workflow-tasks")),
		ActivityRouter: h.activityRouter(t, nil),
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

	counter.enabled.Store(true)
	var next atomic.Int64
	var wg sync.WaitGroup
	for range budgetConcurrency {
		wg.Go(func() {
			for {
				i := next.Add(1)
				if i > budgetWorkflows {
					return
				}
				runID, err := startedRunID(server.StartWorkflow(ctx, "", "budget-steps", fmt.Sprintf("budget-%d", i), "", nil))
				if err != nil {
					t.Error(err)
					return
				}
				waitForServerRunStatus(t, server, runID, historypkg.StatusCompleted, 30*time.Second)
			}
		})
	}
	wg.Wait()
	if t.Failed() {
		return
	}
	perWorkflow, harness := counter.reset()
	logStatementBreakdown(t, fmt.Sprintf("per workflow (%d workflows, harness polling excluded: %d statements)", budgetWorkflows, harness), perWorkflow, budgetWorkflows)

	time.Sleep(budgetIdle)
	idle, _ := counter.reset()
	logStatementBreakdown(t, "idle per second", idle, budgetIdle.Seconds())

	if got := float64(totalStatements(perWorkflow)) / budgetWorkflows; got > statementBudgetPerWorkflow {
		t.Errorf("statements per workflow = %.1f, budget %d", got, statementBudgetPerWorkflow)
	}
	if got := float64(totalStatements(idle)) / budgetIdle.Seconds(); got > idleStatementBudgetPerSecond {
		t.Errorf("idle statements per second = %.1f, budget %.1f", got, idleStatementBudgetPerSecond)
	}
}
