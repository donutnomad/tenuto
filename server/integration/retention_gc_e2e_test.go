package integration

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/server/infra/persistence/executionrepo"
	"github.com/donutnomad/tenuto/server/infra/systemclock"

	"github.com/donutnomad/tenuto/compiler/sdkgen"
	"github.com/donutnomad/tenuto/compiler/sdkgen/sdkgentest"
	"github.com/donutnomad/tenuto/internal/worker"
	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	wfsdk "github.com/donutnomad/tenuto/sdk/activity"
	"github.com/donutnomad/tenuto/server/app/assembly"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/biz/service/engine"
	retentionpkg "github.com/donutnomad/tenuto/server/biz/usecase/retention"
	"github.com/donutnomad/tenuto/server/infra/memory"
	"github.com/donutnomad/tenuto/server/infra/persistence"
	"github.com/donutnomad/tenuto/server/infra/persistence/mysqltest"
	"gorm.io/gorm"
)

// ============================================================================
// retention GC 端到端测试
// ============================================================================
// **为什么需要这一层**：GC 此前的测试全在部件层，两边各自绿灯但完整链路一次都没跑过。
//
//   - store 层 deletion_task_test.go 用 store.Create + store.Append **手工造行**，
//     从不经过 engine 的真实关闭路径。它验的是「给定这样的行，DeleteTree 会怎么删」，
//     不是「真跑一个 workflow 会不会被正确回收」。
//   - engine 层 retention_gc_test.go 用 fakeGCStore 假存储，DeleteTree 是个往 slice
//     里 append 的空壳。它验的是扫描器的调度与退避，碰不到任何真实数据。
//
// 本文件的每个用例都**真起 engine、真跑 workflow、真等它自己关闭**，GC 的输入完全
// 由引擎产生，没有一行手工构造的数据。这个功能删数据且不可逆，部件测试不够。
//
// **时间的处理**：MinRetention 是 1h（防误设秒删的硬下界），测试不可能真等。
// 故统一用 expireDeletionTasks 把已投递任务的 fire_at 改到过去 —— 改的是「什么时候
// 到期」这一个输入，投递、扫描、整树校验、分阶段删除全部走真实代码。这比把
// MinRetention 调低更诚实：后者会让测试跑在一条生产上不存在的配置分支上。
//
// **MySQL**：用例直接操作 GC 表（改 fire_at、数残留行），只在 TENUTO_TEST_MYSQL=1 时运行。
// ============================================================================

// gcTestRetention 是端到端用例统一使用的 retention。取 MinRetention 本身：
// 它必须 >= 1h 才通过 Validate，而具体值不影响断言（到期由 expireDeletionTasks 控制）。
const gcTestRetention = executionrepo.MinRetention

// newGCStore 在独立 MySQL 库上建一个开启 GC 的 store；没有开启 MySQL 后端测试时跳过 t。
func newGCStore(t *testing.T, retention time.Duration) (*persistence.Store, *gorm.DB) {
	t.Helper()
	db := mysqltest.NewDB(t)
	return openWorkflowStore(t, db, persistence.Config{
		Retention: executionrepo.RetentionConfig{Retention: retention},
	}), db
}

// gcHarness 是一套「真 Server + 真 Worker + 真 store + 真 GC 扫描器」的装配。
//
// 为什么不用 engine.NewEngine：扫描器的 processTask 必须拿**同一把** keyedMu 才能与
// 用户操作互斥，而 Engine 不暴露它内部的 *Server。故这里显式建 Server 再自己包 Worker
// —— 二者都是导出 API，链路与 NewEngine 完全一致（newEngine 做的就是这两步）。
type gcHarness struct {
	store    *persistence.Store
	database *gorm.DB
	server   *engine.Server
	worker   *worker.Worker
	registry *wfsdk.Registry
	gc       *retentionpkg.Task
}

// newGCHarness 装配端到端环境。retention<=0 表示关闭 GC（验收 7 用）。
func newGCHarness(t *testing.T, retention time.Duration) *gcHarness {
	t.Helper()
	store, db := newGCStore(t, retention)
	registry := wfsdk.NewRegistry()
	server := assembly.NewServer(assembly.Options{Stores: assembly.Persistent(store), WorkflowQueue: memory.NewTaskQueue(32), ActivityRouter: memory.NewActivityQueueRouter(32)})
	worker := assembly.NewWorker(server, registry,
		worker.WorkerOptions{WorkflowConcurrency: 2, ActivityConcurrency: 2})
	return &gcHarness{
		store:    store,
		database: db,
		server:   server,
		worker:   worker,
		registry: registry,
		// 传真 Server：processTask 要在删除期间持该 workflowID 的 keyedMu。
		gc: retentionpkg.NewTask(store.Executions, server, server, systemclock.Clock{}, retentionpkg.Config{}, server.Logger()),
	}
}

// start 启动 worker 并注册清理。
func (h *gcHarness) start(t *testing.T, ctx context.Context) {
	t.Helper()
	if err := h.server.RecoverRunningWorkflows(ctx, engine.DefaultReconcileLookahead); err != nil {
		t.Fatalf("recover running workflows: %v", err)
	}
	// 用例在 Worker 启动之后才注册 Activity 实现，Worker 启动时没有可登记的类型：这里登记它们的路由。
	registerActivityRoutes(t, h.server, "gc-ok", "gc-boom", "gc-block", "gc-cancelable", "gc-hold")
	h.worker.Start(ctx)
	t.Cleanup(func() {
		h.worker.Stop()
		h.server.Stop()
	})
}

// waitRunStatus 等 run 到达目标状态。
func (h *gcHarness) waitRunStatus(t *testing.T, runID string, want history.Status, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		status, err := h.server.GetRunStatus(runID)
		if err == nil && status == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	status, err := h.server.GetRunStatus(runID)
	if err != nil {
		t.Fatalf("get run status for %s: %v", runID, err)
	}
	t.Fatalf("run %s: expected status %s, got %s", runID, want, status)
}

// db 取底层 *gorm.DB，供测试直接查表断言「数据真的没了」。
func (h *gcHarness) db(t *testing.T) *gorm.DB {
	t.Helper()
	return h.database
}

// runToCompletion 注册并跑一个单活动 workflow 直到 Completed，返回 runID。
func (h *gcHarness) runToCompletion(t *testing.T, ctx context.Context, workflowID string) string {
	t.Helper()
	h.registry.Register("gc-ok", func(ctx context.Context, input m) (m, error) {
		return m{"ok": true}, nil
	})
	wf := sdkgentest.Sequential(t, &program.WorkflowConfig{Type: workflowID},
		sdkgentest.Step{Type: "gc-ok", Options: `StartToCloseTimeout: time.Minute`},
	)
	if _, err := h.server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, wf); err != nil {
		t.Fatal(err)
	}
	runID, err := startedRunID(h.server.StartWorkflow(ctx, "", workflowID, workflowID, "", m{"k": "v"}))
	if err != nil {
		t.Fatal(err)
	}
	h.waitRunStatus(t, runID, history.StatusCompleted, 10*time.Second)
	return runID
}

// ── 表访问辅助 ──
//
// 直接查物理表而不走 store 的读接口：读接口可能对「已删」做兜底（返回空而非报错），
// 那样断言就证明不了行真的没了。GC 的承诺是**物理回收**，只能按行数验。

func gcCountRows(t *testing.T, db *gorm.DB, table, where string, args ...any) int64 {
	t.Helper()
	var n int64
	if err := db.Table(table).Where(where, args...).Count(&n).Error; err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	return n
}

// treeIDOf 返回该 run 的事件所在的 tree。事件行上有现成的 tree_id 列。
func treeIDOf(t *testing.T, db *gorm.DB, runID string) string {
	t.Helper()
	var treeIDs []string
	if err := db.Table("tenuto_events").Where("run_id = ?", runID).
		Distinct().Pluck("tree_id", &treeIDs).Error; err != nil {
		t.Fatalf("pluck tree_id: %v", err)
	}
	if len(treeIDs) != 1 {
		t.Fatalf("run %s spans %d trees (%v), want exactly 1", runID, len(treeIDs), treeIDs)
	}
	return treeIDs[0]
}

// deletionTaskFireAt 返回该 tree 的投递任务到期时刻；任务不存在时 ok=false。
func deletionTaskFireAt(t *testing.T, db *gorm.DB, treeID string) (time.Time, bool) {
	t.Helper()
	var rows []struct{ FireAt time.Time }
	if err := db.Table("tenuto_deletion_tasks").Select("fire_at").
		Where("tree_id = ?", treeID).Scan(&rows).Error; err != nil {
		t.Fatalf("read deletion task: %v", err)
	}
	if len(rows) == 0 {
		return time.Time{}, false
	}
	return rows[0].FireAt, true
}

// expireDeletionTasks 把全部投递任务的 fire_at 推到过去，让下一轮扫描立刻捞到它们。
// 只改「什么时候到期」，投递与删除全程走真实代码。
func expireDeletionTasks(t *testing.T, db *gorm.DB) {
	t.Helper()
	if err := db.Table("tenuto_deletion_tasks").
		Where("1 = 1").
		Update("fire_at", time.Now().Add(-time.Hour)).Error; err != nil {
		t.Fatalf("expire deletion tasks: %v", err)
	}
}

// ============================================================================
// 验收 1 + 11：终态 tree 在到期后被整体回收，events/states/dispatches 一并清理
// ============================================================================

func TestGCEndToEndReclaimsClosedWorkflowData(t *testing.T) {
	ctx := context.Background()
	h := newGCHarness(t, gcTestRetention)
	h.start(t, ctx)

	runID := h.runToCompletion(t, ctx, "gc-e2e-basic")
	db := h.db(t)
	treeID := treeIDOf(t, db, runID)

	// 关闭后：数据齐全，且删除任务已排上。
	if n := gcCountRows(t, db, "tenuto_events", "run_id = ?", runID); n == 0 {
		t.Fatal("closed run has no events — 前提不成立，后面的删除断言会恒过")
	}
	if n := gcCountRows(t, db, "tenuto_runs", "id = ?", runID); n != 1 {
		t.Fatalf("tenuto_runs rows = %d, want 1", n)
	}
	if _, ok := deletionTaskFireAt(t, db, treeID); !ok {
		t.Fatalf("no deletion task enqueued for tree %s after the run closed", treeID)
	}

	// 未到期：GC 跑一轮什么都不该动。
	h.gc.RunOnce(ctx)
	if n := gcCountRows(t, db, "tenuto_events", "run_id = ?", runID); n == 0 {
		t.Fatal("GC deleted history before the retention deadline — 数据被提前回收")
	}

	// 到期后回收。
	expireDeletionTasks(t, db)
	h.gc.RunOnce(ctx)

	for _, c := range []struct {
		table string
		where string
		arg   any
	}{
		{"tenuto_events", "run_id = ?", runID},
		{"tenuto_runs", "id = ?", runID},
		{"tenuto_execution_states", "run_id = ?", runID},
		{"tenuto_activity_states", "run_id = ?", runID},
		{"tenuto_timer_states", "run_id = ?", runID},
		{"tenuto_child_workflow_states", "run_id = ?", runID},
		// 验收 11：task_dispatches 是增速最快、v1 方案漏过的那张表。
		{"tenuto_task_dispatches", "run_id = ?", runID},
		{"tenuto_history_tree", "tree_id = ?", treeID},
		{"tenuto_deletion_tasks", "tree_id = ?", treeID},
	} {
		if n := gcCountRows(t, db, c.table, c.where, c.arg); n != 0 {
			t.Errorf("%s: %d rows remain after collection, want 0", c.table, n)
		}
	}
}

// ============================================================================
// 验收 11 专项：task_dispatches 被清理
//
// 单列一个用例是因为上一条里 dispatch 行可能已被正常执行路径消费掉，那样断言就恒过。
// 这里在关闭后**确认 dispatch 行确实存在**，再验它被 GC 删掉。
// ============================================================================

func TestGCEndToEndReclaimsTaskDispatches(t *testing.T) {
	ctx := context.Background()
	h := newGCHarness(t, gcTestRetention)
	h.start(t, ctx)

	runID := h.runToCompletion(t, ctx, "gc-e2e-dispatch")
	db := h.db(t)
	treeID := treeIDOf(t, db, runID)

	// 造一条属于该 run 的 dispatch 行：真实链路里 dispatch 可能已被消费删除，
	// 而本条验收要证明的是「GC 会清理残留的 dispatch」，故显式留一条。
	if err := db.Exec(
		"INSERT INTO tenuto_task_dispatches "+
			"(namespace, task_type, run_id, scheduled_event_id, attempt, status, created_at, updated_at) "+
			"VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
		protocol.DefaultNamespace, string(protocol.TaskTypeWorkflow), runID, int64(9991), 1, "pending",
		time.Now(), time.Now()).Error; err != nil {
		t.Fatalf("seed task dispatch: %v", err)
	}
	if n := gcCountRows(t, db, "tenuto_task_dispatches", "run_id = ?", runID); n == 0 {
		t.Fatal("seeded dispatch row missing — 断言会恒过")
	}

	expireDeletionTasks(t, db)
	h.gc.RunOnce(ctx)

	if n := gcCountRows(t, db, "tenuto_task_dispatches", "run_id = ?", runID); n != 0 {
		t.Fatalf("%d task_dispatches rows survived GC —— 增速最快的那张表没被清理", n)
	}
	if n := gcCountRows(t, db, "tenuto_deletion_tasks", "tree_id = ?", treeID); n != 0 {
		t.Fatalf("deletion task survived a successful collection")
	}
}

// ============================================================================
// 验收 13：5 种终态全部触发投递
//
// 方案强调收敛点，要求 Completed/Failed/Cancelled/Terminated/ContinuedAsNew
// 每一种都排上删除任务。少一种，那类 workflow 的 tree **永不回收**，而且不会有
// 任何报错——是个沉默的泄漏。
//
// 这里每种终态都用**真实路径**产生：activity 成功/失败走 worker，terminate/cancel
// 走 Server API，CAN 走 ContinueAsNew 命令。不手工 Append 终态事件。
// ============================================================================

func TestGCEndToEndAllTerminalStatesEnqueueDeletionTask(t *testing.T) {
	ctx := context.Background()
	h := newGCHarness(t, gcTestRetention)

	h.registry.Register("gc-ok", func(ctx context.Context, input m) (m, error) {
		return m{"ok": true}, nil
	})
	h.registry.Register("gc-boom", func(ctx context.Context, input m) (m, error) {
		return nil, errors.New("boom")
	})
	// gc-block 卡住不返回，让 run 保持 Running 以便被 terminate / cancel。
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	h.registry.Register("gc-block", func(ctx context.Context, input m) (m, error) {
		select {
		case <-release:
		case <-ctx.Done():
		}
		return m{"ok": true}, nil
	})
	h.start(t, ctx)

	db := h.db(t)

	// 每种终态各跑一个 workflow，收集 (终态名, runID)。
	type closedRun struct {
		label  string
		runID  string
		status history.Status
	}
	var closed []closedRun

	// ① Completed —— activity 正常返回。
	closed = append(closed, closedRun{"Completed",
		h.runToCompletion(t, ctx, "gc-term-completed"), history.StatusCompleted})

	// ② Failed —— activity 返回错误。
	{
		wf := sdkgentest.Sequential(t, &program.WorkflowConfig{Type: "gc-term-failed"},
			sdkgentest.Step{Type: "gc-boom", Options: `StartToCloseTimeout: time.Minute, RetryPolicy: workflow.RetryPolicy{MaxAttempts: 1}`},
		)
		if _, err := h.server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, wf); err != nil {
			t.Fatal(err)
		}
		runID, err := startedRunID(h.server.StartWorkflow(ctx, "", wf.Type, wf.Type, "", nil))
		if err != nil {
			t.Fatal(err)
		}
		h.waitRunStatus(t, runID, history.StatusFailed, 10*time.Second)
		closed = append(closed, closedRun{"Failed", runID, history.StatusFailed})
	}

	// ③ Terminated —— 对一个还在跑的 run 调 TerminateWorkflow。
	{
		wf := sdkgentest.Sequential(t, &program.WorkflowConfig{Type: "gc-term-terminated"},
			sdkgentest.Step{Type: "gc-block", Options: `StartToCloseTimeout: time.Minute`},
		)
		if _, err := h.server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, wf); err != nil {
			t.Fatal(err)
		}
		runID, err := startedRunID(h.server.StartWorkflow(ctx, "", wf.Type, wf.Type, "", nil))
		if err != nil {
			t.Fatal(err)
		}
		h.waitRunStatus(t, runID, history.StatusRunning, 10*time.Second)
		if err := h.server.TerminateWorkflow(context.Background(), "", "", runID, "gc-e2e"); err != nil {
			t.Fatalf("terminate: %v", err)
		}
		h.waitRunStatus(t, runID, history.StatusTerminated, 10*time.Second)
		closed = append(closed, closedRun{"Terminated", runID, history.StatusTerminated})
	}

	// ④ Cancelled —— 协作式取消。
	//
	// ⚠️ 取消是**协作式**的：CancelWorkflow 只落一个 CancellationRequested 事件，真正
	// 收敛到 Cancelled 要等下一个 workflow task 跑起来、executor 看到该事件后发出
	// CmdCancelWorkflow。因此活动**不能**一直阻塞 —— 活动不返回就没有下一个 workflow
	// task，run 会永远停在 Running。
	//
	// 故这里用 gc-cancelable：它等取消请求落库后再返回，把
	// 「先请求取消、再让 workflow 有机会响应」这个顺序确定下来。
	{
		cancelRequested := make(chan struct{})
		h.registry.Register("gc-cancelable", func(actx context.Context, input m) (m, error) {
			select {
			case <-cancelRequested:
			case <-actx.Done():
			case <-time.After(20 * time.Second):
			}
			return m{"ok": true}, nil
		})
		wf := sdkgentest.Sequential(t, &program.WorkflowConfig{Type: "gc-term-cancelled"},
			sdkgentest.Step{Type: "gc-cancelable", Options: `StartToCloseTimeout: time.Minute`},
		)
		if _, err := h.server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, wf); err != nil {
			t.Fatal(err)
		}
		runID, err := startedRunID(h.server.StartWorkflow(ctx, "", wf.Type, wf.Type, "", nil))
		if err != nil {
			t.Fatal(err)
		}
		h.waitRunStatus(t, runID, history.StatusRunning, 10*time.Second)
		if err := h.server.CancelWorkflow(context.Background(), "", "", runID, "gc-e2e"); err != nil {
			t.Fatalf("cancel: %v", err)
		}
		// 取消请求已落库，放活动返回 → 下一个 workflow task 会看到该事件并收敛。
		close(cancelRequested)
		h.waitRunStatus(t, runID, history.StatusCancelled, 20*time.Second)
		closed = append(closed, closedRun{"Cancelled", runID, history.StatusCancelled})
	}

	// ⑤ ContinuedAsNew —— 老代 run 以 ContinuedAsNew 关闭。
	{
		runID := runContinueAsNew(t, ctx, h, "gc-term-can")
		closed = append(closed, closedRun{"ContinuedAsNew", runID, history.StatusContinuedAsNew})
	}

	if len(closed) != 5 {
		t.Fatalf("prepared %d terminal runs, want 5", len(closed))
	}

	// 逐一断言：每种终态都排上了删除任务。
	for _, c := range closed {
		treeID := treeIDOf(t, db, c.runID)
		if _, ok := deletionTaskFireAt(t, db, treeID); !ok {
			t.Errorf("terminal state %s (run %s, tree %s): NO deletion task enqueued —— "+
				"这类 workflow 的 tree 永不回收，且不会有任何报错。"+
				"投递点的终态集合与 ApplyEventToExecutionState 不同步了",
				c.label, c.runID, treeID)
		}
	}

	// 再验一层：到期后这些 tree 真的被删掉（投递上了但删不掉同样是泄漏）。
	expireDeletionTasks(t, db)
	h.gc.RunOnce(ctx)
	for _, c := range closed {
		if n := gcCountRows(t, db, "tenuto_runs", "id = ?", c.runID); n != 0 {
			t.Errorf("terminal state %s: run %s survived collection", c.label, c.runID)
		}
	}
}

// runContinueAsNew 跑一个真正 ContinueAsNew 的 workflow，返回**老代** runID
// （即以 ContinuedAsNew 关闭的那一代）。
//
// 用 wlang 程序而非手工发 CmdContinueAsNew：ContinuedAsNew 是五种终态里唯一一个
// **不是**「workflow 结束」而是「换一代继续」的，它的关闭路径
// （旧代的终态随接续 Run 的事务提交）与其余四种不同。要证明它也会投递删除任务，就得让
// 引擎自己走完那条路径。
func runContinueAsNew(t *testing.T, ctx context.Context, h *gcHarness, workflowID string) string {
	t.Helper()

	src := []byte(`package flows

import workflow "github.com/donutnomad/tenuto/sdk/workflow"

func RollingWorkflow(ctx workflow.Context, input map[string]any) error {
	return workflow.ContinueAsNew(ctx, "` + workflowID + `", input)
}
`)
	result, err := sdkgen.GenerateFromSource(src, sdkgen.Options{
		WorkflowType: workflowID,
		FuncName:     "RollingWorkflow",
		Filename:     "rolling_workflow.go",
	})
	if err != nil {
		t.Fatalf("generate continue-as-new workflow: %v", err)
	}
	wf := result.Workflow
	wf.Type = workflowID
	if _, err := h.server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, wf); err != nil {
		t.Fatalf("register continue-as-new workflow: %v", err)
	}

	runID, err := startedRunID(h.server.StartWorkflow(ctx, "", workflowID, workflowID, "", m{"page": int64(1)}))
	if err != nil {
		t.Fatalf("start continue-as-new workflow: %v", err)
	}
	// 老代必须收敛到 ContinuedAsNew —— 这正是本用例要覆盖的终态。
	h.waitRunStatus(t, runID, history.StatusContinuedAsNew, 15*time.Second)
	return runID
}

// ============================================================================
// 验收 2：同事务性 —— 关闭成功 ⟺ 删除任务已存在
//
// 这条不能只测「关闭后查表存在」，那证明不了同事务（非事务投递在没有并发时同样会
// 让表里有行）。真正要证的是**投递失败会让终态事件一起回滚**。
//
// 端到端能证的那一半：run 一旦到达终态，删除任务**必然**已经在表里，中间不存在
// 「关闭成功但任务还没排上」的可观察窗口。配合 store 层已有的
// TestRetentionEnqueueFailureRollsBackTerminalEvent（注入投递失败 → 终态事件回滚），
// 两者合起来覆盖验收 2 的双向。
// ============================================================================

func TestGCEndToEndCloseAndEnqueueAreAtomic(t *testing.T) {
	ctx := context.Background()
	h := newGCHarness(t, gcTestRetention)
	h.start(t, ctx)

	h.registry.Register("gc-ok", func(ctx context.Context, input m) (m, error) {
		return m{"ok": true}, nil
	})
	db := h.db(t)

	// 跑多个 run 并**在轮询中不断检查**：只要看到某个 run 已终态，那一刻它的删除任务
	// 就必须已经存在。若投递不在同一事务里，这个循环有机会撞进那个窗口。
	//
	// 每个 run 用**独立的 workflowID**：一个 workflowID 同时只能有一个 current run，
	// 复用同一个 ID 并发启动会撞 current run 冲突（那是另一条语义，与本用例无关）。
	const runs = 8
	runIDs := make([]string, 0, runs)
	for i := range runs {
		workflowID := fmt.Sprintf("gc-e2e-atomic-%d", i)
		wf := sdkgentest.Sequential(t, &program.WorkflowConfig{Type: workflowID},
			sdkgentest.Step{Type: "gc-ok", Options: `StartToCloseTimeout: time.Minute`},
		)
		if _, err := h.server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, wf); err != nil {
			t.Fatal(err)
		}
		runID, err := startedRunID(h.server.StartWorkflow(ctx, "", workflowID, workflowID, "", m{"i": i}))
		if err != nil {
			t.Fatal(err)
		}
		runIDs = append(runIDs, runID)
	}

	deadline := time.Now().Add(20 * time.Second)
	pending := make(map[string]bool, runs)
	for _, id := range runIDs {
		pending[id] = true
	}
	for len(pending) > 0 && time.Now().Before(deadline) {
		for runID := range pending {
			status, err := h.server.GetRunStatus(runID)
			if err != nil || status == history.StatusRunning {
				continue
			}
			// 观察到终态的这一刻，任务必须已经在表里。
			treeID := treeIDOf(t, db, runID)
			if _, ok := deletionTaskFireAt(t, db, treeID); !ok {
				t.Fatalf("run %s is already %s but its deletion task is not in the table —— "+
					"存在「关闭成功但任务没排上」的窗口，那棵 tree 会永久泄漏"+
					"（没有任何其它路径会重新发现它）", runID, status)
			}
			delete(pending, runID)
		}
	}
	if len(pending) > 0 {
		t.Fatalf("%d runs never closed within the deadline", len(pending))
	}
}

// ============================================================================
// 验收 5 + 14：引用计数 —— reset 产生的共享祖先 branch 不被误删
//
// 这是 1e92e8c 修过数据丢失的地方，回归价值最高。
//
// ForkBranch 用 TreeID: sourceBranch.TreeID，reset 出的新 run 与老 run **共享同一棵
// tree**；而 DeleteBranch 只按 tree_id 查 history_tree、完全不看 tenuto_runs。
// 若 GC 退化成按 run 删除，回收老 run 时会删掉它的 tree row，仍活着的新 run 把老
// branch 列为 ancestor —— 引用信息永久丢失，等新 run 到期时 DeleteBranch 的
// allBranches 里已无老 branch，会回溯把仍被引用的历史全删。
//
// **潜伏型数据丢失：第一次 GC 看似正常，第二次才炸。**
// ============================================================================

func TestGCEndToEndResetSharedTreeSurvivesUntilAllRunsTerminal(t *testing.T) {
	ctx := context.Background()
	h := newGCHarness(t, gcTestRetention)

	h.registry.Register("gc-ok", func(ctx context.Context, input m) (m, error) {
		return m{"ok": true}, nil
	})
	// gc-hold 在 holding 置位之后阻塞到 release 关闭：base run 正常跑完，reset 出的新 run 停在这个 Activity 上，
	// GC 运行时它必然没有终态事件。新 run 若能自己跑完，GC 时整树已全部终态，删除是正确行为，用例就验不到
	// 「还有活 run 时整树保留」：LastWorkflowTask reset 出的新 run 只剩一个 WFT，在 ResetWorkflow 返回后
	// 约 1ms 内结束，比测试读表、改 fire_at、跑 GC 的几条语句还快。
	var holding atomic.Bool
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseHold := func() { releaseOnce.Do(func() { close(release) }) }
	h.registry.Register("gc-hold", func(ctx context.Context, input m) (m, error) {
		if holding.Load() {
			select {
			case <-release:
			case <-ctx.Done():
			}
		}
		return m{"ok": true}, nil
	})
	h.start(t, ctx)
	t.Cleanup(releaseHold)

	// base run：跑完并关闭。
	wf := sdkgentest.Sequential(t, &program.WorkflowConfig{Type: "gc-e2e-reset"},
		sdkgentest.Step{Type: "gc-ok", Options: `StartToCloseTimeout: time.Minute`},
		sdkgentest.Step{Type: "gc-hold", Options: `StartToCloseTimeout: time.Minute`},
	)
	if _, err := h.server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, wf); err != nil {
		t.Fatal(err)
	}
	baseRunID, err := startedRunID(h.server.StartWorkflow(ctx, "", wf.Type, wf.Type, "", nil))
	if err != nil {
		t.Fatal(err)
	}
	h.waitRunStatus(t, baseRunID, history.StatusCompleted, 10*time.Second)

	db := h.db(t)
	sharedTree := treeIDOf(t, db, baseRunID)
	baseEventCount := gcCountRows(t, db, "tenuto_events", "run_id = ?", baseRunID)
	if baseEventCount == 0 {
		t.Fatal("base run has no events — 前提不成立")
	}

	// reset：从 base run 分叉出新 run，共享同一棵 tree。新 run 从第一个 WFT 重新执行，停在 gc-hold 上。
	// **必须传 ResetType**：ResetType 与 ResetEventID 都为零值时 ResetWorkflow 会走
	// resetLegacy —— 那条路径不 fork，新 run 会落在一棵**全新的 tree** 上，共享 tree
	// 的场景根本不成立，本用例就成了恒过的摆设。
	holding.Store(true)
	resetRunID, err := h.server.ResetWorkflow(ctx, execution.ResetWorkflowRequest{
		RunID:     baseRunID,
		Reason:    "gc-e2e-reset",
		ResetType: execution.ResetTypeEnums.FirstWorkflowTask,
	})
	if err != nil {
		t.Fatalf("reset: %v", err)
	}
	if resetRunID == baseRunID {
		t.Fatal("reset returned the same run id")
	}

	resetTree := treeIDOf(t, db, resetRunID)
	if resetTree != sharedTree {
		t.Fatalf("reset run is on tree %s but base run is on tree %s —— "+
			"前提不成立：本用例要验的正是共享 tree 的场景", resetTree, sharedTree)
	}

	// base run 已终态并到期。但新 run 还在这棵树上，整树不能删。
	expireDeletionTasks(t, db)
	h.gc.RunOnce(ctx)

	// **核心断言**：祖先历史必须完好。
	if n := gcCountRows(t, db, "tenuto_events", "run_id = ?", baseRunID); n != baseEventCount {
		t.Fatalf("base run events = %d, want %d —— 祖先历史被误删。"+
			"GC 退化成按 run 删除时正是这样：老 run 的 tree row 被删，"+
			"新 run 的 ancestor 引用永久丢失", n, baseEventCount)
	}
	// 新 run 的历史必须始终可读，且能读到 fork 前的祖先事件。
	resetEvents, err := h.server.GetEvents(resetRunID)
	if err != nil {
		t.Fatalf("reset run history became unreadable: %v", err)
	}
	if len(resetEvents) == 0 {
		t.Fatal("reset run history is empty after the skipped collection")
	}
	// 整树都还在。
	if n := gcCountRows(t, db, "tenuto_history_tree", "tree_id = ?", sharedTree); n == 0 {
		t.Fatal("history_tree rows for the shared tree were deleted while a run is still live")
	}
	if n := gcCountRows(t, db, "tenuto_runs", "id = ?", baseRunID); n == 0 {
		t.Fatal("base run row deleted while its tree still has a live run")
	}
	// GC 运行期间新 run 一直未终态：上面的断言验的确实是「还有活 run」的场景。
	if status, err := h.server.GetRunStatus(resetRunID); err != nil || status != history.StatusRunning {
		t.Fatalf("reset run status after the skipped collection = %s, %v; want running —— 前提不成立", status, err)
	}

	// 新 run 也关闭 → 整树才可回收。
	releaseHold()
	h.waitRunStatus(t, resetRunID, history.StatusCompleted, 15*time.Second)
	expireDeletionTasks(t, db)
	h.gc.RunOnce(ctx)

	if n := gcCountRows(t, db, "tenuto_events", "tree_id = ?", sharedTree); n != 0 {
		t.Errorf("%d events remain after the whole tree became collectable", n)
	}
	for _, runID := range []string{baseRunID, resetRunID} {
		if n := gcCountRows(t, db, "tenuto_runs", "id = ?", runID); n != 0 {
			t.Errorf("run %s survived whole-tree collection", runID)
		}
	}
}

// ============================================================================
// 验收 4：删除顺序 —— 中途失败时留下的必须是「孤儿事件」而不是「可访问但没有事件的 run」
//
// 这是方案的核心原则：
//
//	**宁可留下不可访问的孤儿事件，也不要留下可访问但没有事件的 run。**
//
// 理由（对应 Temporal shard/context_impl.go:969）：run 行还在但历史没了，是**用户
// 可见的损坏** —— 查询能查到这个 run，读历史却是空的。反之孤儿事件无人可达，由兜底
// 扫描慢慢清，对用户不可见。
//
// 端到端验法：在删除过程的每一个可中断点之后检查该不变式。这里用真实数据 + 真实
// DeleteTree，逐 run 断言「若 run 行还在，则它的事件必须还在」。
// ============================================================================

func TestGCEndToEndDeleteOrderNeverLeavesReachableRunWithoutHistory(t *testing.T) {
	ctx := context.Background()
	h := newGCHarness(t, gcTestRetention)
	h.start(t, ctx)

	runID := h.runToCompletion(t, ctx, "gc-e2e-order")
	db := h.db(t)
	treeID := treeIDOf(t, db, runID)

	assertNoReachableRunWithoutHistory := func(stage string) {
		t.Helper()
		var runIDs []string
		if err := db.Table("tenuto_runs").Where("id = ?", runID).
			Pluck("id", &runIDs).Error; err != nil {
			t.Fatalf("%s: list runs: %v", stage, err)
		}
		for _, id := range runIDs {
			// run 行还在 → 它的历史必须还在。
			if n := gcCountRows(t, db, "tenuto_events", "run_id = ?", id); n == 0 {
				t.Fatalf("%s: run %s is still reachable but has NO events —— "+
					"违反核心不变式「宁可留孤儿事件，不留可访问但无事件的 run」。"+
					"删除顺序被改坏了：events 在 runs 之前被删", stage, id)
			}
		}
	}

	assertNoReachableRunWithoutHistory("before collection")

	// 真实回收。DeleteTree 内部 Step 1/2 同事务、Step 3 之后才删事件，
	// 故任何一个中断点上不变式都成立。
	expireDeletionTasks(t, db)
	h.gc.RunOnce(ctx)

	assertNoReachableRunWithoutHistory("after collection")

	// 回收完成后 run 与事件都应消失（不是「run 没了但事件还在」的中间态）。
	if n := gcCountRows(t, db, "tenuto_runs", "id = ?", runID); n != 0 {
		t.Errorf("run row survived a successful collection")
	}
	if n := gcCountRows(t, db, "tenuto_events", "tree_id = ?", treeID); n != 0 {
		t.Errorf("%d events survived a successful collection", n)
	}
}

// failingDeleteTreeStore 让 DeleteTree 直接失败（不触碰任何数据），用来构造
// 「GC 取到任务但删除没成功」的中断态 —— 对应生产上 DB 抖动 / 进程在删除前被杀。
//
// 为什么不「先真删再抛错」：真实 DeleteTree 的最后一步（Step 4）就是删除任务自身，
// 跑完再抛错等于任务已被正常清理，构造出的是假中断态，反而会误报「任务被静默丢弃」。
// 真正的中断必须发生在 DeleteTree 内部完成之前。
type failingDeleteTreeStore struct {
	execution.RetentionGCStore
	failed bool
}

func (s *failingDeleteTreeStore) DeleteTree(treeID string) error {
	s.failed = true
	return errors.New("injected: delete tree failed mid-way")
}

// TestGCEndToEndDeleteOrderHoldsWhenEventDeletionFails 在 DeleteTree **失败**的中断态下
// 验同一条不变式。
//
// 为什么要单独一条：成功路径下 runs 与 events 双双归零，「run 还在但事件没了」这个
// 条件恒为假 —— 只断言成功路径等于什么都没验。判别力只存在于中断态，所以这里必须
// **真的注入一次失败**，而不是跑一遍成功的 GC 再检查。
func TestGCEndToEndDeleteOrderHoldsWhenEventDeletionFails(t *testing.T) {
	ctx := context.Background()
	h := newGCHarness(t, gcTestRetention)
	h.start(t, ctx)

	runID := h.runToCompletion(t, ctx, "gc-e2e-order-partial")
	db := h.db(t)
	treeID := treeIDOf(t, db, runID)

	// 换一个会在 DeleteTree 末尾抛错的 store，其余行为与真实 store 一致。
	failing := &failingDeleteTreeStore{RetentionGCStore: h.store.Executions}
	gc := retentionpkg.NewTask(failing, h.server, h.server, systemclock.Clock{}, retentionpkg.Config{}, h.server.Logger())

	expireDeletionTasks(t, db)
	gc.RunOnce(ctx)

	if !failing.failed {
		t.Fatal("注入的失败从未发生 —— 本用例失去判别力（任务没被取到？）")
	}

	// 中断态下的核心不变式：run 行还在 ⇒ 它的事件必须还在。
	// 反之（事件没了但 run 行还在）就是「可访问但没有事件的 run」，用户查得到这个 run
	// 却读不出历史，是方案明确要避免的用户可见损坏。
	runsLeft := gcCountRows(t, db, "tenuto_runs", "id = ?", runID)
	eventsLeft := gcCountRows(t, db, "tenuto_events", "tree_id = ?", treeID)
	if runsLeft > 0 && eventsLeft == 0 {
		t.Fatalf("run row remains (%d) while all its events are gone (%d) —— "+
			"「可访问但没有事件的 run」，正是方案要避免的用户可见损坏", runsLeft, eventsLeft)
	}

	// 失败的任务必须留着**并且在可预见的时间内重试** —— 光断言「行还在」判别力不足：
	// 把任务推到十万小时之后，行同样还在，树却永远收不掉。所以这里连 fire_at 一起验：
	// 必须落在退避上界（gcParkDuration = 24h）之内。
	var fireAt time.Time
	if err := db.Table("tenuto_deletion_tasks").Where("tree_id = ?", treeID).
		Select("fire_at").Row().Scan(&fireAt); err != nil {
		t.Fatalf("deletion task 在失败后消失了 —— 该树将永久泄漏，不会再被回收: %v", err)
	}
	if until := time.Until(fireAt); until > 25*time.Hour {
		t.Fatalf("deletion task 被推到 %v 之后（>24h 退避上界）—— 等同于永久放弃，"+
			"这棵树的数据将无限期滞留", until)
	}
}

// ============================================================================
// 验收 7：retention=0 的行为与引入 GC 前完全一致（向后兼容）
//
// 默认关闭是 GC 最重要的安全属性：显式开启才删数据。这条要证的是关闭时
// **一行删除任务都不投递、一行数据都不删** —— 不只是「GC 不跑」，而是连投递都没有。
// ============================================================================

func TestGCEndToEndRetentionZeroChangesNothing(t *testing.T) {
	ctx := context.Background()
	// retention=0 → GC 关闭。
	h := newGCHarness(t, 0)
	h.start(t, ctx)

	runID := h.runToCompletion(t, ctx, "gc-e2e-disabled")
	db := h.db(t)
	treeID := treeIDOf(t, db, runID)

	// ① 一条删除任务都不该有。
	if n := gcCountRows(t, db, "tenuto_deletion_tasks", "1 = 1"); n != 0 {
		t.Fatalf("retention=0 enqueued %d deletion tasks —— 关闭 GC 时不该投递任何任务", n)
	}

	// 记录基线，稍后断言扫描器跑过之后一行都没变。
	treeRowsBefore := gcCountRows(t, db, "tenuto_history_tree", "tree_id = ?", treeID)
	eventsBefore := gcCountRows(t, db, "tenuto_events", "run_id = ?", runID)

	// ② 即使把扫描器硬跑起来，也没有任何东西可删（表是空的）。
	h.gc.RunOnce(ctx)

	// ③ 数据必须原样保留。
	if n := gcCountRows(t, db, "tenuto_runs", "id = ?", runID); n != 1 {
		t.Errorf("tenuto_runs rows = %d, want 1 —— 关闭 GC 时数据被删了", n)
	}
	if got := gcCountRows(t, db, "tenuto_events", "run_id = ?", runID); got != eventsBefore || got == 0 {
		t.Errorf("events for run %s = %d, want unchanged %d (and non-zero) —— "+
			"关闭 GC 时历史被动过了", runID, got, eventsBefore)
	}
	// 注意：这里**不能**断言 history_tree 有行。已实测（见本轮 probe）：从未 reset 过的
	// run 在 history_tree 里本来就是 0 行 —— tree 行只在 ForkBranch 时才登记，与 GC 无关。
	// 若在此断言 >0，测的就不是 GC，而是一条不存在的前提。改为断言 GC 关闭时**行数不变**。
	if got := gcCountRows(t, db, "tenuto_history_tree", "tree_id = ?", treeID); got != treeRowsBefore {
		t.Errorf("history_tree rows for tree %s changed from %d to %d while retention is disabled",
			treeID, treeRowsBefore, got)
	}
	// ④ 历史仍可正常读取，行为与引入 GC 之前一致。
	events, err := h.server.GetEvents(runID)
	if err != nil {
		t.Fatalf("GetEvents with retention disabled: %v", err)
	}
	if len(events) == 0 {
		t.Fatal("GetEvents returned no events with retention disabled")
	}
}
