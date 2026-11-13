package interpreter

import (
	"strings"
	"testing"

	"github.com/donutnomad/tenuto/internal/executor"
	"github.com/donutnomad/tenuto/protocol/history"
	wlregistry "github.com/donutnomad/wlang/registry"
	wlruntime "github.com/donutnomad/wlang/runtime"
)

// ============================================
// per-coroutine host-panic recover 对齐测试（Temporal internal_workflow.go:1220 每协程 run() 的 defer/recover）
// ============================================
// 背景: main 协程有 recover（executor.go:521-531），但 lane 协程（spawnLaneBody）与 update handler
// 协程（spawnUpdateHandlerCoroutine）没有。wlang host 调用经 safeCall 把 panic 转 error（已路由），
// 但协程 body 里、InvokeClosure 之外的【直接 Go panic】（host 侧 nil deref、onDone panic、未来新增的
// 非-safeCall 宿主逻辑 panic）无 recover → 逃逸到协程 goroutine 边界 → 崩整个 worker 进程，而 Temporal
// 隔离成单 workflow 失败。
//
// 测试策略:
// - Error Case: lane 协程 body 直接 host panic → 记为执行错误（不崩进程）
// - Error Case: update handler 协程 body 直接 host panic（activeWLExec nil deref）→ Execute 返回错误
//
// ⚠️ RED 现状: 无 recover 时 panic 在协程【自己的 goroutine】上逃逸（scheduler 在另一 goroutine），
// 测试的 recover 捕不到 → 直接崩掉整个 test 进程。GREEN 后 Execute 干净返回 error。
// ============================================

// Error Case: lane 协程 body 内 host 级直接 panic（InvokeClosure 之外）→ 记为执行错误（不逃逸崩进程）。
func TestLaneCoroutine_HostPanic_ReturnsErrorNotCrash(t *testing.T) {
	exec := NewWorkflowExecutor(nil, nil)
	exec.startScheduler()
	defer exec.sched.Reclaim()
	exec.spawnLaneBody("/0", func() { panic("lane host boom") }, nil)
	exec.sched.RunBatch()

	err := exec.executionError()
	if err == nil {
		t.Fatalf("lane 协程 body 内 host panic 应被 recover 转为执行错误, got nil")
	}
	if !strings.Contains(err.Error(), "lane host boom") {
		t.Fatalf("执行错误应含 panic 信息 %q, got %v", "lane host boom", err)
	}
}

// Error Case: update handler 协程 body 内 host 级直接 panic（activeWLExec 为 nil 的 deref，
// 模拟宿主级 nil deref——非 wlang safeCall 路径）→ Execute 返回错误（不逃逸崩进程）。
//
// 构造: 预置 activeAPI.updateHandlers 含目标 name（等注册循环立即命中 break），但不跑 wlang body →
// activeWLExec 保持 nil → 协程 body 里 e.activeWLExec.InvokeClosure(...) 直接 nil deref panic。
func TestUpdateHandlerCoroutine_HostPanic_ReturnsErrorNotCrash(t *testing.T) {
	started := history.NewWorkflowStartedEvent(nil, "", "", "", "", 0)
	accepted := acceptedEvent(2, "boom", "u-boom", map[string]any{})
	exec := NewWorkflowExecutor(nil, []history.Event{started, accepted})
	// 预置 handler 注册表：非 nil closure 使等注册循环命中 break 后进入 InvokeClosure（nil closure 会被
	// InvokeClosure 首行 cl==nil 短路成普通 error，测不到 host panic）；activeWLExec 保持 nil →
	// e.activeWLExec.InvokeClosure(...) 在 enterHostCall 处对 nil *Executor 解引用 → 宿主级 panic。
	exec.activeAPI = &runtimeWorkflowAPI{
		executor: exec,
		updateHandlers: map[string]registeredUpdateHandler{"boom": {
			Closure:  &wlruntime.Closure{},
			ABI:      executor.UpdateHandlerABI{Kind: executor.UpdateHandlerABITemporalError, BusinessParamTypes: []string{"map<string,any>"}},
			Registry: wlregistry.New(),
		}},
	}

	_, err := exec.Execute()
	if err == nil {
		t.Fatalf("update handler 协程 body 内 host panic 应被 recover 转为 Execute 错误, got nil")
	}
}
