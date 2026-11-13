package integration

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/compiler/sdkgen"
	"github.com/donutnomad/tenuto/internal/worker"
	"github.com/donutnomad/tenuto/protocol"
	historypkg "github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/sdk/activity"
	"github.com/donutnomad/tenuto/sdk/apperror"
	"github.com/donutnomad/tenuto/server/app/assembly"
	"github.com/donutnomad/tenuto/server/biz/service/engine"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

// ============================================
// Workflow 取消端到端（生成的 Go Workflow + 真实 Worker）
// ============================================
// 测试策略:
// - Happy Path: 捕获取消、在 disconnected context 上运行清理 Activity，返回取消错误 → Cancelled；返回 nil → Completed
// - Happy Path: 不理会取消的 Workflow 继续运行并完成
// - Happy Path: WithCancel 作用域取消两个并行 Activity 中的一个，另一个完成，Workflow 完成
// - Happy Path: 心跳的 Activity：WaitForCancellation=true 时 Workflow 在 ActivityCancelled 之后才看到取消错误，
//   false 时立即看到
// - Happy Path: 子 Workflow 的取消传播（等待与不等待）与每种父关闭策略组合
// ============================================

type cancelE2E struct {
	t       *testing.T
	server  *engine.Server
	started chan string // blocking Activity 开始执行时写入它的 Activity ID
	cleaned atomic.Int32
}

// newCancelE2E 启动 Server 与一个同时执行 Workflow 与 Activity 的 Worker。Activity：
//   - cancel.Block 心跳直到被取消，返回 ctx.Err()；input 带 linger 时先再心跳一段时间才返回；
//   - cancel.Cleanup 计数后立即完成；cancel.Fast 立即完成。
func newCancelE2E(t *testing.T) *cancelE2E {
	t.Helper()
	return newCancelE2EWithSlots(t, 8)
}

// newCancelE2EWithSlots 与 newCancelE2E 相同，Worker 同时最多执行 slots 个 Activity。
func newCancelE2EWithSlots(t *testing.T, slots int) *cancelE2E {
	t.Helper()
	h := &cancelE2E{t: t, started: make(chan string, 16)}
	registry := activity.NewRegistry()
	if err := registry.Register("cancel.Block", func(ctx context.Context, input map[string]any) (map[string]any, error) {
		h.started <- activity.GetInfo(ctx).ActivityID
		for ctx.Err() == nil {
			activity.RecordHeartbeat(ctx, map[string]any{"at": time.Now().UnixNano()})
			time.Sleep(10 * time.Millisecond)
		}
		if input["linger"] == true {
			time.Sleep(300 * time.Millisecond)
		}
		return nil, apperror.NewCanceled("stopped")
	}); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register("cancel.Cleanup", func(ctx context.Context, input map[string]any) (map[string]any, error) {
		h.cleaned.Add(1)
		return map[string]any{"cleaned": true}, nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register("cancel.Fast", func(ctx context.Context, input map[string]any) (map[string]any, error) {
		return map[string]any{"ok": true}, nil
	}); err != nil {
		t.Fatal(err)
	}
	h.server = assembly.NewServer(assembly.Options{Stores: assembly.Stores{Executions: memory.NewExecutionManager()},
		WorkflowQueue: memory.NewTaskQueue(64), ActivityRouter: memory.NewActivityQueueRouter(64)})
	w := assembly.NewWorker(h.server, registry, worker.WorkerOptions{WorkflowConcurrency: 2, ActivityConcurrency: slots})
	w.Start(context.Background())
	t.Cleanup(func() {
		w.Stop()
		h.server.Stop()
	})
	return h
}

// register 从 Go 源码生成 id 这个 Workflow 并登记；源码里的 activities.X 解析为 cancel.X。
func (h *cancelE2E) register(id, src string) {
	h.t.Helper()
	generated, err := sdkgen.GenerateFromSource([]byte(src), sdkgen.Options{WorkflowID: id, FuncName: "W", Filename: "w.go"})
	if err != nil {
		h.t.Fatalf("generate %s: %v", id, err)
	}
	for symbol, ref := range generated.Workflow.Activities {
		ref.Type = "cancel." + strings.TrimPrefix(symbol, "activities.")
		generated.Workflow.Activities[symbol] = ref
	}
	if _, err := h.server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, generated.Workflow); err != nil {
		h.t.Fatal(err)
	}
}

func (h *cancelE2E) start(workflowType string, input map[string]any) string {
	h.t.Helper()
	runID, err := startedRunID(h.server.StartWorkflow(context.Background(), "", workflowType, "", "", input))
	if err != nil {
		h.t.Fatal(err)
	}
	return runID
}

func (h *cancelE2E) waitStarted() {
	h.t.Helper()
	select {
	case <-h.started:
	case <-time.After(5 * time.Second):
		h.t.Fatal("blocking activity never started")
	}
}

func (h *cancelE2E) cancel(runID string) {
	h.t.Helper()
	if err := h.server.CancelWorkflow(context.Background(), "", "", runID, "e2e"); err != nil {
		h.t.Fatal(err)
	}
}

func (h *cancelE2E) events(runID string) []historypkg.Event {
	h.t.Helper()
	events, err := h.server.GetEvents(runID)
	if err != nil {
		h.t.Fatal(err)
	}
	return events
}

func eventIndex(events []historypkg.Event, typ historypkg.EventType) int {
	for i, event := range events {
		if event.EventType() == typ {
			return i
		}
	}
	return -1
}

const cleanupWorkflowSrc = `package flows

import (
	"time"

	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/activities"
)

func W(ctx workflow.Context, input map[string]any) error {
	var out map[string]any
	err := workflow.ExecuteActivity(ctx, activities.Block, input, workflow.ActivityOptions{StartToCloseTimeout: time.Minute, HeartbeatTimeout: 10 * time.Second}).Get(ctx, &out)
	if workflow.IsCanceledError(err) {
		dctx, _ := workflow.NewDisconnectedContext(ctx)
		if cleanupErr := workflow.ExecuteActivity(dctx, activities.Cleanup, input, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(dctx, &out); cleanupErr != nil {
			return cleanupErr
		}
		return RESULT
	}
	return err
}
`

// (a)(b) 取消请求交给 Workflow 代码：清理 Activity 在 disconnected context 上运行完成，返回值决定终态。
func TestCancelE2E_CleanupThenReturn(t *testing.T) {
	for name, tc := range map[string]struct {
		result string
		status historypkg.Status
	}{
		"returns canceled error": {"err", historypkg.StatusCancelled},
		"returns nil":            {"nil", historypkg.StatusCompleted},
	} {
		t.Run(name, func(t *testing.T) {
			h := newCancelE2E(t)
			h.register("cleanup-flow", strings.Replace(cleanupWorkflowSrc, "RESULT", tc.result, 1))
			runID := h.start("cleanup-flow", map[string]any{})
			h.waitStarted()
			h.cancel(runID)
			waitForServerRunStatus(t, h.server, runID, tc.status, 10*time.Second)
			if h.cleaned.Load() != 1 {
				t.Fatalf("cleanup ran %d times, want 1", h.cleaned.Load())
			}
			events := h.events(runID)
			if eventIndex(events, historypkg.EventActivityCancelRequested) < 0 {
				t.Fatalf("no ActivityCancelRequested in %+v", events)
			}
		})
	}
}

// (c) 不理会取消：Activity 在 disconnected context 上，取消请求不影响它，Workflow 完成。
func TestCancelE2E_IgnoredCancellationCompletes(t *testing.T) {
	h := newCancelE2E(t)
	h.register("ignore-flow", `package flows

import (
	"time"

	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/activities"
)

func W(ctx workflow.Context, input map[string]any) error {
	dctx, cancel := workflow.NewDisconnectedContext(ctx)
	defer cancel()
	if err := workflow.Sleep(dctx, 200*time.Millisecond); err != nil {
		return err
	}
	var out map[string]any
	return workflow.ExecuteActivity(dctx, activities.Fast, input, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(dctx, &out)
}
`)
	runID := h.start("ignore-flow", map[string]any{})
	waitForServerEventCount(t, h.server, runID, historypkg.EventTimerScheduled, 1, 5*time.Second)
	h.cancel(runID)
	if status, err := h.server.GetRunStatus(runID); err != nil || status != historypkg.StatusRunning {
		t.Fatalf("status after cancel request = %s err=%v, want running", status, err)
	}
	waitForServerRunStatus(t, h.server, runID, historypkg.StatusCompleted, 10*time.Second)
	events := h.events(runID)
	if eventIndex(events, historypkg.EventActivityCancelRequested) >= 0 || eventIndex(events, historypkg.EventTimerCancelled) >= 0 {
		t.Fatalf("disconnected work was cancelled: %+v", events)
	}
}

// (d) WithCancel 作用域取消两个并行 Activity 中的一个：另一个完成后取消作用域，Workflow 完成。
func TestCancelE2E_ScopeCancelsOneOfTwoActivities(t *testing.T) {
	h := newCancelE2E(t)
	h.register("scope-flow", `package flows

import (
	"time"

	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/activities"
)

func W(ctx workflow.Context, input map[string]any) error {
	cctx, cancel := workflow.WithCancel(ctx)
	defer cancel()
	slow := workflow.ExecuteActivity(cctx, activities.Block, input, workflow.ActivityOptions{StartToCloseTimeout: time.Minute, HeartbeatTimeout: 10 * time.Second, WaitForCancellation: true})
	var out map[string]any
	if err := workflow.ExecuteActivity(ctx, activities.Fast, input, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &out); err != nil {
		return err
	}
	cancel()
	if err := slow.Get(ctx, &out); !workflow.IsCanceledError(err) {
		return workflow.NewApplicationError("slow activity was not cancelled", "Test")
	}
	return nil
}
`)
	runID := h.start("scope-flow", map[string]any{})
	waitForServerRunStatus(t, h.server, runID, historypkg.StatusCompleted, 10*time.Second)
	events := h.events(runID)
	if eventIndex(events, historypkg.EventActivityCancelled) < 0 || eventIndex(events, historypkg.EventActivityCompleted) < 0 {
		t.Fatalf("history = %+v, want one activity completed and the other cancelled", events)
	}
}

// (f) 心跳的 Activity：WaitForCancellation=true 时 Workflow 在 ActivityCancelled（带 details）之后结束；
// false 时取消请求一发出就结束，Activity 的确认晚于 Run 关闭、不再记录。
func TestCancelE2E_WaitForCancellation(t *testing.T) {
	const src = `package flows

import (
	"time"

	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/activities"
)

func W(ctx workflow.Context, input map[string]any) error {
	var out map[string]any
	return workflow.ExecuteActivity(ctx, activities.Block, input, workflow.ActivityOptions{StartToCloseTimeout: time.Minute, HeartbeatTimeout: 10 * time.Second, WaitForCancellation: WAIT}).Get(ctx, &out)
}
`
	t.Run("wait", func(t *testing.T) {
		h := newCancelE2E(t)
		h.register("wait-flow", strings.Replace(src, "WAIT", "true", 1))
		runID := h.start("wait-flow", map[string]any{"linger": true})
		h.waitStarted()
		h.cancel(runID)
		waitForServerRunStatus(t, h.server, runID, historypkg.StatusCancelled, 10*time.Second)
		events := h.events(runID)
		cancelled, closed := eventIndex(events, historypkg.EventActivityCancelled), eventIndex(events, historypkg.EventWorkflowCancelled)
		if cancelled < 0 || closed < cancelled {
			t.Fatalf("history = %+v, want ActivityCancelled before WorkflowCancelled", events)
		}
		if details := events[cancelled].ActivityCancelled.Details; len(details) != 1 {
			t.Fatalf("ActivityCancelled details = %+v, want the activity's canceled error details", details)
		}
	})
	t.Run("no wait", func(t *testing.T) {
		h := newCancelE2E(t)
		h.register("nowait-flow", strings.Replace(src, "WAIT", "false", 1))
		runID := h.start("nowait-flow", map[string]any{"linger": true})
		h.waitStarted()
		h.cancel(runID)
		waitForServerRunStatus(t, h.server, runID, historypkg.StatusCancelled, 10*time.Second)
		events := h.events(runID)
		if cancelled := eventIndex(events, historypkg.EventActivityCancelled); cancelled >= 0 && cancelled < eventIndex(events, historypkg.EventWorkflowCancelled) {
			t.Fatalf("history = %+v, want the workflow to close without waiting for the activity", events)
		}
	})
}

// (g) 尚未开始的 Activity 由 Server 立即取消：Worker 只有一个 Activity 槽位，第二个 Activity 排在队列里；
// 取消时它与请求同事务以 ActivityCancelled 结束，从未开始执行。
func TestCancelE2E_NotStartedActivityCancelledByServer(t *testing.T) {
	h := newCancelE2EWithSlots(t, 1)
	h.register("queued-flow", `package flows

import (
	"time"

	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/activities"
)

func W(ctx workflow.Context, input map[string]any) error {
	opts := workflow.ActivityOptions{StartToCloseTimeout: time.Minute, HeartbeatTimeout: 10 * time.Second, WaitForCancellation: true}
	running := workflow.ExecuteActivity(ctx, activities.Block, input, opts)
	queued := workflow.ExecuteActivity(ctx, activities.Block, input, opts)
	var out map[string]any
	firstErr := running.Get(ctx, &out)
	secondErr := queued.Get(ctx, &out)
	if workflow.IsCanceledError(firstErr) && workflow.IsCanceledError(secondErr) {
		return firstErr
	}
	return workflow.NewApplicationError("activities were not cancelled", "Test")
}
`)
	runID := h.start("queued-flow", map[string]any{})
	h.waitStarted()
	h.cancel(runID)
	waitForServerRunStatus(t, h.server, runID, historypkg.StatusCancelled, 10*time.Second)
	events := h.events(runID)
	if n := countEvent(events, historypkg.EventActivityCancelled); n != 2 {
		t.Fatalf("ActivityCancelled count = %d, want 2 in %+v", n, events)
	}
	if n := countEvent(events, historypkg.EventActivityStarted); n > 1 {
		t.Fatalf("queued activity started: %d ActivityStarted events", n)
	}
	select {
	case id := <-h.started:
		t.Fatalf("queued activity %s executed after cancellation", id)
	default:
	}
}

// (h) 子 Workflow 的取消传播：父 Workflow 被取消 → 子 Workflow 收到取消请求并以 Cancelled 结束。
// WaitForCancellation=true 时父 Run 等子 Workflow 结束；false 时父 Run 先关闭，之后父关闭策略处理子 Run。
func TestCancelE2E_ChildCancellation(t *testing.T) {
	const childSrc = `package flows

import (
	"time"

	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/activities"
)

func W(ctx workflow.Context, input map[string]any) error {
	var out map[string]any
	return workflow.ExecuteActivity(ctx, activities.Block, input, workflow.ActivityOptions{StartToCloseTimeout: time.Minute, HeartbeatTimeout: 10 * time.Second, WaitForCancellation: true}).Get(ctx, &out)
}
`
	const parentSrc = `package flows

import workflow "github.com/donutnomad/tenuto/sdk/workflow"

func W(ctx workflow.Context, input map[string]any) error {
	var out map[string]any
	return workflow.ExecuteChildWorkflow(ctx, "child-flow", input, workflow.ChildWorkflowOptions{WaitForCancellation: WAIT, ParentClosePolicy: POLICY}).Get(ctx, &out)
}
`
	policies := map[string]string{
		"terminate":      "workflow.ParentClosePolicyTerminate",
		"request_cancel": "workflow.ParentClosePolicyRequestCancel",
		"abandon":        "workflow.ParentClosePolicyAbandon",
	}
	for _, wait := range []string{"true", "false"} {
		for policyName, policy := range policies {
			t.Run("wait="+wait+"/"+policyName, func(t *testing.T) {
				h := newCancelE2E(t)
				h.register("child-flow", childSrc)
				h.register("parent-flow", strings.NewReplacer("WAIT", wait, "POLICY", policy).Replace(parentSrc))
				parentID := h.start("parent-flow", map[string]any{})
				h.waitStarted()
				h.cancel(parentID)
				waitForServerRunStatus(t, h.server, parentID, historypkg.StatusCancelled, 10*time.Second)

				parentEvents := h.events(parentID)
				requested := eventIndex(parentEvents, historypkg.EventChildWorkflowCancelRequested)
				if requested < 0 {
					t.Fatalf("parent history = %+v, want ChildWorkflowCancelRequested", parentEvents)
				}
				childRunID := parentEvents[requested].ChildWorkflowCancelRequested.ChildRunID
				childClosed := eventIndex(parentEvents, historypkg.EventChildWorkflowCancelled)
				if wait == "true" && (childClosed < 0 || childClosed > eventIndex(parentEvents, historypkg.EventWorkflowCancelled)) {
					t.Fatalf("parent history = %+v, want the parent to wait for ChildWorkflowCancelled", parentEvents)
				}
				// 子 Workflow 自己处理取消；不等子 Workflow 的父 Run 关闭时，Terminate 策略可能先终止它。
				deadline := time.Now().Add(10 * time.Second)
				for {
					status, err := h.server.GetRunStatus(childRunID)
					if err != nil {
						t.Fatal(err)
					}
					if status == historypkg.StatusCancelled || (status == historypkg.StatusTerminated && wait == "false" && policyName == "terminate") {
						break
					}
					if time.Now().After(deadline) {
						t.Fatalf("child status = %s, want cancelled", status)
					}
					time.Sleep(10 * time.Millisecond)
				}
				childEvents := h.events(childRunID)
				if n := countEvent(childEvents, historypkg.EventWorkflowCancellationRequested); n != 1 {
					t.Fatalf("child received %d cancellation requests, want 1", n)
				}
			})
		}
	}
}
