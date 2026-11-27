package integration

// 「Go 源码 → sdkgen → 真实 Worker」端到端测试的公共装置：内存后端的 Server，加上一个注册了测试 Activity 的 Worker。

import (
	"context"
	"strings"
	"sync"
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

const goWorkerActivityPrefix = "gw."

type goWorkerHarness struct {
	t        *testing.T
	server   *engine.Server
	attempts sync.Map // activity type → *atomic.Int32
	started  chan string
	release  chan struct{}
}

func (h *goWorkerHarness) count(name string) *atomic.Int32 {
	v, _ := h.attempts.LoadOrStore(name, new(atomic.Int32))
	return v.(*atomic.Int32)
}

// newGoWorkerHarness 启动 Server 与一个有 slots 个 Activity 槽位的 Worker。Activity：
//   - Sleep 睡 input["ms"] 毫秒，不理会 ctx；
//   - Fail 以 input["type"] 类型的应用错误失败；
//   - HB 心跳一次后停止心跳并睡 input["ms"]；
//   - Block 一直心跳，直到 ctx 结束或测试结束；
//   - Hang 模拟带 ctx 的外部调用对端挂起：只在 ctx 结束时返回（兜底 20s）；
//   - Echo 原样返回输入。
func newGoWorkerHarness(t *testing.T, slots int) *goWorkerHarness {
	t.Helper()
	h := &goWorkerHarness{t: t, started: make(chan string, 64), release: make(chan struct{})}
	registry := activity.NewRegistry()
	reg := func(name string, fn func(ctx context.Context, input map[string]any) (map[string]any, error)) {
		if err := registry.Register(goWorkerActivityPrefix+name, func(ctx context.Context, input map[string]any) (map[string]any, error) {
			h.count(name).Add(1)
			return fn(ctx, input)
		}); err != nil {
			t.Fatal(err)
		}
	}
	reg("Sleep", func(ctx context.Context, input map[string]any) (map[string]any, error) {
		ms, _ := input["ms"].(float64)
		if v, ok := input["ms"].(int64); ok {
			ms = float64(v)
		}
		time.Sleep(time.Duration(ms) * time.Millisecond)
		return map[string]any{"slept": ms}, nil
	})
	reg("Fail", func(ctx context.Context, input map[string]any) (map[string]any, error) {
		typ, _ := input["type"].(string)
		return nil, apperror.New("boom", typ, map[string]any{"attempt": activity.GetInfo(ctx).Attempt})
	})
	reg("HB", func(ctx context.Context, input map[string]any) (map[string]any, error) {
		activity.RecordHeartbeat(ctx, map[string]any{"progress": 7})
		ms, _ := input["ms"].(float64)
		time.Sleep(time.Duration(ms) * time.Millisecond)
		return map[string]any{}, nil
	})
	reg("Block", func(ctx context.Context, input map[string]any) (map[string]any, error) {
		h.started <- activity.GetInfo(ctx).ActivityID
		for ctx.Err() == nil {
			select {
			case <-h.release:
				return map[string]any{"released": true}, nil
			default:
			}
			activity.RecordHeartbeat(ctx, map[string]any{})
			time.Sleep(10 * time.Millisecond)
		}
		return nil, apperror.NewCanceled("stopped")
	})
	reg("Hang", func(ctx context.Context, input map[string]any) (map[string]any, error) {
		h.started <- "hang"
		select {
		case <-ctx.Done():
			h.count("HangCtxDone").Add(1)
			return nil, ctx.Err()
		case <-time.After(20 * time.Second):
			return nil, apperror.New("gave up", "Test")
		}
	})
	reg("Echo", func(ctx context.Context, input map[string]any) (map[string]any, error) {
		return input, nil
	})
	h.server = assembly.NewServer(assembly.Options{Stores: assembly.Stores{Executions: memory.NewExecutionManager()},
		WorkflowQueue: memory.NewTaskQueue(64), ActivityRouter: memory.NewActivityQueueRouter(64)})
	w := assembly.NewWorker(h.server, registry, worker.WorkerOptions{WorkflowConcurrency: 2, ActivityConcurrency: slots})
	w.Start(context.Background())
	t.Cleanup(func() {
		close(h.release)
		w.Stop()
		h.server.Stop()
	})
	return h
}

// register 把 src 里的 W 生成为 Workflow id，activities 包的调用路由到 harness 的测试 Activity。
func (h *goWorkerHarness) register(id, src string) {
	h.t.Helper()
	generated, err := sdkgen.GenerateFromSource([]byte(src), sdkgen.Options{WorkflowType: id, FuncName: "W", Filename: "w.go"})
	if err != nil {
		h.t.Fatalf("generate %s: %v", id, err)
	}
	for symbol, ref := range generated.Workflow.Activities {
		ref.Type = goWorkerActivityPrefix + strings.TrimPrefix(symbol, "activities.")
		generated.Workflow.Activities[symbol] = ref
	}
	if _, err := h.server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, generated.Workflow); err != nil {
		h.t.Fatal(err)
	}
}

func (h *goWorkerHarness) start(workflowType, workflowID string, input map[string]any) string {
	h.t.Helper()
	runID, err := startedRunID(h.server.StartWorkflow(context.Background(), "", workflowType, workflowID, "", input))
	if err != nil {
		h.t.Fatal(err)
	}
	return runID
}

func (h *goWorkerHarness) events(runID string) []historypkg.Event {
	h.t.Helper()
	events, err := h.server.GetEvents(runID)
	if err != nil {
		h.t.Fatal(err)
	}
	return events
}

func (h *goWorkerHarness) output(runID string) map[string]any {
	h.t.Helper()
	waitForServerRunStatusWithHistory(h.t, h.server, runID, historypkg.StatusCompleted, 20*time.Second)
	for _, e := range h.events(runID) {
		if e.WorkflowCompleted != nil {
			return e.WorkflowCompleted.Output
		}
	}
	h.t.Fatal("no WorkflowCompleted")
	return nil
}

func eventTypes(events []historypkg.Event) string {
	var b strings.Builder
	for _, e := range events {
		b.WriteString(string(e.EventType()))
		b.WriteString(" ")
	}
	return b.String()
}

func replaceAll(s string, kv ...string) string {
	for i := 0; i+1 < len(kv); i += 2 {
		s = strings.ReplaceAll(s, kv[i], kv[i+1])
	}
	return s
}

func jsonPayloads(t *testing.T, args ...any) protocol.Payloads {
	t.Helper()
	out := protocol.Payloads{}
	for _, a := range args {
		p, err := protocol.NewJSONPayload(a)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, p)
	}
	return out
}
