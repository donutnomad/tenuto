package client_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"testing/fstest"
	"time"

	"github.com/donutnomad/tenuto/compiler/sdkgen"
	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/sdk/activity"
	"github.com/donutnomad/tenuto/sdk/client"
	"github.com/donutnomad/tenuto/sdk/registration"
	"github.com/donutnomad/tenuto/sdk/worker"
	"github.com/donutnomad/tenuto/server/app/assembly"
	"github.com/donutnomad/tenuto/server/biz/service/engine"
	"github.com/donutnomad/tenuto/server/http/runtimehttp"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

// testEnv 是一个内存后端的 Server（经真实 HTTP 暴露）、一个 Client，以及按需启动的 Worker。
type testEnv struct {
	t      *testing.T
	server *engine.Server
	// executions 是 Server 的存储；测试可以在它上面再装配一个 Server，模拟同一数据库上的另一个副本。
	executions *memory.ExecutionManager
	url        string
	client     client.Client
	counter    atomic.Int64
}

// newTestEnv 启动 Server 与 Client；withWorker 为 true 时再启动一个同时执行 Workflow 与 Activity 的 Worker。
// Activity：test.Count 返回自增计数；test.Cleanup 立即完成。
func newTestEnv(t *testing.T, withWorker bool) *testEnv {
	t.Helper()
	env := &testEnv{t: t, executions: memory.NewExecutionManager()}
	env.server = assembly.NewServer(assembly.Options{
		Stores:        assembly.Stores{Executions: env.executions},
		WorkflowQueue: memory.NewTaskQueue(256), ActivityRouter: memory.NewActivityQueueRouter(256),
	})
	serverCtx, stopServer := context.WithCancel(context.Background())
	httpServer := httptest.NewUnstartedServer(runtimehttp.NewRuntimeHTTPHandler(env.server))
	httpServer.Config.BaseContext = func(net.Listener) context.Context { return serverCtx }
	httpServer.Start()
	env.url = httpServer.URL
	c, err := client.Dial(client.Options{HostPort: env.url, Identity: "client-test"})
	if err != nil {
		t.Fatal(err)
	}
	env.client = c
	t.Cleanup(func() {
		c.Close()
		stopServer()
		httpServer.Close()
		env.server.Stop()
	})
	if withWorker {
		env.startWorker()
	}
	return env
}

func (e *testEnv) startWorker() {
	e.t.Helper()
	registry := activity.NewRegistry()
	must(e.t, registry.Register("test.Count", func(context.Context, map[string]any) (map[string]any, error) {
		return map[string]any{"n": e.counter.Add(1)}, nil
	}))
	must(e.t, registry.Register("test.Cleanup", func(context.Context, map[string]any) (map[string]any, error) {
		return map[string]any{"cleaned": true}, nil
	}))
	w, err := worker.New(e.client, worker.Options{
		Activities: registry, WorkflowConcurrency: 4, ActivityConcurrency: 4,
		Identity: "client-test-worker", BuildID: "client-test",
	})
	must(e.t, err)
	w.Start(context.Background())
	e.t.Cleanup(w.Stop)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	must(e.t, w.WaitReady(ctx))
}

// register 从 Go 源码（入口函数 W）生成 Workflow 并登记为 workflowType；源码里的 activities.X 解析为 test.X。
// configure 可以在登记前修改配置（超时、重试策略）。
func (e *testEnv) register(workflowType, src string, configure ...func(*program.WorkflowConfig)) {
	e.t.Helper()
	config := generateWorkflow(e.t, workflowType, src)
	for _, apply := range configure {
		apply(config)
	}
	if _, err := e.server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, config); err != nil {
		e.t.Fatal(err)
	}
}

func generateWorkflow(t testing.TB, workflowType, src string) *program.WorkflowConfig {
	t.Helper()
	config, err := workflowConfig(workflowType, src)
	if err != nil {
		t.Fatal(err)
	}
	return config
}

// workflowConfig 从 Go 源码（入口函数 W）生成 Workflow 配置；源码里的 activities.X 解析为 test.X。
func workflowConfig(workflowType, src string) (*program.WorkflowConfig, error) {
	generated, err := sdkgen.GenerateFromSource([]byte(src), sdkgen.Options{WorkflowType: workflowType, FuncName: "W", Filename: "w.go"})
	if err != nil {
		return nil, fmt.Errorf("generate %s: %w", workflowType, err)
	}
	for symbol, ref := range generated.Workflow.Activities {
		ref.Type = "test." + strings.TrimPrefix(symbol, "activities.")
		generated.Workflow.Activities[symbol] = ref
	}
	return generated.Workflow, nil
}

// definitionOf 把生成的配置包装成 Worker 启动时登记的定义。
func definitionOf(config *program.WorkflowConfig) (registration.Definition, error) {
	data, err := json.Marshal(config)
	if err != nil {
		return registration.Definition{}, err
	}
	return registration.Definition{FS: fstest.MapFS{registration.ArtifactFile: &fstest.MapFile{Data: data}}}, nil
}

func (e *testEnv) execute(workflowType, workflowID string, args ...any) client.WorkflowRun {
	e.t.Helper()
	run, err := e.client.ExecuteWorkflow(context.Background(), client.StartWorkflowOptions{ID: workflowID}, workflowType, args...)
	if err != nil {
		e.t.Fatalf("ExecuteWorkflow %s: %v", workflowType, err)
	}
	return run
}

// get 在 10s 内等 run 结束。
func get(t *testing.T, run client.WorkflowRun, valuePtr any) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return run.Get(ctx, valuePtr)
}

// eventually 每 10ms 检查一次 cond，5s 内不成立时失败。
func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func must(t testing.TB, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

// 测试用的 Workflow 源码。入口函数都叫 W。
const (
	greetingSrc = `package flows

import workflow "github.com/donutnomad/tenuto/sdk/workflow"

type GreetingIn struct {
	Name string ` + "`json:\"name\"`" + `
	Big  int64  ` + "`json:\"big\"`" + `
}

type GreetingOut struct {
	Greeting string ` + "`json:\"greeting\"`" + `
	Big      int64  ` + "`json:\"big\"`" + `
}

func W(ctx workflow.Context, in GreetingIn) (GreetingOut, error) {
	return GreetingOut{Greeting: "hello " + in.Name, Big: in.Big}, nil
}
`

	failingSrc = `package flows

import workflow "github.com/donutnomad/tenuto/sdk/workflow"

func W(ctx workflow.Context, input map[string]any) error {
	return workflow.NewNonRetryableApplicationError("card declined", "PaymentDeclined", nil, map[string]any{"code": "insufficient_funds"})
}
`

	// waitingSrc 收到两个 add Signal 后完成；Query count 返回已收到的个数。
	waitingSrc = `package flows

import workflow "github.com/donutnomad/tenuto/sdk/workflow"

func W(ctx workflow.Context, input map[string]any) (map[string]any, error) {
	count := 0
	last := ""
	if err := workflow.SetQueryHandler(ctx, "count", func(ctx workflow.Context, input map[string]any) map[string]any {
		return map[string]any{"count": count, "last": last}
	}); err != nil {
		return nil, err
	}
	for i := 0; i < 2; i++ {
		var payload map[string]any
		if err := workflow.WaitSignal(ctx, "add").Get(ctx, &payload); err != nil {
			return nil, err
		}
		last, _ = payload["value"].(string)
		count = count + 1
	}
	return map[string]any{"count": count, "last": last}, nil
}
`

	// awaitSrc 一直等待，取消时返回取消错误。
	awaitSrc = `package flows

import workflow "github.com/donutnomad/tenuto/sdk/workflow"

func W(ctx workflow.Context, input map[string]any) error {
	return workflow.Await(ctx, func() bool { return false })
}
`

	// cleanupSrc 等到取消，然后在 disconnected context 上执行清理 Activity 并正常完成。
	cleanupSrc = `package flows

import (
	"time"

	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/activities"
)

func W(ctx workflow.Context, input map[string]any) (map[string]any, error) {
	if err := workflow.Await(ctx, func() bool { return false }); err != nil {
		dctx, _ := workflow.NewDisconnectedContext(ctx)
		var cleaned map[string]any
		if cleanupErr := workflow.ExecuteActivity(dctx, activities.Cleanup, input, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(dctx, &cleaned); cleanupErr != nil {
			return nil, cleanupErr
		}
		return cleaned, nil
	}
	return nil, nil
}
`

	// continueAsNewSrc 连续 continue-as-new 两次，第三个 Run 返回 n。
	continueAsNewSrc = `package flows

import workflow "github.com/donutnomad/tenuto/sdk/workflow"

type Counter struct {
	N int64 ` + "`json:\"n\"`" + `
}

func W(ctx workflow.Context, in Counter) (Counter, error) {
	if in.N < 2 {
		return Counter{}, workflow.ContinueAsNew(ctx, "", map[string]any{"n": in.N + 1})
	}
	return in, nil
}
`

	// countingSrc 调用 test.Count；计数小于 threshold 时以可重试的应用错误失败。
	countingSrc = `package flows

import (
	"time"

	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/activities"
)

type Count struct {
	N int64 ` + "`json:\"n\"`" + `
}

func W(ctx workflow.Context, input map[string]any) (Count, error) {
	var out Count
	if err := workflow.ExecuteActivity(ctx, activities.Count, input, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &out); err != nil {
		return Count{}, err
	}
	if out.N < THRESHOLD {
		return Count{}, workflow.NewApplicationError("attempt failed", "Flaky")
	}
	return out, nil
}
`

	// updatesSrc 的 set-limit 修改并返回限额，负数时返回应用错误；finish 让 Workflow 完成；stuck 永远不返回。
	updatesSrc = `package flows

import workflow "github.com/donutnomad/tenuto/sdk/workflow"

type LimitResult struct {
	Previous int64 ` + "`json:\"previous\"`" + `
	Current  int64 ` + "`json:\"current\"`" + `
}

func W(ctx workflow.Context, input map[string]any) (map[string]any, error) {
	limit := int64(100)
	done := false
	if err := workflow.SetUpdateHandler(ctx, "set-limit", func(ctx workflow.Context, next int64) (LimitResult, error) {
		if next < 0 {
			return LimitResult{}, workflow.NewApplicationError("limit must not be negative", "InvalidLimit")
		}
		previous := limit
		limit = next
		return LimitResult{Previous: previous, Current: limit}, nil
	}); err != nil {
		return nil, err
	}
	if err := workflow.SetUpdateHandler(ctx, "finish", func(ctx workflow.Context) (LimitResult, error) {
		done = true
		return LimitResult{Current: limit}, nil
	}); err != nil {
		return nil, err
	}
	if err := workflow.SetUpdateHandler(ctx, "stuck", func(ctx workflow.Context) (LimitResult, error) {
		err := workflow.Await(ctx, func() bool { return false })
		return LimitResult{}, err
	}); err != nil {
		return nil, err
	}
	if err := workflow.Await(ctx, func() bool { return done }); err != nil {
		return nil, err
	}
	return map[string]any{"limit": limit}, nil
}
`
)
