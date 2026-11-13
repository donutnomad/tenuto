package app

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/compiler/sdkgen"
	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/runtimeapi"
	"github.com/donutnomad/tenuto/sdk/client"
	"github.com/donutnomad/tenuto/sdk/worker"
)

func TestNewApplicationMemoryBackendUsesOfficialRuntimeSurface(t *testing.T) {
	cfg, err := loadConfig(mapLookup(map[string]string{
		"WORKFLOW_BACKEND":   "memory",
		"WORKFLOW_HTTP_ADDR": "127.0.0.1:0",
	}), "demo-node")
	if err != nil {
		t.Fatal(err)
	}
	app, err := newApplication(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer app.close()
	if app.server == nil || app.workflowStore != nil || app.taskQStore != nil {
		t.Fatal("memory application assembled infrastructure stores")
	}

	app.health.ready.Store(true)
	recorder := httptest.NewRecorder()
	app.httpServer.Handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/health/ready", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("readiness status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
}

// Integration：默认配置下 tenuto-server 自己消费 WFT，不需要任何外部 workflow worker。
func TestRunEmbeddedWorkflowWorkerCompletesWorkflow(t *testing.T) {
	url := runMemoryServer(t, nil)
	c := dialMemoryServer(t, url)

	run := startEmptyWorkflow(t, url, c)

	waitRunCompleted(t, run)
}

// Integration：关闭开关后 WFT 留在队列，由独立 workflow worker 接管。
func TestRunDisabledEmbeddedWorkflowWorkerLeavesWorkflowTasks(t *testing.T) {
	url := runMemoryServer(t, map[string]string{"WORKFLOW_WORKER_ENABLED": "false"})
	c := dialMemoryServer(t, url)

	run := startEmptyWorkflow(t, url, c)

	time.Sleep(300 * time.Millisecond)
	description, err := c.DescribeWorkflowExecution(t.Context(), run.GetID(), run.GetRunID())
	if err != nil {
		t.Fatal(err)
	}
	if description.Status != client.WorkflowExecutionStatusRunning {
		t.Fatalf("run status = %s, want running while no worker consumes workflow tasks", description.Status)
	}

	w, err := worker.New(c, worker.Options{
		Role:     worker.RoleWorkflow,
		Identity: "standalone-workflow-worker",
		BuildID:  "standalone",
	})
	if err != nil {
		t.Fatal(err)
	}
	w.Start(t.Context())
	t.Cleanup(w.Stop)

	waitRunCompleted(t, run)
}

func dialMemoryServer(t *testing.T, url string) client.Client {
	t.Helper()
	c, err := client.Dial(client.Options{HostPort: url})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c
}

// runMemoryServer 用 memory 后端跑完整的 Run，就绪后返回 HTTP 地址；测试结束时走正常关闭。
func runMemoryServer(t *testing.T, overrides map[string]string) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	env := map[string]string{"WORKFLOW_BACKEND": "memory", "WORKFLOW_HTTP_ADDR": address}
	for name, value := range overrides {
		env[name] = value
	}
	cfg, err := loadConfig(mapLookup(env), "embedded-node")
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan error, 1)
	go func() { stopped <- Run(ctx, cfg) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-stopped:
			if err != nil {
				t.Errorf("Run: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Error("Run did not stop")
		}
	})

	url := "http://" + address
	deadline := time.Now().Add(5 * time.Second)
	for {
		response, err := http.Get(url + "/health/ready")
		if err == nil {
			_ = response.Body.Close()
			if response.StatusCode == http.StatusOK {
				return url
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("server not ready: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func startEmptyWorkflow(t *testing.T, url string, c client.Client) client.WorkflowRun {
	t.Helper()
	const source = `package flows

import (
	"time"

	workflow "github.com/donutnomad/tenuto/sdk/workflow"
)

func Flow(ctx workflow.Context) error {
	return nil
}
`
	generated, err := sdkgen.GenerateFromSource([]byte(source), sdkgen.Options{
		WorkflowID: "embedded-flow",
		Name:       "embedded-flow",
		FuncName:   "Flow",
		Filename:   "flow.go",
	})
	if err != nil {
		t.Fatal(err)
	}
	registerWorkflow(t, url, generated.Workflow)
	run, err := c.ExecuteWorkflow(t.Context(), client.StartWorkflowOptions{ID: "embedded-flow"}, "embedded-flow")
	if err != nil {
		t.Fatal(err)
	}
	return run
}

// registerWorkflow 经运维端点登记运行时生成的配置并设为 latest。
func registerWorkflow(t *testing.T, url string, workflow *program.WorkflowConfig) {
	t.Helper()
	data, err := json.Marshal(workflow)
	if err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(runtimeapi.WorkflowRuntimeRegisterRequest{Namespace: protocol.DefaultNamespace, Workflow: data})
	if err != nil {
		t.Fatal(err)
	}
	response, err := http.Post(url+runtimeapi.WorkflowRuntimeRegisterPath, "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		text, _ := io.ReadAll(response.Body)
		t.Fatalf("register workflow: %s: %s", response.Status, text)
	}
}

func waitRunCompleted(t *testing.T, run client.WorkflowRun) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := run.Get(ctx, nil); err != nil {
		t.Fatalf("run %s: %v", run.GetRunID(), err)
	}
}

// WORKFLOW_PPROF_ADDR 开启独立的 pprof 监听；业务 HTTP 端口不提供 /debug/pprof/。缺省不开启。
func TestPprofListenerIsOptInAndSeparate(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	pprofAddr := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	url := runMemoryServer(t, map[string]string{"WORKFLOW_PPROF_ADDR": pprofAddr})

	response, err := http.Get("http://" + pprofAddr + "/debug/pprof/goroutine?debug=1")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if response.StatusCode != http.StatusOK || !bytes.Contains(body, []byte("goroutine profile")) {
		t.Fatalf("pprof status = %d, body = %.200s", response.StatusCode, body)
	}
	response, err = http.Get(url + "/debug/pprof/")
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode == http.StatusOK {
		t.Fatal("business HTTP port serves /debug/pprof/")
	}

	cfg, err := loadConfig(mapLookup(map[string]string{"WORKFLOW_BACKEND": "memory"}), "node")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.PprofAddr != "" {
		t.Fatalf("PprofAddr = %q by default, want disabled", cfg.PprofAddr)
	}
	if _, err := loadConfig(mapLookup(map[string]string{"WORKFLOW_BACKEND": "memory", "WORKFLOW_PPROF_ADDR": "6060"}), "node"); err == nil {
		t.Fatal("WORKFLOW_PPROF_ADDR without a port separator was accepted")
	}
}
