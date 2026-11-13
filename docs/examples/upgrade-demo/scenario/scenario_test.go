package scenario_test

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/docs/examples/upgrade-demo/appa"
	"github.com/donutnomad/tenuto/docs/examples/upgrade-demo/appb"
	"github.com/donutnomad/tenuto/docs/examples/upgrade-demo/scenario"
	"github.com/donutnomad/tenuto/sdk/worker"
	"github.com/donutnomad/tenuto/server/app/assembly"
	"github.com/donutnomad/tenuto/server/http/runtimehttp"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

const testNamespace = "upgrade-demo-test"

// TestUpgradeScenarios 在一个进程里跑完整套场景：内存后端的 Server、真实 HTTP、
// 与 cmd/app-a / cmd/app-b 相同的 appa.Start / appb.Start。
func TestUpgradeScenarios(t *testing.T) {
	url := startServer(t)
	fleet := &inProcessFleet{url: url, workers: map[string]*worker.Worker{}}
	t.Cleanup(fleet.stopAll)

	var transcript bytes.Buffer
	env := &scenario.Env{URL: url, Namespace: testNamespace, Fleet: fleet, Out: &transcript, Timeout: 20 * time.Second}
	err := env.Run()
	t.Log("\n" + transcript.String())
	if err != nil {
		t.Fatal(err)
	}
}

// startServer 用真实 HTTP 暴露一个内存后端的 Server。Server 自己不执行 Workflow Task。
// 组装与 tenuto-server 的 WORKFLOW_BACKEND=memory 相同：内存存储、内存 Workflow Task 队列、
// 按 Namespace + Task Queue + BuildID 投递的内存 Activity 队列。
func startServer(t *testing.T) string {
	t.Helper()
	server := assembly.NewServer(assembly.Options{Stores: assembly.Stores{Executions: memory.NewExecutionManager()}, WorkflowQueue: memory.NewTaskQueue(1024), ActivityRouter: memory.NewActivityQueueRouter(1024)})
	handler := runtimehttp.NewRuntimeHTTPHandler(server)
	// 长轮询跟随 serverCtx 结束，httptest.Server.Close 才不会等到 poll 超时。
	serverCtx, cancel := context.WithCancel(context.Background())
	httpServer := httptest.NewUnstartedServer(handler)
	httpServer.Config.BaseContext = func(net.Listener) context.Context { return serverCtx }
	httpServer.Start()
	t.Cleanup(func() {
		cancel()
		httpServer.Close()
		server.Stop()
	})
	return httpServer.URL
}

// inProcessFleet 把每个「进程」实现成本进程里的一个 Worker，接入代码与真实进程相同。
type inProcessFleet struct {
	url     string
	mu      sync.Mutex
	workers map[string]*worker.Worker
}

func (f *inProcessFleet) Start(ctx context.Context, name, release string) error {
	var w *worker.Worker
	var err error
	switch name {
	case "app-a":
		w, err = appa.Start(ctx, appa.Options{URL: f.url, Namespace: testNamespace, Release: release})
	case "app-b":
		w, err = appb.Start(ctx, appb.Options{URL: f.url, Namespace: testNamespace, Release: release})
	default:
		return fmt.Errorf("unknown app %q", name)
	}
	if err != nil {
		return err
	}
	f.mu.Lock()
	f.workers[name+"/"+release] = w
	f.mu.Unlock()
	return nil
}

func (f *inProcessFleet) Stop(name, release string) error {
	f.mu.Lock()
	w := f.workers[name+"/"+release]
	delete(f.workers, name+"/"+release)
	f.mu.Unlock()
	if w == nil {
		return fmt.Errorf("%s %s is not running", name, release)
	}
	w.Stop()
	return nil
}

func (f *inProcessFleet) stopAll() {
	f.mu.Lock()
	defer f.mu.Unlock()
	for key, w := range f.workers {
		w.Stop()
		delete(f.workers, key)
	}
}
