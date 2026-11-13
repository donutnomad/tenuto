// Command app1 是 multiprocess 示例的 Activity 进程：携带 order-flow 的 v1 / v2 / v3 三代
// Workflow 定义与本地 activity（orders.finalize），按 -version 选择随二进制发布的定义并登记。
// Workflow Task 由 tenuto-server 进程内执行，本进程只 poll orders 队列。
//
// 它不实现 inventory / ledger 队列上的任何 activity——那些由 app-2 进程承接。两个进程的
// 启动顺序互不依赖：Run 走到尚无 Worker 的 activity 时等待，app-2 上线后自动继续。
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"testing/fstest"
	"time"

	v1 "github.com/donutnomad/tenuto/docs/examples/multiprocess/v1"
	v2 "github.com/donutnomad/tenuto/docs/examples/multiprocess/v2"
	v3 "github.com/donutnomad/tenuto/docs/examples/multiprocess/v3"
	wfsdk "github.com/donutnomad/tenuto/sdk/activity"
	"github.com/donutnomad/tenuto/sdk/client"
	"github.com/donutnomad/tenuto/sdk/registration"
	"github.com/donutnomad/tenuto/sdk/worker"
)

func main() {
	url := flag.String("url", envOr("TENUTO_URL", "http://localhost:7400"), "Tenuto Server HTTP 地址")
	namespace := flag.String("namespace", envOr("TENUTO_NAMESPACE", "multiprocess-demo"), "namespace")
	version := flag.String("version", envOr("APP1_VERSION", "v2"), "要登记的 Workflow 版本：v1 / v2 / v3")
	buildID := flag.String("build-id", envOr("APP1_BUILD_ID", ""), "Activity worker build ID（默认 app1-<version>）")
	flag.Parse()
	if *buildID == "" {
		*buildID = "app1-" + *version
	}
	if err := run(*url, *namespace, *version, *buildID); err != nil {
		log.Fatalf("app1: %v", err)
	}
}

func run(url, namespace, version, buildID string) error {
	definition, finalize, err := selectVersion(version)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// 每个升级验证保留独立 current run；仅示例登记独立 Workflow ID。
	workflowID, data, err := definition.Load()
	if err != nil {
		return err
	}
	definitions := []registration.Definition{definition}
	for _, suffix := range []string{"-incompat-v1", "-incompat-v2"} {
		var config map[string]json.RawMessage
		if err := json.Unmarshal(data, &config); err != nil {
			return err
		}
		config["id"], _ = json.Marshal(workflowID + suffix)
		encoded, err := json.Marshal(config)
		if err != nil {
			return err
		}
		definitions = append(definitions, registration.Definition{FS: fstest.MapFS{registration.ArtifactFile: &fstest.MapFile{Data: encoded}}})
	}

	// Start 先上报本进程 Activity 路由，再登记定义，最后开始 poll：定义登记时的路由校验
	// 以 worker 上报的 activity 路由为依据。
	c, err := client.Dial(client.Options{HostPort: url, Namespace: namespace})
	if err != nil {
		return err
	}
	defer c.Close()
	registry := wfsdk.NewRegistry()
	if err := finalize(registry); err != nil {
		return fmt.Errorf("register activities: %w", err)
	}
	w, err := worker.New(c, worker.Options{
		Role:                                   worker.RoleActivity,
		Activities:                             registry,
		ActivityTaskQueues:                     []string{"orders"},
		Identity:                               "app1-activity-" + version,
		BuildID:                                buildID,
		ActivityWorkerVersionDefault:           true,
		ActivityWorkerVersionHeartbeatInterval: 5 * time.Second,
		Workflows:                              definitions,
	})
	if err != nil {
		return err
	}
	w.Start(ctx)
	defer w.Stop()
	readyCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	if err := w.WaitReady(readyCtx); err != nil {
		if ctx.Err() != nil {
			return nil // 启动途中收到退出信号，按正常退出处理
		}
		return fmt.Errorf("worker ready: %w", err)
	}
	log.Printf("ready namespace=%s workflow=order-flow version=%s build=%s", namespace, version, buildID)
	<-ctx.Done()
	return nil
}

// selectVersion 返回指定版本的 Workflow 定义与该版本包里本地 activity 的注册入口。
func selectVersion(version string) (registration.Definition, func(*wfsdk.Registry) error, error) {
	switch version {
	case "v1":
		return v1.WorkflowDefinition(), v1.RegisterActivities, nil
	case "v2":
		return v2.WorkflowDefinition(), v2.RegisterActivities, nil
	case "v3":
		return v3.WorkflowDefinition(), v3.RegisterActivities, nil
	}
	return registration.Definition{}, nil, fmt.Errorf("unknown version %q (want v1, v2 or v3)", version)
}

func envOr(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
