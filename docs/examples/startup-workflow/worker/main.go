// Command worker 是最短的作者启动路径：正常配置并启动 Worker，
// Workflow 定义已经随二进制发布，Start 时自动登记。
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	startupflow "github.com/donutnomad/tenuto/docs/examples/startup-workflow"
	"github.com/donutnomad/tenuto/sdk/activity"
	"github.com/donutnomad/tenuto/sdk/client"
	"github.com/donutnomad/tenuto/sdk/registration"
	"github.com/donutnomad/tenuto/sdk/worker"
)

func main() {
	url := flag.String("url", envOr("TENUTO_URL", "http://localhost:7400"), "Tenuto Server HTTP 地址")
	namespace := flag.String("namespace", envOr("TENUTO_NAMESPACE", "startup-demo"), "namespace")
	flag.Parse()
	if err := run(*url, *namespace); err != nil {
		log.Fatalf("startup example worker: %v", err)
	}
}

func run(url, namespace string) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	c, err := client.Dial(client.Options{HostPort: url, Namespace: namespace})
	if err != nil {
		return err
	}
	defer c.Close()

	registry := activity.NewRegistry()
	// 一行登记本包全部 @Activity 实现；每个实现的契约随 registry 在 Worker 启动时上报。
	if err := startupflow.RegisterActivities(registry); err != nil {
		return fmt.Errorf("register activities: %w", err)
	}
	// 一个 Worker 同时执行 Workflow 与 Activity。Start 先上报 Activity 路由，再登记本包生成的
	// Workflow 定义（登记时的路由校验以上报的路由为依据），然后开始拉取任务；登记失败时
	// 不拉取任务，错误由 WaitReady 返回。
	w, err := worker.New(c, worker.Options{
		Workflows:                              []registration.Definition{startupflow.WorkflowDefinition()},
		Activities:                             registry,
		Identity:                               "startup-worker-1",
		BuildID:                                "startup-v1",
		ActivityWorkerVersionDefault:           true,
		ActivityWorkerVersionHeartbeatInterval: 5 * time.Second,
	})
	if err != nil {
		return err
	}
	w.Start(ctx)
	defer w.Stop()
	readyCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := w.WaitReady(readyCtx); err != nil {
		if ctx.Err() != nil {
			return nil // 启动途中收到退出信号，按正常退出处理
		}
		return fmt.Errorf("worker ready: %w", err)
	}
	log.Printf("ready namespace=%s workflow=startup-greeting", namespace)
	<-ctx.Done()
	return nil
}

func envOr(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
