// Command worker 用真实双 Worker 启动顺序跑 saga 示例：
// Activity Worker 先 Start/WaitReady（Workflow 登记时的路由校验以 activity 路由为依据），
// Workflow Worker 后 Start/WaitReady，定义随二进制发布并在 Start 时自动登记。
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

	sagaflow "github.com/donutnomad/tenuto/docs/examples/saga-workflow"
	"github.com/donutnomad/tenuto/sdk/activity"
	"github.com/donutnomad/tenuto/sdk/client"
	"github.com/donutnomad/tenuto/sdk/registration"
	"github.com/donutnomad/tenuto/sdk/worker"
)

func main() {
	url := flag.String("url", envOr("TENUTO_URL", "http://localhost:7400"), "Tenuto Server HTTP 地址")
	namespace := flag.String("namespace", envOr("TENUTO_NAMESPACE", "saga-demo"), "namespace")
	flag.Parse()
	if err := run(*url, *namespace); err != nil {
		log.Fatalf("saga example worker: %v", err)
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
	if err := sagaflow.RegisterActivities(registry); err != nil {
		return fmt.Errorf("register activities: %w", err)
	}
	activityWorker, err := worker.New(c, worker.Options{
		Role:                                   worker.RoleActivity,
		Activities:                             registry,
		ActivityTaskQueues:                     []string{"saga"},
		Identity:                               "saga-activity-1",
		BuildID:                                "saga-v1",
		ActivityWorkerVersionDefault:           true,
		ActivityWorkerVersionHeartbeatInterval: 5 * time.Second,
	})
	if err != nil {
		return err
	}
	activityWorker.Start(ctx)
	defer activityWorker.Stop()

	readyCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := activityWorker.WaitReady(readyCtx); err != nil {
		if ctx.Err() != nil {
			return nil // 启动途中收到退出信号，按正常退出处理
		}
		return fmt.Errorf("activity worker ready: %w", err)
	}

	workflowWorker, err := worker.New(c, worker.Options{
		Role:                worker.RoleWorkflow,
		WorkflowConcurrency: 1,
		Identity:            "saga-workflow-1",
		BuildID:             "saga-v1",
		Workflows:           []registration.Definition{sagaflow.WorkflowDefinition()},
	})
	if err != nil {
		return err
	}
	workflowWorker.Start(ctx)
	defer workflowWorker.Stop()
	if err := workflowWorker.WaitReady(readyCtx); err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return fmt.Errorf("workflow worker ready: %w", err)
	}
	log.Printf("ready namespace=%s workflow=saga-order", namespace)
	<-ctx.Done()
	return nil
}

func envOr(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
