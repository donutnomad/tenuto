// Command app2 是 multiprocess 示例的纯 activity 进程：在 inventory 与 ledger 队列上领取任务，
// 实现 app-1 编排里调用的全部远程 activity。它不登记任何 Workflow。
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	wfsdk "github.com/donutnomad/tenuto/sdk/activity"
	"github.com/donutnomad/tenuto/sdk/client"
	"github.com/donutnomad/tenuto/sdk/worker"

	"github.com/donutnomad/tenuto/docs/examples/multiprocess/app2"
)

func main() {
	url := flag.String("url", envOr("TENUTO_URL", "http://localhost:7400"), "Tenuto Server HTTP 地址")
	namespace := flag.String("namespace", envOr("TENUTO_NAMESPACE", "multiprocess-demo"), "namespace")
	buildID := flag.String("build-id", envOr("APP2_BUILD_ID", "app2-v1"), "Activity worker build ID")
	queues := flag.String("queues", envOr("APP2_QUEUES", "inventory,ledger"), "订阅的 activity 队列（逗号分隔）")
	flag.Parse()
	if err := run(*url, *namespace, *buildID, splitCSV(*queues)); err != nil {
		log.Fatalf("app2: %v", err)
	}
}

func run(url, namespace, buildID string, queues []string) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	c, err := client.Dial(client.Options{HostPort: url, Namespace: namespace})
	if err != nil {
		return err
	}
	defer c.Close()
	registry := wfsdk.NewRegistry()
	if err := app2.RegisterActivities(registry); err != nil {
		return err
	}
	w, err := worker.New(c, worker.Options{
		Role:                                   worker.RoleActivity,
		Activities:                             registry,
		ActivityTaskQueues:                     queues,
		ActivityConcurrency:                    4,
		Identity:                               "app2-activity-1",
		BuildID:                                buildID,
		ActivityWorkerVersionDefault:           true,
		ActivityWorkerVersionHeartbeatInterval: 5 * time.Second,
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
		return err
	}
	log.Printf("ready namespace=%s queues=%v build=%s contracts=%d", namespace, queues, buildID, len(registry.Contracts()))
	<-ctx.Done()
	return nil
}

func splitCSV(value string) []string {
	var out []string
	for _, item := range strings.Split(value, ",") {
		if item = strings.TrimSpace(item); item != "" {
			out = append(out, item)
		}
	}
	return out
}

func envOr(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
