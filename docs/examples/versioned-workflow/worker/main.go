// Command worker 运行 lifecycle 示例的 workflow worker 与 Activity worker。
//
// Activity 实现来自 activities 包（本地 @Activity 声明），由该包生成的
// `activities.RegisterActivities` 一行登记到 registry，契约随 registry 在 Worker 启动时上报；
// remote 契约 payments.settle 的实现由本 worker 手写提供，在 registerActivities 里单独登记。
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

	"github.com/donutnomad/tenuto/compiler/artifact"
	"github.com/donutnomad/tenuto/docs/examples/versioned-workflow/activities"
	"github.com/donutnomad/tenuto/docs/examples/versioned-workflow/remote"
	"github.com/donutnomad/tenuto/program"
	wfsdk "github.com/donutnomad/tenuto/sdk/activity"
	"github.com/donutnomad/tenuto/sdk/client"
	"github.com/donutnomad/tenuto/sdk/worker"
)

func main() {
	url := flag.String("url", envOr("TENUTO_URL", "http://localhost:7400"), "Tenuto Server HTTP 地址")
	namespace := flag.String("namespace", envOr("TENUTO_NAMESPACE", "lifecycle-demo"), "namespace")
	buildID := flag.String("build-id", "lifecycle-build-v1", "Activity worker build ID")
	identity := flag.String("identity", "lifecycle-activity-v1", "worker identity")
	isDefault := flag.Bool("default", true, "是否为该队列的默认 Activity 路由")
	artifactsDir := flag.String("artifacts", "docs/examples/versioned-workflow/v2", "workflow.json 所在目录（读取其中的 Activity 契约）")
	flag.Parse()

	if err := run(*url, *namespace, *buildID, *identity, *isDefault, *artifactsDir); err != nil {
		log.Fatalf("lifecycle worker: %v", err)
	}
}

func run(url, namespace, buildID, identity string, isDefault bool, artifactsDir string) error {
	artifacts, err := artifact.LoadDir(artifactsDir)
	if err != nil {
		return fmt.Errorf("load artifacts: %w", err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	c, err := client.Dial(client.Options{HostPort: url, Namespace: namespace})
	if err != nil {
		return err
	}
	defer c.Close()

	workflowWorker, err := worker.New(c, worker.Options{
		Role:                worker.RoleWorkflow,
		WorkflowConcurrency: 2,
		Identity:            identity + "-workflow",
		BuildID:             buildID,
	})
	if err != nil {
		return err
	}
	workflowWorker.Start(ctx)

	registry := wfsdk.NewRegistry()
	if err := registerActivities(registry, artifacts.Workflow.Activities); err != nil {
		return fmt.Errorf("register activities: %w", err)
	}
	// registry 里的契约随 Worker 启动上报，服务端据此对照 workflow 侧声明。
	activityWorker, err := worker.New(c, worker.Options{
		Role:                                   worker.RoleActivity,
		Activities:                             registry,
		ActivityConcurrency:                    2,
		Identity:                               identity,
		BuildID:                                buildID,
		ActivityWorkerVersionDefault:           isDefault,
		ActivityWorkerVersionHeartbeatInterval: 5 * time.Second,
	})
	if err != nil {
		return err
	}
	activityWorker.Start(ctx)

	if err := workflowWorker.WaitReady(ctx); err != nil {
		if ctx.Err() != nil {
			return nil // 启动途中收到退出信号，按正常退出处理
		}
		return fmt.Errorf("workflow worker ready: %w", err)
	}
	if err := activityWorker.WaitReady(ctx); err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return fmt.Errorf("activity worker ready: %w", err)
	}
	reported := registry.Contracts()
	log.Printf("ready namespace=%s build=%s identity=%s activities=%d contracts_reported=%d",
		namespace, buildID, identity, len(artifacts.Workflow.Activities), len(reported))
	<-ctx.Done()
	activityWorker.Stop()
	workflowWorker.Stop()
	return nil
}

// registerActivities 登记本 worker 提供的全部 activity 实现。
//
// 本地实现由 activities 包的生成 helper 一行登记。payments.settle 是 remote 契约：
// 实现在本 worker 手写提供、不在 @Activity 标注的包里，所以保留这一条手写登记，
// 并按 workflow.json 里该符号的契约核对。
func registerActivities(registry *wfsdk.Registry, declared map[string]program.ActivityRef) error {
	if err := activities.RegisterActivities(registry); err != nil {
		return err
	}
	register(registry, declared, "remote.SettleRemoteActivity", settle)
	return nil
}

// register 按 workflow.json 里的 type 与 version 登记实现，并把实现的契约与生成产物对照。
//
// 两边都是 wire 指纹（与 Go 类型身份无关）：generator 侧 config.WLangWireFingerprint，
// worker 侧 activity.ContractOf。不一致说明 workflow.json 落后于实现，直接 fail fast，
// 让问题停在启动阶段而不是真实调度期的 contract mismatch。
func register[I, O any](registry *wfsdk.Registry, declared map[string]program.ActivityRef, symbol string, fn func(context.Context, I) (O, error)) {
	ref, ok := declared[symbol]
	if !ok {
		panic(fmt.Sprintf("workflow.json 缺少 Activity 符号 %s（先生成产物再启动 worker）", symbol))
	}
	expected, err := wfsdk.ContractOf[I, O](ref.Type, ref.Version)
	if err != nil {
		panic(fmt.Sprintf("计算 %s 的契约指纹失败: %v", ref.Type, err))
	}
	if expected.Input != ref.Input || expected.Output != ref.Output {
		panic(fmt.Sprintf("workflow.json 的 %s 契约与实现不一致（重新生成产物）：声明 input=%s output=%s，实现 input=%s output=%s",
			ref.Type, ref.Input, ref.Output, expected.Input, expected.Output))
	}
	if err := wfsdk.RegisterActivityOnQueue(registry, ref.TaskQueue, ref.Type, ref.Version, fn); err != nil {
		panic(fmt.Sprintf("注册 %s 失败: %v", ref.Type, err))
	}
}

// settle 是 remote 契约 payments.settle 的实现。
func settle(_ context.Context, in remote.SettleInput) (remote.SettleOutput, error) {
	return remote.SettleOutput{Receipt: fmt.Sprintf("receipt-%s-%d", in.OrderID, in.Amount), Settled: true}, nil
}

func envOr(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
