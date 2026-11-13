package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/donutnomad/tenuto/cmd/tenuto-cluster/workload"
	"github.com/donutnomad/tenuto/cmd/tenuto-cluster/workload/order"
	"github.com/donutnomad/tenuto/cmd/tenuto-cluster/workload/steps"
	"github.com/donutnomad/tenuto/sdk/activity"
	"github.com/donutnomad/tenuto/sdk/client"
	"github.com/donutnomad/tenuto/sdk/registration"
	"github.com/donutnomad/tenuto/sdk/worker"
)

// workerReadyLine 是 Worker 进程登记完成后写入日志的一行，集群据此判断 Worker 就绪。
const workerReadyLine = "tenuto-cluster worker ready"

// workloadDefinitions 是负载的两个 Workflow 定义。
func workloadDefinitions() []registration.Definition {
	return []registration.Definition{order.WorkflowDefinition(), steps.WorkflowDefinition()}
}

// newWorkloadWorker 创建执行负载 Activity 并登记负载 Workflow 定义的 Worker。
func newWorkloadWorker(c client.Client, identity string, role worker.Role, concurrency int) (*worker.Worker, error) {
	registry := activity.NewRegistry()
	if err := order.RegisterActivities(registry); err != nil {
		return nil, err
	}
	if err := steps.RegisterActivities(registry); err != nil {
		return nil, err
	}
	return worker.New(c, worker.Options{
		Role:                         role,
		Workflows:                    workloadDefinitions(),
		Activities:                   registry,
		ActivityTaskQueues:           []string{workload.TaskQueue},
		ActivityConcurrency:          concurrency,
		WorkflowConcurrency:          concurrency,
		Identity:                     identity,
		BuildID:                      "tenuto-cluster-v1",
		ActivityWorkerVersionDefault: true,
	})
}

// workerAuthTokenEnv 把 -auth 的 token 交给 Worker 子进程；走环境变量而不是参数，token 不出现在进程列表里。
const workerAuthTokenEnv = "TENUTO_AUTH_TOKEN"

// runWorker 是 Worker 子进程：只执行 Activity（Workflow Task 由副本内嵌的 Workflow Worker 执行），
// 每次 Activity 执行写入 <out>/activities-<name>.jsonl。登记失败（例如连到的副本刚被 kill）时重试。
func runWorker(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("worker", flag.ExitOnError)
	url := fs.String("url", "", "Tenuto runtime HTTP address")
	namespace := fs.String("namespace", "cluster", "namespace")
	name := fs.String("name", "worker-0", "worker identity")
	out := fs.String("out", ".", "directory of the execution record file")
	concurrency := fs.Int("concurrency", 16, "Activity slots")
	if err := fs.Parse(args); err != nil {
		return err
	}
	recorder, err := workload.OpenRecorder(filepath.Join(*out, "activities-"+*name+".jsonl"), *name)
	if err != nil {
		return err
	}
	defer recorder.Close()
	workload.SetRecorder(recorder)
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	c, err := client.Dial(client.Options{HostPort: *url, Namespace: *namespace, Identity: *name, AuthToken: os.Getenv(workerAuthTokenEnv)})
	if err != nil {
		return err
	}
	defer c.Close()
	for ctx.Err() == nil {
		w, err := newWorkloadWorker(c, *name, worker.RoleActivity, *concurrency)
		if err != nil {
			return err
		}
		w.Start(context.WithoutCancel(ctx))
		readyCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		err = w.WaitReady(readyCtx)
		cancel()
		if err != nil {
			w.Stop()
			logger.Warn("worker registration failed; retrying", "err", err)
			_ = sleepCtx(ctx, time.Second)
			continue
		}
		fmt.Println(workerReadyLine)
		<-ctx.Done()
		w.Stop()
	}
	return nil
}
