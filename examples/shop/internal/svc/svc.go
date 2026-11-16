// Package svc 是三个服务进程共用的启动代码：连接 Tenuto、启动 Worker、等待退出信号。
package svc

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/donutnomad/tenuto/sdk/client"
	"github.com/donutnomad/tenuto/sdk/worker"
)

// Namespace 是 shop 的全部 Workflow 与 Activity 所在的 Namespace。
const Namespace = "shop"

// Env 返回环境变量 name，未设置时返回 fallback。
func Env(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}

// SignalContext 在收到 SIGINT 或 SIGTERM 时取消。
func SignalContext() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
}

// Dial 按 TENUTO_URL 与 TENUTO_AUTH_TOKEN 连接 Tenuto Server。
func Dial() (client.Client, error) {
	return client.Dial(client.Options{
		HostPort:  Env("TENUTO_URL", "http://localhost:7400"),
		Namespace: Namespace,
		AuthToken: os.Getenv("TENUTO_AUTH_TOKEN"),
	})
}

// RunWorker 启动 Worker，登记完成后打印 ready，阻塞到 ctx 取消。
func RunWorker(ctx context.Context, name string, c client.Client, opts worker.Options) error {
	opts.Role = worker.RoleActivity // Workflow Task 由 tenuto-server 执行
	opts.Identity = name + "@" + hostname()
	w, err := worker.New(c, opts)
	if err != nil {
		return err
	}
	w.Start(ctx)
	defer w.Stop()
	readyCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := w.WaitReady(readyCtx); err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return fmt.Errorf("worker not ready: %w", err)
	}
	log.Printf("%s ready: queues=%v", name, opts.ActivityTaskQueues)
	<-ctx.Done()
	log.Printf("%s stopping", name)
	return nil
}

func hostname() string {
	h, err := os.Hostname()
	if err != nil {
		return "unknown"
	}
	return fmt.Sprintf("%s-%d", h, os.Getpid())
}
