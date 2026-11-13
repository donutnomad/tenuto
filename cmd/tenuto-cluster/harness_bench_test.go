package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"runtime"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/cmd/tenuto-cluster/workload"
	"github.com/donutnomad/tenuto/sdk/client"
	"github.com/donutnomad/tenuto/sdk/worker"
	"github.com/donutnomad/tenuto/server/app"
	"github.com/donutnomad/tenuto/telemetry"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// BenchmarkHarness 是进程内负载：内存后端的完整 tenuto-server（HTTP、引擎、内嵌 Workflow Worker）加一个经 HTTP
// 连接的 Activity Worker，闭环并发跑 b.N 个负载 Workflow。报告每秒任务数（Workflow Task + Activity Task）、
// 每个任务的进程 CPU 时间与分配。
//
//	GOWORK=off go test ./cmd/tenuto-cluster -run '^$' -bench Harness -benchtime 40x -cpuprofile cpu.out
//
// seq50 是 50 个顺序 Activity 的 cluster-order（无并行、无子 Workflow），small 是 bench 的 3 步 cluster-steps。
func BenchmarkHarness(b *testing.B) {
	shapes := []struct {
		name        string
		concurrency int
		start       func(ctx context.Context, c client.Client, id string) (client.WorkflowRun, error)
	}{
		{name: "seq50", concurrency: 16, start: func(ctx context.Context, c client.Client, id string) (client.WorkflowRun, error) {
			params := workload.OrderParams{Key: id, Sequential: 50, TimerMs: 1, SignalTimeoutMs: 1}
			return c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: id}, workload.OrderWorkflowType, orderInput(params))
		}},
		{name: "small", concurrency: 64, start: func(ctx context.Context, c client.Client, id string) (client.WorkflowRun, error) {
			params := workload.StepsParams{Key: id, Steps: 3, TimerMs: 10}
			return c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: id}, workload.StepsWorkflowType, params)
		}},
	}
	// Activity Worker 在部署中是独立进程，它的每任务日志不计入副本开销。
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.DiscardHandler))
	defer slog.SetDefault(previous)
	for _, shape := range shapes {
		b.Run(shape.name, func(b *testing.B) {
			// TENUTO_HARNESS_TX=1 时注入记录指标的 MeterProvider，按操作报告每个任务的持久化事务数（指标记录本身
			// 计入 CPU，测 CPU 时不开）。
			var reader *sdkmetric.ManualReader
			var options []app.RunOption
			if os.Getenv("TENUTO_HARNESS_TX") != "" {
				reader = sdkmetric.NewManualReader()
				providers := telemetry.NoopProviders()
				providers.MeterProvider = sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
				options = append(options, app.WithTelemetry(providers))
			}
			url := startHarnessServer(b, options...)
			c, err := client.Dial(client.Options{HostPort: url, Namespace: "cluster"})
			if err != nil {
				b.Fatal(err)
			}
			defer c.Close()
			w, err := newWorkloadWorker(c, "harness-activity", worker.RoleActivity, 64)
			if err != nil {
				b.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			w.Start(ctx)
			defer w.Stop()
			if err := w.WaitReady(ctx); err != nil {
				b.Fatal(err)
			}
			// 预热：Program 加载与连接建立不计入。
			runHarness(b, ctx, c, shape.name+"-warm", 2, shape.concurrency, shape.start)

			txBefore := transactionCounts(b, reader)
			var before runtime.MemStats
			runtime.ReadMemStats(&before)
			cpuBefore := processCPU()
			b.ResetTimer()
			began := time.Now()
			ids := runHarness(b, ctx, c, shape.name, b.N, shape.concurrency, shape.start)
			elapsed := time.Since(began)
			cpu := processCPU() - cpuBefore
			b.StopTimer()
			var after runtime.MemStats
			runtime.ReadMemStats(&after)

			tasks := 0
			for _, id := range ids {
				events, err := fetchHistory(ctx, c, id, "")
				if err != nil {
					b.Fatal(err)
				}
				for _, event := range events {
					if event.WorkflowTaskStarted != nil || event.ActivityStarted != nil {
						tasks++
					}
				}
			}
			b.ReportMetric(float64(tasks)/elapsed.Seconds(), "tasks/s")
			b.ReportMetric(float64(cpu.Microseconds())/float64(tasks), "cpu-µs/task")
			b.ReportMetric(float64(after.Mallocs-before.Mallocs)/float64(tasks), "allocs/task")
			b.ReportMetric(float64(after.TotalAlloc-before.TotalAlloc)/float64(tasks), "B/task")
			b.ReportMetric(float64(tasks)/float64(len(ids)), "tasks/wf")
			if reader != nil {
				total := 0.0
				for operation, count := range transactionCounts(b, reader) {
					delta := float64(count - txBefore[operation])
					total += delta
					b.Logf("%-40s %6.2f tx/task", operation, delta/float64(tasks))
				}
				b.ReportMetric(total/float64(tasks), "tx/task")
			}
		})
	}
}

// runHarness 以 concurrency 个并发闭环启动并等完 n 个 Workflow，返回 Workflow ID。
func runHarness(b *testing.B, ctx context.Context, c client.Client, prefix string, n, concurrency int,
	start func(ctx context.Context, c client.Client, id string) (client.WorkflowRun, error)) []string {
	ids := make([]string, n)
	next := make(chan int)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstErr error
	for range concurrency {
		wg.Go(func() {
			for i := range next {
				id := fmt.Sprintf("%s-%d", prefix, i)
				ids[i] = id
				run, err := start(ctx, c, id)
				if err == nil {
					err = run.Get(ctx, nil)
				}
				if err != nil {
					mu.Lock()
					firstErr = err
					mu.Unlock()
				}
			}
		})
	}
	for i := range n {
		next <- i
	}
	close(next)
	wg.Wait()
	if firstErr != nil {
		b.Fatal(firstErr)
	}
	return ids
}

// transactionCounts 读取 tenuto.persistence.transactions 按操作的累计值；reader 为 nil 时返回 nil。
func transactionCounts(b *testing.B, reader *sdkmetric.ManualReader) map[string]int64 {
	if reader == nil {
		return nil
	}
	var data metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &data); err != nil {
		b.Fatal(err)
	}
	counts := map[string]int64{}
	for _, scope := range data.ScopeMetrics {
		for _, m := range scope.Metrics {
			sum, ok := m.Data.(metricdata.Sum[int64])
			if m.Name != "tenuto.persistence.transactions" || !ok {
				continue
			}
			for _, point := range sum.DataPoints {
				operation, _ := point.Attributes.Value(telemetry.AttrOperation)
				counts[operation.AsString()] += point.Value
			}
		}
	}
	return counts
}

// startHarnessServer 在本进程里运行内存后端的 tenuto-server，就绪后返回 HTTP 地址。
func startHarnessServer(b *testing.B, options ...app.RunOption) string {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	address := listener.Addr().String()
	_ = listener.Close()
	b.Setenv("WORKFLOW_BACKEND", "memory")
	b.Setenv("WORKFLOW_HTTP_ADDR", address)
	cfg, err := app.LoadConfig()
	if err != nil {
		b.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan error, 1)
	go func() { stopped <- app.Run(ctx, cfg, options...) }()
	b.Cleanup(func() {
		cancel()
		<-stopped
	})
	url := "http://" + address
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		response, err := http.Get(url + "/health/ready")
		if err == nil {
			_ = response.Body.Close()
			if response.StatusCode == http.StatusOK {
				return url
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	b.Fatal("harness server did not become ready")
	return ""
}

// processCPU 是本进程累计的用户态加内核态 CPU 时间。
func processCPU() time.Duration {
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		return 0
	}
	return time.Duration(usage.Utime.Nano() + usage.Stime.Nano())
}
