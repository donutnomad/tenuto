package app

import (
	"context"
	"net"
	"net/http"
	"testing"
	"time"
)

// 启动恢复慢于启动期限时副本照常就绪：恢复在后台运行，不占用 WORKFLOW_STARTUP_TIMEOUT。故障注入里重启的副本
// 恢复时向易主中的分区入队，用完启动期限后以 `start embedded workflow worker: context deadline exceeded` 退出。
func TestRunBecomesReadyWhileStartupRecoveryIsSlow(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	cfg, err := loadConfig(mapLookup(map[string]string{
		"WORKFLOW_BACKEND":         "memory",
		"WORKFLOW_HTTP_ADDR":       address,
		"WORKFLOW_STARTUP_TIMEOUT": "300ms",
	}), "slow-recovery-node")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	startupCtx, startupCancel := context.WithTimeout(ctx, cfg.StartupTimeout)
	defer startupCancel()
	startupDeadline, _ := startupCtx.Deadline()
	app, err := newApplication(startupCtx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	recoveryStopped := make(chan struct{})
	app.recoverRuns = func(ctx context.Context) {
		defer close(recoveryStopped)
		<-ctx.Done()
	}

	// close 等后台恢复结束；它返回之后再报告 Run 的结果，下面的断言才看得到关闭的完整效果。
	stopped := make(chan error, 1)
	go func() {
		err := app.run(ctx, startupCtx)
		app.close()
		stopped <- err
	}()

	deadline := startupDeadline.Add(2 * time.Second)
	for {
		select {
		case err := <-stopped:
			t.Fatalf("Run exited during startup: %v", err)
		default:
		}
		response, err := http.Get("http://" + address + "/health/ready")
		if err == nil {
			_ = response.Body.Close()
			if response.StatusCode == http.StatusOK && time.Now().After(startupDeadline) {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("server not ready past the startup timeout while recovery runs: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}

	cancel()
	select {
	case err := <-stopped:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not stop")
	}
	select {
	case <-recoveryStopped:
	default:
		t.Fatal("shutdown did not stop the background recovery")
	}
}
