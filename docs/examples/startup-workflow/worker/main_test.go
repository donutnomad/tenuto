package main

import (
	"bytes"
	"encoding/json"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	startupflow "github.com/donutnomad/tenuto/docs/examples/startup-workflow"
	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/server/app/assembly"
	"github.com/donutnomad/tenuto/server/biz/service/engine"
	"github.com/donutnomad/tenuto/server/http/runtimehttp"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

// TestBinaryRegisters 验证作者最短路径：
// go build → 把二进制复制到没有源码/JSON 的目录 → 直接启动 → 自动登记并激活定义。
//
// 服务端用内存 engine + httptest，不依赖外部服务。
func TestBinaryRegisters(t *testing.T) {
	if testing.Short() {
		t.Skip("需要 go build 的端到端用例")
	}
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("没有 go 工具链")
	}

	const namespace = "startup-demo"
	server := assembly.NewServer(assembly.Options{Stores: assembly.Stores{Executions: memory.NewExecutionManager()}, WorkflowQueue: memory.NewTaskQueue(256), ActivityRouter: memory.NewActivityQueueRouter(256)})
	httpServer := httptest.NewServer(runtimehttp.NewRuntimeHTTPHandler(server))
	defer httpServer.Close()

	deployed := filepath.Join(t.TempDir(), "startup-worker")
	copyFile(t, buildWorkerBinary(t), deployed)
	// 运行目录里只有二进制：没有 workflow 源码，也没有任何 JSON。
	runDir := t.TempDir()

	command := exec.Command(deployed)
	command.Dir = runDir
	command.Env = append(os.Environ(),
		"TENUTO_URL="+httpServer.URL,
		"TENUTO_NAMESPACE="+namespace,
	)
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	var logs lockedBuffer
	command.Stdout = &logs
	command.Stderr = &logs
	if err := command.Start(); err != nil {
		t.Fatalf("启动示例二进制: %v", err)
	}
	t.Cleanup(func() {
		_ = syscall.Kill(-command.Process.Pid, syscall.SIGKILL)
		_, _ = command.Process.Wait()
	})

	waitForRegistration(t, server, namespace, expectedContentVersion(t), &logs)
	// 服务端看到登记时 worker 进程还没走完启动；等它打出 ready 再发信号，退出码与日志才是确定的。
	waitForLog(t, &logs, "ready namespace=")

	if err := command.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("发送 SIGTERM: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- command.Wait() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("worker 退出: %v\n%s", err, logs.String())
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("worker 没有在 SIGTERM 后退出\n%s", logs.String())
	}
	if !strings.Contains(logs.String(), "workflow definition registered") {
		t.Fatalf("启动日志缺少自动登记记录:\n%s", logs.String())
	}
}

func buildWorkerBinary(t *testing.T) string {
	t.Helper()
	binary := filepath.Join(t.TempDir(), "startup-worker")
	build := exec.Command("go", "build", "-o", binary, ".")
	build.Env = append(os.Environ(), "GOWORK=off")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("go build: %v\n%s", err, output)
	}
	return binary
}

func copyFile(t *testing.T, source, target string) {
	t.Helper()
	data, err := os.ReadFile(source)
	if err != nil {
		t.Fatalf("读取 %s: %v", source, err)
	}
	if err := os.WriteFile(target, data, 0o755); err != nil {
		t.Fatalf("写入 %s: %v", target, err)
	}
}

// expectedContentVersion 从二进制里的 workflow.json 算出内容版本。
func expectedContentVersion(t *testing.T) string {
	t.Helper()
	_, workflowJSON, err := startupflow.WorkflowDefinition().Load()
	if err != nil {
		t.Fatalf("读取嵌入式定义: %v", err)
	}
	var workflow program.WorkflowConfig
	if err := json.Unmarshal(workflowJSON, &workflow); err != nil {
		t.Fatalf("解析嵌入式定义: %v", err)
	}
	version, err := program.WorkflowConfigVersion(&workflow)
	if err != nil {
		t.Fatalf("计算内容版本: %v", err)
	}
	return version
}

func waitForRegistration(t *testing.T, runtime *engine.Server, namespace, version string, logs *lockedBuffer) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		result, err := runtime.View().ListWorkflowConfigs(namespace)
		if err == nil {
			for _, item := range result.Items {
				if item.WorkflowID == "startup-greeting" && item.Active && item.LatestVersion == version {
					return
				}
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("30s 内没有登记 id=%s version=%s\n%s", "startup-greeting", version, logs.String())
}

// lockedBuffer 收集子进程输出；写入来自 exec 的拷贝协程，读取来自测试协程。
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func waitForLog(t *testing.T, logs *lockedBuffer, want string) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(logs.String(), want) {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("30s 内日志没有出现 %q\n%s", want, logs.String())
}
