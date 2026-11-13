// Command driver 驱动 upgrade-demo 的全部场景，并在需要切换发布版本时启动、停止
// app-a / app-b 的进程。场景与断言在 ../../scenario；这里只负责进程管理。
//
//	driver -url http://localhost:7400 -bin .run/upgrade-demo/bin -logs .run/upgrade-demo/logs
//
// 每个应用进程都是独立的操作系统进程，日志写到 -logs 目录下的 <app>-<version>.log。
// 任何断言不成立时进程以非 0 退出。
package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/donutnomad/tenuto/docs/examples/upgrade-demo/scenario"
)

func main() {
	url := flag.String("url", "http://localhost:7400", "Tenuto Server HTTP 地址")
	namespace := flag.String("namespace", "upgrade-demo", "namespace")
	bin := flag.String("bin", ".run/upgrade-demo/bin", "app-a / app-b 二进制所在目录")
	logs := flag.String("logs", ".run/upgrade-demo/logs", "应用进程日志目录")
	timeout := flag.Duration("timeout", 30*time.Second, "等待单个条件成立的上限")
	step := flag.Bool("step", false, "在每个检查点暂停，按回车继续（便于在 Console 里查看 Run）")
	flag.Parse()

	fleet := &processFleet{url: *url, namespace: *namespace, bin: *bin, logs: *logs, running: map[string]*exec.Cmd{}}
	// 收到退出信号时先停掉全部应用进程。
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-signals
		fleet.stopAll()
		os.Exit(130)
	}()

	env := &scenario.Env{
		URL: *url, Namespace: *namespace, Fleet: fleet, Out: os.Stdout, Timeout: *timeout,
	}
	if *step {
		stdin := bufio.NewReader(os.Stdin)
		env.Checkpoint = func(what string) {
			fmt.Printf("\n[检查点] %s —— 按回车继续 ", what)
			_, _ = stdin.ReadString('\n')
		}
	}
	err := env.Run()
	fleet.stopAll()
	if err != nil {
		fmt.Fprintf(os.Stderr, "\nRESULT FAIL: %v\n", err)
		os.Exit(1)
	}
}

// processFleet 把每个 (应用, 发布版本) 运行成一个独立进程。
type processFleet struct {
	url, namespace, bin, logs string

	mu      sync.Mutex
	running map[string]*exec.Cmd
}

func (f *processFleet) Start(_ context.Context, app, release string) error {
	name := app + "-" + release
	logPath := filepath.Join(f.logs, name+".log")
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer logFile.Close()
	offset, err := logFile.Seek(0, 2)
	if err != nil {
		return err
	}
	cmd := exec.Command(filepath.Join(f.bin, app),
		"-url", f.url, "-namespace", f.namespace, "-version", release)
	cmd.Stdout, cmd.Stderr = logFile, logFile
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start %s: %w", name, err)
	}
	exited := make(chan struct{})
	go func() { _ = cmd.Wait(); close(exited) }()

	// 应用进程在 Worker 就绪后打印一行 "<app> ready ..."。
	deadline := time.Now().Add(30 * time.Second)
	for {
		if data, err := os.ReadFile(logPath); err == nil && int64(len(data)) >= offset && strings.Contains(string(data[offset:]), app+" ready") {
			break
		}
		select {
		case <-exited:
			return fmt.Errorf("%s exited before it was ready, see %s", name, logPath)
		case <-time.After(50 * time.Millisecond):
		}
		if time.Now().After(deadline) {
			_ = cmd.Process.Kill()
			return fmt.Errorf("%s was not ready within 30s, see %s", name, logPath)
		}
	}
	f.mu.Lock()
	f.running[name] = cmd
	f.mu.Unlock()
	return nil
}

func (f *processFleet) Stop(app, release string) error {
	name := app + "-" + release
	f.mu.Lock()
	cmd := f.running[name]
	delete(f.running, name)
	f.mu.Unlock()
	if cmd == nil {
		return fmt.Errorf("%s is not running", name)
	}
	terminate(cmd)
	return nil
}

func (f *processFleet) stopAll() {
	f.mu.Lock()
	defer f.mu.Unlock()
	for name, cmd := range f.running {
		terminate(cmd)
		delete(f.running, name)
	}
}

// terminate 发 SIGTERM 让进程优雅退出，10 秒内没有退出就强制结束。
func terminate(cmd *exec.Cmd) {
	_ = cmd.Process.Signal(syscall.SIGTERM)
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		// 进程退出后被 Start 里的 Wait 回收，Signal(0) 随即返回错误。
		if err := cmd.Process.Signal(syscall.Signal(0)); err != nil {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	_ = cmd.Process.Kill()
}
