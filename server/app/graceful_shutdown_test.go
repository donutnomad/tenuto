package app

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

// 优雅停机：收到停机信号时仍在途的写请求照常做完并应答结果，长轮询立即以 204 结束。
// 每个 Signal 请求在停机之前只发出请求头与半个请求体，停机之后再发完；它们都要生效并应答带 body 的 200。
func TestGracefulShutdownCompletesInFlightWritesAndEndsLongPolls(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	_ = listener.Close()
	cfg, err := loadConfig(mapLookup(map[string]string{
		"WORKFLOW_BACKEND":        "memory",
		"WORKFLOW_HTTP_ADDR":      address,
		"WORKFLOW_WORKER_ENABLED": "false",
	}), "shutdown-node")
	if err != nil {
		t.Fatal(err)
	}
	app, err := newApplication(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer app.close()
	ctx, shutdown := context.WithCancel(context.Background())
	defer shutdown()
	stopped := make(chan error, 1)
	go func() { stopped <- app.run(ctx, context.Background()) }()
	url := "http://" + address
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(10 * time.Millisecond) {
		if response, err := http.Get(url + "/health/ready"); err == nil {
			_ = response.Body.Close()
			if response.StatusCode == http.StatusOK {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("server not ready")
		}
	}
	run := startEmptyWorkflow(t, url, dialMemoryServer(t, url))

	const requests = 40
	body := fmt.Sprintf(`{"run_id":%q,"name":"poke"}`, run.GetRunID())
	conns := make([]net.Conn, requests)
	for i := range conns {
		conn, err := net.Dial("tcp", address)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		conns[i] = conn
		head := fmt.Sprintf("POST /runtime/workflow/signal HTTP/1.1\r\nHost: x\r\nContent-Type: application/json\r\nContent-Length: %d\r\n\r\n", len(body))
		if _, err := io.WriteString(conn, head+body[:len(body)/2]); err != nil {
			t.Fatal(err)
		}
	}
	// 一个 60 秒的 Activity 长轮询，停机时应立即结束。
	type pollResult struct {
		code    int
		elapsed time.Duration
		err     error
	}
	polled := make(chan pollResult, 1)
	go func() {
		response, err := http.Post(url+"/runtime/activity/poll", "application/json",
			strings.NewReader(`{"task_queue":"shutdown-queue","timeout_millis":60000}`))
		if err != nil {
			polled <- pollResult{err: err}
			return
		}
		_ = response.Body.Close()
		polled <- pollResult{code: response.StatusCode}
	}()
	time.Sleep(200 * time.Millisecond)

	shutdownAt := time.Now()
	shutdown()
	time.Sleep(200 * time.Millisecond)

	var mu sync.Mutex
	var answered, withBody int
	var wg sync.WaitGroup
	for _, conn := range conns {
		wg.Go(func() {
			if _, err := io.WriteString(conn, body[len(body)/2:]); err != nil {
				t.Errorf("finish request body: %v", err)
				return
			}
			response, err := http.ReadResponse(bufio.NewReader(conn), nil)
			if err != nil {
				t.Errorf("read response: %v", err)
				return
			}
			data, _ := io.ReadAll(response.Body)
			_ = response.Body.Close()
			mu.Lock()
			defer mu.Unlock()
			if response.StatusCode != http.StatusOK {
				t.Errorf("signal during shutdown = %d %s, want 200", response.StatusCode, data)
				return
			}
			answered++
			if len(strings.TrimSpace(string(data))) > 0 {
				withBody++
			}
		})
	}
	wg.Wait()
	if err := <-stopped; err != nil {
		t.Fatalf("run: %v", err)
	}
	result := <-polled
	result.elapsed = time.Since(shutdownAt)
	if result.err != nil || result.code != http.StatusNoContent || result.elapsed > 5*time.Second {
		t.Errorf("activity poll during shutdown = %d %v after %s, want 204 promptly", result.code, result.err, result.elapsed)
	}

	events, err := app.server.GetEvents(run.GetRunID())
	if err != nil {
		t.Fatal(err)
	}
	signaled := 0
	for _, event := range events {
		if event.WorkflowSignaled != nil {
			signaled++
		}
	}
	if answered != requests || withBody != requests || signaled != requests {
		t.Fatalf("answered 200 = %d (with body %d), WorkflowSignaled = %d, want %d each", answered, withBody, signaled, requests)
	}
}
