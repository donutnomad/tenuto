package app

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol/runtimeapi"
)

// longPollProbe 起一个**真实的** http.Server（不是 httptest 的默认配置），handler 阻塞
// pollDuration 后才写响应——精确模拟长轮询：等待发生在响应写出之前，正是 WriteTimeout
// 计时覆盖的区间。返回客户端拿到的错误（nil = 长轮询完整跑完没被打断）。
//
// 必须用真 server + 真 TCP：WriteTimeout 是 net/http 在连接层设 SetWriteDeadline 实现的，
// httptest.NewRecorder 那种直接调 ServeHTTP 的方式根本不经过连接层，测不出这个行为。
func longPollProbe(t *testing.T, writeTimeout, pollDuration time.Duration) error {
	t.Helper()

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-time.After(pollDuration):
		case <-r.Context().Done():
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"polled":true}`))
	})

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: defaultReadHeaderTimeout,
		ReadTimeout:       defaultReadTimeout,
		WriteTimeout:      writeTimeout,
		IdleTimeout:       defaultIdleTimeout,
	}
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	})

	// 客户端自身不设超时，确保观察到的任何中断都来自 server 的 WriteTimeout 而非客户端。
	client := &http.Client{}
	request, err := http.NewRequest(http.MethodPost, "http://"+listener.Addr().String()+"/runtime/activity/poll", http.NoBody)
	if err != nil {
		t.Fatal(err)
	}
	response, err := client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if _, err := io.ReadAll(response.Body); err != nil {
		return err
	}
	if response.StatusCode != http.StatusOK {
		return errors.New("unexpected status " + response.Status)
	}
	return nil
}

// TestHTTPWriteTimeoutDoesNotInterruptLongPoll 是本次改动的核心回归防线。
//
// WriteTimeout 从**读完请求头**起算，覆盖「handler 阻塞时长 + 写响应时长」。长轮询的
// handler 会一直挂到有任务或超时（上界 runtimehttp.MaxRuntimePollTimeout），所以 WriteTimeout
// 一旦小于这个上界，每一次正常的长轮询都会被 server 掐断。
//
// 用缩放后的时长跑（poll=600ms，WriteTimeout 按同样比例取 2x）以保持测试快速；
// 比例与生产配置一致：defaultWriteTimeout = 2 * runtimehttp.MaxRuntimePollTimeout。
func TestHTTPWriteTimeoutDoesNotInterruptLongPoll(t *testing.T) {
	const pollDuration = 600 * time.Millisecond

	if err := longPollProbe(t, 2*pollDuration, pollDuration); err != nil {
		t.Fatalf("long poll was interrupted with WriteTimeout=%s > poll=%s: %v",
			2*pollDuration, pollDuration, err)
	}
}

// TestHTTPWriteTimeoutBelowPollDurationBreaksLongPoll 钉住反向事实：WriteTimeout 小于
// 长轮询时长时，连接**确实**会被掐断。
//
// 这条测试是上面那条的对照组——它证明 longPollProbe 真的能观测到打断，而不是因为探针
// 本身失灵才恒过。没有它，上面那条测试可能在任何配置下都"通过"，从而毫无价值。
func TestHTTPWriteTimeoutBelowPollDurationBreaksLongPoll(t *testing.T) {
	const pollDuration = 600 * time.Millisecond

	if err := longPollProbe(t, pollDuration/3, pollDuration); err == nil {
		t.Fatal("expected the long poll to be cut off when WriteTimeout < poll duration, got success")
	}
}

// TestDefaultWriteTimeoutExceedsLongPollBound 把默认值与长轮询上界的关系钉死在编译期常量上。
//
// 上面两条测试用的是缩放时长；这条直接断言**真实生产默认值**满足同一不变式，
// 防止有人调小 defaultWriteTimeout 或调大 MaxRuntimePollTimeout 时只有生产才发现。
func TestDefaultWriteTimeoutExceedsLongPollBound(t *testing.T) {
	if defaultWriteTimeout <= runtimeapi.MaxRuntimePollTimeout {
		t.Fatalf("defaultWriteTimeout = %s must exceed the long poll bound %s",
			defaultWriteTimeout, runtimeapi.MaxRuntimePollTimeout)
	}
	if defaultReadHeaderTimeout <= 0 || defaultReadTimeout <= 0 {
		t.Fatal("read timeouts must be positive")
	}
}

// TestValidateRejectsWriteTimeoutBelowLongPollBound 断言配错会在**启动期**被拦下，
// 而不是等生产上长轮询被神秘掐断才发现。
func TestValidateRejectsWriteTimeoutBelowLongPollBound(t *testing.T) {
	cfg := validTimeoutConfig()
	cfg.HTTP.WriteTimeout = runtimeapi.MaxRuntimePollTimeout // 相等也不行：必须严格大于
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected Validate to reject WriteTimeout equal to the long poll bound")
	}

	cfg = validTimeoutConfig()
	cfg.HTTP.WriteTimeout = runtimeapi.MaxRuntimePollTimeout / 2
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected Validate to reject WriteTimeout below the long poll bound")
	}

	// 0 = 显式不限制，是合法的（Go 的 WriteTimeout 语义），不该被拦。
	cfg = validTimeoutConfig()
	cfg.HTTP.WriteTimeout = 0
	if err := cfg.Validate(); err != nil {
		t.Fatalf("WriteTimeout=0 must remain valid (unlimited): %v", err)
	}

	// 默认值必须通过校验，否则默认配置起不来。
	cfg = validTimeoutConfig()
	cfg.HTTP.WriteTimeout = defaultWriteTimeout
	if err := cfg.Validate(); err != nil {
		t.Fatalf("defaultWriteTimeout must be valid: %v", err)
	}
}

// validTimeoutConfig 返回一份除 HTTP 超时外都合法的配置，供超时校验测试改单个字段。
func validTimeoutConfig() Config {
	return Config{
		Backend:         BackendMySQLTaskQ,
		TaskTokenSecret: []byte("0123456789abcdef0123456789abcdef"),
		HTTP: HTTPConfig{
			Address:           ":7400",
			ShutdownTimeout:   time.Second,
			ReadHeaderTimeout: time.Second,
			ReadTimeout:       time.Second,
			IdleTimeout:       time.Second,
		},
		Database: DatabaseConfig{
			DSN:                 "d",
			HistoryPageTokenKey: []byte("0123456789abcdef0123456789abcdef"),
			MaxOpenConns:        10,
			MaxIdleConns:        5,
			ConnMaxLifetime:     time.Minute,
			ConnMaxIdleTime:     time.Minute,
		},
		NodeID:            "n",
		StartupTimeout:    time.Second,
		ReconcileInterval: time.Second,
		TaskQ: TaskQConfig{
			DSN:                     "d",
			ServiceName:             "matching",
			WorkflowQueue:           "workflow-tasks",
			PollTimeout:             time.Second,
			MembershipPollInterval:  time.Second,
			MembershipReplicaPoints: 1,
		},
	}
}
