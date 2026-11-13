package app

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/donutnomad/taskq/frontend"
	"github.com/donutnomad/taskq/matching"
	"github.com/donutnomad/taskq/membership"
	"github.com/donutnomad/tenuto/telemetry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	gormio "gorm.io/gorm"
)

// 广播地址推导：空 host 与未指定地址落到本机同一非回环 IP（否则成员表里会是 0.0.0.0
// 或回环地址），具体 IP 与主机名原样解析，缺端口直接报错。
func TestBroadcastAddr(t *testing.T) {
	local, err := localIP()
	if err != nil {
		t.Fatal(err)
	}
	if local.IsLoopback() || local.IsUnspecified() {
		t.Fatalf("localIP() = %v; want a dialable non-loopback address on this host", local)
	}
	want := net.JoinHostPort(local.String(), "7235")
	for _, listen := range []string{":7235", "[::]:7235"} {
		got, err := broadcastAddr(listen)
		if err != nil {
			t.Fatalf("broadcastAddr(%q): %v", listen, err)
		}
		if got != want {
			t.Fatalf("broadcastAddr(%q) = %q, want %q (local IP, not the wildcard)", listen, got, want)
		}
	}
	for listen, expected := range map[string]string{"[::1]:0": "[::1]:0", "127.0.0.1:7235": "127.0.0.1:7235"} {
		got, err := broadcastAddr(listen)
		if err != nil {
			t.Fatalf("broadcastAddr(%q): %v", listen, err)
		}
		if got != expected {
			t.Fatalf("broadcastAddr(%q) = %q, want %q", listen, got, expected)
		}
	}
	if _, err := broadcastAddr("::"); err == nil {
		t.Fatal("broadcastAddr(\"::\") should reject an address without a port")
	}
}

// 内嵌 matching 端到端：真实 taskq store + 真实 membership 注册 + gRPC 环回。
// 断言本节点进入成员表、真实 matching 能按成员表路由并完成一次入队/出队，关闭后监听端口释放。
func TestEmbeddedMatchingRoutesThroughMembership(t *testing.T) {
	store := newTaskQStore(t)
	// 先探测空闲端口再关闭、由 matching 重新监听：探测与监听之间端口可能被别的进程占用，
	// 遇到 "address already in use" 换端口重试。
	var (
		addr    string
		cfg     Config
		monitor membership.Monitor
		server  *matching.Server
	)
	for attempt := 0; ; attempt++ {
		err := func() error {
			probe, err := net.Listen("tcp", ":0")
			if err != nil {
				t.Fatal(err)
			}
			addr = probe.Addr().String()
			if err := probe.Close(); err != nil {
				t.Fatal(err)
			}

			cfg = Config{
				NodeID: "embedded-node",
				TaskQ: TaskQConfig{
					ServiceName:   "matching",
					WorkflowQueue: "workflow-tasks",
					PollTimeout:   time.Second,
					Matching:      MatchingConfig{Addr: addr},
				},
			}
			matchingCfg, err := matchingConfig(cfg, addr, nil, store)
			if err != nil {
				t.Fatal(err)
			}
			broadcast, err := netip.ParseAddrPort(matchingCfg.BroadcastAddr)
			if err != nil {
				t.Fatalf("broadcast %q: %v", matchingCfg.BroadcastAddr, err)
			}
			monitor = membership.NewMonitorWithConfig(membership.MonitorConfig{
				AdvertisedServices: []string{cfg.TaskQ.ServiceName},
				WatchedServices:    []string{cfg.TaskQ.ServiceName},
				Store:              store.Membership(),
				BroadcastAddr:      broadcast,
				PollInterval:       50 * time.Millisecond,
				PropagationTime:    50 * time.Millisecond,
				ReplicaPoints:      200,
			})
			monitor.Start()
			if err := monitor.WaitUntilInitialized(t.Context()); err != nil {
				t.Fatal(err)
			}
			matchingCfg.Monitor = monitor
			server, err = matching.NewServer(matchingCfg)
			if err != nil {
				t.Fatal(err)
			}
			if err := server.Start(); err != nil {
				monitor.Stop()
				return err
			}
			return nil
		}()
		if err == nil {
			break
		}
		if attempt >= 4 || !strings.Contains(err.Error(), "address already in use") {
			t.Fatal(err)
		}
	}

	waitForMembershipRecord(t, store.DB())

	fe, err := frontend.New(frontend.Config{
		Monitor:     monitor,
		QueueName:   cfg.TaskQ.WorkflowQueue,
		DialOpts:    []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
		PollTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	if err := fe.Enqueue(ctx, []byte("payload")); err != nil {
		t.Fatalf("enqueue through embedded matching: %v", err)
	}
	lease, err := fe.DequeueWithOptions(ctx, frontend.PollOptions{})
	if err != nil {
		t.Fatalf("dequeue through embedded matching: %v", err)
	}
	if string(lease.Data) != "payload" {
		t.Fatalf("dequeued %q, want %q", lease.Data, "payload")
	}
	if err := fe.Ack(ctx, lease.Token); err != nil {
		t.Fatalf("ack through embedded matching: %v", err)
	}
	fe.Close()

	server.Stop()
	monitor.Stop()

	again, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("matching address was not released: %v", err)
	}
	_ = again.Close()
}

func waitForMembershipRecord(t *testing.T, db *gormio.DB) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		var count int64
		if err := db.Table("taskq_membership_members").Count(&count).Error; err == nil && count > 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("matching never registered itself in taskq_membership_members")
}

// probeListenAddr 在端口被占用时立即给出带地址的错误；:0 解析为具体端口，
// 后续 matching 以该端口对外注册。
func TestProbeListenAddrRejectsBusyPort(t *testing.T) {
	busy, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatal(err)
	}
	defer busy.Close()
	if _, err := probeListenAddr(busy.Addr().String()); err == nil {
		t.Fatal("probeListenAddr accepted a port that is already bound")
	}
	addr, err := probeListenAddr(":0")
	if err != nil {
		t.Fatalf("probeListenAddr(:0): %v", err)
	}
	if _, port, err := net.SplitHostPort(addr); err != nil || port == "0" {
		t.Fatalf("probeListenAddr(:0) = %q, want a concrete port", addr)
	}
}

// 内嵌模式下 readiness 必须反映 matching 的真实运行状态：matching 未在服务时返回 503，
// 否则编排系统会在任务队列不可用时继续把流量送进来。外置模式不检查本进程的 matching 状态。
func TestEmbeddedMatchingReadinessCheck(t *testing.T) {
	enabled := &application{telemetry: telemetry.NoopProviders(), cfg: Config{TaskQ: TaskQConfig{}}}
	if err := enabled.matchingHealthCheck()(t.Context()); err == nil {
		t.Fatal("readiness passed before embedded matching started serving")
	}
	enabled.matchingReady.Store(true)
	if err := enabled.matchingHealthCheck()(t.Context()); err != nil {
		t.Fatalf("readiness failed while embedded matching serves: %v", err)
	}

	disabled := false
	external := &application{telemetry: telemetry.NoopProviders(), cfg: Config{TaskQ: TaskQConfig{Matching: MatchingConfig{Enabled: &disabled}}}}
	recorder := httptest.NewRecorder()
	health := &healthState{checks: []func(context.Context) error{external.matchingHealthCheck()}}
	health.ready.Store(true)
	health.readiness(recorder, httptest.NewRequest(http.MethodGet, "/health/ready", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("external matching mode readiness = %d, want 200", recorder.Code)
	}
}

// Run 等待的退出信号只来自真实 Serve：这里真实启动 matching，断言服务期间信号未就绪、
// Stop 之后才就绪。信号在 Stop 返回后才可靠（Stop 内部先关分区、再 GracefulStop），
// 因此 Run 的 select 在主动关闭期间只可能因其它分支结束，不会误判为故障。
func TestRunStopsWhenEmbeddedMatchingExits(t *testing.T) {
	store := newTaskQStore(t)

	app := &application{telemetry: telemetry.NoopProviders(), cfg: Config{
		NodeID: "embedded-node",
		TaskQ:  TaskQConfig{ServiceName: "matching", WorkflowQueue: "workflow-tasks"},
	}, health: &healthState{}}
	app.monitor = membership.NewMonitorWithConfig(membership.MonitorConfig{
		AdvertisedServices: []string{"matching"},
		WatchedServices:    []string{"matching"},
		Store:              store.Membership(),
		BroadcastAddr:      netip.AddrPortFrom(netip.IPv4Unspecified(), 1),
		PollInterval:       50 * time.Millisecond,
		PropagationTime:    50 * time.Millisecond,
		ReplicaPoints:      200,
	})
	app.taskQStore = store
	if app.matchingExited() != nil {
		t.Fatal("matchingExited must be nil before embedded matching starts")
	}
	// 探测到的空闲端口在 matching 绑定之前可能被并行运行的其它测试进程占走，占用时换一个端口重试。
	for attempt := 1; ; attempt++ {
		addr, err := probeListenAddr(":0")
		if err != nil {
			t.Fatal(err)
		}
		app.matchingAddr = addr
		app.cfg.TaskQ.Matching.Addr = addr
		err = app.startEmbeddedMatching()
		if err == nil {
			break
		}
		if attempt == 5 || !errors.Is(err, syscall.EADDRINUSE) {
			t.Fatal(err)
		}
	}
	defer app.close()

	exited := app.matchingExited()
	select {
	case <-exited:
		t.Fatal("matchingExited fired while the gRPC server is serving")
	default:
	}

	go app.matching.Stop()
	select {
	case <-exited:
	case <-time.After(10 * time.Second):
		t.Fatal("matchingExited never fired after the gRPC server stopped")
	}
}

// TASKQ_MATCHING_ADVERTISE_ADDR 非空时广播它而不是监听地址；空时沿用监听地址推导；
// 校验拒绝缺端口与端口 0。
func TestAdvertisedAddr(t *testing.T) {
	got, err := advertisedAddr(MatchingConfig{AdvertiseAddr: "127.0.0.1:18501"}, "127.0.0.1:17501")
	if err != nil || got != "127.0.0.1:18501" {
		t.Fatalf("advertisedAddr with advertise = %q, %v", got, err)
	}
	got, err = advertisedAddr(MatchingConfig{}, "127.0.0.1:17501")
	if err != nil || got != "127.0.0.1:17501" {
		t.Fatalf("advertisedAddr without advertise = %q, %v", got, err)
	}
	env := validEnvironment()
	env["TASKQ_MATCHING_ADDR"] = "127.0.0.1:17501"
	env["TASKQ_MATCHING_ADVERTISE_ADDR"] = "127.0.0.1:18501"
	cfg, err := loadConfig(mapLookup(env), "workflow-node-a")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.TaskQ.Matching.AdvertiseAddr != "127.0.0.1:18501" {
		t.Fatalf("AdvertiseAddr = %q", cfg.TaskQ.Matching.AdvertiseAddr)
	}
	for _, bad := range []string{"127.0.0.1", "127.0.0.1:0", "127.0.0.1:x"} {
		if err := validateAdvertiseAddr(bad); err == nil {
			t.Errorf("validateAdvertiseAddr(%q) accepted", bad)
		}
	}
	if err := validateAdvertiseAddr(""); err != nil {
		t.Errorf("empty advertise address rejected: %v", err)
	}
	env["TASKQ_MATCHING_ADVERTISE_ADDR"] = "127.0.0.1:0"
	if _, err := loadConfig(mapLookup(env), "workflow-node-a"); err == nil || !strings.Contains(err.Error(), "TASKQ_MATCHING_ADVERTISE_ADDR") {
		t.Fatalf("port 0 err = %v", err)
	}
}
