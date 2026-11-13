package main

import (
	"bufio"
	"errors"
	"io"
	"net"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// echoServer 是回显服务端，返回地址。
func echoServer(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				_, _ = io.Copy(conn, conn)
			}()
		}
	}()
	return listener.Addr().String()
}

func startProxy(t *testing.T, target string) *faultProxy {
	t.Helper()
	proxy, err := newFaultProxy("test proxy", "127.0.0.1:0", "tcp", target, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(proxy.close)
	return proxy
}

// roundTrip 写一行再读回一行，返回耗时。
func roundTrip(t *testing.T, conn net.Conn, line string, timeout time.Duration) (time.Duration, error) {
	t.Helper()
	start := time.Now()
	_ = conn.SetDeadline(start.Add(timeout))
	if _, err := conn.Write([]byte(line + "\n")); err != nil {
		return 0, err
	}
	got, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		return time.Since(start), err
	}
	if strings.TrimSpace(got) != line {
		t.Fatalf("echo = %q, want %q", got, line)
	}
	return time.Since(start), nil
}

func dial(t *testing.T, addr string) net.Conn {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func isTimeout(err error) bool {
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}

func TestProxyForwards(t *testing.T) {
	proxy := startProxy(t, echoServer(t))
	conn := dial(t, proxy.addr())
	for _, line := range []string{"a", strings.Repeat("x", 100_000)} {
		if _, err := roundTrip(t, conn, line, 2*time.Second); err != nil {
			t.Fatal(err)
		}
	}
	if n := proxy.connections(); n != 1 {
		t.Fatalf("connections = %d", n)
	}
}

// db-drop：现有连接被关闭，之后的新连接照常转发。
func TestProxyDropAll(t *testing.T) {
	proxy := startProxy(t, echoServer(t))
	conn := dial(t, proxy.addr())
	if _, err := roundTrip(t, conn, "before", 2*time.Second); err != nil {
		t.Fatal(err)
	}
	if n := proxy.dropAll(); n != 1 {
		t.Fatalf("dropped %d connections", n)
	}
	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
	if _, err := conn.Read(make([]byte, 1)); err == nil || isTimeout(err) {
		t.Fatalf("read after drop = %v, want the connection closed", err)
	}
	if _, err := roundTrip(t, dial(t, proxy.addr()), "after", 2*time.Second); err != nil {
		t.Fatal(err)
	}
}

// db-blackhole：已有连接与新连接都没有回应、也不被关闭；到期后积压的数据送达，连接继续可用。
func TestProxyBlackhole(t *testing.T) {
	proxy := startProxy(t, echoServer(t))
	existing := dial(t, proxy.addr())
	if _, err := roundTrip(t, existing, "warm", 2*time.Second); err != nil {
		t.Fatal(err)
	}
	proxy.add(netRule{Until: time.Now().Add(600 * time.Millisecond), Stall: true})
	if _, err := existing.Write([]byte("held\n")); err != nil {
		t.Fatal(err)
	}
	_ = existing.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
	if _, err := existing.Read(make([]byte, 1)); !isTimeout(err) {
		t.Fatalf("read during blackhole = %v, want a timeout", err)
	}
	fresh := dial(t, proxy.addr()) // 被接受，没有 RST
	if _, err := roundTrip(t, fresh, "new", 150*time.Millisecond); !isTimeout(err) {
		t.Fatalf("new connection during blackhole = %v, want a timeout", err)
	}
	_ = existing.SetReadDeadline(time.Now().Add(3 * time.Second))
	got, err := bufio.NewReader(existing).ReadString('\n')
	if err != nil || got != "held\n" {
		t.Fatalf("after blackhole got %q, %v", got, err)
	}
	if _, err := roundTrip(t, existing, "again", 2*time.Second); err != nil {
		t.Fatal(err)
	}
}

// db-latency：每个方向的每个数据块延迟 latency 送达，到期后恢复。
func TestProxyLatency(t *testing.T) {
	proxy := startProxy(t, echoServer(t))
	conn := dial(t, proxy.addr())
	if _, err := roundTrip(t, conn, "warm", 2*time.Second); err != nil {
		t.Fatal(err)
	}
	until := time.Now().Add(time.Second)
	proxy.add(netRule{Until: until, Latency: 100 * time.Millisecond})
	took, err := roundTrip(t, conn, "slow", 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if took < 190*time.Millisecond {
		t.Fatalf("round trip with 100ms latency each way took %s", took)
	}
	time.Sleep(time.Until(until))
	if took, err := roundTrip(t, conn, "fast", 2*time.Second); err != nil || took > 150*time.Millisecond {
		t.Fatalf("round trip after the latency rule expired took %s, %v", took, err)
	}
}

// 规则按来源匹配；过期规则被移除；多条延迟取最大值。
func TestNetRules(t *testing.T) {
	var rules netRules
	now := time.Now()
	rules.add(netRule{Until: now.Add(time.Minute), Stall: true, From: "replica-1"})
	rules.add(netRule{Until: now.Add(time.Minute), Stall: true, NotFrom: "replica-2"})
	rules.add(netRule{Until: now.Add(time.Minute), Latency: 10 * time.Millisecond})
	rules.add(netRule{Until: now.Add(time.Minute), Latency: 30 * time.Millisecond, From: "replica-2"})
	rules.add(netRule{Until: now.Add(-time.Second), Stall: true})
	if stall, latency := rules.state("replica-2", now); stall || latency != 30*time.Millisecond {
		t.Fatalf("replica-2: %v %s", stall, latency)
	}
	if stall, latency := rules.state("replica-1", now); !stall || latency != 10*time.Millisecond {
		t.Fatalf("replica-1: %v %s", stall, latency)
	}
	if stall, _ := rules.state("", now); !stall {
		t.Fatal("unidentified source is not stalled by the NotFrom rule")
	}
	if len(rules.rules) != 4 {
		t.Fatalf("expired rule kept: %d rules", len(rules.rules))
	}
	if stall, latency := rules.state("replica-1", now.Add(2*time.Minute)); stall || latency != 0 || len(rules.rules) != 0 {
		t.Fatalf("rules after expiry: %v %s %d", stall, latency, len(rules.rules))
	}
}

// api-blackhole：负载均衡器转给这个后端的连接停滞，其他后端不受影响。
func TestBalancerBlackholesOneBackend(t *testing.T) {
	a, b := echoServer(t), echoServer(t)
	lb, err := newBalancer("127.0.0.1:0", []string{a, b}, "")
	if err != nil {
		t.Fatal(err)
	}
	defer lb.close()
	lb.setUp(0, true)
	lb.setUp(1, true)
	lb.add(netRule{Until: time.Now().Add(time.Minute), Stall: true, From: replicaName(1)})
	ok, stalled := 0, 0
	for range 6 {
		if _, err := roundTrip(t, dial(t, lb.listener.Addr().String()), "x", 200*time.Millisecond); err == nil {
			ok++
		} else if isTimeout(err) {
			stalled++
		} else {
			t.Fatal(err)
		}
	}
	if ok != 3 || stalled != 3 {
		t.Fatalf("ok %d, stalled %d; want 3 and 3 (round robin over two backends)", ok, stalled)
	}
}

// 负载均衡器按 readiness 摘除不健康的后端。
func TestBalancerHealthCheck(t *testing.T) {
	lb, err := newBalancer("127.0.0.1:0", []string{"127.0.0.1:1"}, "/health/ready")
	if err != nil {
		t.Fatal(err)
	}
	defer lb.close()
	lb.setUp(0, true)
	if !lb.available(0) {
		t.Fatal("a backend just enabled is unavailable")
	}
	deadline := time.Now().Add(5 * time.Second)
	for lb.available(0) {
		if time.Now().After(deadline) {
			t.Fatal("a backend failing its health check stays available")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestParseLsofPIDs(t *testing.T) {
	out := "p100\nf12\np4242\nf7\n"
	if pid := parseLsofPIDs(strings.NewReader(out), 100); pid != 4242 {
		t.Fatalf("pid = %d", pid)
	}
	if pid := parseLsofPIDs(strings.NewReader("p100\n"), 100); pid != 0 {
		t.Fatalf("pid = %d", pid)
	}
}

// 有 lsof 时识别出连进代理的是哪个进程：另一个进程（curl）连进来时得到它的 PID；测试进程自己连自己时
// 去掉自己后找不到别的进程。
func TestConnectingPID(t *testing.T) {
	if !lsofAvailable() {
		t.Skip("lsof not installed")
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	self := dial(t, listener.Addr().String())
	accepted, err := listener.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer accepted.Close()
	_ = self
	if pid := connectingPID(t.Context(), accepted.RemoteAddr()); pid != 0 {
		t.Fatalf("connectingPID for a connection from this process = %d, want 0", pid)
	}

	curl, err := exec.LookPath("curl")
	if err != nil {
		t.Skip("curl not installed")
	}
	cmd := exec.Command(curl, "-s", "--max-time", "5", "http://"+listener.Addr().String()+"/")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cmd.Process.Kill(); _ = cmd.Wait() }()
	other, err := listener.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	if pid := connectingPID(t.Context(), other.RemoteAddr()); pid != cmd.Process.Pid {
		t.Fatalf("connectingPID = %d, want curl's pid %d", pid, cmd.Process.Pid)
	}
}

// testCluster 是不连数据库的集群骨架：n 个用 sleep 充当的「副本」与一个 Worker，每个副本有数据库与
// matching 代理（指向回显服务端），负载均衡器不做健康检查。
func testCluster(t *testing.T, n int) *cluster {
	t.Helper()
	sleep, err := exec.LookPath("sleep")
	if err != nil {
		t.Skip("sleep not found")
	}
	dir := t.TempDir()
	target := echoServer(t)
	c := &cluster{cfg: commonConfig{Replicas: n, Proxies: true}}
	var backends []string
	for i := range n {
		p := &process{name: replicaName(i), path: sleep, args: []string{"60"}, logPath: filepath.Join(dir, replicaName(i)+".log")}
		if err := p.start(); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = p.kill() })
		c.replicas = append(c.replicas, p)
		c.dbProxy = append(c.dbProxy, startProxy(t, target))
		c.peer = append(c.peer, startProxy(t, target))
		backends = append(backends, target)
	}
	worker := &process{name: "worker-0", path: sleep, args: []string{"60"}, logPath: filepath.Join(dir, "worker-0.log")}
	if err := worker.start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = worker.kill() })
	c.workers = []*process{worker}
	if c.lb, err = newBalancer("127.0.0.1:0", backends, ""); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.lb.close)
	return c
}
