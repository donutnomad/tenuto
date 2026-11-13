package main

import (
	"io"
	"net"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestValidateDBName(t *testing.T) {
	for _, name := range []string{"tenuto_e2e", "tenuto_e2e_1", "tenuto_bench", "tenuto_bench_2026_11"} {
		if err := validateDBName(name); err != nil {
			t.Errorf("validateDBName(%q) = %v", name, err)
		}
	}
	for _, name := range []string{"", "tenuto", "tenuto_test_1", "workflow", "tenuto_e2e-1", "tenuto_e2e`; DROP DATABASE x", "x_tenuto_e2e", "tenuto_bench " + strings.Repeat("x", 70)} {
		if err := validateDBName(name); err == nil {
			t.Errorf("validateDBName(%q) accepted", name)
		}
	}
}

func TestParseBaseDSN(t *testing.T) {
	cfg, err := parseBaseDSN("user:pw@tcp(127.0.0.1:3306)/")
	if err != nil {
		t.Fatal(err)
	}
	dsn := dsnWithDB(cfg, "tenuto_e2e_1")
	if !strings.Contains(dsn, "/tenuto_e2e_1?") || !strings.Contains(dsn, "parseTime=true") {
		t.Fatalf("dsn = %s", dsn)
	}
	if _, err := parseBaseDSN("user:pw@tcp(127.0.0.1:3306)/workflow"); err == nil {
		t.Fatal("a DSN with a database name was accepted")
	}
}

func TestReplicaEnv(t *testing.T) {
	secrets := newClusterSecrets(false)
	base, err := parseBaseDSN("user:pw@tcp(127.0.0.1:3306)/")
	if err != nil {
		t.Fatal(err)
	}
	env := func(i int) map[string]string {
		addrs := replicaAddrs{
			HTTP:      "127.0.0.1:" + strconv.Itoa(replicaHTTPPort(17400, i)),
			Matching:  "127.0.0.1:" + strconv.Itoa(replicaMatchingPort(17400, i)),
			Advertise: "127.0.0.1:" + strconv.Itoa(replicaPeerPort(17400, i)),
			DSN:       dsnVia(base, "tenuto_e2e_1", "127.0.0.1:"+strconv.Itoa(replicaDBPort(17400, i))),
			Pprof:     "127.0.0.1:" + strconv.Itoa(replicaPprofPort(17400, i)),
		}
		out := map[string]string{}
		for _, item := range replicaEnv(addrs, secrets, i, 20, []string{"WORKFLOW_RECONCILE_INTERVAL=10s"}) {
			key, value, _ := strings.Cut(item, "=")
			out[key] = value
		}
		return out
	}
	a, b := env(0), env(1)
	for _, key := range []string{"WORKFLOW_TASK_TOKEN_SECRET", "WORKFLOW_HISTORY_PAGE_TOKEN_KEY_BASE64", "WORKFLOW_BACKEND", "WORKFLOW_RECONCILE_INTERVAL", "WORKFLOW_DB_MAX_OPEN_CONNS", "WORKFLOW_DB_MAX_IDLE_CONNS"} {
		if a[key] == "" || a[key] != b[key] {
			t.Errorf("%s must be set and shared: %q vs %q", key, a[key], b[key])
		}
	}
	// 每个副本经自己的数据库代理连到同一个库。
	for _, key := range []string{"WORKFLOW_NODE_ID", "WORKFLOW_HTTP_ADDR", "TASKQ_MATCHING_ADDR", "TASKQ_MATCHING_ADVERTISE_ADDR", "WORKFLOW_DB_DSN", "WORKFLOW_PPROF_ADDR"} {
		if a[key] == "" || a[key] == b[key] {
			t.Errorf("%s must be set and unique per replica: %q vs %q", key, a[key], b[key])
		}
	}
	if a["WORKFLOW_DB_MAX_OPEN_CONNS"] != "20" || a["WORKFLOW_DB_MAX_IDLE_CONNS"] != "10" {
		t.Errorf("pool = %s / %s, want 20 / 10", a["WORKFLOW_DB_MAX_OPEN_CONNS"], a["WORKFLOW_DB_MAX_IDLE_CONNS"])
	}
	if a["TASKQ_DB_DSN"] != a["WORKFLOW_DB_DSN"] || !strings.Contains(a["WORKFLOW_DB_DSN"], "tcp(127.0.0.1:17701)/tenuto_e2e_1?") {
		t.Errorf("DSN = %s, TaskQ DSN = %s", a["WORKFLOW_DB_DSN"], a["TASKQ_DB_DSN"])
	}
	if len(secrets.TaskTokenSecret) < 32 {
		t.Errorf("task token secret has %d bytes", len(secrets.TaskTokenSecret))
	}
	if a["WORKFLOW_HTTP_ADDR"] != "127.0.0.1:17401" || a["TASKQ_MATCHING_ADDR"] != "127.0.0.1:17501" || a["TASKQ_MATCHING_ADVERTISE_ADDR"] != "127.0.0.1:17601" || a["WORKFLOW_PPROF_ADDR"] != "127.0.0.1:17801" {
		t.Errorf("ports = %s, %s, %s, %s", a["WORKFLOW_HTTP_ADDR"], a["TASKQ_MATCHING_ADDR"], a["TASKQ_MATCHING_ADVERTISE_ADDR"], a["WORKFLOW_PPROF_ADDR"])
	}
	// 不启用代理时不设广播地址；不开 -auth 时不设 token；-db-max-open-conns 0 时用 Server 的缺省连接池。
	for _, item := range replicaEnv(replicaAddrs{HTTP: "h", Matching: "m", DSN: "d"}, secrets, 0, 0, nil) {
		if strings.HasPrefix(item, "TASKQ_MATCHING_ADVERTISE_ADDR=") || strings.HasPrefix(item, "WORKFLOW_HTTP_AUTH_TOKEN=") ||
			strings.HasPrefix(item, "WORKFLOW_DB_MAX_OPEN_CONNS=") || strings.HasPrefix(item, "WORKFLOW_DB_MAX_IDLE_CONNS=") {
			t.Errorf("unexpected variable: %s", item)
		}
	}
	// -auth：随机 token 写进每个副本的 WORKFLOW_HTTP_AUTH_TOKEN，长度满足 Server 的下限。
	authSecrets := newClusterSecrets(true)
	if len(authSecrets.AuthToken) < 16 {
		t.Fatalf("auth token = %q", authSecrets.AuthToken)
	}
	if !slices.Contains(replicaEnv(replicaAddrs{HTTP: "h", Matching: "m", DSN: "d"}, authSecrets, 0, 0, nil), "WORKFLOW_HTTP_AUTH_TOKEN="+authSecrets.AuthToken) {
		t.Error("WORKFLOW_HTTP_AUTH_TOKEN missing with -auth")
	}
	// 上限低于 Server 的缺省空闲连接数（10）时空闲连接数随之降低，否则 Server 拒绝启动。
	if env := replicaEnv(replicaAddrs{HTTP: "h", Matching: "m", DSN: "d"}, secrets, 0, 4, nil); !slices.Contains(env, "WORKFLOW_DB_MAX_IDLE_CONNS=4") {
		t.Errorf("env = %v, want WORKFLOW_DB_MAX_IDLE_CONNS=4", env)
	}
}

// 负载均衡器只把新连接分给 up 的后端。
func TestBalancerSkipsDownBackends(t *testing.T) {
	var backends []string
	for _, name := range []string{"a", "b"} {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer listener.Close()
		backends = append(backends, listener.Addr().String())
		go func() {
			for {
				conn, err := listener.Accept()
				if err != nil {
					return
				}
				_, _ = conn.Write([]byte(name))
				_ = conn.Close()
			}
		}()
	}
	lb, err := newBalancer("127.0.0.1:0", backends, "")
	if err != nil {
		t.Fatal(err)
	}
	defer lb.close()
	read := func() string {
		conn, err := net.DialTimeout("tcp", lb.listener.Addr().String(), time.Second)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
		data, _ := io.ReadAll(conn)
		return string(data)
	}
	if got := read(); got != "" {
		t.Fatalf("with every backend down got %q", got)
	}
	lb.setUp(1, true)
	for range 4 {
		if got := read(); got != "b" {
			t.Fatalf("got %q, want b", got)
		}
	}
	lb.setUp(0, true)
	seen := map[string]bool{}
	for range 4 {
		seen[read()] = true
	}
	if !seen["a"] || !seen["b"] {
		t.Fatalf("round robin reached %v", seen)
	}
}
