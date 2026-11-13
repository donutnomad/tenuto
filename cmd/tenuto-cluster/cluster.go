package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	infradb "github.com/donutnomad/tenuto/server/infra/db"
	mysqldriver "github.com/go-sql-driver/mysql"
)

// ── 数据库 ──

var dbNamePattern = regexp.MustCompile(`^tenuto_(e2e|bench)[A-Za-z0-9_]*$`)

// validateDBName 只放行以 tenuto_e2e / tenuto_bench 开头、只含字母数字下划线的库名：工具只会创建并删除这个库。
func validateDBName(name string) error {
	if len(name) > 64 || !dbNamePattern.MatchString(name) {
		return fmt.Errorf("-db %q: the name must match %s (at most 64 characters)", name, dbNamePattern)
	}
	return nil
}

// parseBaseDSN 解析不带库名的 DSN。
func parseBaseDSN(dsn string) (*mysqldriver.Config, error) {
	cfg, err := mysqldriver.ParseDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse -dsn: %w", err)
	}
	if cfg.DBName != "" {
		return nil, fmt.Errorf("-dsn must not name a database (got %q); pass the database with -db", cfg.DBName)
	}
	// 副本由 tenuto-server 按同样的要求设置；工具自己的核对连接也用同样的参数。
	if err := infradb.ConfigureMySQL(cfg); err != nil {
		return nil, fmt.Errorf("-dsn: %w", err)
	}
	return cfg, nil
}

func dsnWithDB(base *mysqldriver.Config, db string) string {
	cfg := base.Clone()
	cfg.DBName = db
	return cfg.FormatDSN()
}

// dsnVia 是经 proxy（127.0.0.1:port）连到 db 的 DSN，其余参数（账号、超时、TLS 等）沿用 base。
func dsnVia(base *mysqldriver.Config, db, proxy string) string {
	cfg := base.Clone()
	cfg.DBName = db
	cfg.Net, cfg.Addr = "tcp", proxy
	return cfg.FormatDSN()
}

// database 持有工具创建的库。
type database struct {
	base  *mysqldriver.Config
	name  string
	admin *sql.DB // 不带库名：CREATE / DROP DATABASE 与 SHOW GLOBAL STATUS
	db    *sql.DB // 连到 name：只读核对
}

// createDatabase 在库不存在时创建一个空库；库已存在时拒绝运行。表由 startCluster 用 tenuto-server migrate 建出。
func createDatabase(ctx context.Context, dsn, name string) (*database, error) {
	if err := validateDBName(name); err != nil {
		return nil, err
	}
	base, err := parseBaseDSN(dsn)
	if err != nil {
		return nil, err
	}
	admin, err := sql.Open("mysql", base.FormatDSN())
	if err != nil {
		return nil, err
	}
	var existing string
	err = admin.QueryRowContext(ctx, "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = ?", name).Scan(&existing)
	switch {
	case err == nil:
		_ = admin.Close()
		return nil, fmt.Errorf("database %s already exists; refusing to touch it (pick another -db or drop it yourself)", name)
	case !errors.Is(err, sql.ErrNoRows):
		_ = admin.Close()
		return nil, fmt.Errorf("check database %s: %w", name, err)
	}
	if _, err := admin.ExecContext(ctx, "CREATE DATABASE `"+name+"` DEFAULT CHARSET utf8mb4"); err != nil {
		_ = admin.Close()
		return nil, fmt.Errorf("create database %s: %w", name, err)
	}
	d := &database{base: base, name: name, admin: admin}
	if d.db, err = sql.Open("mysql", dsnWithDB(base, name)); err != nil {
		d.drop()
		return nil, err
	}
	return d, nil
}

func (d *database) dsn() string { return dsnWithDB(d.base, d.name) }

// drop 删除工具创建的库并关闭连接。
func (d *database) drop() {
	if d.db != nil {
		_ = d.db.Close()
	}
	if err := validateDBName(d.name); err == nil {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if _, err := d.admin.ExecContext(ctx, "DROP DATABASE IF EXISTS `"+d.name+"`"); err != nil {
			fmt.Fprintf(os.Stderr, "drop database %s: %v\n", d.name, err)
		}
	}
	_ = d.admin.Close()
}

func (d *database) close() {
	_ = d.db.Close()
	_ = d.admin.Close()
}

// reconcilerLeader 返回当前持有 workflow:reconciler 锁的副本 NodeID（锁未过期时）。
func (d *database) reconcilerLeader(ctx context.Context) (string, error) {
	var leader string
	err := d.db.QueryRowContext(ctx,
		"SELECT leader_ip FROM tenuto_leader_lock WHERE lock_name = 'workflow:reconciler' AND expire_time >= NOW(3)").Scan(&leader)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return leader, err
}

// ── 进程 ──

// process 是工具启动的一个子进程（副本或 Worker），可以重启。日志追加到 logPath。
type process struct {
	name    string
	path    string
	args    []string
	env     []string
	logPath string

	mu     sync.Mutex
	cmd    *exec.Cmd
	done   chan struct{}
	paused bool
}

func (p *process) start() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.cmd != nil {
		select {
		case <-p.done:
		default:
			return fmt.Errorf("%s is already running", p.name)
		}
	}
	logFile, err := os.OpenFile(p.logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	fmt.Fprintf(logFile, "==== tenuto-cluster: starting %s at %s\n", p.name, time.Now().Format(time.RFC3339Nano))
	cmd := exec.Command(p.path, p.args...)
	cmd.Env = append(os.Environ(), p.env...)
	cmd.Stdout, cmd.Stderr = logFile, logFile
	// 独立进程组：终端的 Ctrl-C 只发给工具，由工具按顺序停止子进程。
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		_ = logFile.Close()
		return fmt.Errorf("start %s: %w", p.name, err)
	}
	done := make(chan struct{})
	go func() {
		_ = cmd.Wait()
		_ = logFile.Close()
		close(done)
	}()
	p.cmd, p.done, p.paused = cmd, done, false
	return nil
}

// setPath 换成另一个二进制，下次 start 生效（滚动升级）。
func (p *process) setPath(path string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.path = path
}

func (p *process) running() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.cmd == nil {
		return false
	}
	select {
	case <-p.done:
		return false
	default:
		return true
	}
}

func (p *process) pid() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.cmd == nil || p.cmd.Process == nil {
		return 0
	}
	return p.cmd.Process.Pid
}

// signal 给本进程（只给工具启动的这个 PID）发信号。
func (p *process) signal(sig syscall.Signal) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.cmd == nil || p.cmd.Process == nil {
		return fmt.Errorf("%s is not running", p.name)
	}
	if err := p.cmd.Process.Signal(sig); err != nil {
		return fmt.Errorf("signal %s: %w", p.name, err)
	}
	switch sig {
	case syscall.SIGSTOP:
		p.paused = true
	case syscall.SIGCONT:
		p.paused = false
	}
	return nil
}

// kill 发 SIGKILL 并等进程退出。
func (p *process) kill() error {
	if err := p.signal(syscall.SIGKILL); err != nil {
		return err
	}
	p.wait(10 * time.Second)
	return nil
}

// stop 优雅停止：暂停中的先 SIGCONT，再 SIGTERM，超时后 SIGKILL。返回进程是否在 timeout 内自行退出。
func (p *process) stop(timeout time.Duration) bool {
	if !p.running() {
		return true
	}
	p.mu.Lock()
	paused := p.paused
	p.mu.Unlock()
	if paused {
		_ = p.signal(syscall.SIGCONT)
	}
	_ = p.signal(syscall.SIGTERM)
	if !p.wait(timeout) {
		_ = p.signal(syscall.SIGKILL)
		p.wait(5 * time.Second)
		return false
	}
	return true
}

func (p *process) wait(timeout time.Duration) bool {
	p.mu.Lock()
	done := p.done
	p.mu.Unlock()
	if done == nil {
		return true
	}
	select {
	case <-done:
		return true
	case <-time.After(timeout):
		return false
	}
}

// logSize 返回日志当前长度，配合 waitLogLine 只看本次启动之后的输出。
func (p *process) logSize() int64 {
	info, err := os.Stat(p.logPath)
	if err != nil {
		return 0
	}
	return info.Size()
}

// waitLogLine 等日志在 offset 之后出现 needle；进程退出或超时返回错误。
func (p *process) waitLogLine(ctx context.Context, offset int64, needle string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		if data, err := readFrom(p.logPath, offset); err == nil && bytes.Contains(data, []byte(needle)) {
			return nil
		}
		if !p.running() {
			return fmt.Errorf("%s exited before becoming ready; see %s", p.name, p.logPath)
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("%s not ready after %s; see %s", p.name, timeout, p.logPath)
		}
		if err := sleepCtx(ctx, 200*time.Millisecond); err != nil {
			return err
		}
	}
}

func readFrom(path string, offset int64) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	if _, err := file.Seek(offset, io.SeekStart); err != nil {
		return nil, err
	}
	return io.ReadAll(file)
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// ── 副本配置 ──

// clusterSecrets 是所有副本必须相同的密钥，每次运行随机生成。
type clusterSecrets struct {
	TaskTokenSecret     string
	HistoryPageTokenKey string
	// AuthToken 是 -auth 时副本的 WORKFLOW_HTTP_AUTH_TOKEN，Worker 子进程与工具自己的 Client 用它调用 Runtime API；
	// 不开 -auth 时为空。
	AuthToken string
}

func newClusterSecrets(auth bool) clusterSecrets {
	token := make([]byte, 32)
	pageKey := make([]byte, 32)
	_, _ = rand.Read(token)
	_, _ = rand.Read(pageKey)
	secrets := clusterSecrets{TaskTokenSecret: hex.EncodeToString(token), HistoryPageTokenKey: base64.StdEncoding.EncodeToString(pageKey)}
	if auth {
		authToken := make([]byte, 32)
		_, _ = rand.Read(authToken)
		secrets.AuthToken = hex.EncodeToString(authToken)
	}
	return secrets
}

func replicaName(i int) string            { return fmt.Sprintf("replica-%d", i) }
func replicaNodeID(i int) string          { return "tenuto-cluster-" + replicaName(i) }
func replicaHTTPPort(base, i int) int     { return base + 1 + i }
func replicaMatchingPort(base, i int) int { return base + 101 + i }
func replicaPeerPort(base, i int) int     { return base + 201 + i }
func replicaDBPort(base, i int) int       { return base + 301 + i }
func replicaPprofPort(base, i int) int    { return base + 401 + i }

// replicaAddrs 是副本 i 的网络配置。启用代理时 DSN 指向本副本的数据库代理，Advertise 是本副本的
// matching 代理（写进 membership 表，其他副本经它连进来）；不启用时 DSN 直连、Advertise 为空。
type replicaAddrs struct {
	HTTP      string
	Matching  string
	Advertise string
	DSN       string
	// Pprof 是副本的 WORKFLOW_PPROF_ADDR（只监听 127.0.0.1），用于从运行中的副本采 CPU profile。
	Pprof string
}

// replicaEnv 是副本 i 的环境变量（docs/deploy/server.md、docs/deploy/production.md）：
// 同一个 Workflow / TaskQ 库与两个密钥；每个副本唯一的 WORKFLOW_NODE_ID（Sticky 路由、分区租约与
// 选主都用它）、HTTP 端口、matching 地址与广播地址（membership 表按 IP:port 区分副本）。重启沿用同一组值。
// dbMaxOpenConns 大于 0 时设置两个连接池的上限（空闲连接数不超过它）。extra 排在最后，同名变量以它为准。
func replicaEnv(addrs replicaAddrs, secrets clusterSecrets, i, dbMaxOpenConns int, extra []string) []string {
	env := []string{
		"WORKFLOW_BACKEND=mysql-taskq",
		"WORKFLOW_HTTP_ADDR=" + addrs.HTTP,
		"WORKFLOW_NODE_ID=" + replicaNodeID(i),
		"WORKFLOW_DB_DSN=" + addrs.DSN,
		"TASKQ_DB_DSN=" + addrs.DSN,
		"WORKFLOW_TASK_TOKEN_SECRET=" + secrets.TaskTokenSecret,
		"WORKFLOW_HISTORY_PAGE_TOKEN_KEY_BASE64=" + secrets.HistoryPageTokenKey,
		"TASKQ_MATCHING_ADDR=" + addrs.Matching,
	}
	if addrs.Pprof != "" {
		env = append(env, "WORKFLOW_PPROF_ADDR="+addrs.Pprof)
	}
	if addrs.Advertise != "" {
		env = append(env, "TASKQ_MATCHING_ADVERTISE_ADDR="+addrs.Advertise)
	}
	if secrets.AuthToken != "" {
		env = append(env, "WORKFLOW_HTTP_AUTH_TOKEN="+secrets.AuthToken)
	}
	if dbMaxOpenConns > 0 {
		env = append(env, "WORKFLOW_DB_MAX_OPEN_CONNS="+strconv.Itoa(dbMaxOpenConns),
			"WORKFLOW_DB_MAX_IDLE_CONNS="+strconv.Itoa(min(dbMaxOpenConns, 10)))
	}
	return append(env, extra...)
}

// ── 负载均衡 ──

// balancer 是进程内的 TCP 负载均衡器，模拟部署文档里副本前面的网关：新连接轮询分给可用的后端，
// 拨号失败时试下一个。后端可用 = 工具启用了它（setUp）且最近一次健康检查通过（healthPath 非空时每秒
// GET 一次，1s 超时，与常见网关的 readiness 探测相同）。已建立的连接不迁移（与常见 L4 网关一致）：
// 副本被 kill 时连接断开，客户端重连。规则（api-blackhole）按后端名作用于经它转发的连接。
type balancer struct {
	listener   net.Listener
	backends   []string
	healthPath string
	enabled    []atomic.Bool
	healthy    []atomic.Bool
	next       atomic.Uint64
	netRules

	wg     sync.WaitGroup
	stop   chan struct{}
	mu     sync.Mutex
	conns  map[*relayConn]struct{}
	closed bool
}

func newBalancer(address string, backends []string, healthPath string) (*balancer, error) {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, fmt.Errorf("load balancer listen %s: %w", address, err)
	}
	b := &balancer{listener: listener, backends: backends, healthPath: healthPath,
		enabled: make([]atomic.Bool, len(backends)), healthy: make([]atomic.Bool, len(backends)),
		stop: make(chan struct{}), conns: map[*relayConn]struct{}{}}
	b.wg.Add(1)
	go b.serve()
	if healthPath != "" {
		b.wg.Add(1)
		go b.checkHealth()
	}
	return b, nil
}

func (b *balancer) url() string { return "http://" + b.listener.Addr().String() }

// setUp 启用或摘除后端 i；启用时视为健康（调用方刚确认过就绪）。
func (b *balancer) setUp(i int, up bool) {
	b.healthy[i].Store(true)
	b.enabled[i].Store(up)
}

func (b *balancer) available(i int) bool { return b.enabled[i].Load() && b.healthy[i].Load() }

func (b *balancer) checkHealth() {
	defer b.wg.Done()
	client := &http.Client{Timeout: time.Second}
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-b.stop:
			return
		case <-ticker.C:
		}
		for i, backend := range b.backends {
			if !b.enabled[i].Load() {
				continue
			}
			ok := false
			if resp, err := client.Get("http://" + backend + b.healthPath); err == nil {
				_ = resp.Body.Close()
				ok = resp.StatusCode == http.StatusOK
			}
			b.healthy[i].Store(ok)
		}
	}
}

func (b *balancer) serve() {
	defer b.wg.Done()
	for {
		conn, err := b.listener.Accept()
		if err != nil {
			return
		}
		b.wg.Add(1)
		go func() {
			defer b.wg.Done()
			b.handle(conn)
		}()
	}
}

func (b *balancer) handle(client net.Conn) {
	start := b.next.Add(1)
	for k := range len(b.backends) {
		i := int((start + uint64(k)) % uint64(len(b.backends)))
		if !b.available(i) {
			continue
		}
		backend, err := net.DialTimeout("tcp", b.backends[i], time.Second)
		if err != nil {
			continue
		}
		conn := newRelayConn(client, &b.netRules, replicaName(i))
		if !b.track(conn, true) {
			_ = backend.Close()
			conn.close()
			return
		}
		conn.run(func() (net.Conn, error) { return backend, nil })
		b.track(conn, false)
		return
	}
	_ = client.Close()
}

func (b *balancer) track(conn *relayConn, add bool) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !add {
		delete(b.conns, conn)
		return true
	}
	if b.closed {
		return false
	}
	b.conns[conn] = struct{}{}
	return true
}

func (b *balancer) close() {
	_ = b.listener.Close()
	close(b.stop)
	b.mu.Lock()
	b.closed = true
	conns := make([]*relayConn, 0, len(b.conns))
	for conn := range b.conns {
		conns = append(conns, conn)
	}
	b.mu.Unlock()
	for _, conn := range conns {
		conn.close()
	}
	b.wg.Wait()
}

// ── 集群 ──

// cluster 是一次运行的全部子进程：N 个副本、M 个 Worker、它们前面的负载均衡器，以及启用代理时每个副本的
// 数据库代理（副本 → MySQL，Workflow 与 TaskQ 两个连接池都经过它）与 matching 代理（其他副本 → 本副本）。
type cluster struct {
	cfg      commonConfig
	db       *database
	lb       *balancer
	replicas []*process
	workers  []*process
	dbProxy  []*faultProxy
	peer     []*faultProxy
	// identifies 报告 matching 代理能否识别连接来自哪个副本（需要 lsof）；不能时 partition 只隔断入站。
	identifies bool
	// authToken 是 -auth 时调用 Runtime API 的 Bearer token；不开 -auth 时为空。
	authToken string

	mu     sync.Mutex
	faults []faultEvent
}

// buildServer 返回 -server-bin，未指定时把 ./server/cmd/tenuto-server 构建到 -out/bin。
func buildServer(ctx context.Context, cfg commonConfig) (string, error) {
	if cfg.ServerBin != "" {
		return cfg.ServerBin, nil
	}
	serverBin := filepath.Join(cfg.Out, "bin", "tenuto-server")
	fmt.Printf("building tenuto-server into %s\n", serverBin)
	build := exec.CommandContext(ctx, "go", "build", "-o", serverBin, "./server/cmd/tenuto-server")
	build.Env = append(os.Environ(), "GOWORK=off")
	build.Stdout, build.Stderr = os.Stdout, os.Stderr
	if err := build.Run(); err != nil {
		return "", fmt.Errorf("build tenuto-server (run from the repository root or pass -server-bin): %w", err)
	}
	return serverBin, nil
}

// migrate 与生产部署相同：用 serverBin 的 tenuto-server migrate 在库上执行 Tenuto 与 TaskQ 的迁移
// （两者共用这个库），直连 MySQL。
func migrate(ctx context.Context, serverBin, dsn string) error {
	cmd := exec.CommandContext(ctx, serverBin, "migrate")
	cmd.Env = append(os.Environ(), "WORKFLOW_BACKEND=mysql-taskq", "WORKFLOW_DB_DSN="+dsn, "TASKQ_DB_DSN="+dsn)
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("%s migrate: %w\n%s", serverBin, err, out)
	}
	return nil
}

// startCluster 用 serverBin 执行迁移，启动代理、副本、负载均衡器与 Worker，等它们就绪。
func startCluster(ctx context.Context, cfg commonConfig, db *database, serverBin string) (*cluster, error) {
	if err := os.MkdirAll(filepath.Join(cfg.Out, "logs"), 0o755); err != nil {
		return nil, err
	}
	if err := migrate(ctx, serverBin, db.dsn()); err != nil {
		return nil, err
	}
	self, err := os.Executable()
	if err != nil {
		return nil, err
	}
	c := &cluster{cfg: cfg, db: db, identifies: cfg.Proxies && lsofAvailable()}
	if cfg.Proxies && !c.identifies {
		fmt.Println("WARNING: lsof not found: partition and isolate blackhole a replica's inbound matching traffic only, not its outbound")
	}
	secrets := newClusterSecrets(cfg.Auth)
	c.authToken = secrets.AuthToken
	backends := make([]string, cfg.Replicas)
	for i := range cfg.Replicas {
		addrs := replicaAddrs{
			HTTP:     "127.0.0.1:" + strconv.Itoa(replicaHTTPPort(cfg.BasePort, i)),
			Matching: "127.0.0.1:" + strconv.Itoa(replicaMatchingPort(cfg.BasePort, i)),
			DSN:      db.dsn(),
			Pprof:    "127.0.0.1:" + strconv.Itoa(replicaPprofPort(cfg.BasePort, i)),
		}
		if cfg.Proxies {
			if err := c.startProxies(i, addrs.Matching); err != nil {
				c.shutdown()
				return nil, err
			}
			addrs.Advertise = c.peer[i].addr()
			addrs.DSN = dsnVia(db.base, db.name, c.dbProxy[i].addr())
		}
		backends[i] = addrs.HTTP
		c.replicas = append(c.replicas, &process{
			name: replicaName(i), path: serverBin,
			env:     replicaEnv(addrs, secrets, i, cfg.DBMaxOpenConns, cfg.ServerEnv),
			logPath: filepath.Join(cfg.Out, "logs", replicaName(i)+".log"),
		})
	}
	if c.lb, err = newBalancer("127.0.0.1:"+strconv.Itoa(cfg.BasePort), backends, "/health/ready"); err != nil {
		c.shutdown()
		return nil, err
	}
	for i := range cfg.Workers {
		name := fmt.Sprintf("worker-%d", i)
		var workerEnv []string
		if secrets.AuthToken != "" {
			workerEnv = []string{workerAuthTokenEnv + "=" + secrets.AuthToken}
		}
		c.workers = append(c.workers, &process{
			name: name, path: self, env: workerEnv,
			args: []string{"worker", "-url", c.lb.url(), "-namespace", cfg.Namespace, "-name", name,
				"-out", cfg.Out, "-concurrency", strconv.Itoa(cfg.WorkerConcurrency)},
			logPath: filepath.Join(cfg.Out, "logs", name+".log"),
		})
	}
	// 副本同时启动（与滚动部署首次上线相同），全部就绪后再启动 Worker。
	errs := make(chan error, cfg.Replicas)
	for i := range c.replicas {
		go func() { errs <- c.startReplica(ctx, i) }()
	}
	var startErr error
	for range c.replicas {
		startErr = errors.Join(startErr, <-errs)
	}
	if startErr != nil {
		c.shutdown()
		return nil, startErr
	}
	for i := range c.workers {
		if err := c.startWorker(ctx, i); err != nil {
			c.shutdown()
			return nil, err
		}
	}
	return c, nil
}

// startProxies 启动副本 i 的数据库代理与 matching 代理。
func (c *cluster) startProxies(i int, matching string) error {
	cfg := c.cfg
	db, err := newFaultProxy("db proxy of "+replicaName(i), "127.0.0.1:"+strconv.Itoa(replicaDBPort(cfg.BasePort, i)), c.db.base.Net, c.db.base.Addr, nil)
	if err != nil {
		return err
	}
	c.dbProxy = append(c.dbProxy, db)
	var identify func(net.Addr) string
	if c.identifies {
		identify = c.identifyReplica
	}
	peer, err := newFaultProxy("matching proxy of "+replicaName(i), "127.0.0.1:"+strconv.Itoa(replicaPeerPort(cfg.BasePort, i)), "tcp", matching, identify)
	if err != nil {
		return err
	}
	c.peer = append(c.peer, peer)
	return nil
}

// identifyReplica 返回从 remote 连进 matching 代理的副本名；不是工具启动的副本时返回空串。
func (c *cluster) identifyReplica(remote net.Addr) string {
	pid := connectingPID(context.Background(), remote)
	if pid == 0 {
		return ""
	}
	for i, replica := range c.replicas {
		if replica.pid() == pid {
			return replicaName(i)
		}
	}
	return ""
}

// startReplica 启动副本 i，等 /health/ready 返回 200 后让负载均衡器把新连接分给它。
func (c *cluster) startReplica(ctx context.Context, i int) error {
	replica := c.replicas[i]
	if err := replica.start(); err != nil {
		return err
	}
	ready := "http://127.0.0.1:" + strconv.Itoa(replicaHTTPPort(c.cfg.BasePort, i)) + "/health/ready"
	client := &http.Client{Timeout: time.Second}
	deadline := time.Now().Add(c.cfg.ReadyTimeout)
	for {
		if resp, err := client.Get(ready); err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				c.lb.setUp(i, true)
				return nil
			}
		}
		if !replica.running() {
			return fmt.Errorf("%s exited before becoming ready; see %s", replica.name, replica.logPath)
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("%s not ready after %s; see %s", replica.name, c.cfg.ReadyTimeout, replica.logPath)
		}
		if err := sleepCtx(ctx, 250*time.Millisecond); err != nil {
			return err
		}
	}
}

func (c *cluster) startWorker(ctx context.Context, i int) error {
	worker := c.workers[i]
	offset := worker.logSize()
	if err := worker.start(); err != nil {
		return err
	}
	return worker.waitLogLine(ctx, offset, workerReadyLine, c.cfg.ReadyTimeout)
}

// recordFault 把一次故障或恢复记入时间线（供执行记录分析推断重复执行的原因），返回记录的时刻。
func (c *cluster) recordFault(action, target string) time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now()
	c.faults = append(c.faults, faultEvent{At: now, Action: action, Target: target})
	fmt.Printf("%s fault: %s %s\n", now.Format("15:04:05.000"), action, target)
	return now
}

func (c *cluster) faultLog() []faultEvent {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]faultEvent(nil), c.faults...)
}

// shutdown 停止全部子进程（Worker 先停，副本后停）、负载均衡器与代理。
func (c *cluster) shutdown() {
	var wg sync.WaitGroup
	for _, worker := range c.workers {
		wg.Add(1)
		go func() { defer wg.Done(); worker.stop(15 * time.Second) }()
	}
	wg.Wait()
	for _, replica := range c.replicas {
		wg.Add(1)
		go func() { defer wg.Done(); replica.stop(25 * time.Second) }()
	}
	wg.Wait()
	if c.lb != nil {
		c.lb.close()
	}
	for _, proxy := range append(slices.Clone(c.dbProxy), c.peer...) {
		proxy.close()
	}
}
