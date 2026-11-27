package app

import (
	"context"
	"database/sql"
	"net"
	"net/netip"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/donutnomad/taskq/membership"
	infradb "github.com/donutnomad/tenuto/server/infra/db"
	"github.com/donutnomad/tenuto/server/infra/persistence/mysqltest"
	"github.com/donutnomad/tenuto/telemetry"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

func TestMain(m *testing.M) {
	code := m.Run()
	mysqltest.Terminate()
	os.Exit(code)
}

// newUnconnectedDB 返回一个还没有建立连接的 MySQL 句柄：database/sql 在第一次查询时才拨号，
// 关闭期的所有权测试不需要服务端。
func newUnconnectedDB(t *testing.T) *gorm.DB {
	t.Helper()
	db, err := gorm.Open(mysql.New(mysql.Config{
		DSN:                       "tenuto:unused@tcp(127.0.0.1:1)/tenuto?parseTime=true",
		SkipInitializeWithVersion: true,
	}), &gorm.Config{TranslateError: true, DisableAutomaticPing: true})
	if err != nil {
		t.Fatal(err)
	}
	return db
}

// *gorm.DB 必须免适配器地满足注入接口，否则依赖方无法直接传入自己的连接。
// 同时确认接口本身足以取回具体类型（Migrator/Callback 等靠 WithContext 派生）。
func TestGormDBSatisfiesInjectionInterface(t *testing.T) {
	var db infradb.DB = newUnconnectedDB(t)
	if db.WithContext(t.Context()).Migrator() == nil {
		t.Fatal("cannot reach *gorm.DB facilities through the DB interface")
	}
}

// 注入数据库后，对应的 DSN 与连接池配置不再是必填项。
func TestValidateSkipsDSNWhenDatabasesInjected(t *testing.T) {
	cfg, err := loadConfig(mapLookup(mysqlTaskQEnv()), "demo-node")
	if err != nil {
		t.Fatal(err)
	}
	cfg.Database.DSN = ""
	cfg.TaskQ.DSN = ""
	cfg.Database.MaxOpenConns = 0
	cfg.Database.MaxIdleConns = 0
	cfg.Database.ConnMaxLifetime = 0
	cfg.Database.ConnMaxIdleTime = 0

	if err := cfg.validate(true, true); err != nil {
		t.Fatalf("validate with both databases injected: %v", err)
	}
	// 未注入时这些字段仍然必填，避免放宽校验波及默认路径。
	if err := cfg.validate(false, false); err == nil {
		t.Fatal("validate accepted an empty DSN without injection")
	}
}

// 只注入 workflow 一侧时，TaskQ 仍要自行拨号，故连接池配置必须继续校验。
func TestValidateStillChecksPoolWhenOnlyWorkflowInjected(t *testing.T) {
	cfg, err := loadConfig(mapLookup(mysqlTaskQEnv()), "demo-node")
	if err != nil {
		t.Fatal(err)
	}
	cfg.Database.DSN = ""
	cfg.Database.MaxOpenConns = 0

	err = cfg.validate(true, false)
	if err == nil || !strings.Contains(err.Error(), "pool settings") {
		t.Fatalf("expected pool settings error, got %v", err)
	}
}

// 注入的连接归调用方所有：应用关闭后它必须仍然可用。需要能执行查询的连接，只在 MySQL 上运行。
func TestCloseKeepsInjectedDatabaseOpen(t *testing.T) {
	injected := mysqltest.NewEmptyDB(t)
	app := &application{
		telemetry: telemetry.NoopProviders(),
		health:    &healthState{},
		options:   runOptions{workflowDB: injected},
	}
	app.close()

	if err := injected.Exec("SELECT 1").Error; err != nil {
		t.Fatalf("injected database was closed by the application: %v", err)
	}
}

// 与上一条对偶：本进程自行拨号建立的连接池必须在关停时释放，否则每次重启都泄漏。
// 这条不变式此前无人覆盖，Store.Close 的所有权守卫才得以静默吞掉默认部署的关闭。
func TestSelfDialedDatabasesAreClosed(t *testing.T) {
	_, workflowSQL := newSelfDialedDB(t)
	_, taskQSQL := newSelfDialedDB(t)

	app := &application{
		telemetry:      telemetry.NoopProviders(),
		health:         &healthState{},
		workflowDB:     workflowSQL,
		taskQDB:        taskQSQL,
		ownsWorkflowDB: true,
		ownsTaskQDB:    true,
	}
	app.close()

	if err := workflowSQL.Ping(); err == nil {
		t.Fatal("self-dialed workflow pool is still open after close")
	}
	if err := taskQSQL.Ping(); err == nil {
		t.Fatal("self-dialed TaskQ pool is still open after close")
	}
}

func newSelfDialedDB(t *testing.T) (*gorm.DB, *sql.DB) {
	t.Helper()
	db := newUnconnectedDB(t)
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	return db, sqlDB
}

// WithMembershipStore 必须真的把数据源换掉：Monitor 应该从注入的 store 取成员，
// GetResolver 能解析出 fake 返回的地址。这条覆盖注入口的**核心价值**——
// 若 app.go 仍写死 a.taskQStore.Membership()，环上就不会出现该成员。
func TestInjectedMembershipStoreDrivesServiceDiscovery(t *testing.T) {
	const serviceName = "matching"
	member := membership.MemberRecord{
		RPCAddress:  net.IPv4(192, 0, 2, 10),
		RPCPort:     7235,
		ServiceName: serviceName,
	}
	fake := &fakeMemberStore{members: []membership.MemberRecord{member}}

	monitor := membership.NewMonitorWithConfig(membership.MonitorConfig{
		WatchedServices: []string{serviceName},
		Store:           fake,
		PollInterval:    20 * time.Millisecond,
		PropagationTime: 20 * time.Millisecond,
		ReplicaPoints:   200,
	})
	monitor.Start()
	defer monitor.Stop()

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := monitor.WaitUntilInitialized(ctx); err != nil {
		t.Fatal(err)
	}

	resolver, err := monitor.GetResolver(serviceName)
	if err != nil {
		t.Fatalf("resolve %q from injected store: %v", serviceName, err)
	}
	want := memberAddress(member)
	waitFor(t, func() bool {
		host, err := resolver.Lookup("any-workflow-key")
		return err == nil && host.GetAddress() == want
	}, "ring never resolved the injected member "+want)

	if get, _ := fake.Counts(); get == 0 {
		t.Fatal("injected store was never asked for active members")
	}
}

// 注册方向：内嵌 matching 时 monitor 带 AdvertisedServices 与广播地址，心跳写进注入的 store，
// 其它副本才能从同一数据源发现本节点。
func TestInjectedMembershipStoreReceivesEmbeddedRegistration(t *testing.T) {
	const serviceName = "matching"
	addr := testAddrPort(t, "127.0.0.1:7235")
	fake := &fakeMemberStore{}
	monitor := membership.NewMonitorWithConfig(membership.MonitorConfig{
		AdvertisedServices: []string{serviceName},
		WatchedServices:    []string{serviceName},
		Store:              fake,
		BroadcastAddr:      addr,
		PollInterval:       20 * time.Millisecond,
		PropagationTime:    20 * time.Millisecond,
		ReplicaPoints:      200,
	})
	monitor.Start()
	defer monitor.Stop()

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := monitor.WaitUntilInitialized(ctx); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		_, upsert := fake.Counts()
		return upsert > 0
	}, "monitor never registered with the injected store")

	// 注入 store 的回读延迟（真实注册中心各有传播期）：成员表里出现本节点后，
	// 环必须能把它解析出来，否则内嵌 matching 在真实部署中无人可连。
	fake.SetMembers([]membership.MemberRecord{
		{RPCAddress: []byte(addr.Addr().AsSlice()), RPCPort: addr.Port(), ServiceName: serviceName},
	})
	resolver, err := monitor.GetResolver(serviceName)
	if err != nil {
		t.Fatal(err)
	}
	local := net.JoinHostPort(addr.Addr().String(), strconv.Itoa(int(addr.Port())))
	waitFor(t, func() bool {
		host, err := resolver.Lookup("workflow-tasks/0")
		return err == nil && host.GetAddress() == local
	}, "ring never resolved the locally advertised member "+local)
}

func memberAddress(m membership.MemberRecord) string {
	return net.JoinHostPort(m.RPCAddress.String(), strconv.Itoa(int(m.RPCPort)))
}

func testAddrPort(t *testing.T, s string) netip.AddrPort {
	t.Helper()
	addr, err := netip.ParseAddrPort(s)
	if err != nil {
		t.Fatal(err)
	}
	return addr
}

func waitFor(t *testing.T, cond func() bool, message string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal(message)
}

// 注入口必须真的接到 app 的装配逻辑上，而不是只存进 options 就没人读。
func TestMembershipStoreOptionIsResolved(t *testing.T) {
	fake := &fakeMemberStore{}
	resolved := resolveRunOptions([]RunOption{WithMembershipStore(fake)})
	if resolved.membershipStore != membership.MemberStore(fake) {
		t.Fatal("WithMembershipStore did not reach runOptions")
	}
	// 不注入时必须是 nil，app.go 才会回落到 TaskQ 自带的 DB store（向后兼容）。
	if def := resolveRunOptions(nil); def.membershipStore != nil {
		t.Fatal("membership store defaulted to non-nil; the DB-backed fallback would be bypassed")
	}
}

// 注入的 store 归调用方：应用关停不得把它关掉（与注入数据库的所有权语义一致）。
func TestCloseKeepsInjectedMembershipStoreUsable(t *testing.T) {
	fake := &fakeMemberStore{}
	app := &application{
		telemetry: telemetry.NoopProviders(),
		health:    &healthState{},
		options:   runOptions{membershipStore: fake},
	}
	app.close()

	if _, err := fake.GetActiveMembers(t.Context(), time.Minute); err != nil {
		t.Fatalf("injected membership store is unusable after close: %v", err)
	}
}

func mysqlTaskQEnv() map[string]string {
	return map[string]string{
		"WORKFLOW_BACKEND":                       "mysql-taskq",
		"WORKFLOW_HTTP_ADDR":                     "127.0.0.1:0",
		"WORKFLOW_DB_DSN":                        "user:pass@tcp(127.0.0.1:3306)/workflow",
		"WORKFLOW_HISTORY_PAGE_TOKEN_KEY_BASE64": strings.Repeat("A", 44),
		"WORKFLOW_TASK_TOKEN_SECRET":             strings.Repeat("s", 32),
		"TASKQ_DB_DSN":                           "user:pass@tcp(127.0.0.1:3306)/taskq",
		"TASKQ_ACTIVITY_QUEUES_JSON":             `{"default":{"physical":"activity-tasks"}}`,
	}
}
