package app

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/donutnomad/tenuto/server/biz/domain/clock"
	"github.com/donutnomad/tenuto/server/infra/persistence/executionrepo"
	"github.com/donutnomad/tenuto/server/infra/systemclock"

	"github.com/donutnomad/taskq/frontend"
	"github.com/donutnomad/taskq/matching"
	"github.com/donutnomad/taskq/membership"
	taskqgormstore "github.com/donutnomad/taskq/store/gormstore"
	"github.com/donutnomad/tenuto/internal/executor"
	"github.com/donutnomad/tenuto/internal/worker"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/server/app/assembly"
	"github.com/donutnomad/tenuto/server/biz/domain/dispatch"
	"github.com/donutnomad/tenuto/server/biz/service/engine"
	"github.com/donutnomad/tenuto/server/biz/usecase/batches"
	"github.com/donutnomad/tenuto/server/biz/usecase/retention"
	"github.com/donutnomad/tenuto/server/biz/usecase/schedules"
	"github.com/donutnomad/tenuto/server/http/runtimehttp"
	"github.com/donutnomad/tenuto/server/infra/db"
	leaderinfra "github.com/donutnomad/tenuto/server/infra/leader"
	"github.com/donutnomad/tenuto/server/infra/memory"
	"github.com/donutnomad/tenuto/server/infra/persistence"
	tenutosql "github.com/donutnomad/tenuto/server/infra/sql"
	"github.com/donutnomad/tenuto/server/infra/taskqueue/taskq"
	"github.com/donutnomad/tenuto/telemetry"
	mysqldriver "github.com/go-sql-driver/mysql"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	gormmysql "gorm.io/driver/mysql"
	"gorm.io/gorm"
)

type application struct {
	cfg     Config
	options runOptions
	// clock 是进程唯一的时间来源：Server、执行存储与保留期任务共用它。
	clock         clock.Clock
	server        *engine.Server
	workflowStore *persistence.Store
	taskQStore    *taskqgormstore.Store
	// workflowDB / taskQDB 供 health check 做 Ping，并在本进程自行拨号时由 close 负责关闭；
	// 由 ownsWorkflowDB / ownsTaskQDB 区分（注入的连接归调用方，不得关闭）。
	workflowDB     *sql.DB
	workflowHandle db.DB
	taskQDB        *sql.DB
	ownsWorkflowDB bool
	ownsTaskQDB    bool
	monitor        membership.Monitor
	// matching 是内嵌的 taskq matching 服务；TASKQ_MATCHING_ENABLED=false 时为 nil（外置模式）。
	matching *matching.Server
	// matchingAddr 是内嵌 matching 的实际监听地址（probeListenAddr 解析后的结果）。
	matchingAddr string
	// matchingReady 在 matching gRPC Server 启动后置 true，close 时置 false，readiness 据此判断。
	matchingReady  atomic.Bool
	frontends      []*frontend.Frontend
	activityRouter *taskq.ActivityQueueRouter
	health         *healthState
	httpServer     *http.Server
	// pprofServer 是 WORKFLOW_PPROF_ADDR 的调试监听；未开启时为 nil。
	pprofServer *http.Server
	// healthServer 是 WORKFLOW_HEALTH_ADDR 的明文健康检查监听；未开启时为 nil。
	healthServer *http.Server
	// scheduleDriver、batchDriver 与 retentionGC 是驱动 Server 的后台任务（单副本时是本地驱动，多副本时是
	// dbleader manager），由 close() 在停止 Server 之前停止。
	scheduleDriver interface{ Stop() }
	batchDriver    interface{ Stop() }
	retentionGC    interface{ Stop() }
	// workflowWorker 在本进程内执行 Workflow Task；WORKFLOW_WORKER_ENABLED=false 时为 nil。
	workflowWorker *worker.Worker
	// reconciler 是 MySQL 后端的周期补偿 leader manager；内存后端用 reconcilerCancel 与 reconcilerWG。
	reconciler       interface{ Stop() }
	reconcilerCancel context.CancelFunc
	reconcilerWG     sync.WaitGroup
	// recoverRuns 是启动恢复（默认 recoverRunningWorkflows），由 startRecovery 在后台运行；recoveryCancel 与
	// recoveryWG 让 close 在停止 Server 之前结束它。
	recoverRuns    func(context.Context)
	recoveryCancel context.CancelFunc
	recoveryWG     sync.WaitGroup
	closeOnce      sync.Once
	telemetry      telemetry.Providers
	// poolMetricUnregisters 注销连接池的 OTel 异步回调。必须在关连接池**之前**调用：
	// 回调持有 *sql.DB，关闭后仍被采集线程回调会读到已关闭的池。
	poolMetricUnregisters []func() error
}

// Run 启动 Tenuto Server 并阻塞至 ctx 取消或 HTTP 服务出错。
//
// 除数据库外的依赖（HTTP、TaskQ、Telemetry 等）均通过 cfg 配置；
// 数据库可用 WithDatabase / WithTaskQDatabase 注入调用方持有的连接，
// 此时对应的 DSN 与连接池配置不再需要填写，连接也不会被本函数关闭。
func Run(ctx context.Context, cfg Config, options ...RunOption) error {
	resolved := resolveRunOptions(options)
	if err := cfg.validate(resolved.workflowDB != nil, resolved.taskQDB != nil); err != nil {
		return err
	}
	startupCtx, startupCancel := context.WithTimeout(ctx, cfg.StartupTimeout)
	defer startupCancel()

	app, err := newApplication(startupCtx, cfg, options...)
	if err != nil {
		return err
	}
	defer app.close()
	return app.run(ctx, startupCtx)
}

// run 完成启动并服务到 ctx 取消或 HTTP 服务出错。startupCtx 只约束启动步骤；启动恢复在后台运行，不占用它。
func (a *application) run(ctx, startupCtx context.Context) error {
	cfg := a.cfg
	lifecycleCtx, lifecycleCancel := context.WithCancel(ctx)
	defer lifecycleCancel()
	if cfg.PprofAddr != "" {
		if err := a.startPprof(cfg.PprofAddr); err != nil {
			return err
		}
	}
	a.startRecovery(lifecycleCtx)
	if err := a.registerWorkflowArtifacts(startupCtx); err != nil {
		return err
	}
	if err := a.startWorkflowWorker(lifecycleCtx, startupCtx); err != nil {
		return err
	}

	// 后台任务先于监听启动：启动失败时由 a.close 停掉已启动的部分，不会留下已在监听的 HTTP 服务。
	scheduleTask := schedules.NewDriverTask(a.server.ScheduleUseCase())
	batchTask := batches.NewDriverTask(a.server.Batches())
	if cfg.Backend == BackendMemory {
		a.scheduleDriver = schedules.StartLocalDriver(scheduleTask)
		a.batchDriver = batches.StartLocalDriver(batchTask)
	} else {
		scheduleDriver, err := leaderinfra.StartScheduleDriver(scheduleTask, a.workflowHandle, cfg.NodeID)
		if err != nil {
			return err
		}
		a.scheduleDriver = scheduleDriver
		batchDriver, err := leaderinfra.StartBatchDriver(batchTask, a.workflowHandle, cfg.NodeID)
		if err != nil {
			return err
		}
		a.batchDriver = batchDriver
	}
	if err := a.startRetentionGC(); err != nil {
		return err
	}
	if err := a.startReconciler(lifecycleCtx); err != nil {
		return err
	}
	listener, err := net.Listen("tcp", cfg.HTTP.Address)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", cfg.HTTP.Address, err)
	}
	if cfg.HTTP.HealthAddress != "" {
		if err := a.startHealthListener(cfg.HTTP.HealthAddress); err != nil {
			_ = listener.Close()
			return err
		}
	}

	a.httpServer.BaseContext = func(net.Listener) context.Context { return lifecycleCtx }
	serveErrors := make(chan error, 1)
	go func() {
		if cfg.HTTP.TLSCertFile != "" {
			serveErrors <- a.httpServer.ServeTLS(listener, "", "")
			return
		}
		serveErrors <- a.httpServer.Serve(listener)
	}()

	a.health.ready.Store(true)
	a.telemetry.Logger.Info("Tenuto Server is ready", "address", listener.Addr().String(), "node_id", cfg.NodeID)

	var runErr error
	select {
	case <-ctx.Done():
	case err := <-serveErrors:
		if !errors.Is(err, http.ErrServerClosed) {
			runErr = fmt.Errorf("serve workflow HTTP: %w", err)
		}
	case <-a.matchingExited():
		// matching 是本进程的 TaskQ 入口，Serve 退出后无法再投递或领取任何任务；
		// 主动关闭（close）发生在本 select 之后，这里只可能是意外退出。
		runErr = fmt.Errorf("embedded matching gRPC server stopped")
	}

	a.health.ready.Store(false)
	lifecycleCancel()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), cfg.HTTP.ShutdownTimeout)
	defer shutdownCancel()
	if err := a.httpServer.Shutdown(shutdownCtx); err != nil {
		_ = a.httpServer.Close()
		runErr = errors.Join(runErr, fmt.Errorf("shutdown workflow HTTP: %w", err))
	}
	return runErr
}

func newApplication(ctx context.Context, cfg Config, options ...RunOption) (_ *application, err error) {
	runOptions := resolveRunOptions(options)
	app := &application{cfg: cfg, options: runOptions, clock: systemclock.Clock{}, health: &healthState{}, telemetry: runOptions.telemetry}
	app.recoverRuns = app.recoverRunningWorkflows
	defer func() {
		if err != nil {
			app.close()
		}
	}()
	taskTokenCodec, err := dispatch.NewTaskTokenCodec(cfg.TaskTokenSecret)
	if err != nil {
		return nil, err
	}
	listenerTLS, err := httpTLSConfig(cfg.HTTP)
	if err != nil {
		return nil, err
	}

	if cfg.Backend == BackendMemory {
		app.server = assembly.NewServer(assembly.Options{
			Stores:         assembly.Stores{Executions: memory.NewExecutionManager().WithClock(app.clock)},
			WorkflowQueue:  memory.NewTaskQueue(1024),
			ActivityRouter: memory.NewActivityQueueRouter(1024),
			TaskTokens:     taskTokenCodec,
			Clock:          app.clock,
		}, engine.WithTelemetry(app.telemetry))
	} else if err := app.configureMySQLTaskQ(ctx, taskTokenCodec); err != nil {
		return nil, err
	}
	runtimeHandler := otelhttp.NewHandler(
		runtimehttp.NewRuntimeHTTPHandler(app.server, runtimehttp.WithTransientErrors(isTransient)),
		"tenuto.runtime",
		otelhttp.WithTracerProvider(app.telemetry.TracerProvider),
		otelhttp.WithMeterProvider(app.telemetry.MeterProvider),
		otelhttp.WithPropagators(app.telemetry.Propagator),
	)
	app.httpServer = &http.Server{
		Addr:              cfg.HTTP.Address,
		Handler:           newHTTPHandler(runtimeHandler, app.health, app.partitionAdmin(), newBearerAuth(cfg.HTTP.AuthTokens)),
		TLSConfig:         listenerTLS,
		ReadHeaderTimeout: cfg.HTTP.ReadHeaderTimeout,
		ReadTimeout:       cfg.HTTP.ReadTimeout,
		// WriteTimeout 必须大于 runtimehttp.MaxRuntimePollTimeout，否则会掐断长轮询。
		// 该约束由 Config.validate 在启动期强制。
		WriteTimeout:   cfg.HTTP.WriteTimeout,
		IdleTimeout:    cfg.HTTP.IdleTimeout,
		MaxHeaderBytes: 1 << 20,
	}
	return app, nil
}

// isTransient 是 Runtime API 识别依赖瞬时失败的分类：MySQL 的连接与锁错误，以及 TaskQ 在分区换主期间用完重试次数。
func isTransient(err error) bool {
	return db.IsTransient(err) || taskq.IsTransient(err)
}

func (a *application) configureMySQLTaskQ(ctx context.Context, taskTokens *dispatch.TaskTokenCodec) error {
	// 注入优先：调用方给了 DB 就直接用（连接池与生命周期归调用方），
	// 否则按 Config 的 DSN 自行拨号。
	var workflowDB db.DB
	var err error
	if a.options.workflowDB != nil {
		workflowDB = a.options.workflowDB
		if a.workflowDB, err = workflowDB.DB(); err != nil {
			return fmt.Errorf("resolve injected workflow database: %w", err)
		}
	} else {
		var workflowSQL *sql.DB
		workflowDB, workflowSQL, err = openMySQL(ctx, a.cfg.Database.DSN, a.cfg.Database)
		if err != nil {
			return fmt.Errorf("open workflow database: %w", err)
		}
		a.workflowDB = workflowSQL
		a.ownsWorkflowDB = true
	}
	a.workflowHandle = workflowDB
	a.workflowStore, err = persistence.New(workflowDB, persistence.Config{
		HistoryPageTokenKey: a.cfg.Database.HistoryPageTokenKey,
		Retention: executionrepo.RetentionConfig{
			Retention: a.cfg.Retention.Retention,
			Jitter:    a.cfg.Retention.Jitter,
		},
		Clock: a.clock,
	})
	if err != nil {
		return fmt.Errorf("initialize workflow store: %w", err)
	}
	if err := a.checkSchema(ctx, workflowDB, tenutosql.TenutoMigrations); err != nil {
		return err
	}

	var taskQDB db.DB
	if a.options.taskQDB != nil {
		taskQDB = a.options.taskQDB
		if a.taskQDB, err = taskQDB.DB(); err != nil {
			return fmt.Errorf("resolve injected TaskQ database: %w", err)
		}
	} else {
		var taskQSQL *sql.DB
		taskQDB, taskQSQL, err = openMySQL(ctx, a.cfg.TaskQ.DSN, a.cfg.Database)
		if err != nil {
			return fmt.Errorf("open TaskQ database: %w", err)
		}
		a.taskQDB = taskQSQL
		a.ownsTaskQDB = true
	}
	if err := a.checkSchema(ctx, taskQDB, tenutosql.TaskQMigrations); err != nil {
		return err
	}
	a.taskQStore, err = taskqgormstore.New(taskQDB, taskqgormstore.Config{})
	if err != nil {
		return fmt.Errorf("initialize TaskQ store: %w", err)
	}

	if err := a.initializeQueuePartitions(ctx); err != nil {
		return err
	}

	// 服务发现数据源：优先用调用方注入的 store（如接到 nacos），否则回落到 TaskQ 自带的
	// DB 轮询 membership。注入的 store 归调用方，close 时不碰它。
	membershipStore := a.options.membershipStore
	if membershipStore == nil {
		membershipStore = a.taskQStore.Membership()
	}
	// 内嵌 matching 时本进程既是注册者也是观察者：AdvertisedServices 非空即写心跳，
	// 广播地址在 matching 启动前由 TASKQ_MATCHING_ADVERTISE_ADDR 或实际监听地址推导好。
	monitorConfig := membership.MonitorConfig{
		WatchedServices: []string{a.cfg.TaskQ.ServiceName},
		Store:           membershipStore,
		PollInterval:    a.cfg.TaskQ.MembershipPollInterval,
		PropagationTime: a.cfg.TaskQ.MembershipPollInterval,
		ReplicaPoints:   a.cfg.TaskQ.MembershipReplicaPoints,
	}
	if a.cfg.TaskQ.MatchingEnabled() {
		actualAddr, err := probeListenAddr(a.cfg.TaskQ.MatchingAddr())
		if err != nil {
			return err
		}
		broadcast, err := advertisedAddr(a.cfg.TaskQ.Matching, actualAddr)
		if err != nil {
			return err
		}
		broadcastAddrPort, err := netip.ParseAddrPort(broadcast)
		if err != nil {
			return fmt.Errorf("parse matching broadcast address %q: %w", broadcast, err)
		}
		monitorConfig.AdvertisedServices = []string{a.cfg.TaskQ.ServiceName}
		monitorConfig.BroadcastAddr = broadcastAddrPort
		a.matchingAddr = actualAddr
	}
	a.monitor = membership.NewMonitorWithConfig(monitorConfig)
	a.monitor.Start()
	if err := a.monitor.WaitUntilInitialized(ctx); err != nil {
		return fmt.Errorf("initialize TaskQ membership: %w", err)
	}
	if a.cfg.TaskQ.MatchingEnabled() {
		if err := a.startEmbeddedMatching(); err != nil {
			return err
		}
	}

	dialOptions, err := taskQDialOptions(a.cfg.TaskQ.TLS, a.telemetry)
	if err != nil {
		return err
	}
	workflowFrontend, err := a.openWorkflowFrontend(ctx, dialOptions)
	if err != nil {
		return err
	}
	a.activityRouter = taskq.NewLazyActivityQueueRouter(
		a.activityPhysicalOverrides(),
		a.dialActivityFrontend(dialOptions),
	)
	a.server = assembly.NewServer(assembly.Options{
		Stores:         assembly.Persistent(a.workflowStore),
		WorkflowQueue:  taskq.NewQueue(workflowFrontend),
		ActivityRouter: a.activityRouter,
		TaskTokens:     taskTokens,
		Clock:          a.clock,
	}, engine.WithTelemetry(a.telemetry))

	resolver, err := a.monitor.GetResolver(a.cfg.TaskQ.ServiceName)
	if err != nil {
		return fmt.Errorf("resolve TaskQ service %q: %w", a.cfg.TaskQ.ServiceName, err)
	}
	a.health.checks = []func(context.Context) error{
		a.workflowDB.PingContext,
		a.taskQDB.PingContext,
		func(context.Context) error {
			_, err := resolver.Lookup("tenuto-server/readiness")
			return err
		},
	}
	if a.cfg.TaskQ.MatchingEnabled() {
		a.health.checks = append(a.health.checks, a.matchingHealthCheck())
	}
	// 两个池分开注册：workflow 与 taskq 的饱和原因完全不同（前者是历史读写，后者是
	// 长轮询占用），合成一个指标就分不出该调谁的 MaxOpenConns。
	if err := a.registerPoolMetrics("workflow", a.workflowDB); err != nil {
		return err
	}
	if err := a.registerPoolMetrics("taskq", a.taskQDB); err != nil {
		return err
	}
	return nil
}

// matchingHealthCheck 报告内嵌 matching 是否在服务。外置模式下本进程不托管 matching，
// 检查恒为通过，避免把「没有内嵌 matching」误报成故障。
func (a *application) matchingHealthCheck() func(context.Context) error {
	return func(context.Context) error {
		if a.cfg.TaskQ.MatchingEnabled() && !a.matchingReady.Load() {
			return fmt.Errorf("embedded matching is not serving")
		}
		return nil
	}
}

// startEmbeddedMatching 在本进程内托管 taskq matching。监听在构造前已由 probeListenAddr
// 探测过，端口冲突在这里以明确错误返回；matching 失败即启动失败，不降级为外置模式。
func (a *application) startEmbeddedMatching() error {
	matchingCfg, err := matchingConfig(a.cfg, a.matchingAddr, a.monitor, a.taskQStore)
	if err != nil {
		return err
	}
	server, err := matching.NewServer(matchingCfg)
	if err != nil {
		return fmt.Errorf("initialize embedded matching: %w", err)
	}
	if err := server.Start(); err != nil {
		return fmt.Errorf("start embedded matching: %w", err)
	}
	a.matching = server
	a.matchingReady.Store(true)
	a.telemetry.Logger.Info("embedded matching started",
		"listen_addr", matchingCfg.ListenAddr, "broadcast_addr", matchingCfg.BroadcastAddr)
	return nil
}

// matchingExited 返回内嵌 matching gRPC 服务端的退出信号；未启用内嵌时返回 nil
// （该 select 分支永远不就绪）。
func (a *application) matchingExited() <-chan struct{} {
	if a.matching == nil {
		return nil
	}
	return a.matching.Exited()
}

// probeListenAddr 绑定配置的监听地址取得实际地址（支持 :0），随即释放，交给 matching 自行监听。
// 绑定失败（端口被占用、地址不可用）在启动期直接报错，错误信息带上具体地址。
func probeListenAddr(addr string) (string, error) {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return "", fmt.Errorf("listen on matching address %s: %w", addr, err)
	}
	actual := listener.Addr().String()
	if err := listener.Close(); err != nil {
		return "", fmt.Errorf("close matching probe listener %s: %w", actual, err)
	}
	return actual, nil
}

// registerPoolMetrics 注册单个连接池的饱和度指标，并把注销函数记下来供 close 调用。
// 注入的连接虽然不归本进程关闭，指标仍要注册——可观测性与所有权无关。
func (a *application) registerPoolMetrics(name string, db *sql.DB) error {
	unregister, err := engine.RegisterDBPoolMetrics(a.telemetry, name, db)
	if err != nil {
		return fmt.Errorf("register %s pool metrics: %w", name, err)
	}
	a.poolMetricUnregisters = append(a.poolMetricUnregisters, unregister)
	return nil
}

// partitionAdmin 返回喂给 /admin/partitions 端点的窄接口。memory 后端没有 taskQStore，
// 返回显式 nil 接口（而非 typed-nil），端点据此回 501。
func (a *application) partitionAdmin() partitionAdmin {
	if a.taskQStore == nil {
		return nil
	}
	return a.taskQStore
}

// activityPhysicalOverrides 把 ActivityQueues 投影成 route→physical 的裸 map，喂给
// ActivityQueueRouter（其签名只认物理名 override，不关心分区数）。分区数字段在
// initializeQueuePartitions 里单独消费。
func (a *application) activityPhysicalOverrides() map[string]string {
	if len(a.cfg.TaskQ.ActivityQueues) == 0 {
		return nil
	}
	overrides := make(map[string]string, len(a.cfg.TaskQ.ActivityQueues))
	for route, queue := range a.cfg.TaskQ.ActivityQueues {
		overrides[route] = queue.Physical
	}
	return overrides
}

// initializeQueuePartitions 在 taskQStore 就绪后、router 建立前，把配置里声明的启动期
// 分区数一次性落到 DB 的 override 行。activity route 的物理名经 wftaskq.PhysicalQueueName
// 单一事实源解析（与 router 运行时路由同源，杜绝漂移）；workflow 的物理名即 WorkflowQueue
// 本身。每个 queue 经 growOnlyInitPartitions 落 read=write=partitions。写失败即 fatal
// （grow-only 拒绝除外，见该函数）。
func (a *application) initializeQueuePartitions(ctx context.Context) error {
	overrides := a.activityPhysicalOverrides()
	for route, queue := range a.cfg.TaskQ.ActivityQueues {
		if queue.Partitions < 1 {
			continue
		}
		physical := taskq.PhysicalQueueName(overrides, protocol.DefaultNamespace, route)
		if err := a.growOnlyInitPartitions(ctx, physical, queue.Partitions); err != nil {
			return fmt.Errorf("initialize partitions for activity queue %q (physical %s): %w", route, physical, err)
		}
	}
	if a.cfg.TaskQ.WorkflowQueuePartitions >= 1 {
		workflowQueue := a.cfg.TaskQ.WorkflowQueue
		if err := a.growOnlyInitPartitions(ctx, workflowQueue, a.cfg.TaskQ.WorkflowQueuePartitions); err != nil {
			return fmt.Errorf("initialize partitions for workflow queue %q: %w", workflowQueue, err)
		}
	}
	return nil
}

// growOnlyInitPartitions 把配置声明的分区数落到 DB，grow-only 由 taskq 的 GrowPartitions
// 单条条件 UPDATE 原子完成（不再自己 read-check-write，杜绝并发下的 lost update）：
//   - 目标 >= 当前 override → 写入；
//   - 目标 == 当前 override → 幂等 no-op；
//   - 目标 < 当前 override → 返回包装 ErrPartitionShrinkRejected 的错误，这里**不 fatal**，
//     只记一条 slog 并跳过，保留更大的运行时值（进程重启不会把运行时经 /admin/partitions
//     扩上去的分区数静默缩回配置值）；
//   - 该 queue 首次配置（override 行不存在）→ 由 GrowPartitions 内部的插入分支覆盖，
//     无需调用方特判「无既有值」。
//
// 注意基准变化：GrowPartitions 比较的是本 queue 的 **override 行**，不像 ResolvePartitions 会
// fallback 到全局 `_default_`。启动期落自己的配置本就不该被别的组件维护的全局默认值当作下界。
// 其它真实 DB 错误照常 fatal。
func (a *application) growOnlyInitPartitions(ctx context.Context, physical string, target int) error {
	err := a.taskQStore.GrowPartitions(ctx, physical, target, target)
	if errors.Is(err, taskqgormstore.ErrPartitionShrinkRejected) {
		a.telemetry.Logger.Info("skip partition init, current >= configured, keeping runtime value",
			"queue", physical, "configured", target, "err", err)
		return nil
	}
	return err
}

// openWorkflowFrontend 只预建 workflow queue 的 frontend——server 自身固定要 poll 它。
// activity frontend 不再预建，改由 ActivityQueueRouter 按 route lazy-dial（见
// dialActivityFrontend），生命周期归 router。
func (a *application) openWorkflowFrontend(ctx context.Context, dialOptions []grpc.DialOption) (*frontend.Frontend, error) {
	client, err := frontend.NewWithContext(ctx, frontend.Config{
		ResolverProvider: a.monitor,
		ServiceName:      a.cfg.TaskQ.ServiceName,
		QueueName:        a.cfg.TaskQ.WorkflowQueue,
		DialOpts:         dialOptions,
		PollTimeout:      a.cfg.TaskQ.PollTimeout,
	})
	if err != nil {
		return nil, fmt.Errorf("initialize TaskQ queue %q: %w", a.cfg.TaskQ.WorkflowQueue, err)
	}
	a.frontends = append(a.frontends, client)
	return client, nil
}

// dialActivityFrontend 返回给 ActivityQueueRouter 用的 lazy-dial 工厂：按物理 queue 名
// 建一个独立生命周期的 frontend，由 router 缓存、空闲回收并在 Close 时收掉。
func (a *application) dialActivityFrontend(dialOptions []grpc.DialOption) func(context.Context, string) (*frontend.Frontend, error) {
	return func(ctx context.Context, physicalQueueName string) (*frontend.Frontend, error) {
		return frontend.NewWithContext(ctx, frontend.Config{
			ResolverProvider: a.monitor,
			ServiceName:      a.cfg.TaskQ.ServiceName,
			QueueName:        physicalQueueName,
			DialOpts:         dialOptions,
			PollTimeout:      a.cfg.TaskQ.PollTimeout,
		})
	}
}

// startRetentionGC 启动 retention GC 扫描器。
//
// retention == 0（默认）时不启动任何东西，并打一条 WARN —— 让使用方看得见自己在裸奔：
// 数据无界增长是个沉默的故障，不提示就没人会发现。
//
// memory 后端在 Config.validateRetention 已被拦下（retention>0 时启动报错），
// 走到这里时 memory 的 retention 必然为 0，故不需要再判后端。
//
// 多副本用 dbleader（与 StartScheduleDriver 同一张 tenuto_leader_lock 表，只是换
// lock_name）。⚠️ 选主不替代幂等：租约切换瞬间可能双 leader，最终互斥仍靠
// DELETE 幂等 + DeleteBranch 幂等兜底。
func (a *application) startRetentionGC() error {
	if !a.cfg.Retention.Enabled() {
		a.telemetry.Logger.Warn("workflow retention GC is disabled; terminal workflow data grows without bound",
			"hint", "set WORKFLOW_RETENTION (>= 1h) to enable automatic reclamation")
		return nil
	}
	gcConfig := retention.Config{
		ScanInterval:     a.cfg.Retention.ScanInterval,
		Batch:            a.cfg.Retention.Batch,
		ScavengeInterval: a.cfg.Retention.ScavengeInterval,
		ScavengeMinAge:   a.cfg.Retention.ScavengeMinAge,
		ScavengeBatch:    a.cfg.Retention.ScavengeBatch,
	}
	// *engine.Server 满足回收用例的 WorkflowLocker 与 Metrics 两个端口。
	task := retention.NewTask(a.workflowStore.Executions, a.server, a.server, a.clock, gcConfig, a.telemetry.Logger)
	retentionGC, err := leaderinfra.StartRetentionGC(task, a.workflowHandle, a.cfg.NodeID)
	if err != nil {
		return err
	}
	a.retentionGC = retentionGC
	a.telemetry.Logger.Info("workflow retention GC started",
		"retention", a.cfg.Retention.Retention,
		"jitter", a.cfg.Retention.Jitter,
		"scan_interval", gcConfig.ScanInterval,
		"batch", gcConfig.Batch,
		"scavenge_interval", gcConfig.ScavengeInterval,
		"scavenge_min_age", gcConfig.ScavengeMinAge)
	return nil
}

// startWorkflowWorker 在本进程内启动 workflow worker：Workflow Task 只依赖服务端已存的
// Program 与 History，直接经 *engine.Server 执行。ctx 决定 worker 寿命，readyCtx 限定启动等待。
func (a *application) startWorkflowWorker(ctx, readyCtx context.Context) error {
	if !a.cfg.WorkflowWorker.IsEnabled() {
		a.telemetry.Logger.Info("embedded workflow worker is disabled; standalone workflow workers must consume workflow tasks",
			"hint", "unset WORKFLOW_WORKER_ENABLED to execute workflow tasks in tenuto-server")
		return nil
	}
	cfg := a.cfg.WorkflowWorker.withDefaults()
	cache, err := executor.NewExecutionCache(executor.ExecutionCacheConfig{
		MaxEntries:        cfg.CacheMaxEntries,
		MaxEstimatedBytes: int64(cfg.CacheMaxBytes),
		IdleTTL:           cfg.CacheIdleTTL,
	})
	if err != nil {
		return fmt.Errorf("create embedded workflow worker cache: %w", err)
	}
	worker := worker.NewWorkflowWorker(a.server, worker.WorkerOptions{
		WorkflowConcurrency: cfg.Concurrency,
		WorkflowTaskTimeout: cfg.TaskTimeout,
		Identity:            a.cfg.NodeID,
		BuildID:             a.cfg.Telemetry.ServiceVersion,
		ExecutionCache:      cache,
		Telemetry:           a.telemetry,
	})
	worker.Start(ctx)
	a.workflowWorker = worker
	if err := worker.WaitReady(readyCtx); err != nil {
		return fmt.Errorf("start embedded workflow worker: %w", err)
	}
	a.telemetry.Logger.Info("embedded workflow worker started", "identity", a.cfg.NodeID, "concurrency", cfg.Concurrency, "task_timeout", cfg.TaskTimeout)
	return nil
}

// startRecovery 在后台运行启动恢复。恢复逐个 Run 补偿副作用、重新入队，耗时随 Running 的 Run 数增长，分区
// 易主时入队还要等新 owner 接管；它与周期 reconciler 一样每一步幂等，可以与正常服务并发，因此不占用启动
// 期限，副本不会因恢复慢而在启动期退出。
func (a *application) startRecovery(ctx context.Context) {
	recoveryCtx, cancel := context.WithCancel(ctx)
	a.recoveryCancel = cancel
	a.recoveryWG.Go(func() { a.recoverRuns(recoveryCtx) })
}

// recoverRunningWorkflows 补偿重启前的 Running Run，再回落已过期的 sticky 投递。失败只记日志：单个 Run 的错误
// 已逐个汇总，未完成的部分由周期 reconciler 继续补偿。
func (a *application) recoverRunningWorkflows(ctx context.Context) {
	started := a.clock.Now()
	if err := a.server.RecoverRunningWorkflows(ctx, a.reconcileLoop().Lookahead()); err != nil {
		a.telemetry.Logger.Error("recover running workflows incomplete; reconciler will retry", "err", err)
	}
	if err := a.server.FallbackExpiredStickyWorkflowTasks(ctx); err != nil {
		a.telemetry.Logger.Error("reconcile sticky workflow tasks incomplete; reconciler will retry", "err", err)
	}
	a.telemetry.Logger.Info("startup recovery finished", "duration", a.clock.Now().Sub(started))
}

// reconcileLoop 是本应用的周期补偿循环（见 engine.Reconciler）。
func (a *application) reconcileLoop() engine.Reconciler {
	return engine.Reconciler{Server: a.server, Interval: a.cfg.ReconcileInterval, FullScanInterval: a.cfg.ReconcileFullScanInterval}
}

// startReconciler 启动周期补偿。单副本（内存后端）在本进程里运行；MySQL 后端经 dbleader 选主，只有 leader
// 副本扫描，副本数不放大扫描量。补偿的每一步都幂等，租约切换瞬间的两个 leader 不会造成重复副作用。
func (a *application) startReconciler(ctx context.Context) error {
	task := a.reconcileLoop()
	if a.cfg.Backend != BackendMemory {
		reconciler, err := leaderinfra.StartReconciler(task, a.workflowHandle, a.cfg.NodeID)
		if err != nil {
			return err
		}
		a.reconciler = reconciler
		return nil
	}
	reconcileCtx, cancel := context.WithCancel(ctx)
	a.reconcilerCancel = cancel
	a.reconcilerWG.Add(1)
	go func() {
		defer a.reconcilerWG.Done()
		_ = task.Start(reconcileCtx)
	}()
	return nil
}

func (a *application) close() {
	a.closeOnce.Do(func() {
		if a.health != nil {
			a.health.ready.Store(false)
		}
		if a.pprofServer != nil {
			_ = a.pprofServer.Close()
		}
		if a.healthServer != nil {
			_ = a.healthServer.Close()
		}
		if a.recoveryCancel != nil {
			a.recoveryCancel()
		}
		a.recoveryWG.Wait()
		if a.reconcilerCancel != nil {
			a.reconcilerCancel()
		}
		a.reconcilerWG.Wait()
		if a.reconciler != nil {
			a.reconciler.Stop()
		}
		// 内嵌 workflow worker 的 ack/nack 依赖 TaskQ 与存储，先于它们停止。
		if a.workflowWorker != nil {
			a.workflowWorker.Stop()
		}
		// schedule 驱动、批量操作驱动与 GC 扫描器先于 Server 停止：进行中的 schedule 触发与批量操作可能启动 Run
		// 并挂上定时器，GC 扫描器会写库；它们停下后 Server 再停定时器，连接池最后关闭。
		if a.scheduleDriver != nil {
			a.scheduleDriver.Stop()
		}
		if a.batchDriver != nil {
			a.batchDriver.Stop()
		}
		if a.retentionGC != nil {
			a.retentionGC.Stop()
		}
		if a.server != nil {
			a.server.Stop()
		}
		// lazy-dial 的 activity frontend 由 router 自管，先停它（停 GC + Close 缓存
		// frontend）；workflow frontend 仍走 a.frontends 路径，两者互不重叠、不会 double-close。
		if a.activityRouter != nil {
			a.activityRouter.Close()
		}
		for i := len(a.frontends) - 1; i >= 0; i-- {
			a.frontends[i].Close()
		}
		// frontend 已断开后停内嵌 matching：它先 cancel 解开长轮询，再优雅停止 gRPC 并释放监听端口。
		if a.matching != nil {
			a.matching.Stop()
			a.matchingReady.Store(false)
		}
		if a.monitor != nil {
			a.monitor.Stop()
		}
		// 注销池指标回调必须早于关闭连接池：回调闭包持有 *sql.DB，
		// 池关掉后若采集线程仍回调进来就会读到已关闭的池。
		for _, unregister := range a.poolMetricUnregisters {
			if err := unregister(); err != nil {
				a.telemetry.Logger.Error("unregister database pool metrics failed", "err", err)
			}
		}
		// 注入的连接归调用方所有，本进程只关自己拨号建立的那些。
		// 两个 store 的 Close 都会无条件关闭底层连接池，故一律不经由它们，
		// 直接关本进程持有的 *sql.DB——所有权信息（owns*）只在这里齐备。
		if a.ownsTaskQDB && a.taskQDB != nil {
			if err := a.taskQDB.Close(); err != nil {
				a.telemetry.Logger.Error("close TaskQ database failed", "err", err)
			}
		}
		if a.ownsWorkflowDB && a.workflowDB != nil {
			if err := a.workflowDB.Close(); err != nil {
				a.telemetry.Logger.Error("close workflow database failed", "err", err)
			}
		}
	})
}

// checkSchema 确认库已执行完本二进制的全部迁移，库落后时启动失败并提示运行 tenuto-server migrate。
// 库比二进制新（新版本已执行 migrate，本副本仍是旧版本）时记警告并继续：迁移保持上一个版本可用（见 docs/deploy/server.md），
// 滚动升级与回滚期间旧副本可以重启。
func (a *application) checkSchema(ctx context.Context, database db.DB, migrations tenutosql.Migrations) error {
	unknown, err := migrations.Check(ctx, database)
	if err != nil {
		return fmt.Errorf("check database schema: %w", err)
	}
	if len(unknown) > 0 {
		a.telemetry.Logger.Warn("database schema is newer than this binary; continuing because migrations keep the previous release working",
			"component", migrations.Component, "binary_schema_version", migrations.Latest(), "unknown_migrations", unknown)
	}
	return nil
}

func openMySQL(ctx context.Context, dsn string, pool DatabaseConfig) (*gorm.DB, *sql.DB, error) {
	parsed, err := mysqldriver.ParseDSN(dsn)
	if err != nil {
		return nil, nil, fmt.Errorf("parse MySQL DSN: %w", err)
	}
	// parseTime、interpolateParams、clientFoundRows 与 loc 由 Server 设置，DSN 里写成别的值也不生效。
	parsed.Loc = time.UTC
	if err := db.ConfigureMySQL(parsed); err != nil {
		return nil, nil, err
	}
	db, err := gorm.Open(gormmysql.Open(parsed.FormatDSN()), &gorm.Config{TranslateError: true})
	if err != nil {
		return nil, nil, err
	}
	sqlDB, err := db.DB()
	if err != nil {
		return nil, nil, err
	}
	sqlDB.SetMaxOpenConns(pool.MaxOpenConns)
	sqlDB.SetMaxIdleConns(pool.MaxIdleConns)
	sqlDB.SetConnMaxLifetime(pool.ConnMaxLifetime)
	sqlDB.SetConnMaxIdleTime(pool.ConnMaxIdleTime)
	if err := sqlDB.PingContext(ctx); err != nil {
		_ = sqlDB.Close()
		return nil, nil, err
	}
	return db, sqlDB, nil
}

func taskQDialOptions(cfg TaskQTLSConfig, providers telemetry.Providers) ([]grpc.DialOption, error) {
	providers = telemetry.Resolve(providers)
	options := []grpc.DialOption{grpc.WithStatsHandler(otelgrpc.NewClientHandler(
		otelgrpc.WithTracerProvider(providers.TracerProvider),
		otelgrpc.WithMeterProvider(providers.MeterProvider),
		otelgrpc.WithPropagators(providers.Propagator),
	))}
	if !cfg.Enabled {
		return append(options, grpc.WithTransportCredentials(insecure.NewCredentials())), nil
	}
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: cfg.ServerName}
	if cfg.CAFile != "" {
		pem, err := os.ReadFile(cfg.CAFile)
		if err != nil {
			return nil, fmt.Errorf("read TASKQ_TLS_CA_FILE: %w", err)
		}
		roots, err := x509.SystemCertPool()
		if err != nil || roots == nil {
			roots = x509.NewCertPool()
		}
		if !roots.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("TASKQ_TLS_CA_FILE contains no certificates")
		}
		tlsConfig.RootCAs = roots
	}
	if cfg.CertFile != "" {
		certificate, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("load TaskQ client certificate: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{certificate}
	}
	return append(options, grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))), nil
}
