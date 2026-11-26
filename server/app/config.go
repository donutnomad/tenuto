package app

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/donutnomad/tenuto/server/infra/persistence/executionrepo"

	"github.com/donutnomad/tenuto/internal/worker"
	"github.com/donutnomad/tenuto/protocol/runtimeapi"
	"github.com/donutnomad/tenuto/server/biz/domain/dispatch"
	"github.com/donutnomad/tenuto/server/biz/service/engine"
	"github.com/donutnomad/tenuto/server/biz/usecase/retention"
	"github.com/donutnomad/tenuto/telemetry"
)

const (
	BackendMySQLTaskQ Backend = "mysql-taskq"
	BackendMemory     Backend = "memory"

	defaultHTTPAddress       = ":7400"
	defaultWorkflowTaskQueue = "workflow-tasks"
	defaultTaskQServiceName  = "matching"
	// defaultMatchingAddr 是内嵌 matching 的 gRPC 监听地址；广播地址由它推导（见 matching.go）。
	defaultMatchingAddr      = ":7235"
	defaultStartupTimeout    = 30 * time.Second
	defaultShutdownTimeout   = 20 * time.Second
	defaultReadHeaderTimeout = 5 * time.Second
	defaultIdleTimeout       = 2 * time.Minute
	// defaultReadTimeout 限制读完整个请求体的时长，防慢速 body 长期占用 goroutine。
	// 不受长轮询影响：长轮询的等待发生在**请求读完之后**的 handler 里。
	defaultReadTimeout = 30 * time.Second
	// defaultWriteTimeout 必须大于长轮询的时长上界，否则会把正常的长轮询连接掐断。
	//
	// 上界由 runtimehttp.MaxRuntimePollTimeout（1 分钟）在**服务端**钳死：
	// runtimePollContext 对客户端传来的 TimeoutMillis 取 min，两个长轮询 handler
	// 都走它，无旁路。故 handler 阻塞时长 ≤ 1 分钟。
	//
	// WriteTimeout 从读完请求头起算（不是从开始写响应起算），覆盖的是
	// 「poll 阻塞 + 响应序列化」的总和，故在上界之上留 100% 余量。
	defaultWriteTimeout      = 2 * runtimeapi.MaxRuntimePollTimeout
	defaultReconcileInterval = 30 * time.Second
	// 全量补偿兜底没有到期索引的崩溃窗口，频率远低于按到期索引的周期补偿。
	defaultReconcileFullScanInterval = 10 * time.Minute
	defaultTaskQPollTimeout          = 30 * time.Second
	defaultMembershipPollInterval    = 3 * time.Second
	defaultMembershipReplicaPoints   = 200
	defaultDatabaseMaxOpenConns      = 50
	// defaultDatabaseReadTimeout 大于 InnoDB 行锁等待的缺省上限（innodb_lock_wait_timeout = 50s），
	// 锁等待仍以 MySQL 1205 返回，不被当成连接失败。
	defaultDatabaseReadTimeout  = 60 * time.Second
	defaultDatabaseWriteTimeout = 30 * time.Second
	// defaultMaxInFlightPerConn 是 WORKFLOW_HTTP_MAX_IN_FLIGHT 缺省值相对 Workflow 连接池大小的倍数。
	defaultMaxInFlightPerConn      = 4
	defaultDatabaseMaxIdleConns    = 10
	defaultDatabaseConnMaxLifetime = 30 * time.Minute
	defaultDatabaseConnMaxIdleTime = 5 * time.Minute
	// memory 后端未设 WORKFLOW_TASK_TOKEN_SECRET 时使用的开发密钥。
	defaultDevelopmentTokenSecret = "workflow-task-token-development-secret"
	defaultOTELServiceName        = "tenuto-server"

	// 内嵌 workflow worker 的默认值；WFT 超时沿用 engine.DefaultWorkflowTaskTimeout。
	defaultWorkflowWorkerConcurrency     = 8
	defaultWorkflowWorkerCacheMaxEntries = 1000
	defaultWorkflowWorkerCacheMaxBytes   = 256 << 20
	defaultWorkflowWorkerCacheIdleTTL    = 5 * time.Minute

	// defaultRetention 为 0：**显式开启才删数据**。默认回收是不可接受的语义变化。
	defaultRetention       = time.Duration(0)
	defaultRetentionJitter = 30 * time.Minute
	defaultGCScanInterval  = retention.DefaultScanInterval
	defaultGCBatch         = retention.DefaultBatch
	// 孤儿兜底扫描：低频 + 年龄下界 + 单轮上限，对应 Temporal 的 history scavenger。
	defaultScavengeInterval = retention.DefaultScavengeInterval
	defaultScavengeMinAge   = retention.DefaultScavengeMinAge
	defaultScavengeBatch    = retention.DefaultScavengeBatch
)

type Backend string

type Config struct {
	Backend         Backend
	TaskTokenSecret []byte
	HTTP            HTTPConfig
	Database        DatabaseConfig
	TaskQ           TaskQConfig
	Telemetry       telemetry.Config
	NodeID          string
	StartupTimeout  time.Duration
	// ReconcileInterval 是周期补偿的间隔：每轮只处理有到期或待处理事项的 Running Run。
	ReconcileInterval time.Duration
	// ReconcileFullScanInterval 是全量补偿的间隔：逐个检查全部 Running Run，兜底没有到期索引的崩溃窗口。
	ReconcileFullScanInterval time.Duration
	// Retention 控制终态数据的自动回收。零值 = 关闭，行为与引入 GC 之前完全一致。
	Retention RetentionConfig
	// Artifacts 控制启动时加载已生成的 Workflow 产物目录（WORKFLOW_ARTIFACTS）。
	Artifacts ArtifactConfig
	// WorkflowWorker 控制本进程内执行 Workflow Task 的 worker。
	WorkflowWorker WorkflowWorkerConfig
	// QueryConcurrency 是同时在本进程重放 History 求值 Query 的上限（WORKFLOW_QUERY_CONCURRENCY）；<=0 取引擎默认值。
	QueryConcurrency int
	// PprofAddr 非空时在这个地址上提供 net/http/pprof（WORKFLOW_PPROF_ADDR），缺省关闭。调试用，不鉴权，
	// 只绑定本机或内网地址。
	PprofAddr string
}

// WorkflowWorkerConfig 是内嵌 workflow worker 的配置。零值即默认配置：Enabled 为 nil
// 表示启用，其余字段 <=0 时取默认值，直接构造 Config 的调用方与 LoadConfig 语义一致。
type WorkflowWorkerConfig struct {
	Enabled         *bool
	Concurrency     int
	TaskTimeout     time.Duration
	CacheMaxEntries int
	CacheMaxBytes   int
	CacheIdleTTL    time.Duration
}

// IsEnabled 报告本进程是否执行 Workflow Task。nil 表示默认启用。
func (c WorkflowWorkerConfig) IsEnabled() bool {
	return c.Enabled == nil || *c.Enabled
}

func (c WorkflowWorkerConfig) withDefaults() WorkflowWorkerConfig {
	if c.Concurrency <= 0 {
		c.Concurrency = defaultWorkflowWorkerConcurrency
	}
	if c.TaskTimeout <= 0 {
		c.TaskTimeout = worker.DefaultWorkflowTaskTimeout
	}
	if c.CacheMaxEntries <= 0 {
		c.CacheMaxEntries = defaultWorkflowWorkerCacheMaxEntries
	}
	if c.CacheMaxBytes <= 0 {
		c.CacheMaxBytes = defaultWorkflowWorkerCacheMaxBytes
	}
	if c.CacheIdleTTL <= 0 {
		c.CacheIdleTTL = defaultWorkflowWorkerCacheIdleTTL
	}
	return c
}

// ArtifactConfig 是生成产物启动加载配置。
type ArtifactConfig struct {
	// Dirs 是产物目录；递归查找 workflow.json。空 = 关闭。
	Dirs []string
	// Namespace 是产物登记到的 namespace。
	Namespace string
	// Publish 为 true 时启动加载会把 latest 指向这些产物；默认只登记，
	// 保证旧 Worker 重启不会把 latest 拉回旧版本。
	Publish bool
}

// RetentionConfig 是 retention GC 的配置。
type RetentionConfig struct {
	// Retention 是终态到可回收之间的最小间隔。0 表示关闭 GC（默认）。
	// 显式开启才会删数据 —— 默认删数据是不可接受的语义。
	Retention time.Duration
	// Jitter 是叠加在到期时刻上的随机抖动上界，防批量关闭同秒打爆 DB。
	Jitter time.Duration
	// ScanInterval 是扫描器两轮之间的间隔。
	ScanInterval time.Duration
	// Batch 是单轮最多处理的 tree 数。
	Batch int
	// ScavengeInterval 是孤儿兜底扫描的间隔（默认每天一次）。兜底扫描回收主路径
	// **结构上够不着**的残留：treeID=="" 的终态 run、以及 run 已删但事件还在的孤儿。
	ScavengeInterval time.Duration
	// ScavengeMinAge 是兜底扫描的年龄下界，只回收比它更老的数据。**防误删的核心
	// 防线**：判定孤儿靠「关联行查不到」，而在途写入存在短暂的不一致窗口。
	ScavengeMinAge time.Duration
	// ScavengeBatch 是单轮单类孤儿的处理上限。
	ScavengeBatch int
}

// Enabled 报告 GC 是否开启。
func (c RetentionConfig) Enabled() bool { return c.Retention > 0 }

type HTTPConfig struct {
	Address     string
	TLSCertFile string
	TLSKeyFile  string
	// ClientCAFile 非空时 HTTP 监听要求客户端出示由这份 CA 签发的证书（WORKFLOW_HTTP_TLS_CLIENT_CA_FILE，mTLS），
	// 需要同时配置 TLSCertFile 与 TLSKeyFile。
	ClientCAFile string
	// AuthTokens 非空时 /runtime/* 与 /admin/* 要求 `Authorization: Bearer <token>`，与其中任一 token 相同即通过
	// （WORKFLOW_HTTP_AUTH_TOKEN 或 WORKFLOW_HTTP_AUTH_TOKEN_FILE）。/health/* 不鉴权。
	AuthTokens []string
	// HealthAddress 非空时另开一个明文 HTTP 监听，只提供 /health/live 与 /health/ready（WORKFLOW_HEALTH_ADDR），
	// 供无法出示客户端证书的编排平台探针使用。
	HealthAddress     string
	ShutdownTimeout   time.Duration
	ReadHeaderTimeout time.Duration
	// ReadTimeout 是读完整个请求（含 body）的上限。
	ReadTimeout time.Duration
	// WriteTimeout 是从读完请求头到写完响应的上限。**必须大于长轮询的时长上界**
	// （runtimehttp.MaxRuntimePollTimeout），否则正常的长轮询会被 server 掐断。
	WriteTimeout time.Duration
	IdleTimeout  time.Duration
	// MaxInFlight 是 /runtime/* 上同时处理的非长轮询请求数上限（WORKFLOW_HTTP_MAX_IN_FLIGHT），超出时应答 503 与
	// Retry-After。缺省为 WORKFLOW_DB_MAX_OPEN_CONNS 的 defaultMaxInFlightPerConn 倍：请求在连接池上排队的深度
	// 有界，数据库变慢时积压不会无限增长。长轮询（领取 Task、wait-closed、Update）不计入。
	MaxInFlight int
}

type DatabaseConfig struct {
	DSN                 string
	HistoryPageTokenKey []byte
	MaxOpenConns        int
	MaxIdleConns        int
	ConnMaxLifetime     time.Duration
	ConnMaxIdleTime     time.Duration
	// ReadTimeout / WriteTimeout 是 DSN 没有给出 readTimeout / writeTimeout 时使用的连接 I/O 超时：数据库失联或
	// 主从切换时，挂在连接上的读写在这个期限内以连接错误返回，不无限期占住连接与请求。0 表示不设。
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
}

type TaskQConfig struct {
	DSN                     string
	ServiceName             string
	WorkflowQueue           string
	WorkflowQueuePartitions int
	ActivityQueues          map[string]ActivityQueueConfig
	PollTimeout             time.Duration
	MembershipPollInterval  time.Duration
	MembershipReplicaPoints int
	TLS                     TaskQTLSConfig
	// Matching 控制本进程是否托管 matching（内嵌模式）。
	Matching MatchingConfig
}

// MatchingConfig 是内嵌 matching 的配置。Addr 是 gRPC 监听地址。AdvertiseAddr 为空时由 Addr
// 推导写进 membership 表的广播地址（推导规则见 broadcastAddr）；非空时广播它（同样按 broadcastAddr
// 解析 host），用于其他副本经代理、NAT 或端口映射才能到达本副本的部署。
//
// Enabled 是 *bool 而非 bool：默认托管 matching，而 bool 零值表达不了「未设置」。nil 表示
// 采用默认值 true，直接构造 Config 的调用方与 LoadConfig 得到同一语义。
type MatchingConfig struct {
	Enabled       *bool
	Addr          string
	AdvertiseAddr string
}

// MatchingEnabled 报告本进程是否托管 matching。nil 表示默认启用。
func (c TaskQConfig) MatchingEnabled() bool {
	return c.Matching.Enabled == nil || *c.Matching.Enabled
}

// MatchingAddr 返回内嵌 matching 的监听地址，未配置时取默认值。
func (c TaskQConfig) MatchingAddr() string {
	if c.Matching.Addr == "" {
		return defaultMatchingAddr
	}
	return c.Matching.Addr
}

// ActivityQueueConfig 是单个 activity route 的配置：Physical 是其物理 queue 名；
// Partitions>=1 时表示启动初始化对该物理 queue 覆盖 read=write=Partitions，
// 0/省略表示不覆盖分区（走运行时/默认）。
type ActivityQueueConfig struct {
	Physical   string
	Partitions int
}

type TaskQTLSConfig struct {
	Enabled    bool
	CAFile     string
	ServerName string
	CertFile   string
	KeyFile    string
}

func LoadConfig() (Config, error) {
	hostname, err := os.Hostname()
	if err != nil {
		return Config{}, fmt.Errorf("resolve hostname: %w", err)
	}
	return loadConfig(os.LookupEnv, hostname)
}

type envLookup func(string) (string, bool)

func loadConfig(lookup envLookup, hostname string) (Config, error) {
	backend := Backend(envOrDefault(lookup, "WORKFLOW_BACKEND", string(BackendMySQLTaskQ)))
	if backend != BackendMySQLTaskQ && backend != BackendMemory {
		return Config{}, fmt.Errorf("WORKFLOW_BACKEND must be %q or %q", BackendMySQLTaskQ, BackendMemory)
	}

	var databaseDSN string
	var tokenKey []byte
	var activityQueues map[string]ActivityQueueConfig
	var taskTokenSecret string
	if backend == BackendMySQLTaskQ {
		var err error
		databaseDSN, err = requiredEnv(lookup, "WORKFLOW_DB_DSN")
		if err != nil {
			return Config{}, err
		}
		tokenKey, err = decodeTokenKey(lookup)
		if err != nil {
			return Config{}, err
		}
		activityQueues, err = activityQueueConfig(lookup)
		if err != nil {
			return Config{}, err
		}
		taskTokenSecret, err = requiredEnv(lookup, "WORKFLOW_TASK_TOKEN_SECRET")
		if err != nil {
			return Config{}, err
		}
	} else {
		taskTokenSecret = envOrDefault(lookup, "WORKFLOW_TASK_TOKEN_SECRET", defaultDevelopmentTokenSecret)
	}

	cfg := Config{
		Backend:         backend,
		TaskTokenSecret: []byte(taskTokenSecret),
		HTTP: HTTPConfig{
			Address:           envOrDefault(lookup, "WORKFLOW_HTTP_ADDR", defaultHTTPAddress),
			TLSCertFile:       envOrDefault(lookup, "WORKFLOW_HTTP_TLS_CERT_FILE", ""),
			TLSKeyFile:        envOrDefault(lookup, "WORKFLOW_HTTP_TLS_KEY_FILE", ""),
			ClientCAFile:      envOrDefault(lookup, "WORKFLOW_HTTP_TLS_CLIENT_CA_FILE", ""),
			HealthAddress:     envOrDefault(lookup, "WORKFLOW_HEALTH_ADDR", ""),
			ShutdownTimeout:   defaultShutdownTimeout,
			ReadHeaderTimeout: defaultReadHeaderTimeout,
			ReadTimeout:       defaultReadTimeout,
			WriteTimeout:      defaultWriteTimeout,
			IdleTimeout:       defaultIdleTimeout,
		},
		Database: DatabaseConfig{
			DSN:                 databaseDSN,
			HistoryPageTokenKey: tokenKey,
			MaxOpenConns:        defaultDatabaseMaxOpenConns,
			MaxIdleConns:        defaultDatabaseMaxIdleConns,
			ConnMaxLifetime:     defaultDatabaseConnMaxLifetime,
			ConnMaxIdleTime:     defaultDatabaseConnMaxIdleTime,
			ReadTimeout:         defaultDatabaseReadTimeout,
			WriteTimeout:        defaultDatabaseWriteTimeout,
		},
		TaskQ: TaskQConfig{
			DSN:                     envOrDefault(lookup, "TASKQ_DB_DSN", databaseDSN),
			ServiceName:             envOrDefault(lookup, "TASKQ_SERVICE_NAME", defaultTaskQServiceName),
			WorkflowQueue:           envOrDefault(lookup, "TASKQ_WORKFLOW_QUEUE", defaultWorkflowTaskQueue),
			ActivityQueues:          activityQueues,
			PollTimeout:             defaultTaskQPollTimeout,
			MembershipPollInterval:  defaultMembershipPollInterval,
			MembershipReplicaPoints: defaultMembershipReplicaPoints,
			TLS: TaskQTLSConfig{
				CAFile:     envOrDefault(lookup, "TASKQ_TLS_CA_FILE", ""),
				ServerName: envOrDefault(lookup, "TASKQ_TLS_SERVER_NAME", ""),
				CertFile:   envOrDefault(lookup, "TASKQ_TLS_CERT_FILE", ""),
				KeyFile:    envOrDefault(lookup, "TASKQ_TLS_KEY_FILE", ""),
			},
			Matching: MatchingConfig{
				Addr:          envOrDefault(lookup, "TASKQ_MATCHING_ADDR", defaultMatchingAddr),
				AdvertiseAddr: envOrDefault(lookup, "TASKQ_MATCHING_ADVERTISE_ADDR", ""),
			},
		},
		Artifacts: ArtifactConfig{
			Dirs:      splitNonEmpty(envOrDefault(lookup, "WORKFLOW_ARTIFACTS", ""), ","),
			Namespace: envOrDefault(lookup, "WORKFLOW_ARTIFACTS_NAMESPACE", "default"),
		},
		NodeID:                    envOrDefault(lookup, "WORKFLOW_NODE_ID", hostname),
		PprofAddr:                 envOrDefault(lookup, "WORKFLOW_PPROF_ADDR", ""),
		StartupTimeout:            defaultStartupTimeout,
		ReconcileInterval:         defaultReconcileInterval,
		ReconcileFullScanInterval: defaultReconcileFullScanInterval,
		QueryConcurrency:          engine.DefaultQueryConcurrency,
		Retention: RetentionConfig{
			Retention:        defaultRetention,
			Jitter:           defaultRetentionJitter,
			ScanInterval:     defaultGCScanInterval,
			Batch:            defaultGCBatch,
			ScavengeInterval: defaultScavengeInterval,
			ScavengeMinAge:   defaultScavengeMinAge,
			ScavengeBatch:    defaultScavengeBatch,
		},
		Telemetry: telemetry.Config{
			ServiceName:          envOrDefault(lookup, "OTEL_SERVICE_NAME", defaultOTELServiceName),
			TraceSampleRatio:     1,
			MetricExportInterval: telemetry.DefaultMetricExportInterval,
			LogLevel:             slog.LevelInfo,
			ShutdownTimeout:      telemetry.DefaultShutdownTimeout,
		},
	}
	cfg.Telemetry.ServiceInstanceID = cfg.NodeID
	authTokens, err := loadAuthTokens(lookup)
	if err != nil {
		return Config{}, err
	}
	cfg.HTTP.AuthTokens = authTokens

	if err := applyBool(lookup, "WORKFLOW_ARTIFACTS_PUBLISH", &cfg.Artifacts.Publish); err != nil {
		return Config{}, err
	}
	if err := applyBoolPtr(lookup, "TASKQ_MATCHING_ENABLED", &cfg.TaskQ.Matching.Enabled); err != nil {
		return Config{}, err
	}
	if err := applyBoolPtr(lookup, "WORKFLOW_WORKER_ENABLED", &cfg.WorkflowWorker.Enabled); err != nil {
		return Config{}, err
	}
	if err := applyBool(lookup, "TASKQ_TLS_ENABLED", &cfg.TaskQ.TLS.Enabled); err != nil {
		return Config{}, err
	}
	if err := applyBool(lookup, "TENUTO_OTEL_ENABLED", &cfg.Telemetry.Enabled); err != nil {
		return Config{}, err
	}

	durations := []struct {
		name   string
		target *time.Duration
	}{
		{"WORKFLOW_STARTUP_TIMEOUT", &cfg.StartupTimeout},
		{"WORKFLOW_RECONCILE_INTERVAL", &cfg.ReconcileInterval},
		{"WORKFLOW_RECONCILE_FULL_SCAN_INTERVAL", &cfg.ReconcileFullScanInterval},
		{"WORKFLOW_HTTP_SHUTDOWN_TIMEOUT", &cfg.HTTP.ShutdownTimeout},
		{"WORKFLOW_HTTP_READ_HEADER_TIMEOUT", &cfg.HTTP.ReadHeaderTimeout},
		{"WORKFLOW_HTTP_READ_TIMEOUT", &cfg.HTTP.ReadTimeout},
		{"WORKFLOW_HTTP_WRITE_TIMEOUT", &cfg.HTTP.WriteTimeout},
		{"WORKFLOW_HTTP_IDLE_TIMEOUT", &cfg.HTTP.IdleTimeout},
		{"WORKFLOW_DB_CONN_MAX_LIFETIME", &cfg.Database.ConnMaxLifetime},
		{"WORKFLOW_DB_CONN_MAX_IDLE_TIME", &cfg.Database.ConnMaxIdleTime},
		{"TASKQ_POLL_TIMEOUT", &cfg.TaskQ.PollTimeout},
		{"TASKQ_MEMBERSHIP_POLL_INTERVAL", &cfg.TaskQ.MembershipPollInterval},
		// WORKFLOW_RETENTION 走 applyDuration 而非 applyPositiveDuration：
		// 0 是合法值（= 关闭 GC），下界由 Validate 统一把关。
		{"WORKFLOW_RETENTION", &cfg.Retention.Retention},
		{"WORKFLOW_RETENTION_JITTER", &cfg.Retention.Jitter},
	}
	if err := applyTraceSampleRatio(lookup, &cfg.Telemetry.TraceSampleRatio); err != nil {
		return Config{}, err
	}
	if err := applyLogLevel(lookup, &cfg.Telemetry.LogLevel); err != nil {
		return Config{}, err
	}
	for _, value := range durations {
		if err := applyDuration(lookup, value.name, value.target); err != nil {
			return Config{}, err
		}
	}
	telemetryDurations := []struct {
		name   string
		target *time.Duration
	}{
		{"TENUTO_OTEL_METRIC_EXPORT_INTERVAL", &cfg.Telemetry.MetricExportInterval},
		{"TENUTO_OTEL_SHUTDOWN_TIMEOUT", &cfg.Telemetry.ShutdownTimeout},
		{"WORKFLOW_GC_SCAN_INTERVAL", &cfg.Retention.ScanInterval},
		{"WORKFLOW_GC_SCAVENGE_INTERVAL", &cfg.Retention.ScavengeInterval},
		{"WORKFLOW_GC_SCAVENGE_MIN_AGE", &cfg.Retention.ScavengeMinAge},
		{"WORKFLOW_WORKER_TASK_TIMEOUT", &cfg.WorkflowWorker.TaskTimeout},
		{"WORKFLOW_WORKER_CACHE_IDLE_TTL", &cfg.WorkflowWorker.CacheIdleTTL},
	}
	for _, value := range telemetryDurations {
		if err := applyPositiveDuration(lookup, value.name, value.target); err != nil {
			return Config{}, err
		}
	}

	integers := []struct {
		name   string
		target *int
	}{
		{"WORKFLOW_DB_MAX_OPEN_CONNS", &cfg.Database.MaxOpenConns},
		{"WORKFLOW_DB_MAX_IDLE_CONNS", &cfg.Database.MaxIdleConns},
		{"TASKQ_MEMBERSHIP_REPLICA_POINTS", &cfg.TaskQ.MembershipReplicaPoints},
		{"WORKFLOW_GC_BATCH", &cfg.Retention.Batch},
		{"WORKFLOW_GC_SCAVENGE_BATCH", &cfg.Retention.ScavengeBatch},
		{"WORKFLOW_WORKER_CONCURRENCY", &cfg.WorkflowWorker.Concurrency},
		{"WORKFLOW_WORKER_CACHE_MAX_ENTRIES", &cfg.WorkflowWorker.CacheMaxEntries},
		{"WORKFLOW_WORKER_CACHE_MAX_BYTES", &cfg.WorkflowWorker.CacheMaxBytes},
		{"WORKFLOW_QUERY_CONCURRENCY", &cfg.QueryConcurrency},
	}
	for _, value := range integers {
		if err := applyPositiveInt(lookup, value.name, value.target); err != nil {
			return Config{}, err
		}
	}
	if err := applyPositiveInt(lookup, "WORKFLOW_HTTP_MAX_IN_FLIGHT", &cfg.HTTP.MaxInFlight); err != nil {
		return Config{}, err
	}
	if cfg.HTTP.MaxInFlight == 0 {
		cfg.HTTP.MaxInFlight = defaultMaxInFlightPerConn * cfg.Database.MaxOpenConns
	}
	// TASKQ_WORKFLOW_QUEUE_PARTITIONS 可选：>=1 才覆盖 workflow queue 分区，省略=0=不覆盖。
	if err := applyPositiveInt(lookup, "TASKQ_WORKFLOW_QUEUE_PARTITIONS", &cfg.TaskQ.WorkflowQueuePartitions); err != nil {
		return Config{}, err
	}

	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// Validate 按「所有数据库均由本包依 DSN 自行拨号」校验配置。
func (c Config) Validate() error {
	return c.validate(false, false)
}

// validate 在注入数据库时放宽对应的 DSN / 连接池校验：
// 这些字段只服务于本包自行拨号的路径，连接由调用方提供时它们不再被读取。
func (c Config) validate(workflowDBInjected, taskQDBInjected bool) error {
	if c.Backend != BackendMySQLTaskQ && c.Backend != BackendMemory {
		return fmt.Errorf("WORKFLOW_BACKEND must be %q or %q", BackendMySQLTaskQ, BackendMemory)
	}
	if strings.TrimSpace(c.HTTP.Address) == "" {
		return fmt.Errorf("WORKFLOW_HTTP_ADDR is empty")
	}
	if (c.HTTP.TLSCertFile == "") != (c.HTTP.TLSKeyFile == "") {
		return fmt.Errorf("WORKFLOW_HTTP_TLS_CERT_FILE and WORKFLOW_HTTP_TLS_KEY_FILE must be configured together")
	}
	if c.HTTP.ClientCAFile != "" && c.HTTP.TLSCertFile == "" {
		return fmt.Errorf("WORKFLOW_HTTP_TLS_CLIENT_CA_FILE requires WORKFLOW_HTTP_TLS_CERT_FILE and WORKFLOW_HTTP_TLS_KEY_FILE")
	}
	if err := validateAuthTokens(c.HTTP.AuthTokens); err != nil {
		return err
	}
	if c.HTTP.HealthAddress != "" {
		if _, port, err := net.SplitHostPort(c.HTTP.HealthAddress); err != nil || port == "" {
			return fmt.Errorf("WORKFLOW_HEALTH_ADDR %q must be host:port", c.HTTP.HealthAddress)
		}
	}
	if strings.TrimSpace(c.NodeID) == "" {
		return fmt.Errorf("WORKFLOW_NODE_ID is empty")
	}
	if c.StartupTimeout <= 0 || c.ReconcileInterval <= 0 || c.ReconcileFullScanInterval <= 0 || c.HTTP.ShutdownTimeout <= 0 || c.HTTP.ReadHeaderTimeout <= 0 || c.HTTP.IdleTimeout <= 0 {
		return fmt.Errorf("workflow timeouts and reconcile interval must be positive")
	}
	if c.HTTP.MaxInFlight <= 0 {
		return fmt.Errorf("WORKFLOW_HTTP_MAX_IN_FLIGHT must be positive")
	}
	// ReadTimeout / WriteTimeout 允许为 0（= 不限制，Go 的语义），但不允许为负。
	if c.HTTP.ReadTimeout < 0 || c.HTTP.WriteTimeout < 0 {
		return fmt.Errorf("WORKFLOW_HTTP_READ_TIMEOUT and WORKFLOW_HTTP_WRITE_TIMEOUT must not be negative")
	}
	// **配错就启动失败，而不是等生产上长轮询被神秘掐断**：WriteTimeout 小于长轮询上界时，
	// worker 的每次长轮询都会在写响应前被 server 断开，表现为客户端 EOF、任务分发停摆。
	// 这是个极难从现象倒推回配置的故障，故在启动期拦下。
	if c.HTTP.WriteTimeout > 0 && c.HTTP.WriteTimeout <= runtimeapi.MaxRuntimePollTimeout {
		return fmt.Errorf("WORKFLOW_HTTP_WRITE_TIMEOUT (%s) must exceed the long poll upper bound %s, or be 0 to disable",
			c.HTTP.WriteTimeout, runtimeapi.MaxRuntimePollTimeout)
	}
	if c.PprofAddr != "" {
		if _, port, err := net.SplitHostPort(c.PprofAddr); err != nil || port == "" {
			return fmt.Errorf("WORKFLOW_PPROF_ADDR %q must be host:port", c.PprofAddr)
		}
	}
	if len(c.TaskTokenSecret) < dispatch.MinimumTaskTokenSecretLength {
		return fmt.Errorf("WORKFLOW_TASK_TOKEN_SECRET must contain at least %d bytes", dispatch.MinimumTaskTokenSecretLength)
	}
	if err := c.Telemetry.WithDefaults().Validate(); err != nil {
		return err
	}
	// retention 校验必须在 memory 的 early return **之前**：memory 后端明确不支持 GC，
	// 配了 WORKFLOW_RETENTION 却静默忽略，会让使用方以为数据在被回收。
	if err := c.validateRetention(); err != nil {
		return err
	}
	if c.Backend == BackendMemory {
		return nil
	}
	// HistoryPageTokenKey 与注入无关，两条路径都要求。
	if len(c.Database.HistoryPageTokenKey) < 32 {
		return fmt.Errorf("WORKFLOW_HISTORY_PAGE_TOKEN_KEY_BASE64 must decode to at least 32 bytes")
	}
	if !workflowDBInjected && c.Database.DSN == "" {
		return fmt.Errorf("WORKFLOW_DB_DSN is empty")
	}
	// Database 的连接池设置同时服务于 workflow 与 TaskQ 的拨号，
	// 只要还有一侧需要本包自行拨号就必须有效。
	if !workflowDBInjected || !taskQDBInjected {
		if c.Database.MaxOpenConns <= 0 || c.Database.MaxIdleConns <= 0 || c.Database.ConnMaxLifetime <= 0 || c.Database.ConnMaxIdleTime <= 0 {
			return fmt.Errorf("workflow database pool settings must be positive")
		}
		if c.Database.MaxIdleConns > c.Database.MaxOpenConns {
			return fmt.Errorf("WORKFLOW_DB_MAX_IDLE_CONNS must be less than or equal to WORKFLOW_DB_MAX_OPEN_CONNS")
		}
	}
	if !taskQDBInjected && c.TaskQ.DSN == "" {
		return fmt.Errorf("TaskQ database configuration is required")
	}
	if c.TaskQ.ServiceName == "" || c.TaskQ.WorkflowQueue == "" {
		return fmt.Errorf("TaskQ service and workflow queue configuration is required")
	}
	if c.TaskQ.MatchingEnabled() {
		if err := validateMatchingAddr(c.TaskQ.MatchingAddr()); err != nil {
			return err
		}
		if err := validateAdvertiseAddr(c.TaskQ.Matching.AdvertiseAddr); err != nil {
			return err
		}
	}
	// TASKQ_ACTIVITY_QUEUES_JSON 可缺省，未配置的 route 走 activity: 前缀约定。
	// 它是可选的分区数/物理名 override，这里只在有配置时校验其合法性。
	for route, queue := range c.TaskQ.ActivityQueues {
		if route == "" || queue.Physical == "" {
			return fmt.Errorf("TaskQ activity queue route and physical queue names must be non-empty")
		}
		if queue.Physical == c.TaskQ.WorkflowQueue {
			return fmt.Errorf("TaskQ workflow and activity queues must use distinct physical queue names")
		}
		if queue.Partitions < 0 || queue.Partitions > MaxQueuePartitions {
			return fmt.Errorf("TaskQ activity queue %q partitions must be between 0 and %d", route, MaxQueuePartitions)
		}
	}
	if c.TaskQ.WorkflowQueuePartitions < 0 || c.TaskQ.WorkflowQueuePartitions > MaxQueuePartitions {
		return fmt.Errorf("TASKQ_WORKFLOW_QUEUE_PARTITIONS must be between 0 and %d", MaxQueuePartitions)
	}
	if c.TaskQ.PollTimeout <= 0 || c.TaskQ.MembershipPollInterval <= 0 || c.TaskQ.MembershipReplicaPoints <= 0 {
		return fmt.Errorf("TaskQ poll, membership interval, and replica points must be positive")
	}
	if (c.TaskQ.TLS.CertFile == "") != (c.TaskQ.TLS.KeyFile == "") {
		return fmt.Errorf("TASKQ_TLS_CERT_FILE and TASKQ_TLS_KEY_FILE must be configured together")
	}
	if !c.TaskQ.TLS.Enabled && (c.TaskQ.TLS.CAFile != "" || c.TaskQ.TLS.ServerName != "" || c.TaskQ.TLS.CertFile != "") {
		return fmt.Errorf("TASKQ_TLS_ENABLED must be true when TaskQ TLS files or server name are configured")
	}
	return nil
}

// validateRetention 校验 retention GC 配置。
//
// memory 后端在此**启动即报错**而不是静默降级：memory 的 ForkBranch 是 copy-on-fork，
// 没有共享 tree，GC 的整树模型根本不适用；且进程退出即释放，本就不需要回收。
// 配了却不生效比明确报错危险得多 —— 使用方会以为磁盘在被回收。
func (c Config) validateRetention() error {
	r := c.Retention
	if r.Retention < 0 {
		return fmt.Errorf("WORKFLOW_RETENTION must not be negative")
	}
	if r.Retention == 0 {
		// GC 关闭：其余参数不再约束（它们没有消费者）。
		return nil
	}
	if c.Backend == BackendMemory {
		return fmt.Errorf("WORKFLOW_RETENTION must be 0 for the %q backend: %w",
			BackendMemory, retention.ErrUnsupported)
	}
	// 下界防误设：秒级 retention 会让刚关闭的 workflow 立刻被删，历史再也查不到。
	if r.Retention < executionrepo.MinRetention {
		return fmt.Errorf("WORKFLOW_RETENTION must be at least %s when enabled, got %s",
			executionrepo.MinRetention, r.Retention)
	}
	if r.Jitter < 0 {
		return fmt.Errorf("WORKFLOW_RETENTION_JITTER must not be negative")
	}
	if r.ScanInterval <= 0 {
		return fmt.Errorf("WORKFLOW_GC_SCAN_INTERVAL must be a positive duration")
	}
	if r.Batch <= 0 {
		return fmt.Errorf("WORKFLOW_GC_BATCH must be a positive integer")
	}
	if r.ScavengeInterval <= 0 {
		return fmt.Errorf("WORKFLOW_GC_SCAVENGE_INTERVAL must be a positive duration")
	}
	if r.ScavengeBatch <= 0 {
		return fmt.Errorf("WORKFLOW_GC_SCAVENGE_BATCH must be a positive integer")
	}
	// min_age 的下界不是性能考量而是**安全考量**：兜底扫描判定孤儿靠「关联行查不到」，
	// 而正常写入过程中存在短暂的不一致窗口（run 行与它的事件由不同事务写入）。把
	// min_age 设成分钟级会让在途写入的数据被当成孤儿删掉 —— 那是真正的数据丢失。
	// 下界取 1h，与 MinRetention 同量级。
	if r.ScavengeMinAge < executionrepo.MinRetention {
		return fmt.Errorf("WORKFLOW_GC_SCAVENGE_MIN_AGE must be at least %s, got %s",
			executionrepo.MinRetention, r.ScavengeMinAge)
	}
	return nil
}

func decodeTokenKey(lookup envLookup) ([]byte, error) {
	encoded, err := requiredEnv(lookup, "WORKFLOW_HISTORY_PAGE_TOKEN_KEY_BASE64")
	if err != nil {
		return nil, err
	}
	key, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, fmt.Errorf("decode WORKFLOW_HISTORY_PAGE_TOKEN_KEY_BASE64: %w", err)
	}
	return key, nil
}

// activityQueueConfig 解析 TASKQ_ACTIVITY_QUEUES_JSON。它是**可选**的分区数/物理名 override：
// 缺省即返回空 map（未配置的 route 走 activity: 前缀约定）。
//
// 每条 value 是对象 `{"physical":"payments-q","partitions":8}`：物理名 payments-q，
// partitions 可省略，给出时覆盖 read=write=8。
func activityQueueConfig(lookup envLookup) (map[string]ActivityQueueConfig, error) {
	raw := envOrDefault(lookup, "TASKQ_ACTIVITY_QUEUES_JSON", "")
	if raw == "" {
		return nil, nil
	}
	var parsed map[string]struct {
		Physical   string `json:"physical"`
		Partitions *int   `json:"partitions"`
	}
	if err := json.Unmarshal([]byte(raw), &parsed); err != nil {
		return nil, fmt.Errorf("decode TASKQ_ACTIVITY_QUEUES_JSON: %w", err)
	}
	normalized := make(map[string]ActivityQueueConfig, len(parsed))
	for route, value := range parsed {
		route = strings.TrimSpace(route)
		if route == "" {
			return nil, fmt.Errorf("TASKQ_ACTIVITY_QUEUES_JSON contains an empty route")
		}
		if _, exists := normalized[route]; exists {
			return nil, fmt.Errorf("TASKQ_ACTIVITY_QUEUES_JSON contains duplicate route %q after trimming", route)
		}
		queue := ActivityQueueConfig{Physical: strings.TrimSpace(value.Physical)}
		if queue.Physical == "" {
			return nil, fmt.Errorf("TASKQ_ACTIVITY_QUEUES_JSON route %q has an empty physical queue", route)
		}
		if value.Partitions != nil {
			if *value.Partitions < 1 {
				return nil, fmt.Errorf("TASKQ_ACTIVITY_QUEUES_JSON route %q partitions must be at least 1 when present", route)
			}
			queue.Partitions = *value.Partitions
		}
		normalized[route] = queue
	}
	return normalized, nil
}

func requiredEnv(lookup envLookup, name string) (string, error) {
	value, ok := lookup(name)
	value = strings.TrimSpace(value)
	if !ok || value == "" {
		return "", fmt.Errorf("%s is required", name)
	}
	return value, nil
}

func envOrDefault(lookup envLookup, name, fallback string) string {
	value, ok := lookup(name)
	value = strings.TrimSpace(value)
	if !ok || value == "" {
		return fallback
	}
	return value
}

func applyBool(lookup envLookup, name string, target *bool) error {
	value, ok, err := lookupBool(lookup, name)
	if err != nil || !ok {
		return err
	}
	*target = value
	return nil
}

// applyBoolPtr 与 applyBool 相同，但未设置时保留 nil，供「未设置即取默认值」的字段使用。
func applyBoolPtr(lookup envLookup, name string, target **bool) error {
	value, ok, err := lookupBool(lookup, name)
	if err != nil || !ok {
		return err
	}
	*target = &value
	return nil
}

func lookupBool(lookup envLookup, name string) (bool, bool, error) {
	raw, ok := lookup(name)
	if !ok || strings.TrimSpace(raw) == "" {
		return false, false, nil
	}
	value, err := strconv.ParseBool(strings.TrimSpace(raw))
	if err != nil {
		return false, false, fmt.Errorf("parse %s: %w", name, err)
	}
	return value, true, nil
}

func applyDuration(lookup envLookup, name string, target *time.Duration) error {
	raw, ok := lookup(name)
	if !ok || strings.TrimSpace(raw) == "" {
		return nil
	}
	value, err := time.ParseDuration(strings.TrimSpace(raw))
	if err != nil {
		return fmt.Errorf("parse %s: %w", name, err)
	}
	*target = value
	return nil
}

func applyPositiveDuration(lookup envLookup, name string, target *time.Duration) error {
	raw, ok := lookup(name)
	if !ok || strings.TrimSpace(raw) == "" {
		return nil
	}
	value, err := time.ParseDuration(strings.TrimSpace(raw))
	if err != nil {
		return fmt.Errorf("parse %s: %w", name, err)
	}
	if value <= 0 {
		return fmt.Errorf("%s must be a positive duration", name)
	}
	*target = value
	return nil
}

func applyPositiveInt(lookup envLookup, name string, target *int) error {
	raw, ok := lookup(name)
	if !ok || strings.TrimSpace(raw) == "" {
		return nil
	}
	value, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil || value <= 0 {
		return fmt.Errorf("%s must be a positive integer", name)
	}
	*target = value
	return nil
}

func applyTraceSampleRatio(lookup envLookup, target *float64) error {
	raw, ok := lookup("TENUTO_OTEL_TRACE_SAMPLE_RATIO")
	if !ok || strings.TrimSpace(raw) == "" {
		return nil
	}
	value, err := strconv.ParseFloat(strings.TrimSpace(raw), 64)
	if err != nil || math.IsNaN(value) || math.IsInf(value, 0) || value <= 0 || value > 1 {
		return fmt.Errorf("TENUTO_OTEL_TRACE_SAMPLE_RATIO must be greater than 0 and at most 1")
	}
	*target = value
	return nil
}

func applyLogLevel(lookup envLookup, target *slog.Level) error {
	raw, ok := lookup("TENUTO_OTEL_LOG_LEVEL")
	if !ok || strings.TrimSpace(raw) == "" {
		return nil
	}
	var value slog.Level
	if err := value.UnmarshalText([]byte(strings.ToUpper(strings.TrimSpace(raw)))); err != nil {
		return fmt.Errorf("parse TENUTO_OTEL_LOG_LEVEL: %w", err)
	}
	*target = value
	return nil
}

func splitNonEmpty(value, sep string) []string {
	var out []string
	for _, item := range strings.Split(value, sep) {
		item = strings.TrimSpace(item)
		if item != "" {
			out = append(out, item)
		}
	}
	return out
}
