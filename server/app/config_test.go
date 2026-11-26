package app

import (
	"encoding/base64"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	workerpkg "github.com/donutnomad/tenuto/internal/worker"
	"github.com/donutnomad/tenuto/server/biz/usecase/retention"
)

func TestLoadConfigProductionDefaults(t *testing.T) {
	cfg, err := loadConfig(mapLookup(validEnvironment()), "workflow-node-a")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.HTTP.Address != ":7400" {
		t.Fatalf("HTTP address = %q", cfg.HTTP.Address)
	}
	if cfg.Backend != BackendMySQLTaskQ {
		t.Fatalf("backend = %q", cfg.Backend)
	}
	if len(cfg.TaskTokenSecret) < 32 {
		t.Fatalf("task token secret length = %d", len(cfg.TaskTokenSecret))
	}
	if cfg.NodeID != "workflow-node-a" {
		t.Fatalf("node ID = %q", cfg.NodeID)
	}
	if cfg.TaskQ.DSN != cfg.Database.DSN {
		t.Fatalf("TaskQ DSN should inherit workflow DSN")
	}
	if cfg.TaskQ.WorkflowQueue != "workflow-tasks" {
		t.Fatalf("workflow queue = %q", cfg.TaskQ.WorkflowQueue)
	}
	if got := cfg.TaskQ.ActivityQueues["default/default"]; got.Physical != "activity-default" {
		t.Fatalf("activity queue = %+v", got)
	}
	if !cfg.TaskQ.MatchingEnabled() || cfg.TaskQ.MatchingAddr() != defaultMatchingAddr {
		t.Fatalf("embedded matching = %+v, want enabled on %s by default", cfg.TaskQ.Matching, defaultMatchingAddr)
	}
	if zero := (TaskQConfig{}); !zero.MatchingEnabled() || zero.MatchingAddr() != defaultMatchingAddr {
		t.Fatalf("zero TaskQConfig = %+v, want embedded matching enabled on %s", zero, defaultMatchingAddr)
	}
}

// 外置 matching 模式：只关开关即可，地址字段不再参与校验。
func TestLoadConfigExternalMatchingDisablesEmbeddedMatching(t *testing.T) {
	env := validEnvironment()
	env["TASKQ_MATCHING_ENABLED"] = "false"
	env["TASKQ_MATCHING_ADDR"] = "not-an-address"
	cfg, err := loadConfig(mapLookup(env), "workflow-node-a")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.TaskQ.MatchingEnabled() {
		t.Fatal("TASKQ_MATCHING_ENABLED=false did not disable embedded matching")
	}
}

func TestLoadConfigEmbeddedWorkflowWorkerDefaults(t *testing.T) {
	cfg, err := loadConfig(mapLookup(validEnvironment()), "workflow-node-a")
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.WorkflowWorker.IsEnabled() {
		t.Fatal("embedded workflow worker must be enabled by default")
	}
	worker := cfg.WorkflowWorker.withDefaults()
	if worker.Concurrency != 8 || worker.TaskTimeout != workerpkg.DefaultWorkflowTaskTimeout ||
		worker.CacheMaxEntries != 1000 || worker.CacheMaxBytes != 256<<20 || worker.CacheIdleTTL != 5*time.Minute {
		t.Fatalf("workflow worker defaults = %+v", worker)
	}
}

func TestLoadConfigParsesEmbeddedWorkflowWorkerSettings(t *testing.T) {
	env := validEnvironment()
	env["WORKFLOW_WORKER_ENABLED"] = "false"
	env["WORKFLOW_WORKER_CONCURRENCY"] = "3"
	env["WORKFLOW_WORKER_TASK_TIMEOUT"] = "30s"
	env["WORKFLOW_WORKER_CACHE_MAX_ENTRIES"] = "50"
	env["WORKFLOW_WORKER_CACHE_MAX_BYTES"] = "1048576"
	env["WORKFLOW_WORKER_CACHE_IDLE_TTL"] = "1m"
	cfg, err := loadConfig(mapLookup(env), "workflow-node-a")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.WorkflowWorker.IsEnabled() {
		t.Fatal("WORKFLOW_WORKER_ENABLED=false must disable the embedded worker")
	}
	worker := cfg.WorkflowWorker.withDefaults()
	if worker.Concurrency != 3 || worker.TaskTimeout != 30*time.Second ||
		worker.CacheMaxEntries != 50 || worker.CacheMaxBytes != 1<<20 || worker.CacheIdleTTL != time.Minute {
		t.Fatalf("workflow worker settings = %+v", worker)
	}
}

func TestLoadConfigRejectsInvalidEmbeddedWorkflowWorkerSettings(t *testing.T) {
	for name, value := range map[string]string{
		"WORKFLOW_WORKER_ENABLED":           "maybe",
		"WORKFLOW_WORKER_CONCURRENCY":       "0",
		"WORKFLOW_WORKER_TASK_TIMEOUT":      "0s",
		"WORKFLOW_WORKER_CACHE_MAX_ENTRIES": "-1",
		"WORKFLOW_WORKER_CACHE_MAX_BYTES":   "many",
		"WORKFLOW_WORKER_CACHE_IDLE_TTL":    "soon",
	} {
		t.Run(name, func(t *testing.T) {
			env := validEnvironment()
			env[name] = value
			if _, err := loadConfig(mapLookup(env), "workflow-node-a"); err == nil || !strings.Contains(err.Error(), name) {
				t.Fatalf("err = %v, want mention of %s", err, name)
			}
		})
	}
}

func TestLoadConfigMemoryBackendNeedsNoInfrastructureConfiguration(t *testing.T) {
	cfg, err := loadConfig(mapLookup(map[string]string{
		"WORKFLOW_BACKEND":   "memory",
		"WORKFLOW_HTTP_ADDR": "127.0.0.1:7400",
	}), "demo-node")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Backend != BackendMemory {
		t.Fatalf("backend = %q", cfg.Backend)
	}
	if string(cfg.TaskTokenSecret) != defaultDevelopmentTokenSecret {
		t.Fatalf("memory task token secret = %q", cfg.TaskTokenSecret)
	}
	if cfg.Database.DSN != "" || cfg.TaskQ.DSN != "" || len(cfg.TaskQ.ActivityQueues) != 0 {
		t.Fatal("memory backend loaded infrastructure configuration")
	}
}

func TestLoadConfigOpenTelemetryDefaults(t *testing.T) {
	cfg, err := loadConfig(mapLookup(map[string]string{
		"WORKFLOW_BACKEND":   "memory",
		"WORKFLOW_HTTP_ADDR": "127.0.0.1:7400",
	}), "demo-node")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Telemetry.Enabled {
		t.Fatal("OpenTelemetry must be opt-in")
	}
	if cfg.Telemetry.TraceSampleRatio != 1 {
		t.Fatalf("trace sample ratio = %v", cfg.Telemetry.TraceSampleRatio)
	}
	if cfg.Telemetry.MetricExportInterval != time.Minute {
		t.Fatalf("metric export interval = %v", cfg.Telemetry.MetricExportInterval)
	}
	if cfg.Telemetry.LogLevel != slog.LevelInfo {
		t.Fatalf("log level = %v", cfg.Telemetry.LogLevel)
	}
	if cfg.Telemetry.ShutdownTimeout != 5*time.Second {
		t.Fatalf("shutdown timeout = %v", cfg.Telemetry.ShutdownTimeout)
	}
	if cfg.Telemetry.ServiceInstanceID != "demo-node" {
		t.Fatalf("service instance ID = %q", cfg.Telemetry.ServiceInstanceID)
	}
}

func TestLoadConfigParsesOpenTelemetrySettings(t *testing.T) {
	env := map[string]string{
		"WORKFLOW_BACKEND":                   "memory",
		"WORKFLOW_HTTP_ADDR":                 "127.0.0.1:7400",
		"TENUTO_OTEL_ENABLED":                "true",
		"TENUTO_OTEL_TRACE_SAMPLE_RATIO":     "0.25",
		"TENUTO_OTEL_METRIC_EXPORT_INTERVAL": "15s",
		"TENUTO_OTEL_LOG_LEVEL":              "DEBUG",
		"TENUTO_OTEL_SHUTDOWN_TIMEOUT":       "9s",
	}
	cfg, err := loadConfig(mapLookup(env), "demo-node")
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.Telemetry.Enabled || cfg.Telemetry.TraceSampleRatio != 0.25 {
		t.Fatalf("enabled/ratio = %v/%v", cfg.Telemetry.Enabled, cfg.Telemetry.TraceSampleRatio)
	}
	if cfg.Telemetry.MetricExportInterval != 15*time.Second || cfg.Telemetry.ShutdownTimeout != 9*time.Second {
		t.Fatalf("metric/shutdown = %v/%v", cfg.Telemetry.MetricExportInterval, cfg.Telemetry.ShutdownTimeout)
	}
	if cfg.Telemetry.LogLevel != slog.LevelDebug {
		t.Fatalf("log level = %v", cfg.Telemetry.LogLevel)
	}
}

func TestLoadConfigRejectsInvalidOpenTelemetrySettings(t *testing.T) {
	tests := []struct {
		name  string
		value string
	}{
		{name: "TENUTO_OTEL_ENABLED", value: "sometimes"},
		{name: "TENUTO_OTEL_TRACE_SAMPLE_RATIO", value: "1.5"},
		{name: "TENUTO_OTEL_TRACE_SAMPLE_RATIO", value: "NaN"},
		{name: "TENUTO_OTEL_METRIC_EXPORT_INTERVAL", value: "soon"},
		{name: "TENUTO_OTEL_LOG_LEVEL", value: "verbose"},
		{name: "TENUTO_OTEL_SHUTDOWN_TIMEOUT", value: "0s"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			env := map[string]string{
				"WORKFLOW_BACKEND":   "memory",
				"WORKFLOW_HTTP_ADDR": "127.0.0.1:7400",
				test.name:            test.value,
			}
			if _, err := loadConfig(mapLookup(env), "demo-node"); err == nil || !strings.Contains(err.Error(), test.name) {
				t.Fatalf("error = %v", err)
			}
		})
	}
}

func TestLoadConfigRejectsInvalidProductionContracts(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(map[string]string)
		message string
	}{
		{
			name: "unknown backend",
			mutate: func(env map[string]string) {
				env["WORKFLOW_BACKEND"] = "filesystem"
			},
			message: "WORKFLOW_BACKEND must be",
		},
		{
			name: "missing task token secret",
			mutate: func(env map[string]string) {
				delete(env, "WORKFLOW_TASK_TOKEN_SECRET")
			},
			message: "WORKFLOW_TASK_TOKEN_SECRET is required",
		},
		{
			name: "short task token secret",
			mutate: func(env map[string]string) {
				env["WORKFLOW_TASK_TOKEN_SECRET"] = "short"
			},
			message: "WORKFLOW_TASK_TOKEN_SECRET must contain at least 32 bytes",
		},
		{
			name: "missing workflow database",
			mutate: func(env map[string]string) {
				delete(env, "WORKFLOW_DB_DSN")
			},
			message: "WORKFLOW_DB_DSN is required",
		},
		{
			name: "short history token key",
			mutate: func(env map[string]string) {
				env["WORKFLOW_HISTORY_PAGE_TOKEN_KEY_BASE64"] = base64.StdEncoding.EncodeToString([]byte("short"))
			},
			message: "at least 32 bytes",
		},
		{
			name: "shared workflow and activity physical queue",
			mutate: func(env map[string]string) {
				env["TASKQ_ACTIVITY_QUEUES_JSON"] = `{"default/default":{"physical":"workflow-tasks"}}`
			},
			message: "distinct physical queue names",
		},
		{
			name: "partial HTTP TLS configuration",
			mutate: func(env map[string]string) {
				env["WORKFLOW_HTTP_TLS_CERT_FILE"] = "/cert.pem"
			},
			message: "must be configured together",
		},
		{
			name: "TaskQ TLS material with TLS disabled",
			mutate: func(env map[string]string) {
				env["TASKQ_TLS_CA_FILE"] = "/ca.pem"
			},
			message: "TASKQ_TLS_ENABLED must be true",
		},
		{
			name: "invalid activity queue JSON",
			mutate: func(env map[string]string) {
				env["TASKQ_ACTIVITY_QUEUES_JSON"] = `{"default/default":{"physical":"activity-default"}} trailing`
			},
			message: "decode TASKQ_ACTIVITY_QUEUES_JSON",
		},
		{
			name: "invalid matching address",
			mutate: func(env map[string]string) {
				env["TASKQ_MATCHING_ADDR"] = "7235"
			},
			message: "TASKQ_MATCHING_ADDR",
		},
		{
			name: "unresolvable matching host",
			mutate: func(env map[string]string) {
				env["TASKQ_MATCHING_ADDR"] = "tenuto-matching.invalid:7235"
			},
			message: "resolve TASKQ_MATCHING_ADDR host",
		},
		{
			name: "invalid matching enable switch",
			mutate: func(env map[string]string) {
				env["TASKQ_MATCHING_ENABLED"] = "yes"
			},
			message: "parse TASKQ_MATCHING_ENABLED",
		},
		{
			name: "invalid artifact publish switch",
			mutate: func(env map[string]string) {
				env["WORKFLOW_ARTIFACTS_PUBLISH"] = "ture"
			},
			message: "parse WORKFLOW_ARTIFACTS_PUBLISH",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			env := validEnvironment()
			test.mutate(env)
			_, err := loadConfig(mapLookup(env), "workflow-node-a")
			if err == nil || !strings.Contains(err.Error(), test.message) {
				t.Fatalf("error = %v, want containing %q", err, test.message)
			}
		})
	}
}

func TestLoadConfigParsesExplicitSettings(t *testing.T) {
	env := validEnvironment()
	env["WORKFLOW_HTTP_ADDR"] = "127.0.0.1:8080"
	env["WORKFLOW_NODE_ID"] = "workflow-01"
	env["WORKFLOW_RECONCILE_INTERVAL"] = "45s"
	env["WORKFLOW_DB_MAX_OPEN_CONNS"] = "80"
	env["WORKFLOW_DB_MAX_IDLE_CONNS"] = "20"
	env["TASKQ_ACTIVITY_QUEUES_JSON"] = `{"tenant-a/payments":{"physical":"activity-tenant-a-payments"},"tenant-a/default":{"physical":"activity-tenant-a-default"}}`

	cfg, err := loadConfig(mapLookup(env), "ignored-hostname")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.HTTP.Address != "127.0.0.1:8080" || cfg.NodeID != "workflow-01" {
		t.Fatalf("HTTP/node = %q/%q", cfg.HTTP.Address, cfg.NodeID)
	}
	if cfg.Database.MaxOpenConns != 80 || cfg.Database.MaxIdleConns != 20 {
		t.Fatalf("database pool = %d/%d", cfg.Database.MaxOpenConns, cfg.Database.MaxIdleConns)
	}
	if len(cfg.TaskQ.ActivityQueues) != 2 {
		t.Fatalf("activity routes = %v", cfg.TaskQ.ActivityQueues)
	}
}

func TestLoadConfigAllowsAbsentActivityQueues(t *testing.T) {
	env := validEnvironment()
	delete(env, "TASKQ_ACTIVITY_QUEUES_JSON")

	cfg, err := loadConfig(mapLookup(env), "workflow-node-a")
	if err != nil {
		t.Fatalf("activity queues should be optional: %v", err)
	}
	if len(cfg.TaskQ.ActivityQueues) != 0 {
		t.Fatalf("activity queues = %v, want empty when unset", cfg.TaskQ.ActivityQueues)
	}
}

func TestLoadConfigParsesObjectActivityQueues(t *testing.T) {
	env := validEnvironment()
	env["TASKQ_ACTIVITY_QUEUES_JSON"] = `{"payments":{"physical":"payments-q","partitions":8}}`

	cfg, err := loadConfig(mapLookup(env), "workflow-node-a")
	if err != nil {
		t.Fatal(err)
	}
	got := cfg.TaskQ.ActivityQueues["payments"]
	if got.Physical != "payments-q" || got.Partitions != 8 {
		t.Fatalf("object form = %+v, want {payments-q 8}", got)
	}
}

func TestLoadConfigRejectsInvalidActivityQueueValues(t *testing.T) {
	tests := map[string]string{
		"partitions must be at least 1 when present": `{"x":{"physical":"x-q","partitions":0}}`,
		"has an empty physical queue":                `{"x":{"partitions":4}}`,
		"decode TASKQ_ACTIVITY_QUEUES_JSON":          `{"x":"x-q"}`,
	}
	for want, raw := range tests {
		t.Run(want, func(t *testing.T) {
			env := validEnvironment()
			env["TASKQ_ACTIVITY_QUEUES_JSON"] = raw
			if _, err := loadConfig(mapLookup(env), "workflow-node-a"); err == nil || !strings.Contains(err.Error(), want) {
				t.Fatalf("err = %v, want containing %q", err, want)
			}
		})
	}
}

func TestLoadConfigParsesWorkflowQueuePartitions(t *testing.T) {
	env := validEnvironment()
	env["TASKQ_WORKFLOW_QUEUE_PARTITIONS"] = "6"

	cfg, err := loadConfig(mapLookup(env), "workflow-node-a")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.TaskQ.WorkflowQueuePartitions != 6 {
		t.Fatalf("workflow partitions = %d, want 6", cfg.TaskQ.WorkflowQueuePartitions)
	}
}

func TestLoadConfigRejectsPartitionsAboveMaximum(t *testing.T) {
	env := validEnvironment()
	env["TASKQ_WORKFLOW_QUEUE_PARTITIONS"] = "1025"
	if _, err := loadConfig(mapLookup(env), "workflow-node-a"); err == nil || !strings.Contains(err.Error(), "between 0 and 1024") {
		t.Fatalf("workflow partitions err = %v", err)
	}
	env = validEnvironment()
	env["TASKQ_ACTIVITY_QUEUES_JSON"] = `{"default/payments":{"physical":"payments","partitions":1025}}`
	if _, err := loadConfig(mapLookup(env), "workflow-node-a"); err == nil || !strings.Contains(err.Error(), "between 0 and 1024") {
		t.Fatalf("activity partitions err = %v", err)
	}
}

func TestLoadConfigWorkflowQueuePartitionsDefaultsToZero(t *testing.T) {
	cfg, err := loadConfig(mapLookup(validEnvironment()), "workflow-node-a")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.TaskQ.WorkflowQueuePartitions != 0 {
		t.Fatalf("workflow partitions = %d, want 0 when unset", cfg.TaskQ.WorkflowQueuePartitions)
	}
}

func TestValidateRejectsActivityPhysicalEqualToWorkflowQueue(t *testing.T) {
	cfg := Config{
		Backend:                   BackendMySQLTaskQ,
		TaskTokenSecret:           []byte("0123456789abcdef0123456789abcdef"),
		HTTP:                      HTTPConfig{Address: ":7400", ShutdownTimeout: time.Second, ReadHeaderTimeout: time.Second, IdleTimeout: time.Second},
		Database:                  DatabaseConfig{DSN: "d", HistoryPageTokenKey: []byte("0123456789abcdef0123456789abcdef"), MaxOpenConns: 10, MaxIdleConns: 5, ConnMaxLifetime: time.Minute, ConnMaxIdleTime: time.Minute},
		NodeID:                    "n",
		StartupTimeout:            time.Second,
		ReconcileInterval:         time.Second,
		ReconcileFullScanInterval: time.Minute,
		TaskQ: TaskQConfig{
			DSN:                     "d",
			ServiceName:             "matching",
			WorkflowQueue:           "workflow-tasks",
			ActivityQueues:          map[string]ActivityQueueConfig{"x": {Physical: "workflow-tasks"}},
			PollTimeout:             time.Second,
			MembershipPollInterval:  time.Second,
			MembershipReplicaPoints: 1,
		},
	}
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "distinct physical queue names") {
		t.Fatalf("err = %v, want distinct physical queue names", err)
	}
}

// ── 验收 7：retention 默认关闭，行为与引入 GC 之前完全一致 ──

func TestLoadConfigRetentionDefaultsToDisabled(t *testing.T) {
	cfg, err := loadConfig(mapLookup(validEnvironment()), "workflow-node-a")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Retention.Enabled() {
		t.Fatal("retention GC must be opt-in; deleting data by default is unacceptable")
	}
	if cfg.Retention.Retention != 0 {
		t.Fatalf("default retention = %s, want 0", cfg.Retention.Retention)
	}
	// 关闭时其余参数仍应有可用默认值，开启时无需逐个再配。
	if cfg.Retention.Jitter != defaultRetentionJitter {
		t.Errorf("default jitter = %s, want %s", cfg.Retention.Jitter, defaultRetentionJitter)
	}
	if cfg.Retention.ScanInterval != defaultGCScanInterval {
		t.Errorf("default scan interval = %s, want %s", cfg.Retention.ScanInterval, defaultGCScanInterval)
	}
	if cfg.Retention.Batch != defaultGCBatch {
		t.Errorf("default batch = %d, want %d", cfg.Retention.Batch, defaultGCBatch)
	}
}

func TestLoadConfigParsesRetentionSettings(t *testing.T) {
	env := validEnvironment()
	env["WORKFLOW_RETENTION"] = "168h"
	env["WORKFLOW_RETENTION_JITTER"] = "5m"
	env["WORKFLOW_GC_SCAN_INTERVAL"] = "15s"
	env["WORKFLOW_GC_BATCH"] = "42"

	cfg, err := loadConfig(mapLookup(env), "workflow-node-a")
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.Retention.Enabled() {
		t.Fatal("retention should be enabled")
	}
	if cfg.Retention.Retention != 168*time.Hour {
		t.Errorf("retention = %s, want 168h", cfg.Retention.Retention)
	}
	if cfg.Retention.Jitter != 5*time.Minute {
		t.Errorf("jitter = %s, want 5m", cfg.Retention.Jitter)
	}
	if cfg.Retention.ScanInterval != 15*time.Second {
		t.Errorf("scan interval = %s, want 15s", cfg.Retention.ScanInterval)
	}
	if cfg.Retention.Batch != 42 {
		t.Errorf("batch = %d, want 42", cfg.Retention.Batch)
	}
	// 未显式配置时兜底扫描取默认值：低频 + 7 天下界 + 单轮上限。
	if cfg.Retention.ScavengeInterval != retention.DefaultScavengeInterval {
		t.Errorf("scavenge interval = %s, want %s",
			cfg.Retention.ScavengeInterval, retention.DefaultScavengeInterval)
	}
	if cfg.Retention.ScavengeMinAge != retention.DefaultScavengeMinAge {
		t.Errorf("scavenge min age = %s, want %s",
			cfg.Retention.ScavengeMinAge, retention.DefaultScavengeMinAge)
	}
	if cfg.Retention.ScavengeBatch != retention.DefaultScavengeBatch {
		t.Errorf("scavenge batch = %d, want %d",
			cfg.Retention.ScavengeBatch, retention.DefaultScavengeBatch)
	}
}

// 兜底扫描的三个参数可被环境变量覆盖。
func TestLoadConfigParsesScavengeSettings(t *testing.T) {
	env := validEnvironment()
	env["WORKFLOW_RETENTION"] = "168h"
	env["WORKFLOW_GC_SCAVENGE_INTERVAL"] = "6h"
	env["WORKFLOW_GC_SCAVENGE_MIN_AGE"] = "72h"
	env["WORKFLOW_GC_SCAVENGE_BATCH"] = "17"

	cfg, err := loadConfig(mapLookup(env), "workflow-node-a")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Retention.ScavengeInterval != 6*time.Hour {
		t.Errorf("scavenge interval = %s, want 6h", cfg.Retention.ScavengeInterval)
	}
	if cfg.Retention.ScavengeMinAge != 72*time.Hour {
		t.Errorf("scavenge min age = %s, want 72h", cfg.Retention.ScavengeMinAge)
	}
	if cfg.Retention.ScavengeBatch != 17 {
		t.Errorf("scavenge batch = %d, want 17", cfg.Retention.ScavengeBatch)
	}
}

// min_age 的下界是**安全护栏**而非调优参数：兜底扫描判定孤儿靠「关联行查不到」，
// 而正常写入存在短暂的不一致窗口。分钟级的 min_age 会把在途写入的数据当成孤儿删掉。
func TestLoadConfigRejectsTinyScavengeMinAge(t *testing.T) {
	env := validEnvironment()
	env["WORKFLOW_RETENTION"] = "168h"
	env["WORKFLOW_GC_SCAVENGE_MIN_AGE"] = "5m"

	_, err := loadConfig(mapLookup(env), "workflow-node-a")
	if err == nil {
		t.Fatal("a sub-hour scavenge min age must be rejected: it would delete in-flight writes")
	}
	if !strings.Contains(err.Error(), "WORKFLOW_GC_SCAVENGE_MIN_AGE") {
		t.Fatalf("error %q should name the offending setting", err)
	}
}

// ── 验收 12：memory 后端 retention>0 时启动报错 ──
//
// 明确报错而非静默降级：memory 的 ForkBranch 是 copy-on-fork，无共享 tree，
// GC 的整树模型不适用。配了却不生效会让使用方以为磁盘在被回收。
func TestLoadConfigMemoryBackendRejectsRetention(t *testing.T) {
	_, err := loadConfig(mapLookup(map[string]string{
		"WORKFLOW_BACKEND":   "memory",
		"WORKFLOW_HTTP_ADDR": "127.0.0.1:7400",
		"WORKFLOW_RETENTION": "24h",
	}), "demo-node")
	if err == nil {
		t.Fatal("memory backend with retention > 0 must fail at startup, not silently ignore it")
	}
	if !errors.Is(err, retention.ErrUnsupported) {
		t.Fatalf("error = %v, want it to wrap ErrRetentionGCUnsupported", err)
	}
	if !strings.Contains(err.Error(), "WORKFLOW_RETENTION") {
		t.Fatalf("error %q should name the offending setting", err)
	}
}

func TestLoadConfigMemoryBackendAllowsZeroRetention(t *testing.T) {
	cfg, err := loadConfig(mapLookup(map[string]string{
		"WORKFLOW_BACKEND":   "memory",
		"WORKFLOW_HTTP_ADDR": "127.0.0.1:7400",
		"WORKFLOW_RETENTION": "0",
	}), "demo-node")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Retention.Enabled() {
		t.Fatal("retention should stay disabled")
	}
}

// ── retention 下界校验：防误设秒删 ──

func TestLoadConfigRejectsInvalidRetentionSettings(t *testing.T) {
	cases := []struct {
		name string
		env  map[string]string
		want string
	}{
		{
			name: "retention below the one hour minimum",
			env:  map[string]string{"WORKFLOW_RETENTION": "30s"},
			want: "WORKFLOW_RETENTION",
		},
		{
			name: "negative retention",
			env:  map[string]string{"WORKFLOW_RETENTION": "-1h"},
			want: "WORKFLOW_RETENTION",
		},
		{
			name: "negative jitter",
			env:  map[string]string{"WORKFLOW_RETENTION": "24h", "WORKFLOW_RETENTION_JITTER": "-1m"},
			want: "WORKFLOW_RETENTION_JITTER",
		},
		{
			name: "non positive scan interval",
			env:  map[string]string{"WORKFLOW_RETENTION": "24h", "WORKFLOW_GC_SCAN_INTERVAL": "0s"},
			want: "WORKFLOW_GC_SCAN_INTERVAL",
		},
		{
			name: "non positive batch",
			env:  map[string]string{"WORKFLOW_RETENTION": "24h", "WORKFLOW_GC_BATCH": "0"},
			want: "WORKFLOW_GC_BATCH",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			env := validEnvironment()
			for key, value := range tc.env {
				env[key] = value
			}
			_, err := loadConfig(mapLookup(env), "workflow-node-a")
			if err == nil {
				t.Fatal("expected the invalid retention setting to be rejected")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %q should mention %q", err, tc.want)
			}
		})
	}
}

func validEnvironment() map[string]string {
	return map[string]string{
		"WORKFLOW_DB_DSN":                        "workflow:secret@tcp(mysql:3306)/workflow",
		"WORKFLOW_HISTORY_PAGE_TOKEN_KEY_BASE64": base64.StdEncoding.EncodeToString([]byte("0123456789abcdef0123456789abcdef")),
		"WORKFLOW_TASK_TOKEN_SECRET":             "0123456789abcdef0123456789abcdef",
		"TASKQ_ACTIVITY_QUEUES_JSON":             `{"default/default":{"physical":"activity-default"}}`,
	}
}

func mapLookup(values map[string]string) envLookup {
	return func(name string) (string, bool) {
		value, ok := values[name]
		return value, ok
	}
}

// migrate 只需要 MySQL 后端与 WORKFLOW_DB_DSN；配置不全时在连接数据库之前失败。
func TestMigrateRequiresMySQLBackendAndDSN(t *testing.T) {
	logger := slog.New(slog.DiscardHandler)
	cases := map[string]struct {
		env  map[string]string
		want string
	}{
		"memory backend": {map[string]string{"WORKFLOW_BACKEND": "memory", "WORKFLOW_DB_DSN": "u:p@tcp(127.0.0.1:1)/db"}, "WORKFLOW_BACKEND=mysql-taskq"},
		"missing dsn":    {map[string]string{}, "WORKFLOW_DB_DSN is required"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if err := migrate(t.Context(), mapLookup(tc.env), logger); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want it to contain %q", err, tc.want)
			}
		})
	}
}
