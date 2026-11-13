package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/donutnomad/tenuto/cmd/tenuto-cluster/workload"
	"github.com/donutnomad/tenuto/sdk/client"
)

// soakConfig 是 soak 模式的参数。
type soakConfig struct {
	commonConfig
	orderWorkload
	Duration       time.Duration
	StartRate      float64
	FaultEvery     time.Duration
	FaultSet       string
	Seed           uint64
	SampleEvery    time.Duration
	VerifyDelay    time.Duration
	Verifiers      int
	OverdueSlack   time.Duration
	RecoverTimeout time.Duration
	RunTimeout     time.Duration
	Retention      time.Duration
	Drain          time.Duration
	Settle         time.Duration
}

func parseSoakFlags(args []string) (soakConfig, []faultStep, error) {
	var cfg soakConfig
	fs := flag.NewFlagSet("soak", flag.ContinueOnError)
	cfg.commonConfig.register(fs, "", true)
	cfg.orderWorkload.register(fs)
	fs.DurationVar(&cfg.Duration, "duration", time.Hour, "how long to start workflows and inject faults")
	fs.Float64Var(&cfg.StartRate, "start-rate", 2, "workflow starts per second")
	fs.DurationVar(&cfg.FaultEvery, "fault-every", 2*time.Minute, "inject one random fault from -fault-set this often (0 = no faults)")
	fs.StringVar(&cfg.FaultSet, "fault-set", defaultSoakFaults, "faults to pick from, fault,... (same syntax as failover -faults without the time)")
	fs.Uint64Var(&cfg.Seed, "seed", uint64(time.Now().UnixNano()), "random seed for the fault order and targets")
	fs.DurationVar(&cfg.SampleEvery, "sample-every", time.Minute, "write a sample line to soak.jsonl this often")
	fs.DurationVar(&cfg.VerifyDelay, "verify-delay", 10*time.Second, "verify a run this long after it closed")
	fs.IntVar(&cfg.Verifiers, "verifiers", 4, "concurrent run verifications")
	fs.DurationVar(&cfg.OverdueSlack, "overdue-slack", 30*time.Second, "a running run is overdue this long after its fault-free duration")
	fs.DurationVar(&cfg.RecoverTimeout, "recover-timeout", 5*time.Minute, "violation when overdue runs have not drained this long after a fault ended")
	fs.DurationVar(&cfg.RunTimeout, "run-timeout", 15*time.Minute, "violation when a run is still running this long after its start")
	fs.DurationVar(&cfg.Retention, "retention", time.Hour, "WORKFLOW_RETENTION of the replicas (0 = GC off; the server accepts at least 1h); also sets a 1m jitter and scan interval")
	fs.DurationVar(&cfg.Drain, "drain", 10*time.Minute, "after -duration, wait at most this long for running workflows to close")
	fs.DurationVar(&cfg.Settle, "settle", 45*time.Second, "wait after every workflow closed before the database checks")
	if err := fs.Parse(args); err != nil {
		return cfg, nil, err
	}
	if cfg.DB == "" {
		return cfg, nil, errors.New("-db is required (e.g. -db tenuto_e2e_soak)")
	}
	if err := cfg.commonConfig.validate(); err != nil {
		return cfg, nil, err
	}
	if err := cfg.orderWorkload.validate(); err != nil {
		return cfg, nil, err
	}
	if cfg.Duration <= 0 || cfg.StartRate <= 0 || cfg.FaultEvery < 0 || cfg.SampleEvery <= 0 || cfg.Verifiers < 1 || cfg.Retention < 0 {
		return cfg, nil, errors.New("-duration, -start-rate, -sample-every and -verifiers must be positive; -fault-every and -retention not negative")
	}
	var steps []faultStep
	if cfg.FaultEvery > 0 {
		var err error
		if steps, err = parseFaultSet(cfg.FaultSet, cfg.Replicas); err != nil {
			return cfg, nil, err
		}
		for _, step := range steps {
			if step.needsProxies() && !cfg.Proxies {
				return cfg, nil, fmt.Errorf("fault %s needs -proxies", step)
			}
		}
	}
	if cfg.Retention > 0 {
		// 工具的缺省值排在前面，-server-env 里的同名变量覆盖它们。
		cfg.ServerEnv = append(envList{
			"WORKFLOW_RETENTION=" + cfg.Retention.String(),
			"WORKFLOW_RETENTION_JITTER=1m",
			"WORKFLOW_GC_SCAN_INTERVAL=1m",
		}, cfg.ServerEnv...)
	}
	return cfg, steps, nil
}

// ── 记账 ──

// soakRun 是一个运行中的 Run。
type soakRun struct {
	id       string
	started  time.Time
	due      time.Time // 无故障时应已结束的时刻 + -overdue-slack
	closedAt time.Time
}

// recovery 是一次故障之后的恢复：故障结束到过期 Run 的积压清空的时间。
type recovery struct {
	Action      string    `json:"action"`
	Target      string    `json:"target"`
	Start       time.Time `json:"start"`
	End         time.Time `json:"end"`
	RecoveredAt time.Time `json:"recovered_at,omitzero"`
	Seconds     float64   `json:"time_to_recover_sec"`
}

// executionTotals 是逐个 Run 对照执行记录的累计结果。
type executionTotals struct {
	Activities   int            `json:"activities"`
	Executions   int            `json:"executions"`
	Distribution map[string]int `json:"executions_per_activity"`
	Causes       map[string]int `json:"re_execution_causes,omitempty"`
	// ActivityRetries 是完成的 Activity 的 attempt - 1 之和（Server 记录的重试）。
	ActivityRetries int `json:"activity_retries"`
	// Unmatched 是 30 分钟内没有被任何核对取走的执行记录（属于核对之后才写完的 attempt）。
	Unmatched int `json:"unmatched_records"`
	Malformed int `json:"malformed_records"`
}

func (t *executionTotals) add(report executionReport, completed map[activityKey]int) {
	t.Activities += report.Activities
	t.Executions += report.Executions
	for k, v := range report.Distribution {
		t.Distribution[k] += v
	}
	for k, v := range report.CausesSummary {
		t.Causes[k] += v
	}
	t.Unmatched += report.Unmatched
	for _, attempt := range completed {
		t.ActivityRetries += max(attempt-1, 0)
	}
}

// recoveryBook 跟踪故障结束后的恢复。overdue 为 0 时，所有已结束的故障记为已恢复。
type recoveryBook struct {
	mu      sync.Mutex
	pending []recovery
	done    []recovery
}

func (b *recoveryBook) ended(end faultEnd) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.pending = append(b.pending, recovery{Action: end.Action, Target: end.Target, Start: end.Start, End: end.End})
}

// observe 记录 now 时的过期 Run 数，返回超过 timeout 仍未恢复的故障。
func (b *recoveryBook) observe(now time.Time, overdue int, timeout time.Duration) (stuck []recovery) {
	b.mu.Lock()
	defer b.mu.Unlock()
	kept := b.pending[:0]
	for _, r := range b.pending {
		switch {
		case overdue == 0 && !r.End.After(now):
			r.RecoveredAt = now
			r.Seconds = now.Sub(r.End).Seconds()
			b.done = append(b.done, r)
		default:
			if timeout > 0 && now.Sub(r.End) > timeout {
				stuck = append(stuck, r)
			}
			kept = append(kept, r)
		}
	}
	b.pending = kept
	return stuck
}

func (b *recoveryBook) snapshot() (done, pending []recovery) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return slices.Clone(b.done), slices.Clone(b.pending)
}

// maxRecovery 返回每种故障与全部故障的最长恢复时间（秒）。
func maxRecovery(done []recovery) (map[string]float64, float64) {
	byAction := map[string]float64{}
	overall := 0.0
	for _, r := range done {
		byAction[r.Action] = max(byAction[r.Action], r.Seconds)
		overall = max(overall, r.Seconds)
	}
	return byAction, overall
}

// ── 执行记录增量读取 ──

// recordTailer 增量读取 Worker 的执行记录文件，按 Run ID 暂存，由核对取走；内存只随未核对的 Run 增长。
type recordTailer struct {
	dir string

	mu        sync.Mutex
	offsets   map[string]int64
	byRun     map[string][]workload.Execution
	lastSeen  map[string]time.Time
	malformed int
	unmatched int
}

func newRecordTailer(dir string) *recordTailer {
	return &recordTailer{dir: dir, offsets: map[string]int64{}, byRun: map[string][]workload.Execution{}, lastSeen: map[string]time.Time{}}
}

// poll 读取各文件上次读到之后的完整行；最后一个不完整的行留到下次。
func (t *recordTailer) poll() error {
	paths, err := filepath.Glob(filepath.Join(t.dir, "activities-*.jsonl"))
	if err != nil {
		return err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	now := time.Now()
	for _, path := range paths {
		data, err := readFrom(path, t.offsets[path])
		if err != nil {
			return err
		}
		end := bytes.LastIndexByte(data, '\n')
		if end < 0 {
			continue
		}
		t.offsets[path] += int64(end + 1)
		records, bad, err := parseExecutions(bytes.NewReader(data[:end+1]))
		if err != nil {
			return fmt.Errorf("read %s: %w", path, err)
		}
		t.malformed += bad
		for _, record := range records {
			t.byRun[record.RunID] = append(t.byRun[record.RunID], record)
			t.lastSeen[record.RunID] = now
		}
	}
	return nil
}

// take 读取新记录后取走 runIDs 的全部记录。
func (t *recordTailer) take(runIDs []string) ([]workload.Execution, error) {
	if err := t.poll(); err != nil {
		return nil, err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	var out []workload.Execution
	for _, id := range runIDs {
		out = append(out, t.byRun[id]...)
		delete(t.byRun, id)
		delete(t.lastSeen, id)
	}
	return out, nil
}

// prune 丢弃 olderThan 之内没有新记录、也没有被取走的 Run 的记录，计入 unmatched。
func (t *recordTailer) prune(olderThan time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()
	cutoff := time.Now().Add(-olderThan)
	for id, seen := range t.lastSeen {
		if seen.Before(cutoff) {
			t.unmatched += len(t.byRun[id])
			delete(t.byRun, id)
			delete(t.lastSeen, id)
		}
	}
}

func (t *recordTailer) stats() (pendingRuns, malformed, unmatched int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.byRun), t.malformed, t.unmatched
}

// ── 日志计数 ──

// logCounter 增量统计日志里含 needle 的行数。
type logCounter struct {
	needle  []byte
	offsets map[string]int64
	count   int64
}

func (l *logCounter) scan(paths []string) int64 {
	for _, path := range paths {
		data, err := readFrom(path, l.offsets[path])
		if err != nil {
			continue
		}
		end := bytes.LastIndexByte(data, '\n')
		if end < 0 {
			continue
		}
		l.offsets[path] += int64(end + 1)
		l.count += int64(bytes.Count(data[:end+1], l.needle))
	}
	return l.count
}

// redeliveryLine 是 reconciler 补投一次投递的日志（docs/operations/cluster-testing.md）。
const redeliveryLine = "task dispatched but not claimed; redelivering"

// ── 采样 ──

// soakSample 是 soak.jsonl 的一行。
type soakSample struct {
	At               time.Time         `json:"at"`
	ElapsedSec       float64           `json:"elapsed_sec"`
	Started          int               `json:"started"`
	Closed           int               `json:"closed"`
	Verified         int               `json:"verified"`
	InFlight         int               `json:"in_flight"`
	Overdue          int               `json:"overdue"`
	Faults           []string          `json:"processes_in_fault,omitempty"`
	Processes        []processSample   `json:"processes"`
	Rows             map[string]int64  `json:"rows"`
	TaskQBacklog     map[string]int64  `json:"taskq_backlog"`
	ThreadsConnected int64             `json:"mysql_threads_connected"`
	Redeliveries     int64             `json:"redeliveries"`
	ReExecutions     int               `json:"re_executions"`
	ActivityRetries  int               `json:"activity_retries"`
	PendingRecords   int               `json:"runs_with_pending_records"`
	Tool             toolUsage         `json:"tool"`
	Errors           map[string]string `json:"sample_errors,omitempty"`
}

// processSample 是一个进程的 RSS 与两次采样之间的 CPU%（按单核计）。tenuto-server 没有 pprof 端点，
// goroutine 数不可得。
type processSample struct {
	Name    string  `json:"name"`
	PID     int     `json:"pid"`
	Running bool    `json:"running"`
	RSSMiB  float64 `json:"rss_mib"`
	CPUPct  float64 `json:"cpu_pct"`
}

// toolUsage 是工具自身的内存与 goroutine：验证工具的内存不随 Run 总数增长。
type toolUsage struct {
	HeapMiB    float64 `json:"heap_mib"`
	Goroutines int     `json:"goroutines"`
}

var tableNamePattern = regexp.MustCompile(`^[A-Za-z0-9_]+$`)

// rowCounts 返回库里每张表的行数（COUNT(*)，只读）。
func rowCounts(ctx context.Context, db *sql.DB, schema string) (map[string]int64, error) {
	rows, err := db.QueryContext(ctx, "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = 'BASE TABLE'", schema)
	if err != nil {
		return nil, err
	}
	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			_ = rows.Close()
			return nil, err
		}
		if tableNamePattern.MatchString(name) {
			tables = append(tables, name)
		}
	}
	_ = rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	out := map[string]int64{}
	for _, table := range tables {
		var n int64
		if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM `"+table+"`").Scan(&n); err != nil {
			return nil, fmt.Errorf("count %s: %w", table, err)
		}
		out[table] = n
	}
	return out, nil
}

// cpuTracker 由两次 ps 采样的累计 CPU 时间算 CPU%。
type cpuTracker struct {
	pid map[string]int
	cpu map[string]float64
	at  map[string]time.Time
}

func (t *cpuTracker) sample(ctx context.Context, p *process) processSample {
	out := processSample{Name: p.name, Running: p.running()}
	pid := p.pid()
	if pid == 0 || !out.Running {
		return out
	}
	out.PID = pid
	raw, err := exec.CommandContext(ctx, "ps", "-o", "time=,rss=", "-p", strconv.Itoa(pid)).Output()
	if err != nil {
		return out
	}
	cpuSeconds, rssKiB, err := parsePS(string(raw))
	if err != nil {
		return out
	}
	now := time.Now()
	out.RSSMiB = float64(rssKiB) / 1024
	if t.pid[p.name] == pid {
		if wall := now.Sub(t.at[p.name]).Seconds(); wall > 0 {
			out.CPUPct = 100 * (cpuSeconds - t.cpu[p.name]) / wall
		}
	}
	t.pid[p.name], t.cpu[p.name], t.at[p.name] = pid, cpuSeconds, now
	return out
}

// ── 泄漏指标 ──

// slopePerHour 是 (时间, 值) 的最小二乘斜率，单位：值/小时。少于 3 个点或时间跨度为 0 时 ok 为 false。
func slopePerHour(at []time.Time, values []float64) (float64, bool) {
	if len(at) < 3 || len(at) != len(values) {
		return 0, false
	}
	var sx, sy, sxx, sxy float64
	n := float64(len(at))
	for i := range at {
		x := at[i].Sub(at[0]).Hours()
		sx += x
		sy += values[i]
		sxx += x * x
		sxy += x * values[i]
	}
	denominator := n*sxx - sx*sx
	if denominator == 0 {
		return 0, false
	}
	return (n*sxy - sx*sy) / denominator, true
}

// leakReport 是 soak 结束时的泄漏指标：各进程 RSS 的增长斜率（同一 PID 最长的一段、去掉 soak 开始与每次
// 进程启动之后的预热期），
// 各表行数的增长斜率。
type leakReport struct {
	RSSSlopeMiBPerHour map[string]float64 `json:"rss_slope_mib_per_hour"`
	RSSWindow          map[string]string  `json:"rss_window"`
	RowsSlopePerHour   map[string]float64 `json:"rows_slope_per_hour"`
	ToolHeapSlope      float64            `json:"tool_heap_slope_mib_per_hour"`
	Goroutines         string             `json:"goroutines"`
	Warnings           []string           `json:"warnings,omitempty"`
}

func computeLeaks(samples []soakSample, warmup time.Duration) leakReport {
	report := leakReport{RSSSlopeMiBPerHour: map[string]float64{}, RSSWindow: map[string]string{}, RowsSlopePerHour: map[string]float64{},
		Goroutines: "not available: tenuto-server exposes no pprof endpoint; RSS is the leak indicator"}
	if len(samples) == 0 {
		return report
	}
	begin := samples[0].At.Add(warmup)
	type series struct {
		pid    int
		at     []time.Time
		values []float64
	}
	best := map[string]series{}
	current := map[string]series{}
	// started 是每个进程（按名称与 PID）第一次出现在样本里的时间。重启后的进程同样有预热期：Go 堆、连接池与
	// Sticky Cache 在前几分钟增长到稳态，算进斜率会把预热当成泄漏。
	type startedAt struct {
		pid int
		at  time.Time
	}
	started := map[string]startedAt{}
	var toolAt []time.Time
	var toolHeap []float64
	rows := map[string]*series{}
	for _, sample := range samples {
		for _, p := range sample.Processes {
			if p.Running && p.PID != 0 && started[p.Name].pid != p.PID {
				started[p.Name] = startedAt{pid: p.PID, at: sample.At}
			}
		}
		if sample.At.Before(begin) {
			continue
		}
		toolAt, toolHeap = append(toolAt, sample.At), append(toolHeap, sample.Tool.HeapMiB)
		for _, p := range sample.Processes {
			if !p.Running || p.PID == 0 || sample.At.Before(started[p.Name].at.Add(warmup)) {
				continue
			}
			s := current[p.Name]
			if s.pid != p.PID {
				s = series{pid: p.PID}
			}
			s.at, s.values = append(s.at, sample.At), append(s.values, p.RSSMiB)
			current[p.Name] = s
			if len(s.at) > len(best[p.Name].at) {
				best[p.Name] = s
			}
		}
		for table, n := range sample.Rows {
			s := rows[table]
			if s == nil {
				s = &series{}
				rows[table] = s
			}
			s.at, s.values = append(s.at, sample.At), append(s.values, float64(n))
		}
	}
	for name, s := range best {
		slope, ok := slopePerHour(s.at, s.values)
		if !ok {
			continue
		}
		report.RSSSlopeMiBPerHour[name] = slope
		span := s.at[len(s.at)-1].Sub(s.at[0])
		report.RSSWindow[name] = fmt.Sprintf("pid %d, %d samples over %s", s.pid, len(s.at), span.Round(time.Second))
		// 按斜率外推的增长超过窗口起点 RSS 的 25% 时提示。
		if growth := slope * span.Hours(); slope > 0 && s.values[0] > 0 && growth > 0.25*s.values[0] {
			report.Warnings = append(report.Warnings, fmt.Sprintf("%s RSS grows %.1f MiB/h (%.0f MiB over %s from %.0f MiB)", name, slope, growth, span.Round(time.Second), s.values[0]))
		}
	}
	for table, s := range rows {
		if slope, ok := slopePerHour(s.at, s.values); ok {
			report.RowsSlopePerHour[table] = slope
		}
	}
	report.ToolHeapSlope, _ = slopePerHour(toolAt, toolHeap)
	slices.Sort(report.Warnings)
	return report
}

// ── soak ──

// soakSummary 是 soak-summary.json 的内容。
type soakSummary struct {
	Pass             bool               `json:"pass"`
	DB               string             `json:"db"`
	Replicas         int                `json:"replicas"`
	Workers          int                `json:"workers"`
	Seed             uint64             `json:"seed"`
	StartedAt        time.Time          `json:"started_at"`
	FinishedAt       time.Time          `json:"finished_at"`
	Started          int                `json:"started"`
	Closed           int                `json:"closed"`
	Verified         int                `json:"verified"`
	StartRetries     int64              `json:"start_retries"`
	SignalRetries    int64              `json:"signal_retries"`
	SignalOutcome    map[string]int     `json:"signal_outcome"`
	DuplicateSignals int                `json:"runs_with_duplicate_signals"`
	Faults           map[string]int     `json:"faults"`
	Recoveries       []recovery         `json:"recoveries"`
	Unrecovered      []recovery         `json:"unrecovered,omitempty"`
	MaxRecoverySec   float64            `json:"max_time_to_recover_sec"`
	MaxRecoveryBy    map[string]float64 `json:"max_time_to_recover_sec_by_action"`
	Redeliveries     int64              `json:"redeliveries"`
	Executions       executionTotals    `json:"executions"`
	Leaks            leakReport         `json:"leaks"`
	Retention        string             `json:"retention"`
	Database         dbReport           `json:"database"`
	Timeline         []faultEvent       `json:"timeline"`
	FailedRuns       []runCheck         `json:"failed_runs,omitempty"`
	Violations       []string           `json:"violations,omitempty"`
	Warnings         []string           `json:"warnings,omitempty"`
}

// soak 是一次 soak 运行的状态。
type soak struct {
	cfg   soakConfig
	progs programs
	cl    *cluster
	db    *database
	c     client.Client
	inj   *injector
	fail  context.CancelCauseFunc

	tailer       *recordTailer
	recoveries   recoveryBook
	redeliveries logCounter
	cpu          cpuTracker
	samplesFile  *os.File

	startRetries, signalRetries atomic.Int64

	mu         sync.Mutex
	inFlight   map[string]*soakRun
	started    int
	closed     int
	verified   int
	summary    soakSummary
	executions executionTotals
	samples    []soakSample
}

func runSoak(ctx context.Context, args []string) error {
	cfg, steps, err := parseSoakFlags(args)
	if err != nil {
		return err
	}
	progs, err := loadPrograms()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(cfg.Out, 0o755); err != nil {
		return err
	}
	serverBin, err := buildServer(ctx, cfg.commonConfig)
	if err != nil {
		return err
	}
	db, err := createDatabase(ctx, cfg.DSN, cfg.DB)
	if err != nil {
		return err
	}
	defer finishDatabase(db, cfg.Keep)
	cl, err := startCluster(ctx, cfg.commonConfig, db, serverBin)
	if err != nil {
		return err
	}
	defer cl.shutdown()
	c, err := dialCluster(cl, "tenuto-cluster-soak")
	if err != nil {
		return err
	}
	defer c.Close()
	samplesFile, err := os.OpenFile(filepath.Join(cfg.Out, "soak.jsonl"), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	defer samplesFile.Close()
	fmt.Printf("cluster ready: %d replicas, %d workers, load balancer %s, output %s\n", cfg.Replicas, cfg.Workers, cl.lb.url(), cfg.Out)

	s := &soak{
		cfg: cfg, progs: progs, cl: cl, db: db, c: c, inj: newInjector(cl, cfg.Seed),
		tailer:       newRecordTailer(cfg.Out),
		redeliveries: logCounter{needle: []byte(redeliveryLine), offsets: map[string]int64{}},
		cpu:          cpuTracker{pid: map[string]int{}, cpu: map[string]float64{}, at: map[string]time.Time{}},
		samplesFile:  samplesFile,
		inFlight:     map[string]*soakRun{},
		summary: soakSummary{DB: cfg.DB, Replicas: cfg.Replicas, Workers: cfg.Workers, Seed: cfg.Seed, StartedAt: time.Now(),
			SignalOutcome: map[string]int{}, Faults: map[string]int{}},
		executions: executionTotals{Distribution: map[string]int{}, Causes: map[string]int{}},
	}
	s.inj.ended = s.recoveries.ended
	if cfg.Retention > 0 {
		s.summary.Retention = fmt.Sprintf("WORKFLOW_RETENTION=%s, jitter 1m, scan interval 1m: closed runs become reclaimable %s after they close", cfg.Retention, cfg.Retention)
	} else {
		s.summary.Retention = "off"
	}
	runErr := s.run(ctx, steps)
	if err := s.finish(ctx, runErr); err != nil {
		return err
	}
	if ctx.Err() != nil && !errors.Is(runErr, errViolations) {
		return ctx.Err()
	}
	if !s.summary.Pass {
		return errViolations
	}
	return nil
}

// violate 记一条违规并让 soak 立即结束（fail fast）。
func (s *soak) violate(format string, args ...any) {
	message := fmt.Sprintf(format, args...)
	s.mu.Lock()
	s.summary.Violations = append(s.summary.Violations, message)
	s.mu.Unlock()
	fmt.Printf("%s VIOLATION: %s\n", time.Now().Format("15:04:05.000"), message)
	s.fail(errViolations)
}

// run 启动负载、故障、核对、监视与采样，-duration 后排空；有违规时立即返回 errViolations。
func (s *soak) run(ctx context.Context, steps []faultStep) error {
	runCtx, fail := context.WithCancelCause(ctx)
	defer fail(nil)
	s.fail = fail
	start := time.Now()
	end := start.Add(s.cfg.Duration)

	verifyQueue := make(chan *soakRun, 4096)
	var verifiers sync.WaitGroup
	for range s.cfg.Verifiers {
		verifiers.Add(1)
		go func() { defer verifiers.Done(); s.verifyLoop(runCtx, verifyQueue) }()
	}
	monitorCtx, stopMonitor := context.WithCancel(runCtx)
	var background sync.WaitGroup
	background.Add(2)
	go func() { defer background.Done(); s.monitorLoop(monitorCtx) }()
	go func() { defer background.Done(); s.sampleLoop(monitorCtx, start) }()
	faultsDone := make(chan struct{})
	go func() { s.faultLoop(runCtx, start, end, steps); close(faultsDone) }()

	var waiters, signals sync.WaitGroup
	s.startLoop(runCtx, start, end, verifyQueue, &waiters, &signals)
	<-faultsDone
	fmt.Printf("%s soak window over; draining %d running workflows (at most %s)\n", time.Now().Format("15:04:05.000"), s.inFlightCount(), s.cfg.Drain)
	drained := make(chan struct{})
	go func() { waiters.Wait(); signals.Wait(); close(drained) }()
	// 正常排空后关闭队列，核对完剩下的 Run；违规或中断时 runCtx 已取消，核对与等待 Run 的 goroutine 随之退出
	// （此时不关闭队列：等待 Run 的 goroutine 可能还在发送）。
	select {
	case <-drained:
		close(verifyQueue)
	case <-runCtx.Done():
	case <-time.After(s.cfg.Drain):
		s.violate("%d runs still running %s after the soak window", s.inFlightCount(), s.cfg.Drain)
	}
	verifiers.Wait()
	stopMonitor()
	background.Wait()
	if cause := context.Cause(runCtx); errors.Is(cause, errViolations) {
		return errViolations
	}
	return runCtx.Err()
}

func (s *soak) inFlightCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.inFlight)
}

// startLoop 按 -start-rate 启动 Run 直到 end；每个 Run 有一个 goroutine 等它结束后交给核对。
func (s *soak) startLoop(ctx context.Context, start, end time.Time, verifyQueue chan<- *soakRun, waiters, signals *sync.WaitGroup) {
	interval := time.Duration(float64(time.Second) / s.cfg.StartRate)
	expected := s.cfg.expectedDuration() + s.cfg.OverdueSlack
	for i := 0; ; i++ {
		at := start.Add(time.Duration(i) * interval)
		if !at.Before(end) || sleepCtx(ctx, time.Until(at)) != nil {
			return
		}
		id := fmt.Sprintf("soak-%07d", i)
		if err := startOrder(ctx, s.c, id, s.cfg.params(id), 2*time.Minute, &s.startRetries); err != nil {
			if ctx.Err() == nil {
				s.violate("start %s failed for 2m: %v", id, err)
			}
			return
		}
		run := &soakRun{id: id, started: time.Now()}
		run.due = run.started.Add(expected)
		s.mu.Lock()
		s.inFlight[id] = run
		s.started++
		s.mu.Unlock()
		signalLater(ctx, s.c, id, s.cfg.orderWorkload, signals, &s.signalRetries)
		waiters.Add(1)
		go func() {
			defer waiters.Done()
			if !s.waitClosed(ctx, id) {
				return
			}
			run.closedAt = time.Now()
			s.mu.Lock()
			delete(s.inFlight, id)
			s.closed++
			s.mu.Unlock()
			select {
			case verifyQueue <- run:
			case <-ctx.Done():
			}
		}()
	}
}

// waitClosed 等 Run 结束（WorkflowRun.Get 的 wait-closed 长轮询）。Get 出错时用 Describe 判断 Run 是否已经
// 结束（失败的 Run 也交给核对报告），没结束就再等。
func (s *soak) waitClosed(ctx context.Context, id string) bool {
	for ctx.Err() == nil {
		var out orderOutput
		err := s.c.GetWorkflow(ctx, id, "").Get(ctx, &out)
		if err == nil {
			return true
		}
		if description, describeErr := s.c.DescribeWorkflowExecution(ctx, id, ""); describeErr == nil && description.Status != client.WorkflowExecutionStatusRunning {
			return true
		}
		if sleepCtx(ctx, time.Second) != nil {
			return false
		}
	}
	return false
}

// verifyLoop 在 Run 结束 -verify-delay 后核对它：结果、History 不变式、重放（failover 的同一套核对），
// 以及它与子 Run 的执行记录。
func (s *soak) verifyLoop(ctx context.Context, queue <-chan *soakRun) {
	for {
		var run *soakRun
		select {
		case <-ctx.Done():
			return
		case next, ok := <-queue:
			if !ok {
				return
			}
			run = next
		}
		if sleepCtx(ctx, time.Until(run.closedAt.Add(s.cfg.VerifyDelay))) != nil {
			return
		}
		check := verifyOrder(ctx, s.c, s.progs, run.id, s.cfg.params(run.id))
		if ctx.Err() != nil {
			return
		}
		runIDs := make([]string, 0, 1+s.cfg.Children)
		for key := range check.completed {
			if !slices.Contains(runIDs, key.RunID) {
				runIDs = append(runIDs, key.RunID)
			}
		}
		records, err := s.tailer.take(runIDs)
		if err != nil {
			s.violate("read execution records: %v", err)
			continue
		}
		report := analyzeExecutions(check.completed, groupExecutions(records), s.cl.faultLog())
		s.mu.Lock()
		s.verified++
		if check.Signal != "" {
			s.summary.SignalOutcome[check.Signal]++
		}
		if check.Signals > 1 {
			s.summary.DuplicateSignals++
		}
		s.executions.add(report, check.completed)
		if len(check.Problems) > 0 && len(s.summary.FailedRuns) < maxFailedRuns {
			s.summary.FailedRuns = append(s.summary.FailedRuns, check)
		}
		s.mu.Unlock()
		if len(check.Problems) > 0 {
			s.violate("%s: %s", run.id, strings.Join(check.Problems, "; "))
		}
		for _, violation := range report.Violations {
			s.violate("%s", violation)
		}
	}
}

// faultLoop 每 -fault-every 从集合里随机取一个故障注入，直到 end；返回前等全部故障恢复。
func (s *soak) faultLoop(ctx context.Context, start, end time.Time, steps []faultStep) {
	defer s.inj.wait()
	if len(steps) == 0 {
		return
	}
	for k := 1; ; k++ {
		at := start.Add(time.Duration(k) * s.cfg.FaultEvery)
		if !at.Before(end) || sleepCtx(ctx, time.Until(at)) != nil {
			return
		}
		step := steps[s.inj.randomInt(len(steps))]
		if err := s.inj.apply(ctx, step); err != nil {
			fmt.Printf("%s fault %s skipped: %v\n", time.Now().Format("15:04:05.000"), step, err)
			continue
		}
		s.mu.Lock()
		s.summary.Faults[step.Action]++
		s.mu.Unlock()
	}
}

// monitorLoop 每秒检查：卡住的 Run、故障之后迟迟不恢复的积压、不在故障中却退出了的进程。
func (s *soak) monitorLoop(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			overdue, stuck := s.overdue(now)
			if stuck != "" {
				s.violate("run %s is still running %s after its start", stuck, s.cfg.RunTimeout)
				return
			}
			for _, r := range s.recoveries.observe(now, overdue, s.cfg.RecoverTimeout) {
				s.violate("%d overdue runs did not drain within %s after %s on %s ended", overdue, s.cfg.RecoverTimeout, r.Action, r.Target)
				return
			}
			for _, p := range append(slices.Clone(s.cl.replicas), s.cl.workers...) {
				if !p.running() && !s.inj.inFault(p.name) && ctx.Err() == nil {
					s.violate("%s exited without an injected fault; see %s", p.name, p.logPath)
					return
				}
			}
		}
	}
}

// overdue 返回 now 时过期（超过无故障时长 + -overdue-slack）的 Run 数，以及一个超过 -run-timeout 的 Run。
func (s *soak) overdue(now time.Time) (int, string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for id, run := range s.inFlight {
		if now.After(run.due) {
			n++
		}
		if now.Sub(run.started) > s.cfg.RunTimeout {
			return n, id
		}
	}
	return n, ""
}

// sampleLoop 立即采样一次，之后每 -sample-every 一次，结束时再采样一次。
func (s *soak) sampleLoop(ctx context.Context, start time.Time) {
	ticker := time.NewTicker(s.cfg.SampleEvery)
	defer ticker.Stop()
	s.sample(ctx, start)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.sample(ctx, start)
		}
	}
}

func (s *soak) sample(ctx context.Context, start time.Time) {
	now := time.Now()
	sample := soakSample{At: now, ElapsedSec: now.Sub(start).Seconds(), Errors: map[string]string{}}
	s.tailer.prune(30 * time.Minute)
	if err := s.tailer.poll(); err != nil {
		sample.Errors["records"] = err.Error()
	}
	overdue, _ := s.overdue(now)
	sample.Overdue = overdue
	sample.Faults = s.inj.busyNames()
	for _, p := range append(slices.Clone(s.cl.replicas), s.cl.workers...) {
		sample.Processes = append(sample.Processes, s.cpu.sample(ctx, p))
	}
	queryCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	var err error
	if sample.Rows, err = rowCounts(queryCtx, s.db.db, s.db.name); err != nil {
		sample.Errors["rows"] = err.Error()
	}
	if sample.TaskQBacklog, err = countBy(queryCtx, s.db.db, "SELECT queue_name, COUNT(*) FROM taskq_tasks WHERE dispatched_at IS NULL GROUP BY queue_name"); err != nil {
		sample.Errors["taskq"] = err.Error()
	}
	var name string
	if err := s.db.admin.QueryRowContext(queryCtx, "SHOW GLOBAL STATUS LIKE 'Threads_connected'").Scan(&name, &sample.ThreadsConnected); err != nil {
		sample.Errors["threads"] = err.Error()
	}
	logs := make([]string, len(s.cl.replicas))
	for i, replica := range s.cl.replicas {
		logs[i] = replica.logPath
	}
	sample.Redeliveries = s.redeliveries.scan(logs)
	sample.PendingRecords, _, _ = s.tailer.stats()
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	sample.Tool = toolUsage{HeapMiB: float64(mem.HeapInuse) / (1 << 20), Goroutines: runtime.NumGoroutine()}
	if len(sample.Errors) == 0 {
		sample.Errors = nil
	}

	s.mu.Lock()
	sample.Started, sample.Closed, sample.Verified, sample.InFlight = s.started, s.closed, s.verified, len(s.inFlight)
	sample.ReExecutions = s.executions.Executions - s.executions.Activities
	sample.ActivityRetries = s.executions.ActivityRetries
	s.samples = append(s.samples, sample)
	s.mu.Unlock()
	line, _ := json.Marshal(sample)
	_, _ = s.samplesFile.Write(append(line, '\n'))
	fmt.Printf("%s sample: started %d closed %d verified %d in flight %d overdue %d redeliveries %d backlog %v\n",
		now.Format("15:04:05.000"), sample.Started, sample.Closed, sample.Verified, sample.InFlight, sample.Overdue, sample.Redeliveries, sample.TaskQBacklog)
}

// finish 写 soak-summary.json。正常结束（没有违规、没有中断）时先等 -settle 再只读核对库。
func (s *soak) finish(ctx context.Context, runErr error) error {
	if runErr == nil {
		fmt.Printf("waiting %s before the database checks\n", s.cfg.Settle)
		if err := sleepCtx(ctx, s.cfg.Settle); err != nil {
			return err
		}
		s.sample(ctx, s.summary.StartedAt)
		database, err := checkDatabase(ctx, s.db.db, s.cfg.Namespace)
		s.mu.Lock()
		if err != nil {
			s.summary.Violations = append(s.summary.Violations, fmt.Sprintf("database checks failed: %v", err))
		}
		s.summary.Database = database
		s.summary.Violations = append(s.summary.Violations, database.Violations...)
		s.summary.Warnings = append(s.summary.Warnings, database.Warnings...)
		s.mu.Unlock()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	sum := &s.summary
	sum.FinishedAt = time.Now()
	sum.Started, sum.Closed, sum.Verified = s.started, s.closed, s.verified
	sum.StartRetries, sum.SignalRetries = s.startRetries.Load(), s.signalRetries.Load()
	sum.Timeline = s.cl.faultLog()
	sum.Recoveries, sum.Unrecovered = s.recoveries.snapshot()
	sum.MaxRecoveryBy, sum.MaxRecoverySec = maxRecovery(sum.Recoveries)
	if len(s.samples) > 0 {
		sum.Redeliveries = s.samples[len(s.samples)-1].Redeliveries
	}
	_, malformed, unmatched := s.tailer.stats()
	s.executions.Malformed, s.executions.Unmatched = malformed, s.executions.Unmatched+unmatched
	sum.Executions = s.executions
	sum.Leaks = computeLeaks(s.samples, min(10*time.Minute, s.cfg.Duration/5))
	sum.Warnings = append(sum.Warnings, sum.Leaks.Warnings...)
	if sum.DuplicateSignals > 0 {
		sum.Warnings = append(sum.Warnings, fmt.Sprintf("%d runs received the signal more than once (the client retried a signal whose response was lost)", sum.DuplicateSignals))
	}
	if malformed > 0 {
		sum.Warnings = append(sum.Warnings, fmt.Sprintf("%d execution record lines are incomplete (a worker was killed while writing)", malformed))
	}
	if runErr == nil {
		s.checkRetention()
	}
	// 被中断（Ctrl-C）的 soak 不算通过；摘要照写，记下中断前的结果。
	sum.Pass = len(sum.Violations) == 0 && runErr == nil
	if err := writeJSON(filepath.Join(s.cfg.Out, "soak-summary.json"), sum); err != nil {
		return err
	}
	printSoakSummary(*sum, s.cfg.Out)
	return nil
}

// checkRetention 在 soak 足够长（retention + 30 分钟）时核对 GC 确实回收过 Run：Run 行数少于启动过的 Run
// （含子 Run）总数。调用方持有 s.mu。
func (s *soak) checkRetention() {
	sum := &s.summary
	if s.cfg.Retention <= 0 || len(s.samples) == 0 {
		return
	}
	elapsed := s.samples[len(s.samples)-1].At.Sub(sum.StartedAt)
	if elapsed < s.cfg.Retention+30*time.Minute {
		sum.Warnings = append(sum.Warnings, fmt.Sprintf("the soak ran %s, shorter than retention %s + 30m: retention GC was not exercised", elapsed.Round(time.Second), s.cfg.Retention))
		return
	}
	created := int64(s.started * (1 + s.cfg.Children))
	if rows, ok := s.samples[len(s.samples)-1].Rows["tenuto_runs"]; ok && rows >= created {
		sum.Violations = append(sum.Violations, fmt.Sprintf("retention GC reclaimed no run: tenuto_runs has %d rows for %d runs started %s ago (retention %s)", rows, created, elapsed.Round(time.Second), s.cfg.Retention))
	}
}

func printSoakSummary(s soakSummary, out string) {
	fmt.Println()
	fmt.Println("==== soak summary ====")
	fmt.Printf("started %d, closed %d, verified %d   start retries %d   signal retries %d\n", s.Started, s.Closed, s.Verified, s.StartRetries, s.SignalRetries)
	fmt.Printf("faults: %s\n", formatCounts(s.Faults))
	fmt.Printf("max time to recover: %.0fs overall", s.MaxRecoverySec)
	for _, action := range sortedKeys(s.MaxRecoveryBy) {
		fmt.Printf("  %s=%.0fs", action, s.MaxRecoveryBy[action])
	}
	fmt.Println()
	e := s.Executions
	fmt.Printf("redeliveries: %d   activity retries: %d   executions per activity: %s   unmatched records: %d\n", s.Redeliveries, e.ActivityRetries, formatCounts(e.Distribution), e.Unmatched)
	for _, cause := range sortedKeys(e.Causes) {
		fmt.Printf("  %5d  %s\n", e.Causes[cause], cause)
	}
	fmt.Println("leak indicators (RSS slope, MiB/hour):")
	for _, name := range sortedKeys(s.Leaks.RSSSlopeMiBPerHour) {
		fmt.Printf("  %-12s %8.1f   (%s)\n", name, s.Leaks.RSSSlopeMiBPerHour[name], s.Leaks.RSSWindow[name])
	}
	fmt.Printf("  tool heap    %8.1f\n", s.Leaks.ToolHeapSlope)
	fmt.Println("row growth (rows/hour):")
	for _, table := range sortedKeys(s.Leaks.RowsSlopePerHour) {
		fmt.Printf("  %-40s %10.0f\n", table, s.Leaks.RowsSlopePerHour[table])
	}
	fmt.Printf("retention: %s\n", s.Retention)
	for _, warning := range s.Warnings {
		fmt.Println("WARNING:", warning)
	}
	for _, violation := range limit(s.Violations, 50) {
		fmt.Println("VIOLATION:", violation)
	}
	switch {
	case s.Pass:
		fmt.Printf("PASS  (samples: %s, details: %s)\n", filepath.Join(out, "soak.jsonl"), filepath.Join(out, "soak-summary.json"))
	case len(s.Violations) == 0:
		fmt.Printf("INTERRUPTED  (details: %s)\n", filepath.Join(out, "soak-summary.json"))
	default:
		fmt.Printf("FAIL: %d violations  (details: %s)\n", len(s.Violations), filepath.Join(out, "soak-summary.json"))
	}
}
