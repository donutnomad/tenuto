package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"math"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/donutnomad/tenuto/cmd/tenuto-cluster/workload"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/sdk/client"
)

type benchConfig struct {
	commonConfig
	Rate           float64
	Concurrency    int
	Duration       time.Duration
	Steps          int
	Timer          time.Duration
	Activity       time.Duration
	Deadline       time.Duration
	HistorySample  int
	SampleInterval time.Duration
	Workflows      int
	PayloadBytes   int
}

func parseBenchFlags(args []string) (benchConfig, error) {
	var cfg benchConfig
	fs := flag.NewFlagSet("bench", flag.ContinueOnError)
	cfg.register(fs, "", false)
	fs.Float64Var(&cfg.Rate, "rate", 0, "open loop: workflow starts per second")
	fs.IntVar(&cfg.Concurrency, "concurrency", 0, "closed loop: workflows in flight (default 20 when -rate is not set)")
	fs.DurationVar(&cfg.Duration, "duration", time.Minute, "how long to start workflows")
	fs.IntVar(&cfg.Steps, "steps", 3, fmt.Sprintf("sequential activities per workflow (history length; at most %d)", maxSequential))
	fs.IntVar(&cfg.Workflows, "workflows", 0, "stop starting after this many workflows (0 = only -duration limits)")
	fs.IntVar(&cfg.PayloadBytes, "payload-bytes", 0, "padding carried in every activity input and output and in the workflow input")
	fs.DurationVar(&cfg.Timer, "timer", 100*time.Millisecond, "timer after the activities (0 = no timer)")
	fs.DurationVar(&cfg.Activity, "activity", 0, "how long each activity runs")
	fs.DurationVar(&cfg.Deadline, "deadline", 5*time.Minute, "how long to wait for in-flight workflows after -duration")
	fs.IntVar(&cfg.HistorySample, "history-sample", 0, "read the history of at most this many completed workflows (0 = all)")
	fs.DurationVar(&cfg.SampleInterval, "sample-interval", 2*time.Second, "CPU/RSS and Threads_connected sampling interval")
	if err := fs.Parse(args); err != nil {
		return cfg, err
	}
	if cfg.DB == "" {
		return cfg, errors.New("-db is required (e.g. -db tenuto_bench_1)")
	}
	if err := cfg.validate(); err != nil {
		return cfg, err
	}
	if cfg.Rate > 0 && cfg.Concurrency > 0 {
		return cfg, errors.New("-rate and -concurrency are exclusive")
	}
	if cfg.Rate <= 0 && cfg.Concurrency <= 0 {
		cfg.Concurrency = 20
	}
	if cfg.Duration <= 0 || cfg.Steps < 0 || cfg.Steps > maxSequential || cfg.Timer < 0 || cfg.Activity < 0 || cfg.Workflows < 0 || cfg.PayloadBytes < 0 {
		return cfg, fmt.Errorf("-duration must be positive; -steps 0..%d; -timer, -activity, -workflows and -payload-bytes not negative", maxSequential)
	}
	return cfg, nil
}

// benchRun 是一次 Workflow 的调用端测量。
type benchRun struct {
	ID          string
	Started     time.Time
	StartTook   time.Duration
	StartErr    error
	CompletedAt time.Time
	GetTook     time.Duration
	GetErr      error
	WrongResult bool
}

// benchReport 是 bench.json 的内容。时间单位是毫秒。
type benchReport struct {
	DB          string  `json:"db"`
	Replicas    int     `json:"replicas"`
	Workers     int     `json:"workers"`
	Rate        float64 `json:"rate,omitempty"`
	Concurrency int     `json:"concurrency,omitempty"`
	DurationSec float64 `json:"duration_sec"`
	Steps       int     `json:"steps"`
	Payload     int     `json:"payload_bytes"`
	TimerMs     int64   `json:"timer_ms"`
	ActivityMs  int64   `json:"activity_ms"`

	Started      int            `json:"started"`
	Completed    int            `json:"completed"`
	StartErrors  int            `json:"start_errors"`
	GetErrors    int            `json:"get_errors"`
	WrongResults int            `json:"wrong_results"`
	ErrorSamples map[string]int `json:"error_samples,omitempty"`

	// ThroughputPerSec 是 -duration 窗口内完成的 Workflow 数 / 窗口秒数；OverallPerSec 是全部完成数 / 首次启动到最后完成的秒数。
	ThroughputPerSec float64 `json:"throughput_per_sec"`
	OverallPerSec    float64 `json:"overall_per_sec"`

	StartLatency           latencyStats `json:"start_latency_ms"`
	ClientCompletion       latencyStats `json:"client_completion_latency_ms"`
	HistoryCompletion      latencyStats `json:"history_completion_latency_ms"`
	WorkflowTaskScheduling latencyStats `json:"workflow_task_schedule_to_start_ms"`
	ActivityScheduling     latencyStats `json:"activity_schedule_to_start_ms"`
	HistoriesRead          int          `json:"histories_read"`

	MySQL     mysqlReport    `json:"mysql"`
	Processes []processUsage `json:"processes"`
	Notes     []string       `json:"notes"`
}

func runBench(ctx context.Context, args []string) error {
	cfg, err := parseBenchFlags(args)
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
	httpClient := &http.Client{Timeout: 30 * time.Second, Transport: &http.Transport{MaxIdleConnsPerHost: 256}}
	c, err := client.Dial(client.Options{HostPort: cl.lb.url(), Namespace: cfg.Namespace, Identity: "tenuto-cluster-bench", AuthToken: cl.authToken, HTTPClient: httpClient})
	if err != nil {
		return err
	}
	defer c.Close()
	fmt.Printf("cluster ready: %d replicas, %d workers, load balancer %s, output %s\n", cfg.Replicas, cfg.Workers, cl.lb.url(), cfg.Out)

	sampler := newResourceSampler(db.admin, append(slices.Clone(cl.replicas), cl.workers...))
	samplerCtx, stopSampler := context.WithCancel(ctx)
	samplerDone := make(chan struct{})
	go func() { sampler.run(samplerCtx, cfg.SampleInterval); close(samplerDone) }()
	before, err := globalStatus(ctx, db.admin)
	if err != nil {
		stopSampler()
		return err
	}

	start := time.Now()
	runs := drive(ctx, c, cfg, start)
	finished := time.Now()
	after, err := globalStatus(ctx, db.admin)
	stopSampler()
	<-samplerDone
	if err != nil {
		return err
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}

	report := benchReport{DB: cfg.DB, Replicas: cfg.Replicas, Workers: cfg.Workers, Rate: cfg.Rate, Concurrency: cfg.Concurrency,
		DurationSec: cfg.Duration.Seconds(), Steps: cfg.Steps, Payload: cfg.PayloadBytes, TimerMs: cfg.Timer.Milliseconds(), ActivityMs: cfg.Activity.Milliseconds()}
	summarizeRuns(&report, runs, start, cfg.Duration)
	fmt.Printf("reading histories of completed workflows\n")
	readBenchHistories(ctx, c, &report, runs, cfg.HistorySample)
	report.MySQL = mysqlDelta(before, after, finished.Sub(start), report.Completed, sampler.maxThreadsConnected())
	report.Processes = sampler.usage()
	report.Notes = []string{
		"client_completion_latency_ms is start → WorkflowRun.Get returning. Get long-polls wait-closed: the replica serving it returns at once when it closed the run itself and within its 100ms watch interval when another replica did. history_completion_latency_ms (WorkflowStarted → closing event) is the server-side figure.",
		"MySQL counters are SHOW GLOBAL STATUS deltas for the whole server: other databases on the same server are included.",
		"All numbers depend on the hardware, the MySQL configuration and the fact that every process runs on one machine.",
	}
	if err := writeJSON(filepath.Join(cfg.Out, "bench.json"), report); err != nil {
		return err
	}
	printBenchReport(report, cfg.Out)
	if report.WrongResults > 0 {
		return errViolations
	}
	return nil
}

// drive 按 -rate（开环）或 -concurrency（闭环）启动 Workflow 直到 -duration 结束，再等在途的 Workflow 最多 -deadline。
func drive(ctx context.Context, c client.Client, cfg benchConfig, start time.Time) []*benchRun {
	var mu sync.Mutex
	var runs []*benchRun
	var wg sync.WaitGroup
	end := start.Add(cfg.Duration)
	waitCtx, cancel := context.WithDeadline(ctx, end.Add(cfg.Deadline))
	defer cancel()
	seq := 0
	// nextID 返回下一个 Workflow ID；达到 -workflows 时返回 false。
	nextID := func() (string, bool) {
		mu.Lock()
		defer mu.Unlock()
		if cfg.Workflows > 0 && seq >= cfg.Workflows {
			return "", false
		}
		seq++
		return fmt.Sprintf("bench-%07d", seq), true
	}
	pad := workload.Pad(cfg.PayloadBytes)
	one := func() bool {
		id, ok := nextID()
		if !ok {
			return false
		}
		run := runOne(waitCtx, c, cfg, id, pad)
		mu.Lock()
		runs = append(runs, run)
		mu.Unlock()
		return true
	}
	if cfg.Rate > 0 {
		interval := time.Duration(float64(time.Second) / cfg.Rate)
		for i := 0; ; i++ {
			at := start.Add(time.Duration(i) * interval)
			if !at.Before(end) || sleepCtx(ctx, time.Until(at)) != nil {
				break
			}
			wg.Add(1)
			go func() { defer wg.Done(); one() }()
		}
	} else {
		for range cfg.Concurrency {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for time.Now().Before(end) && ctx.Err() == nil {
					one()
				}
			}()
		}
	}
	wg.Wait()
	return runs
}

func runOne(ctx context.Context, c client.Client, cfg benchConfig, id, pad string) *benchRun {
	params := workload.StepsParams{Key: id, Steps: cfg.Steps, TimerMs: cfg.Timer.Milliseconds(), ActivityMs: cfg.Activity.Milliseconds(), Pad: pad}
	run := &benchRun{ID: id, Started: time.Now()}
	handle, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: id}, workload.StepsWorkflowType, params)
	run.StartTook = time.Since(run.Started)
	if err != nil {
		run.StartErr = err
		return run
	}
	var out stepsOutput
	err = handle.Get(ctx, &out)
	run.CompletedAt = time.Now()
	run.GetTook = run.CompletedAt.Sub(run.Started)
	run.GetErr = err
	if err == nil && (out.Total != workload.ExpectedStepsTotal(id, cfg.Steps) || out.Steps != cfg.Steps) {
		run.WrongResult = true
	}
	return run
}

func summarizeRuns(report *benchReport, runs []*benchRun, start time.Time, window time.Duration) {
	var startLatency, completion []time.Duration
	report.ErrorSamples = map[string]int{}
	inWindow := 0
	var last time.Time
	for _, run := range runs {
		report.Started++
		if run.StartErr != nil {
			report.StartErrors++
			report.ErrorSamples["start: "+firstLine(run.StartErr.Error())]++
			continue
		}
		startLatency = append(startLatency, run.StartTook)
		if run.GetErr != nil {
			report.GetErrors++
			report.ErrorSamples["get: "+firstLine(run.GetErr.Error())]++
			continue
		}
		if run.WrongResult {
			report.WrongResults++
		}
		report.Completed++
		completion = append(completion, run.GetTook)
		if run.CompletedAt.Before(start.Add(window)) {
			inWindow++
		}
		if run.CompletedAt.After(last) {
			last = run.CompletedAt
		}
	}
	if len(report.ErrorSamples) == 0 {
		report.ErrorSamples = nil
	}
	report.ThroughputPerSec = float64(inWindow) / window.Seconds()
	if elapsed := last.Sub(start).Seconds(); elapsed > 0 {
		report.OverallPerSec = float64(report.Completed) / elapsed
	}
	report.StartLatency = summarize(startLatency)
	report.ClientCompletion = summarize(completion)
}

func firstLine(s string) string {
	line, _, _ := strings.Cut(s, "\n")
	if len(line) > 160 {
		line = line[:160]
	}
	return line
}

// readBenchHistories 读取已完成 Workflow 的 History，计算服务端视角的延迟。
func readBenchHistories(ctx context.Context, c client.Client, report *benchReport, runs []*benchRun, sample int) {
	var ids []string
	for _, run := range runs {
		if run.StartErr == nil && run.GetErr == nil {
			ids = append(ids, run.ID)
		}
	}
	if sample > 0 && len(ids) > sample {
		ids = ids[:sample]
	}
	var mu sync.Mutex
	var completion, wft, activities []time.Duration
	sem := make(chan struct{}, 16)
	var wg sync.WaitGroup
	for _, id := range ids {
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			events, err := fetchHistory(ctx, c, id, "")
			if err != nil {
				return
			}
			timing := historyTimings(events)
			mu.Lock()
			defer mu.Unlock()
			report.HistoriesRead++
			if timing.completion > 0 {
				completion = append(completion, timing.completion)
			}
			wft = append(wft, timing.workflowTasks...)
			activities = append(activities, timing.activities...)
		}()
	}
	wg.Wait()
	report.HistoryCompletion = summarize(completion)
	report.WorkflowTaskScheduling = summarize(wft)
	report.ActivityScheduling = summarize(activities)
}

// timings 是从一个 Run 的 History 时间戳算出的延迟。
type timings struct {
	completion    time.Duration   // WorkflowStarted → 关闭事件
	workflowTasks []time.Duration // WorkflowTaskScheduled → WorkflowTaskStarted
	activities    []time.Duration // ActivityScheduled → 第一次 ActivityStarted
}

func historyTimings(events []history.Event) timings {
	var t timings
	byID := make(map[int64]history.Event, len(events))
	activityStarted := map[int64]bool{}
	for _, event := range events {
		byID[event.ID] = event
		switch {
		case event.WorkflowTaskStarted != nil:
			if scheduled, ok := byID[event.WorkflowTaskStarted.ScheduledEventID]; ok {
				t.workflowTasks = append(t.workflowTasks, event.Timestamp.Sub(scheduled.Timestamp))
			}
		case event.ActivityStarted != nil:
			id := event.ActivityStarted.ScheduledEventID
			if scheduled, ok := byID[id]; ok && !activityStarted[id] && history.NormalizeActivityAttempt(event.ActivityStarted.Attempt) == 1 {
				activityStarted[id] = true
				t.activities = append(t.activities, event.Timestamp.Sub(scheduled.Timestamp))
			}
		case isClosingEvent(event):
			if len(events) > 0 && events[0].WorkflowStarted != nil {
				t.completion = event.Timestamp.Sub(events[0].Timestamp)
			}
		}
	}
	return t
}

// ── 统计 ──

// latencyStats 是一组时长的分布，单位毫秒。
type latencyStats struct {
	Count int     `json:"count"`
	Mean  float64 `json:"mean"`
	P50   float64 `json:"p50"`
	P95   float64 `json:"p95"`
	P99   float64 `json:"p99"`
	Max   float64 `json:"max"`
}

// summarize 用 nearest-rank 法求分位数：p 分位是排序后第 ceil(p*n) 个值。
func summarize(values []time.Duration) latencyStats {
	if len(values) == 0 {
		return latencyStats{}
	}
	sorted := slices.Clone(values)
	slices.Sort(sorted)
	var sum time.Duration
	for _, v := range sorted {
		sum += v
	}
	ms := func(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }
	rank := func(p float64) float64 {
		i := int(math.Ceil(p*float64(len(sorted)))) - 1
		return ms(sorted[max(i, 0)])
	}
	return latencyStats{
		Count: len(sorted), Mean: ms(sum / time.Duration(len(sorted))),
		P50: rank(0.50), P95: rank(0.95), P99: rank(0.99), Max: ms(sorted[len(sorted)-1]),
	}
}

// ── MySQL 计数器 ──

// mysqlCounters 是 bench 前后读取的 SHOW GLOBAL STATUS 计数器。
var mysqlCounters = []string{"Questions", "Com_select", "Com_insert", "Com_update", "Com_delete", "Innodb_row_lock_waits", "Innodb_row_lock_time"}

type counterDelta struct {
	Delta       float64 `json:"delta"`
	PerSecond   float64 `json:"per_second"`
	PerWorkflow float64 `json:"per_workflow"`
}

type mysqlReport struct {
	Counters            map[string]counterDelta `json:"counters"`
	ThreadsConnectedMax int64                   `json:"threads_connected_max"`
}

// globalStatus 只读地取 mysqlCounters。
func globalStatus(ctx context.Context, admin *sql.DB) (map[string]float64, error) {
	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(mysqlCounters)), ",")
	args := make([]any, len(mysqlCounters))
	for i, name := range mysqlCounters {
		args[i] = name
	}
	rows, err := admin.QueryContext(ctx, "SHOW GLOBAL STATUS WHERE Variable_name IN ("+placeholders+")", args...)
	if err != nil {
		return nil, fmt.Errorf("SHOW GLOBAL STATUS: %w", err)
	}
	defer rows.Close()
	out := map[string]float64{}
	for rows.Next() {
		var name, value string
		if err := rows.Scan(&name, &value); err != nil {
			return nil, err
		}
		out[name], _ = strconv.ParseFloat(value, 64)
	}
	return out, rows.Err()
}

func mysqlDelta(before, after map[string]float64, elapsed time.Duration, completed int, threadsMax int64) mysqlReport {
	report := mysqlReport{Counters: map[string]counterDelta{}, ThreadsConnectedMax: threadsMax}
	for _, name := range mysqlCounters {
		delta := after[name] - before[name]
		item := counterDelta{Delta: delta}
		if elapsed > 0 {
			item.PerSecond = delta / elapsed.Seconds()
		}
		if completed > 0 {
			item.PerWorkflow = delta / float64(completed)
		}
		report.Counters[name] = item
	}
	return report
}

// ── 进程 CPU 与内存 ──

// processUsage 是一个进程在采样期间的 CPU 与 RSS。CPU 百分比按单核计（200 表示两个核满载）。
type processUsage struct {
	Name       string  `json:"name"`
	CPUAvgPct  float64 `json:"cpu_avg_pct"`
	CPUMaxPct  float64 `json:"cpu_max_pct"`
	RSSAvgMiB  float64 `json:"rss_avg_mib"`
	RSSMaxMiB  float64 `json:"rss_max_mib"`
	SampleSize int     `json:"samples"`
}

type resourceSampler struct {
	admin     *sql.DB
	processes []*process

	mu         sync.Mutex
	cpu        map[string][]float64
	rss        map[string][]float64
	threadsMax int64
	lastCPU    map[string]float64
	lastAt     map[string]time.Time
	lastPID    map[string]int
}

func newResourceSampler(admin *sql.DB, processes []*process) *resourceSampler {
	return &resourceSampler{admin: admin, processes: processes, cpu: map[string][]float64{}, rss: map[string][]float64{},
		lastCPU: map[string]float64{}, lastAt: map[string]time.Time{}, lastPID: map[string]int{}}
}

func (s *resourceSampler) run(ctx context.Context, interval time.Duration) {
	for {
		s.sample(ctx)
		if sleepCtx(ctx, interval) != nil {
			return
		}
	}
}

func (s *resourceSampler) sample(ctx context.Context) {
	var name string
	var threads int64
	if err := s.admin.QueryRowContext(ctx, "SHOW GLOBAL STATUS LIKE 'Threads_connected'").Scan(&name, &threads); err == nil {
		s.mu.Lock()
		s.threadsMax = max(s.threadsMax, threads)
		s.mu.Unlock()
	}
	for _, p := range s.processes {
		pid := p.pid()
		if pid == 0 || !p.running() {
			continue
		}
		out, err := exec.CommandContext(ctx, "ps", "-o", "time=,rss=", "-p", strconv.Itoa(pid)).Output()
		if err != nil {
			continue
		}
		cpuSeconds, rssKiB, err := parsePS(string(out))
		if err != nil {
			continue
		}
		now := time.Now()
		s.mu.Lock()
		if s.lastPID[p.name] == pid {
			if wall := now.Sub(s.lastAt[p.name]).Seconds(); wall > 0 {
				s.cpu[p.name] = append(s.cpu[p.name], 100*(cpuSeconds-s.lastCPU[p.name])/wall)
			}
		}
		s.lastPID[p.name], s.lastCPU[p.name], s.lastAt[p.name] = pid, cpuSeconds, now
		s.rss[p.name] = append(s.rss[p.name], float64(rssKiB)/1024)
		s.mu.Unlock()
	}
}

// parsePS 解析 `ps -o time=,rss=` 的一行：累计 CPU 时间（Linux "[DD-]HH:MM:SS"，macOS "M:SS.ss"）与 RSS（KiB）。
func parsePS(line string) (cpuSeconds float64, rssKiB int64, err error) {
	fields := strings.Fields(line)
	if len(fields) != 2 {
		return 0, 0, fmt.Errorf("unexpected ps output %q", line)
	}
	if cpuSeconds, err = parseCPUTime(fields[0]); err != nil {
		return 0, 0, err
	}
	rssKiB, err = strconv.ParseInt(fields[1], 10, 64)
	return cpuSeconds, rssKiB, err
}

func parseCPUTime(value string) (float64, error) {
	days := 0
	if d, rest, ok := strings.Cut(value, "-"); ok {
		n, err := strconv.Atoi(d)
		if err != nil {
			return 0, fmt.Errorf("cpu time %q: %w", value, err)
		}
		days, value = n, rest
	}
	parts := strings.Split(value, ":")
	if len(parts) < 2 || len(parts) > 3 {
		return 0, fmt.Errorf("cpu time %q: want [DD-][HH:]MM:SS", value)
	}
	seconds := 0.0
	for _, part := range parts {
		n, err := strconv.ParseFloat(part, 64)
		if err != nil {
			return 0, fmt.Errorf("cpu time %q: %w", value, err)
		}
		seconds = seconds*60 + n
	}
	return float64(days)*86400 + seconds, nil
}

func (s *resourceSampler) maxThreadsConnected() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.threadsMax
}

func (s *resourceSampler) usage() []processUsage {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []processUsage
	for _, p := range s.processes {
		usage := processUsage{Name: p.name, SampleSize: len(s.rss[p.name])}
		usage.CPUAvgPct, usage.CPUMaxPct = meanMax(s.cpu[p.name])
		usage.RSSAvgMiB, usage.RSSMaxMiB = meanMax(s.rss[p.name])
		out = append(out, usage)
	}
	return out
}

func meanMax(values []float64) (float64, float64) {
	if len(values) == 0 {
		return 0, 0
	}
	var sum, top float64
	for _, v := range values {
		sum += v
		top = max(top, v)
	}
	return sum / float64(len(values)), top
}

func printBenchReport(r benchReport, out string) {
	fmt.Println()
	fmt.Println("==== bench summary ====")
	if r.Rate > 0 {
		fmt.Printf("open loop %.1f starts/s for %.0fs", r.Rate, r.DurationSec)
	} else {
		fmt.Printf("closed loop %d in flight for %.0fs", r.Concurrency, r.DurationSec)
	}
	fmt.Printf("; workflow = %d activities (%dms each, %d payload bytes) + timer %dms; %d replicas, %d workers\n", r.Steps, r.ActivityMs, r.Payload, r.TimerMs, r.Replicas, r.Workers)
	fmt.Printf("started %d, completed %d, start errors %d, get errors %d, wrong results %d\n", r.Started, r.Completed, r.StartErrors, r.GetErrors, r.WrongResults)
	for message, n := range r.ErrorSamples {
		fmt.Printf("  %5d  %s\n", n, message)
	}
	fmt.Printf("throughput: %.1f completed/s in the window, %.1f/s overall\n", r.ThroughputPerSec, r.OverallPerSec)
	fmt.Printf("%-36s %8s %9s %9s %9s %9s %9s\n", "latency (ms)", "count", "mean", "p50", "p95", "p99", "max")
	for _, row := range []struct {
		name  string
		stats latencyStats
	}{
		{"start call", r.StartLatency},
		{"completion, client (Get polls ≤1s)", r.ClientCompletion},
		{"completion, history", r.HistoryCompletion},
		{"workflow task schedule→start", r.WorkflowTaskScheduling},
		{"activity schedule→start", r.ActivityScheduling},
	} {
		fmt.Printf("%-36s %8d %9.1f %9.1f %9.1f %9.1f %9.1f\n", row.name, row.stats.Count, row.stats.Mean, row.stats.P50, row.stats.P95, row.stats.P99, row.stats.Max)
	}
	fmt.Printf("MySQL (global, includes other databases): threads_connected max %d\n", r.MySQL.ThreadsConnectedMax)
	for _, name := range mysqlCounters {
		counter := r.MySQL.Counters[name]
		fmt.Printf("  %-24s %12.0f  %10.1f/s  %8.1f/workflow\n", name, counter.Delta, counter.PerSecond, counter.PerWorkflow)
	}
	fmt.Printf("%-12s %9s %9s %10s %10s\n", "process", "cpu avg%", "cpu max%", "rss avg", "rss max")
	for _, p := range r.Processes {
		fmt.Printf("%-12s %9.1f %9.1f %8.0fMi %8.0fMi\n", p.Name, p.CPUAvgPct, p.CPUMaxPct, p.RSSAvgMiB, p.RSSMaxMiB)
	}
	fmt.Printf("details: %s\n", filepath.Join(out, "bench.json"))
}
