package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"maps"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/donutnomad/tenuto/cmd/tenuto-cluster/workload"
	"github.com/donutnomad/tenuto/sdk/client"
)

// orderWorkload 是 failover、soak、upgrade 共用的 cluster-order 负载参数。
type orderWorkload struct {
	Sequential    int
	Parallel      int
	Children      int
	ChildSteps    int
	PayloadBytes  int
	Activity      time.Duration
	Timer         time.Duration
	SignalDelay   time.Duration
	SignalTimeout time.Duration
}

// maxSequential 是 -activities 的上限：每个顺序 Activity 约 7 个事件，500 个约 3500 个事件。
const maxSequential = 1000

func (w *orderWorkload) register(fs *flag.FlagSet) {
	fs.IntVar(&w.Sequential, "activities", 2, fmt.Sprintf("sequential activities per workflow (history length; at most %d)", maxSequential))
	fs.IntVar(&w.Parallel, "parallel", 3, "parallel activities per workflow")
	fs.IntVar(&w.Children, "children", 1, "child workflows per workflow, started in parallel (fan-out)")
	fs.IntVar(&w.ChildSteps, "child-steps", 3, "sequential activities in each child workflow")
	fs.IntVar(&w.PayloadBytes, "payload-bytes", 0, "padding carried in every activity input and output and in the workflow input")
	fs.DurationVar(&w.Activity, "activity", time.Second, "how long each activity runs")
	fs.DurationVar(&w.Timer, "timer", 5*time.Second, "timer in the workflow and in each child")
	fs.DurationVar(&w.SignalDelay, "signal-delay", 8*time.Second, "send the signal this long after the start")
	fs.DurationVar(&w.SignalTimeout, "signal-timeout", 2*time.Minute, "the workflow stops waiting for the signal after this long")
}

func (w orderWorkload) validate() error {
	if w.Sequential < 0 || w.Sequential > maxSequential || w.Parallel < 0 || w.Children < 0 || w.ChildSteps < 0 || w.PayloadBytes < 0 {
		return fmt.Errorf("-activities must be 0..%d; -parallel, -children, -child-steps and -payload-bytes not negative", maxSequential)
	}
	return nil
}

func (w orderWorkload) params(id string) workload.OrderParams {
	return workload.OrderParams{
		Key: id, Sequential: w.Sequential, Parallel: w.Parallel, Children: w.Children, ChildSteps: w.ChildSteps,
		TimerMs: w.Timer.Milliseconds(), SignalTimeoutMs: w.SignalTimeout.Milliseconds(), ActivityMs: w.Activity.Milliseconds(),
		PadBytes: w.PayloadBytes,
	}
}

// expectedDuration 是没有故障时一个 Run 从启动到结束的大致时长（不含排队与调度延迟）。
func (w orderWorkload) expectedDuration() time.Duration {
	beforeSignal := time.Duration(w.Sequential)*w.Activity + w.Timer
	if w.Parallel > 0 {
		beforeSignal += w.Activity
	}
	if w.SignalDelay > beforeSignal {
		beforeSignal = w.SignalDelay
	}
	children := time.Duration(0)
	if w.Children > 0 {
		children = time.Duration(w.ChildSteps)*w.Activity + w.Timer
	}
	return beforeSignal + children
}

// orderInput 是启动时的输入：按 PadBytes 填入共用的填充字符串。
func orderInput(params workload.OrderParams) workload.OrderParams {
	params.Pad = workload.Pad(params.PadBytes)
	return params
}

// orderRun 是 failover、upgrade 共用的运行参数：启动 -workflows 个 Run，期间执行场景，最后全量核对。
type orderRun struct {
	commonConfig
	orderWorkload
	Workflows int
	StartRate float64
	Seed      uint64
	Deadline  time.Duration
	Settle    time.Duration
}

func (r *orderRun) register(fs *flag.FlagSet) {
	r.commonConfig.register(fs, "", true)
	r.orderWorkload.register(fs)
	fs.IntVar(&r.Workflows, "workflows", 300, "cluster-order workflows to start")
	fs.Float64Var(&r.StartRate, "start-rate", 10, "workflow starts per second")
	fs.Uint64Var(&r.Seed, "seed", uint64(time.Now().UnixNano()), "random seed for fault targets")
	fs.DurationVar(&r.Deadline, "deadline", 10*time.Minute, "how long to wait for every workflow to close after the last start")
	fs.DurationVar(&r.Settle, "settle", 45*time.Second, "wait after every workflow closed before the database checks (reconciler grace period + one cycle)")
}

func (r *orderRun) validate() error {
	if r.DB == "" {
		return errors.New("-db is required (e.g. -db tenuto_e2e_1)")
	}
	if err := r.commonConfig.validate(); err != nil {
		return err
	}
	if r.Workflows < 1 || r.StartRate <= 0 {
		return errors.New("-workflows and -start-rate must be positive")
	}
	return r.orderWorkload.validate()
}

type failoverConfig struct {
	orderRun
	Faults string
}

func parseFailoverFlags(args []string) (failoverConfig, []faultStep, error) {
	var cfg failoverConfig
	fs := flag.NewFlagSet("failover", flag.ContinueOnError)
	cfg.register(fs)
	fs.StringVar(&cfg.Faults, "faults", defaultFaults, "fault schedule at:fault,... (see the docs for the fault syntax; empty = none)")
	if err := fs.Parse(args); err != nil {
		return cfg, nil, err
	}
	if err := cfg.validate(); err != nil {
		return cfg, nil, err
	}
	steps, err := parseFaultSchedule(cfg.Faults, cfg.Replicas)
	if err != nil {
		return cfg, nil, err
	}
	for _, step := range steps {
		if step.needsProxies() && !cfg.Proxies {
			return cfg, nil, fmt.Errorf("fault %s needs -proxies", step)
		}
	}
	return cfg, steps, nil
}

// failoverSummary 是 failover 与 upgrade 的 summary.json。
type failoverSummary struct {
	Mode             string          `json:"mode"`
	Pass             bool            `json:"pass"`
	DB               string          `json:"db"`
	Replicas         int             `json:"replicas"`
	Workers          int             `json:"workers"`
	Seed             uint64          `json:"seed"`
	StartedAt        time.Time       `json:"started_at"`
	FinishedAt       time.Time       `json:"finished_at"`
	Workflows        int             `json:"workflows"`
	Completed        int             `json:"completed"`
	StartRetries     int64           `json:"start_retries"`
	SignalRetries    int64           `json:"signal_retries"`
	SignalOutcome    map[string]int  `json:"signal_outcome"`
	DuplicateSignals int             `json:"runs_with_duplicate_signals"`
	Timeline         []faultEvent    `json:"timeline"`
	FailedRuns       []runCheck      `json:"failed_runs,omitempty"`
	Executions       executionReport `json:"executions"`
	Database         dbReport        `json:"database"`
	Notes            []string        `json:"notes,omitempty"`
	Violations       []string        `json:"violations,omitempty"`
	Warnings         []string        `json:"warnings,omitempty"`
}

// maxFailedRuns 是 summary.json 里保留完整核对结果的失败 Run 数；其余只计入 violations。
const maxFailedRuns = 200

func runFailover(ctx context.Context, args []string) error {
	cfg, steps, err := parseFailoverFlags(args)
	if err != nil {
		return err
	}
	serverBin, err := buildServer(ctx, cfg.commonConfig)
	if err != nil {
		return err
	}
	return runOrders(ctx, "failover", cfg.orderRun, serverBin, func(ctx context.Context, cl *cluster, start time.Time) ([]string, []string) {
		newInjector(cl, cfg.Seed).run(ctx, start, steps)
		return nil, nil
	})
}

// scenario 与启动 Workflow 同时运行（故障计划、滚动升级），返回写入 summary 的说明与违规。
type scenario func(ctx context.Context, cl *cluster, start time.Time) (notes, violations []string)

// runOrders 建库、用 serverBin 启动集群，按速率启动 -workflows 个 cluster-order Run 并同时运行 during；
// 全部 Run 关闭后逐个核对（History 读完即丢弃）、对照执行记录并只读核对库。
func runOrders(ctx context.Context, mode string, cfg orderRun, serverBin string, during scenario) error {
	progs, err := loadPrograms()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(cfg.Out, 0o755); err != nil {
		return err
	}
	summary := failoverSummary{Mode: mode, DB: cfg.DB, Replicas: cfg.Replicas, Workers: cfg.Workers, Seed: cfg.Seed, StartedAt: time.Now(), Workflows: cfg.Workflows}
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
	c, err := dialCluster(cl, "tenuto-cluster")
	if err != nil {
		return err
	}
	defer c.Close()
	fmt.Printf("cluster ready: %d replicas, %d workers, load balancer %s, output %s\n", cfg.Replicas, cfg.Workers, cl.lb.url(), cfg.Out)

	// 场景与启动同时开始。
	start := time.Now()
	var notes, scenarioViolations []string
	scenarioDone := make(chan struct{})
	go func() { notes, scenarioViolations = during(ctx, cl, start); close(scenarioDone) }()
	// 返回前等场景结束（含进行中的重启），之后的 shutdown 才能停掉全部进程。
	defer func() { <-scenarioDone }()

	var startRetries, signalRetries atomic.Int64
	var signals sync.WaitGroup
	interval := time.Duration(float64(time.Second) / cfg.StartRate)
	for i := range cfg.Workflows {
		if err := sleepCtx(ctx, time.Until(start.Add(time.Duration(i)*interval))); err != nil {
			return err
		}
		id := orderWorkflowID(i)
		if err := startOrder(ctx, c, id, cfg.params(id), cfg.Deadline, &startRetries); err != nil {
			return fmt.Errorf("start %s: %w", id, err)
		}
		signalLater(ctx, c, id, cfg.orderWorkload, &signals, &signalRetries)
		if (i+1)%50 == 0 || i+1 == cfg.Workflows {
			fmt.Printf("%s started %d/%d workflows\n", time.Now().Format("15:04:05.000"), i+1, cfg.Workflows)
		}
	}
	signals.Wait()
	<-scenarioDone
	summary.Notes = notes
	summary.Violations = append(summary.Violations, scenarioViolations...)
	if err := waitAllClosed(ctx, c, cfg.Deadline); err != nil {
		if ctx.Err() != nil {
			return err
		}
		fmt.Println(err)
	}
	fmt.Printf("waiting %s before the database checks\n", cfg.Settle)
	if err := sleepCtx(ctx, cfg.Settle); err != nil {
		return err
	}

	// 核对。
	summary.StartRetries, summary.SignalRetries = startRetries.Load(), signalRetries.Load()
	summary.Timeline = cl.faultLog()
	completed := map[activityKey]int{}
	summary.SignalOutcome = map[string]int{}
	verifyOrders(ctx, c, progs, cfg.Workflows, func(i int) (string, workload.OrderParams) {
		id := orderWorkflowID(i)
		return id, cfg.params(id)
	}, func(check runCheck) {
		if check.Status == string(client.WorkflowExecutionStatusCompleted) {
			summary.Completed++
		}
		if check.Signal != "" {
			summary.SignalOutcome[check.Signal]++
		}
		if check.Signals > 1 {
			summary.DuplicateSignals++
		}
		if len(check.Problems) > 0 {
			if len(summary.FailedRuns) < maxFailedRuns {
				summary.FailedRuns = append(summary.FailedRuns, check)
			}
			summary.Violations = append(summary.Violations, fmt.Sprintf("%s: %s", check.WorkflowID, strings.Join(check.Problems, "; ")))
		}
		maps.Copy(completed, check.completed)
	})
	records, malformed, err := readExecutionRecords(cfg.Out)
	if err != nil {
		return err
	}
	if malformed > 0 {
		summary.Warnings = append(summary.Warnings, fmt.Sprintf("%d execution record lines are incomplete (a worker was killed while writing)", malformed))
	}
	summary.Executions = analyzeExecutions(completed, groupExecutions(records), summary.Timeline)
	records = nil
	summary.Violations = append(summary.Violations, summary.Executions.Violations...)
	if summary.DuplicateSignals > 0 {
		summary.Warnings = append(summary.Warnings, fmt.Sprintf("%d runs received the signal more than once (the client retried a signal whose response was lost)", summary.DuplicateSignals))
	}
	if n := summary.SignalOutcome["timeout"]; n > 0 {
		summary.Warnings = append(summary.Warnings, fmt.Sprintf("%d runs took the signal-timeout branch", n))
	}
	if summary.Database, err = checkDatabase(ctx, db.db, cfg.Namespace); err != nil {
		summary.Violations = append(summary.Violations, fmt.Sprintf("database checks failed: %v", err))
	}
	summary.Violations = append(summary.Violations, summary.Database.Violations...)
	summary.Warnings = append(summary.Warnings, summary.Database.Warnings...)
	summary.FinishedAt = time.Now()
	summary.Pass = len(summary.Violations) == 0
	if err := writeJSON(filepath.Join(cfg.Out, "summary.json"), summary); err != nil {
		return err
	}
	printFailoverSummary(summary, cfg.Out)
	if !summary.Pass {
		return errViolations
	}
	return nil
}

// finishDatabase 在运行结束时删除库，-keep 时只关闭连接。
func finishDatabase(db *database, keep bool) {
	if keep {
		fmt.Printf("keeping database %s\n", db.name)
		db.close()
		return
	}
	db.drop()
}

// dialCluster 连到集群的负载均衡器。
func dialCluster(cl *cluster, identity string) (client.Client, error) {
	return client.Dial(client.Options{HostPort: cl.lb.url(), Namespace: cl.cfg.Namespace, Identity: identity, AuthToken: cl.authToken,
		HTTPClient: &http.Client{Timeout: 30 * time.Second, Transport: &http.Transport{MaxIdleConnsPerHost: 64}}})
}

// startOrder 启动一个 cluster-order Run，失败时在 within 内重试；上一次请求已启动成功（响应丢失）也算成功。
func startOrder(ctx context.Context, c client.Client, id string, params workload.OrderParams, within time.Duration, retries *atomic.Int64) error {
	input := orderInput(params)
	return retry(ctx, within, retries, func() error {
		_, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: id}, workload.OrderWorkflowType, input)
		if errors.Is(err, client.ErrWorkflowExecutionAlreadyStarted) {
			return nil
		}
		return err
	})
}

// signalLater 在 SignalDelay 之后给 id 发 Signal，失败时在 SignalTimeout 内重试。
func signalLater(ctx context.Context, c client.Client, id string, w orderWorkload, wg *sync.WaitGroup, retries *atomic.Int64) {
	wg.Add(1)
	time.AfterFunc(w.SignalDelay, func() {
		defer wg.Done()
		if err := retry(ctx, w.SignalTimeout, retries, func() error {
			return c.SignalWorkflow(ctx, id, "", workload.SignalName, map[string]any{"from": "tenuto-cluster"})
		}); err != nil && ctx.Err() == nil {
			fmt.Printf("signal %s gave up: %v\n", id, err)
		}
	})
}

func orderWorkflowID(i int) string { return fmt.Sprintf("order-%05d", i) }

// retry 重试 fn，直到成功、超过 within 或 ctx 结束。副本被 kill 或暂停时调用会失败，换一个连接重试即可。
func retry(ctx context.Context, within time.Duration, counter *atomic.Int64, fn func() error) error {
	deadline := time.Now().Add(within)
	for {
		err := fn()
		if err == nil {
			return nil
		}
		if time.Now().After(deadline) {
			return err
		}
		counter.Add(1)
		if sleepErr := sleepCtx(ctx, 500*time.Millisecond); sleepErr != nil {
			return sleepErr
		}
	}
}

// waitAllClosed 等 namespace 里没有 running 的 Run（含子 Run），最多 deadline。
func waitAllClosed(ctx context.Context, c client.Client, deadline time.Duration) error {
	until := time.Now().Add(deadline)
	last := -1
	for {
		result, err := c.ListWorkflow(ctx, client.ListWorkflowOptions{Statuses: []client.WorkflowExecutionStatus{client.WorkflowExecutionStatusRunning}, Limit: 1})
		if err == nil {
			if result.Total == 0 {
				fmt.Printf("%s every run is closed\n", time.Now().Format("15:04:05.000"))
				return nil
			}
			if result.Total != last {
				fmt.Printf("%s %d runs still running\n", time.Now().Format("15:04:05.000"), result.Total)
				last = result.Total
			}
		}
		if time.Now().After(until) {
			return fmt.Errorf("runs still running after %s; they are reported below", deadline)
		}
		if err := sleepCtx(ctx, 2*time.Second); err != nil {
			return err
		}
	}
}

// verifyOrders 并发核对 n 个 cluster-order Run，每个结果交给 visit（串行调用），不保留全部结果。
func verifyOrders(ctx context.Context, c client.Client, progs programs, n int, run func(i int) (string, workload.OrderParams), visit func(runCheck)) {
	var mu sync.Mutex
	sem := make(chan struct{}, 16)
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			id, params := run(i)
			check := verifyOrder(ctx, c, progs, id, params)
			mu.Lock()
			defer mu.Unlock()
			visit(check)
		}()
	}
	wg.Wait()
}

// readExecutionRecords 读取 out 下全部 activities-*.jsonl。
func readExecutionRecords(out string) ([]workload.Execution, int, error) {
	paths, err := filepath.Glob(filepath.Join(out, "activities-*.jsonl"))
	if err != nil {
		return nil, 0, err
	}
	var all []workload.Execution
	malformed := 0
	for _, path := range paths {
		file, err := os.Open(path)
		if err != nil {
			return nil, 0, err
		}
		records, bad, err := parseExecutions(file)
		_ = file.Close()
		if err != nil {
			return nil, 0, fmt.Errorf("read %s: %w", path, err)
		}
		all = append(all, records...)
		malformed += bad
	}
	return all, malformed, nil
}

func writeJSON(path string, v any) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(data, '\n'), 0o644)
}

func printFailoverSummary(s failoverSummary, out string) {
	fmt.Println()
	fmt.Printf("==== %s summary ====\n", s.Mode)
	fmt.Printf("workflows completed: %d/%d   start retries: %d   signal retries: %d\n", s.Completed, s.Workflows, s.StartRetries, s.SignalRetries)
	fmt.Printf("signal outcome: %v\n", s.SignalOutcome)
	fmt.Println("timeline:")
	for _, event := range s.Timeline {
		fmt.Printf("  %s  %-40s %s\n", event.At.Format("15:04:05.000"), event.Action, event.Target)
	}
	e := s.Executions
	fmt.Printf("activities: %d   executions: %d   unmatched records: %d\n", e.Activities, e.Executions, e.Unmatched)
	fmt.Printf("executions per activity: %s\n", formatCounts(e.Distribution))
	if len(e.CausesSummary) > 0 {
		fmt.Println("re-execution causes (inferred):")
		for _, cause := range sortedKeys(e.CausesSummary) {
			fmt.Printf("  %5d  %s\n", e.CausesSummary[cause], cause)
		}
	}
	d := s.Database
	fmt.Printf("closed runs' activity states: %s   timer states: %s   child states: %s\n", formatCounts(d.ActivityStates), formatCounts(d.TimerStates), formatCounts(d.ChildStates))
	fmt.Printf("closed runs' dispatch rows: %s   TaskQ tasks: %s\n", formatCounts(d.Dispatches), formatCounts(d.TaskQTasks))
	for _, note := range s.Notes {
		fmt.Println("NOTE:", note)
	}
	for _, warning := range s.Warnings {
		fmt.Println("WARNING:", warning)
	}
	for _, violation := range limit(s.Violations, 50) {
		fmt.Println("VIOLATION:", violation)
	}
	if s.Pass {
		fmt.Printf("PASS  (details: %s)\n", filepath.Join(out, "summary.json"))
	} else {
		fmt.Printf("FAIL: %d violations  (details: %s)\n", len(s.Violations), filepath.Join(out, "summary.json"))
	}
}

// formatCounts 把计数表按键排序写成 "a=1 b=2"。
func formatCounts[V int | int64](m map[string]V) string {
	if len(m) == 0 {
		return "-"
	}
	parts := make([]string, 0, len(m))
	for _, key := range sortedKeys(m) {
		parts = append(parts, fmt.Sprintf("%s=%d", key, m[key]))
	}
	return strings.Join(parts, " ")
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
