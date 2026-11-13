package main

import (
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/cmd/tenuto-cluster/workload"
)

// 故障结束后，过期 Run 清空的时刻就是恢复时刻；超过时限仍未恢复的故障报告出来。
func TestRecoveryBook(t *testing.T) {
	base := time.Date(2026, 11, 12, 10, 0, 0, 0, time.UTC)
	at := func(s int) time.Time { return base.Add(time.Duration(s) * time.Second) }
	var book recoveryBook
	book.ended(faultEnd{Action: actionKillLeader, Target: "replica-0", Start: at(0), End: at(10)})
	book.ended(faultEnd{Action: actionDBBlackhole, Target: "replica-1", Start: at(20), End: at(40)})
	if stuck := book.observe(at(12), 5, time.Minute); len(stuck) != 0 {
		t.Fatalf("stuck = %+v", stuck)
	}
	// 积压在 30s 清空：第一次故障已结束，恢复时间 20s；第二次还没结束，不算恢复。
	book.observe(at(30), 0, time.Minute)
	done, pending := book.snapshot()
	if len(done) != 1 || done[0].Seconds != 20 || len(pending) != 1 {
		t.Fatalf("done = %+v, pending = %+v", done, pending)
	}
	if stuck := book.observe(at(101), 3, time.Minute); len(stuck) != 1 || stuck[0].Action != actionDBBlackhole {
		t.Fatalf("stuck = %+v", stuck)
	}
	book.observe(at(105), 0, time.Minute)
	done, pending = book.snapshot()
	if len(done) != 2 || done[1].Seconds != 65 || len(pending) != 0 {
		t.Fatalf("done = %+v, pending = %+v", done, pending)
	}
	byAction, overall := maxRecovery(done)
	if overall != 65 || byAction[actionKillLeader] != 20 || byAction[actionDBBlackhole] != 65 {
		t.Fatalf("max recovery = %v, %v", byAction, overall)
	}
}

func TestSlopePerHour(t *testing.T) {
	base := time.Now()
	var at []time.Time
	var values []float64
	for i := range 7 {
		at = append(at, base.Add(time.Duration(i)*10*time.Minute))
		values = append(values, 100+float64(i)*5) // 每 10 分钟 +5 → 30/小时
	}
	slope, ok := slopePerHour(at, values)
	if !ok || math.Abs(slope-30) > 1e-9 {
		t.Fatalf("slope = %v, %v", slope, ok)
	}
	if _, ok := slopePerHour(at[:2], values[:2]); ok {
		t.Fatal("two points gave a slope")
	}
}

// 泄漏指标取同一 PID 最长的一段，去掉预热期（重启后的进程从它第一次出现起算）；增长超过起点 25% 时给出警告。
func TestComputeLeaks(t *testing.T) {
	base := time.Date(2026, 11, 12, 10, 0, 0, 0, time.UTC)
	var samples []soakSample
	for i := range 13 {
		at := base.Add(time.Duration(i) * 10 * time.Minute)
		pid := 100
		if i >= 3 {
			pid = 200 // 第 3 个样本起是重启后的进程
		}
		samples = append(samples, soakSample{
			At: at,
			Processes: []processSample{
				{Name: "replica-0", PID: pid, Running: true, RSSMiB: 100 + float64(i)*10},
				{Name: "worker-0", PID: 300, Running: true, RSSMiB: 50},
			},
			Rows: map[string]int64{"tenuto_runs": int64(1000 + i*100)},
			Tool: toolUsage{HeapMiB: 20},
		})
	}
	report := computeLeaks(samples, 10*time.Minute)
	if slope := report.RSSSlopeMiBPerHour["replica-0"]; math.Abs(slope-60) > 1e-9 {
		t.Fatalf("replica-0 slope = %v", slope)
	}
	if !strings.HasPrefix(report.RSSWindow["replica-0"], "pid 200, 9 samples") {
		t.Fatalf("window = %q", report.RSSWindow["replica-0"])
	}
	if slope := report.RSSSlopeMiBPerHour["worker-0"]; slope != 0 {
		t.Fatalf("worker-0 slope = %v", slope)
	}
	if slope := report.RowsSlopePerHour["tenuto_runs"]; math.Abs(slope-600) > 1e-6 {
		t.Fatalf("rows slope = %v", slope)
	}
	if len(report.Warnings) != 1 || !strings.Contains(report.Warnings[0], "replica-0") {
		t.Fatalf("warnings = %v", report.Warnings)
	}
}

// soak2 里重启后的副本 RSS 在前 10 分钟从 66 MiB 涨到约 140 MiB 后持平，所有副本每次启动都如此；
// 预热段不计入斜率，持平的进程不报警。
func TestComputeLeaksSkipsWarmupAfterRestart(t *testing.T) {
	base := time.Date(2026, 11, 12, 10, 0, 0, 0, time.UTC)
	var samples []soakSample
	for i := range 60 {
		rss := 140.0
		pid := 100
		if i >= 20 {
			pid = 200 // 第 20 分钟重启
			rss = min(66+float64(i-20)*8, 140)
		}
		samples = append(samples, soakSample{
			At:        base.Add(time.Duration(i) * time.Minute),
			Processes: []processSample{{Name: "replica-1", PID: pid, Running: true, RSSMiB: rss}},
		})
	}
	report := computeLeaks(samples, 10*time.Minute)
	if !strings.HasPrefix(report.RSSWindow["replica-1"], "pid 200, 30 samples") {
		t.Fatalf("window = %q", report.RSSWindow["replica-1"])
	}
	if len(report.Warnings) != 0 {
		t.Fatalf("warnings = %v", report.Warnings)
	}
}

// 执行记录增量读取：只读完整的行，按 Run 取走，长时间没人取的记为 unmatched。
func TestRecordTailer(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "activities-worker-0.jsonl")
	line := func(run, activity, phase string) string {
		data, _ := json.Marshal(workload.Execution{Phase: phase, Worker: "worker-0", RunID: run, ActivityID: activity, Attempt: 1, Time: time.Now()})
		return string(data) + "\n"
	}
	write := func(text string) {
		file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			t.Fatal(err)
		}
		defer file.Close()
		if _, err := file.WriteString(text); err != nil {
			t.Fatal(err)
		}
	}
	tailer := newRecordTailer(dir)
	partial := line("run-b", "#a1", "end")
	write(line("run-a", "#a1", "start") + line("run-a", "#a1", "end") + line("run-b", "#a1", "start") + partial[:10])
	records, err := tailer.take([]string{"run-a"})
	if err != nil || len(records) != 2 {
		t.Fatalf("run-a records = %v, %v", records, err)
	}
	write(partial[10:] + line("run-c", "#a1", "start"))
	records, err = tailer.take([]string{"run-b"})
	if err != nil || len(records) != 2 || records[1].Phase != "end" {
		t.Fatalf("run-b records = %+v, %v", records, err)
	}
	tailer.prune(-time.Second) // 立即过期
	pending, malformed, unmatched := tailer.stats()
	if pending != 0 || malformed != 0 || unmatched != 1 {
		t.Fatalf("pending %d malformed %d unmatched %d", pending, malformed, unmatched)
	}
	if records, _ := tailer.take([]string{"run-a"}); len(records) != 0 {
		t.Fatalf("records taken twice: %v", records)
	}
}

func TestLogCounter(t *testing.T) {
	path := filepath.Join(t.TempDir(), "replica-0.log")
	counter := logCounter{needle: []byte(redeliveryLine), offsets: map[string]int64{}}
	if err := os.WriteFile(path, []byte("x "+redeliveryLine+" run_id=a\nother\n"+redeliveryLine[:8]), 0o644); err != nil {
		t.Fatal(err)
	}
	if n := counter.scan([]string{path}); n != 1 {
		t.Fatalf("count = %d", n)
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = file.WriteString(redeliveryLine[8:] + "\n" + redeliveryLine + "\n")
	_ = file.Close()
	if n := counter.scan([]string{path, filepath.Join(t.TempDir(), "missing.log")}); n != 3 {
		t.Fatalf("count = %d", n)
	}
}

func TestExecutionTotals(t *testing.T) {
	totals := executionTotals{Distribution: map[string]int{}, Causes: map[string]int{}}
	completed := map[activityKey]int{{RunID: "r", ActivityID: "#a1"}: 1, {RunID: "r", ActivityID: "#a2"}: 3}
	totals.add(executionReport{Activities: 2, Executions: 3, Distribution: map[string]int{"1": 1, "2": 1}, CausesSummary: map[string]int{"x": 1}}, completed)
	totals.add(executionReport{Activities: 1, Executions: 1, Distribution: map[string]int{"1": 1}, Unmatched: 2}, map[activityKey]int{{RunID: "s", ActivityID: "#a1"}: 1})
	if totals.Activities != 3 || totals.Executions != 4 || totals.Distribution["1"] != 2 || totals.Causes["x"] != 1 || totals.ActivityRetries != 2 || totals.Unmatched != 2 {
		t.Fatalf("totals = %+v", totals)
	}
}

func TestParseSoakFlags(t *testing.T) {
	cfg, steps, err := parseSoakFlags([]string{"-dsn", "u:p@tcp(127.0.0.1:3306)/", "-db", "tenuto_e2e_soak", "-duration", "2h", "-server-env", "WORKFLOW_RETENTION_JITTER=5m"})
	if err != nil {
		t.Fatal(err)
	}
	if len(steps) != 11 || cfg.Duration != 2*time.Hour || !cfg.Proxies {
		t.Fatalf("cfg = %+v, %d steps", cfg, len(steps))
	}
	// 工具的保留期缺省值在前，-server-env 覆盖它们（同名变量以后者为准）。
	if i, j := slices.Index(cfg.ServerEnv, "WORKFLOW_RETENTION_JITTER=1m"), slices.Index(cfg.ServerEnv, "WORKFLOW_RETENTION_JITTER=5m"); i < 0 || j < i || !slices.Contains(cfg.ServerEnv, "WORKFLOW_RETENTION=1h0m0s") {
		t.Fatalf("server env = %v", cfg.ServerEnv)
	}
	if _, _, err := parseSoakFlags([]string{"-dsn", "u:p@tcp(127.0.0.1:3306)/", "-db", "tenuto_e2e_soak", "-proxies=false"}); err == nil {
		t.Fatal("db faults without proxies were accepted")
	}
	if cfg, steps, err := parseSoakFlags([]string{"-dsn", "u:p@tcp(127.0.0.1:3306)/", "-db", "tenuto_e2e_soak", "-fault-every", "0", "-retention", "0"}); err != nil || len(steps) != 0 || len(cfg.ServerEnv) != 0 {
		t.Fatalf("no faults, no retention = %v, %v, %v", cfg.ServerEnv, steps, err)
	}
}

func TestOrderWorkloadExpectedDuration(t *testing.T) {
	w := orderWorkload{Sequential: 2, Parallel: 3, Children: 2, ChildSteps: 3, Activity: time.Second, Timer: 5 * time.Second, SignalDelay: 8 * time.Second}
	// 信号前：2+1 个 Activity + 5s Timer = 8s，与信号延迟 8s 相同；子 Workflow 并行：3s + 5s。
	if got := w.expectedDuration(); got != 16*time.Second {
		t.Fatalf("expected duration = %s", got)
	}
	w.Children = 0
	if got := w.expectedDuration(); got != 8*time.Second {
		t.Fatalf("expected duration without children = %s", got)
	}
}
