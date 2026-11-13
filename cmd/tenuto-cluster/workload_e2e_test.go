package main

import (
	"context"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/cmd/tenuto-cluster/workload"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/sdk/client"
	"github.com/donutnomad/tenuto/sdk/worker"
	"github.com/donutnomad/tenuto/server/app/assembly"
	"github.com/donutnomad/tenuto/server/http/runtimehttp"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

// 负载经 sdk/client 与 sdk/worker 在内存 Server 上跑完：结果等于输入算出的期望值，History 满足不变式，
// 从头重放没有非确定性错误，每个完成的 Activity 都有执行记录；Signal 不到达时走超时分支。
func TestWorkloadOnMemoryServer(t *testing.T) {
	server := assembly.NewServer(assembly.Options{
		Stores:        assembly.Stores{Executions: memory.NewExecutionManager()},
		WorkflowQueue: memory.NewTaskQueue(64), ActivityRouter: memory.NewActivityQueueRouter(64),
	})
	httpServer := httptest.NewServer(runtimehttp.NewRuntimeHTTPHandler(server))
	defer func() {
		httpServer.CloseClientConnections()
		httpServer.Close()
		server.Stop()
	}()

	out := t.TempDir()
	recorder, err := workload.OpenRecorder(filepath.Join(out, "activities-w.jsonl"), "w")
	if err != nil {
		t.Fatal(err)
	}
	workload.SetRecorder(recorder)
	defer func() {
		workload.SetRecorder(nil)
		_ = recorder.Close()
	}()

	c, err := client.Dial(client.Options{HostPort: httpServer.URL, Namespace: "cluster"})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	// 内存 Server 没有内嵌 Workflow Worker：这里的 Worker 同时执行 Workflow Task 与 Activity。
	w, err := newWorkloadWorker(c, "w", worker.RoleBoth, 8)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	w.Start(ctx)
	defer w.Stop()
	if err := w.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	progs, err := loadPrograms()
	if err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		id         string
		signal     bool
		wantSignal string
		params     workload.OrderParams
	}{
		{id: "order-signal", signal: true, wantSignal: "received",
			params: workload.OrderParams{Key: "order-signal", Sequential: 2, Parallel: 3, Children: 1, ChildSteps: 2, TimerMs: 10, SignalTimeoutMs: 30_000, ActivityMs: 1}},
		{id: "order-timeout", wantSignal: "timeout",
			params: workload.OrderParams{Key: "order-timeout", Sequential: 2, Parallel: 2, Children: 1, ChildSteps: 1, TimerMs: 1, SignalTimeoutMs: 50}},
		// 规模参数：较长的顺序链、子 Workflow 扇出与填充载荷。
		{id: "order-scale", signal: true, wantSignal: "received",
			params: workload.OrderParams{Key: "order-scale", Sequential: 12, Parallel: 2, Children: 3, ChildSteps: 2, TimerMs: 1, SignalTimeoutMs: 30_000, PadBytes: 2048}},
	}
	completed := map[activityKey]int{}
	for _, tc := range cases {
		run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: tc.id}, workload.OrderWorkflowType, orderInput(tc.params))
		if err != nil {
			t.Fatal(err)
		}
		if tc.signal {
			if err := c.SignalWorkflow(ctx, tc.id, "", workload.SignalName, map[string]any{"from": "test"}); err != nil {
				t.Fatal(err)
			}
		}
		var out orderOutput
		if err := run.Get(ctx, &out); err != nil {
			t.Fatalf("%s: %v", tc.id, err)
		}
		if out.Signal != tc.wantSignal {
			t.Fatalf("%s: signal = %q, want %q", tc.id, out.Signal, tc.wantSignal)
		}
		check := verifyOrder(ctx, c, progs, tc.id, tc.params)
		if len(check.Problems) > 0 {
			t.Fatalf("%s: %v", tc.id, check.Problems)
		}
		if want := tc.params.Sequential + tc.params.Parallel + tc.params.Children*tc.params.ChildSteps; len(check.completed) != want {
			t.Fatalf("%s: %d completed activities, want %d", tc.id, len(check.completed), want)
		}
		for key, attempt := range check.completed {
			completed[key] = attempt
		}
	}

	// bench 的轻量 Workflow 直接启动。
	steps := workload.StepsParams{Key: "steps-1", Steps: 3, TimerMs: 5}
	run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: "steps-1"}, workload.StepsWorkflowType, steps)
	if err != nil {
		t.Fatal(err)
	}
	var stepsOut stepsOutput
	if err := run.Get(ctx, &stepsOut); err != nil {
		t.Fatal(err)
	}
	if check := verifySteps(ctx, c, progs, "steps-1", steps); len(check.Problems) > 0 {
		t.Fatalf("steps-1: %v", check.Problems)
	}
	events, err := fetchHistory(ctx, c, "steps-1", "")
	if err != nil {
		t.Fatal(err)
	}
	// 重放能发现命令序列的变化：把一个 ActivityScheduled 的 ActivityID 改掉后重放报非确定性错误。
	mutated := append([]history.Event(nil), events...)
	for i := range mutated {
		if mutated[i].ActivityScheduled != nil {
			scheduled := *mutated[i].ActivityScheduled
			scheduled.ActivityID = "changed"
			mutated[i].ActivityScheduled = &scheduled
			break
		}
	}
	if err := replayHistory(progs.steps, mutated); err == nil || !strings.Contains(err.Error(), "nondeterministic") {
		t.Fatalf("replay of a changed history = %v, want a nondeterministic error", err)
	}
	if timing := historyTimings(events); timing.completion <= 0 || len(timing.workflowTasks) == 0 || len(timing.activities) != 3 {
		t.Fatalf("history timings = %+v, want a completion, workflow tasks and 3 activity starts", timing)
	}

	file, err := os.Open(filepath.Join(out, "activities-w.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	records, malformed, err := parseExecutions(file)
	if err != nil || malformed != 0 {
		t.Fatalf("parse records: %v, %d malformed", err, malformed)
	}
	report := analyzeExecutions(completed, groupExecutions(records), nil)
	if len(report.Violations) > 0 || report.Executions != len(completed) || report.Distribution["1"] != len(completed) {
		t.Fatalf("execution report = %+v, want exactly one execution per completed activity", report)
	}
}
