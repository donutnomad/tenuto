package persistence

import (
	"fmt"
	"testing"
	"time"

	historypkg "github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
)

// startCaseRun 建一个已启动的 Run（Run 行、WorkflowStarted 与 current 指针）。
func startCaseRun(t *testing.T, store *Store, runID, workflowID string) *execution.Run {
	t.Helper()
	run := execution.NewRun(execution.NewRunOptions{ID: runID, WorkflowID: workflowID, WorkflowType: "wf", WorkflowConfigVersion: "v1", CreatedAt: time.Now()})
	started := historypkg.NewWorkflowStartedEvent(nil, runID, "", workflowID, "Workflow", 0)
	if err := store.Executions.RunInTx(func(tx execution.ExecutionTx) error {
		return execution.CommitStartedRun(tx, run, "", runID, runID, started)
	}); err != nil {
		t.Fatalf("start workflow id %q: %v", workflowID, err)
	}
	return run
}

// 只差大小写或尾随空格的 Workflow ID 各有自己的 current Run（tenuto_runs 的排序规则是 utf8mb4_0900_bin）。
func TestWorkflowIDIsCaseSensitiveOnMySQL(t *testing.T) {
	store := requireMySQL(t)
	upper := startCaseRun(t, store, "run-case-upper", "Order-aB")
	lower := startCaseRun(t, store, "run-case-lower", "order-Ab")
	// utf8mb4_0900_bin 是 NO PAD：尾随空格参与比较，与内存后端一致。
	padded := startCaseRun(t, store, "run-case-padded", "order-Ab ")
	for _, run := range []*execution.Run{upper, lower, padded} {
		current, err := store.Executions.GetCurrentRunID("default", run.WorkflowID)
		if err != nil || current != run.ID {
			t.Errorf("GetCurrentRunID(%q) = %q, %v, want %q", run.WorkflowID, current, err, run.ID)
		}
	}
}

// 只差大小写或尾随空格的 Signal requestID 是不同的请求：tenuto_events.dedupe_key 区分大小写，两条 Signal 都记下。
func TestSignalRequestIDDedupeIsCaseSensitiveOnMySQL(t *testing.T) {
	store := requireMySQL(t)
	const runID = "run-case-signal"
	startCaseRun(t, store, runID, "wf-case-signal")
	ids := map[string]int64{}
	for _, requestID := range []string{"req-Ab", "req-aB", "req-aB "} {
		if err := store.Executions.RunInTx(func(tx execution.ExecutionTx) error {
			run, err := tx.GetRunForUpdate(runID)
			if err != nil {
				return err
			}
			id, err := execution.RecordSignal(tx, run, runID, runID, "pay", requestID, map[string]any{"request": requestID})
			ids[requestID] = id
			return err
		}); err != nil {
			t.Fatal(err)
		}
	}
	events, err := store.Executions.GetEvents(runID)
	if err != nil {
		t.Fatal(err)
	}
	signals := 0
	for _, event := range events {
		if event.WorkflowSignaled != nil {
			signals++
		}
	}
	if signals != 3 || ids["req-Ab"] == ids["req-aB"] || ids["req-aB"] == ids["req-aB "] {
		t.Fatalf("signals with request ids differing only in case = %d events (ids %v), want 3", signals, ids)
	}
}

// ListRunningTargetWorkers 只返回 Running Run 指定的 Worker，去重。
func TestListRunningTargetWorkersOnMySQL(t *testing.T) {
	store := requireMySQL(t)
	for i, target := range []string{"pod-a", "pod-a", ""} {
		run := execution.NewRun(execution.NewRunOptions{ID: fmt.Sprintf("run-target-%d", i), WorkflowID: fmt.Sprintf("wf-target-%d", i), WorkflowType: "wf", WorkflowConfigVersion: "v1", CreatedAt: time.Now()})
		if err := run.ApplyStartPolicy("", target); err != nil {
			t.Fatal(err)
		}
		started := historypkg.NewWorkflowStartedEvent(nil, run.ID, "", run.WorkflowID, "Workflow", 0)
		if err := store.Executions.RunInTx(func(tx execution.ExecutionTx) error {
			return execution.CommitStartedRun(tx, run, "", run.ID, run.ID, started)
		}); err != nil {
			t.Fatal(err)
		}
	}
	targets, err := store.Executions.ListRunningTargetWorkers()
	if err != nil {
		t.Fatal(err)
	}
	if len(targets) != 1 || targets[0] != (execution.TargetWorker{Namespace: "default", Identity: "pod-a"}) {
		t.Fatalf("targets = %+v, want the single default/pod-a", targets)
	}
}
