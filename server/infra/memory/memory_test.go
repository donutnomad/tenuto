package memory

import (
	"errors"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/biz/domain/execution/executiontest"
)

func TestRunStoreClonesRunsAndMaps(t *testing.T) {
	store := NewExecutionManager()
	run := &execution.Run{
		ID:                    "run-clone",
		BranchToken:           executiontest.BranchToken("run-clone"),
		WorkflowID:            "wf",
		WorkflowConfigVersion: "v1",
		Status:                history.StatusRunning,
		Input:                 map[string]any{"order_id": "o-1"},
		Output:                map[string]any{"ok": false},
		SearchAttributes:      map[string]string{"business_id": "biz-1"},
		CreatedAt:             time.Now(),
	}
	if err := store.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(run) }); err != nil {
		t.Fatal(err)
	}

	run.Status = history.StatusFailed
	run.Input["order_id"] = "mutated"
	run.SearchAttributes["business_id"] = "mutated"

	got, err := store.Get("run-clone")
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != history.StatusRunning || got.Input["order_id"] != "o-1" || got.SearchAttributes["business_id"] != "biz-1" {
		t.Fatalf("store kept caller-owned state: %+v", got)
	}

	got.Status = history.StatusCompleted
	got.Input["order_id"] = "changed-by-reader"
	got.SearchAttributes["business_id"] = "changed-by-reader"
	again, err := store.Get("run-clone")
	if err != nil {
		t.Fatal(err)
	}
	if again.Status != history.StatusRunning || again.Input["order_id"] != "o-1" || again.SearchAttributes["business_id"] != "biz-1" {
		t.Fatalf("Get returned internal state: %+v", again)
	}

	output := map[string]any{"ok": true}
	executiontest.UpdateRun(t, store, "run-clone", func(run *execution.Run) error {
		return run.MarkTerminal(history.StatusCompleted, output)
	})
	output["ok"] = false
	updated, err := store.Get("run-clone")
	if err != nil {
		t.Fatal(err)
	}
	if updated.Status != history.StatusCompleted || updated.Output["ok"] != true {
		t.Fatalf("Update stored caller-owned state: %+v", updated)
	}
}

func TestRunStoreListsReturnClonedRuns(t *testing.T) {
	store := NewRunStore()
	run := &execution.Run{
		ID:                    "run-list-clone",
		WorkflowID:            "wf",
		WorkflowConfigVersion: "v1",
		Status:                history.StatusRunning,
		Input:                 map[string]any{"order_id": "o-1"},
		CreatedAt:             time.Now(),
	}
	if err := store.create(run); err != nil {
		t.Fatal(err)
	}

	listed, err := store.List(execution.StoreListFilter{})
	if err != nil {
		t.Fatal(err)
	}
	listed.Runs[0].Status = history.StatusFailed
	listed.Runs[0].Input["order_id"] = "mutated"

	again, err := store.List(execution.StoreListFilter{})
	if err != nil {
		t.Fatal(err)
	}
	if len(again.Runs) != 1 || again.Runs[0].Status != history.StatusRunning || again.Runs[0].Input["order_id"] != "o-1" {
		t.Fatalf("List returned internal state: %+v", again.Runs)
	}
}

// Error Case：Run 不存在时读取与 append 都返回 ErrRunNotFound，history 与 execution state 保持为空。
func TestMissingRunReturnsErrRunNotFound(t *testing.T) {
	mgr := NewExecutionManager()
	if _, err := mgr.Get("missing"); !errors.Is(err, execution.ErrRunNotFound) {
		t.Fatalf("Get error = %v, want ErrRunNotFound", err)
	}
	if _, err := mgr.Append("missing", "missing", "missing", history.NewWorkflowStartedEvent(nil, "missing", "", "wf", "Workflow", 0)); !errors.Is(err, execution.ErrRunNotFound) {
		t.Fatalf("Append error = %v, want ErrRunNotFound", err)
	}
	if _, _, err := mgr.AppendWorkflowTaskScheduled("missing", "missing", "missing", history.NewWorkflowTaskScheduledEvent(1000)); !errors.Is(err, execution.ErrRunNotFound) {
		t.Fatalf("AppendWorkflowTaskScheduled error = %v, want ErrRunNotFound", err)
	}
	if events, _ := mgr.GetEvents("missing"); len(events) != 0 {
		t.Fatalf("missing run got %d events", len(events))
	}
	if _, err := mgr.GetWorkflowExecutionState("missing"); !errors.Is(err, execution.ErrRunNotFound) {
		t.Fatalf("GetWorkflowExecutionState error = %v, want ErrRunNotFound", err)
	}
}

// Happy Path：CountRuns 按 (namespace, status) 分组计数。
func TestCountRunsGroupsByNamespaceAndStatus(t *testing.T) {
	store := NewRunStore()
	for id, run := range map[string]*execution.Run{
		"a-1": {Namespace: "tenant-a", Status: history.StatusRunning},
		"a-2": {Namespace: "tenant-a", Status: history.StatusRunning},
		"a-3": {Namespace: "tenant-a", Status: history.StatusCompleted},
		"b-1": {Namespace: "tenant-b", Status: history.StatusFailed},
	} {
		run.ID, run.WorkflowID, run.WorkflowConfigVersion = id, id, "v1"
		if err := store.create(run); err != nil {
			t.Fatal(err)
		}
	}
	counts, err := store.CountRuns("")
	if err != nil {
		t.Fatal(err)
	}
	got := make(map[execution.RunCount]int)
	for _, count := range counts {
		got[execution.RunCount{Namespace: count.Namespace, Status: count.Status}] = count.Count
	}
	want := map[execution.RunCount]int{
		{Namespace: "tenant-a", Status: history.StatusRunning}:   2,
		{Namespace: "tenant-a", Status: history.StatusCompleted}: 1,
		{Namespace: "tenant-b", Status: history.StatusFailed}:    1,
	}
	if len(got) != len(want) {
		t.Fatalf("counts = %+v, want %+v", got, want)
	}
	for key, count := range want {
		if got[key] != count {
			t.Fatalf("counts = %+v, want %+v", got, want)
		}
	}
	// 给出 namespace 时只统计这个 namespace。
	counts, err = store.CountRuns("tenant-b")
	if err != nil {
		t.Fatal(err)
	}
	if len(counts) != 1 || counts[0].Namespace != "tenant-b" || counts[0].Count != 1 {
		t.Fatalf("tenant-b counts = %+v", counts)
	}
}

// Edge Case: 两个调用方各持一份快照，先后更新不同字段。后提交的一方只写自己改过的字段，
// 先提交的字段保留（多副本下的 Server 实例之间没有共享的进程内锁）。
func TestRunUpdateWritesOnlyChangedFields(t *testing.T) {
	store := NewExecutionManager()
	if err := store.RunInTx(func(tx execution.ExecutionTx) error {
		return tx.CreateRun(&execution.Run{ID: "run-stale", BranchToken: executiontest.BranchToken("run-stale"), WorkflowID: "wf", Status: history.StatusRunning})
	}); err != nil {
		t.Fatal(err)
	}
	stale, err := store.Get("run-stale")
	if err != nil {
		t.Fatal(err)
	}
	executiontest.UpdateRun(t, store, "run-stale", func(run *execution.Run) error {
		run.MarkConfigError("contract mismatch")
		return nil
	})
	if err := store.RunInTx(func(tx execution.ExecutionTx) error {
		stale.ApplyTimeouts(time.Hour, 0, time.Now())
		return tx.UpdateRun(stale)
	}); err != nil {
		t.Fatal(err)
	}
	got, err := store.Get("run-stale")
	if err != nil {
		t.Fatal(err)
	}
	if got.ConfigError != "contract mismatch" || got.DeadlineAt == nil {
		t.Fatalf("run after two updates: config_error=%q deadline=%v", got.ConfigError, got.DeadlineAt)
	}
}
