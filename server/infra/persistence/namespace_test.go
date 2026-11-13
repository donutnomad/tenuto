package persistence

import (
	"testing"
	"time"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/program/programtest"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
)

func TestWorkflowConfigLatestIsScopedByNamespace(t *testing.T) {
	store := newTestStore(t)
	wfA := &program.WorkflowConfig{
		ID:      "tenant-flow",
		Name:    "tenant-a",
		Program: programtest.Noop(),
		Activities: map[string]program.ActivityRef{
			"step": {Symbol: "step", Type: "complete"},
		},
	}
	refA, err := store.Definitions.SaveWorkflowConfigInNamespace("tenant-a", wfA)
	if err != nil {
		t.Fatal(err)
	}
	wfB := &program.WorkflowConfig{
		ID:      "tenant-flow",
		Name:    "tenant-b",
		Program: programtest.Noop(),
		Activities: map[string]program.ActivityRef{
			"step": {Symbol: "step", Type: "complete"},
		},
	}
	refB, err := store.Definitions.SaveWorkflowConfigInNamespace("tenant-b", wfB)
	if err != nil {
		t.Fatal(err)
	}

	latestA, err := store.Definitions.LatestWorkflowConfigRefInNamespace("tenant-a", "tenant-flow")
	if err != nil {
		t.Fatal(err)
	}
	latestB, err := store.Definitions.LatestWorkflowConfigRefInNamespace("tenant-b", "tenant-flow")
	if err != nil {
		t.Fatal(err)
	}
	if latestA != refA || latestB != refB {
		t.Fatalf("latest refs crossed namespace: got %+v %+v, want %+v %+v", latestA, latestB, refA, refB)
	}

	loadedA, err := store.Definitions.GetWorkflowConfig(refA)
	if err != nil {
		t.Fatal(err)
	}
	loadedB, err := store.Definitions.GetWorkflowConfig(refB)
	if err != nil {
		t.Fatal(err)
	}
	if loadedA.Name != "tenant-a" || loadedB.Name != "tenant-b" {
		t.Fatalf("configs crossed namespace: %+v %+v", loadedA, loadedB)
	}
}

func TestActivityWorkerVersionsAreScopedByNamespace(t *testing.T) {
	store := newTestStore(t)
	now := time.Now()
	_, err := store.Dispatches.UpsertActivityWorkerVersionInNamespace("tenant-a", protocol.ActivityWorkerVersion{
		Identity:      "worker-1",
		BuildID:       "build-a",
		TaskQueues:    []string{"queue-a"},
		ActivityTypes: []string{"payments.Capture"},
	}, now, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.Dispatches.UpsertActivityWorkerVersionInNamespace("tenant-b", protocol.ActivityWorkerVersion{
		Identity:      "worker-1",
		BuildID:       "build-b",
		TaskQueues:    []string{"queue-b"},
		ActivityTypes: []string{"payments.Capture"},
	}, now, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Dispatches.HeartbeatActivityWorkerVersionInNamespace("tenant-a", "worker-1", "build-a", now.Add(time.Second), time.Minute); err != nil {
		t.Fatal(err)
	}

	workers, err := store.Dispatches.ListActiveActivityWorkerVersions(now.Add(2 * time.Second))
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]protocol.ActivityWorkerVersion{}
	for _, worker := range workers {
		seen[worker.Namespace+"/"+worker.BuildID] = worker
	}
	if seen["tenant-a/build-a"].TaskQueues[0] != "queue-a" || seen["tenant-b/build-b"].TaskQueues[0] != "queue-b" {
		t.Fatalf("worker versions crossed namespace: %+v", workers)
	}
}

func TestTaskDispatchRowsAreScopedByNamespace(t *testing.T) {
	store := newTestStore(t)
	taskA := protocol.Task{
		Namespace:        "tenant-a",
		Type:             protocol.TaskTypeActivity,
		RunID:            "same-run-id-for-dispatch",
		ScheduledEventID: 7,
		Attempt:          1,
		ActivityType:     "payments.Capture",
		TaskQueue:        "payments",
	}
	taskB := taskA
	taskB.Namespace = "tenant-b"

	if _, acquired, err := store.Dispatches.AcquireTaskDispatch(taskA, time.Minute); err != nil || !acquired {
		t.Fatalf("tenant-a acquire = %v, %v", acquired, err)
	}
	if _, acquired, err := store.Dispatches.AcquireTaskDispatch(taskB, time.Minute); err != nil || !acquired {
		t.Fatalf("tenant-b acquire = %v, %v", acquired, err)
	}
	stateA, ok, err := store.Dispatches.GetTaskDispatch(taskA)
	if err != nil || !ok || stateA.Namespace != "tenant-a" {
		t.Fatalf("tenant-a dispatch state wrong: state=%+v ok=%v err=%v", stateA, ok, err)
	}
	stateB, ok, err := store.Dispatches.GetTaskDispatch(taskB)
	if err != nil || !ok || stateB.Namespace != "tenant-b" {
		t.Fatalf("tenant-b dispatch state wrong: state=%+v ok=%v err=%v", stateB, ok, err)
	}
}

// Happy Path：CountRuns 用一条 GROUP BY 返回每个 (namespace, status) 的 Run 数量。
func TestCountRunsGroupsByNamespaceAndStatus(t *testing.T) {
	store := newTestStore(t)
	runs := []*execution.Run{
		{ID: "count-a-1", Namespace: "tenant-a", WorkflowID: "wf-1", WorkflowConfigVersion: "v1", Status: history.StatusRunning, CreatedAt: time.Now()},
		{ID: "count-a-2", Namespace: "tenant-a", WorkflowID: "wf-2", WorkflowConfigVersion: "v1", Status: history.StatusRunning, CreatedAt: time.Now()},
		{ID: "count-a-3", Namespace: "tenant-a", WorkflowID: "wf-3", WorkflowConfigVersion: "v1", Status: history.StatusCompleted, CreatedAt: time.Now()},
		{ID: "count-b-1", Namespace: "tenant-b", WorkflowID: "wf-1", WorkflowConfigVersion: "v1", Status: history.StatusFailed, CreatedAt: time.Now()},
	}
	for _, run := range runs {
		if err := store.Executions.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(rootBranch(t, run)) }); err != nil {
			t.Fatal(err)
		}
	}
	counts, err := store.Executions.CountRuns()
	if err != nil {
		t.Fatal(err)
	}
	got := make(map[string]int)
	for _, count := range counts {
		got[count.Namespace+"/"+string(count.Status)] = count.Count
	}
	want := map[string]int{"tenant-a/running": 2, "tenant-a/completed": 1, "tenant-b/failed": 1}
	if len(got) != len(want) {
		t.Fatalf("counts = %+v, want %+v", got, want)
	}
	for key, count := range want {
		if got[key] != count {
			t.Fatalf("counts = %+v, want %+v", got, want)
		}
	}
}
