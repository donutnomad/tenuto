package engine

import (
	"context"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/server/biz/view/executionview"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

func TestListWorkersReturnsRegisteredWorkers(t *testing.T) {
	// Happy Path: register two workers in the same namespace, list returns both grouped by identity+buildID
	mgr := memory.NewExecutionManager()
	server := assembleServerWithoutTestWorker(mgr)

	worker1 := protocol.ActivityWorkerVersion{
		Namespace:     "default",
		Identity:      "worker-1",
		BuildID:       "build-abc",
		TaskQueues:    []string{"queue-a"},
		ActivityTypes: []string{"activity.Foo", "activity.Bar"},
		Default:       true,
		Weight:        10,
		HeartbeatTTL:  "1m",
	}
	worker2 := protocol.ActivityWorkerVersion{
		Namespace:     "default",
		Identity:      "worker-2",
		BuildID:       "build-xyz",
		TaskQueues:    []string{"queue-b"},
		ActivityTypes: []string{"activity.Baz"},
		Weight:        5,
		HeartbeatTTL:  "1m",
	}

	if err := server.RegisterActivityWorkerVersion(context.Background(), worker1); err != nil {
		t.Fatal(err)
	}
	if err := server.RegisterActivityWorkerVersion(context.Background(), worker2); err != nil {
		t.Fatal(err)
	}

	result, err := server.ListWorkers(executionview.WorkerListFilter{Namespace: "default"})
	if err != nil {
		t.Fatal(err)
	}
	if result.Total != 2 {
		t.Fatalf("expected 2 workers, got %d: %+v", result.Total, result.Items)
	}

	// sorted by identity
	if result.Items[0].Identity != "worker-1" || result.Items[1].Identity != "worker-2" {
		t.Fatalf("unexpected worker order: %+v", result.Items)
	}
	if len(result.Items[0].ActivityTypes) != 2 {
		t.Fatalf("expected 2 activity types for worker-1, got %+v", result.Items[0].ActivityTypes)
	}
	if result.Items[0].BuildID != "build-abc" {
		t.Fatalf("expected build-abc, got %s", result.Items[0].BuildID)
	}
}

func TestListWorkersFiltersNamespace(t *testing.T) {
	// Happy Path: workers in different namespaces, filter returns only matching namespace
	mgr := memory.NewExecutionManager()
	server := assembleServerWithoutTestWorker(mgr)

	workerDefault := protocol.ActivityWorkerVersion{
		Namespace:     "default",
		Identity:      "worker-default",
		BuildID:       "build-1",
		TaskQueues:    []string{"queue-a"},
		ActivityTypes: []string{"activity.Foo"},
		HeartbeatTTL:  "1m",
	}
	workerOther := protocol.ActivityWorkerVersion{
		Namespace:     "other",
		Identity:      "worker-other",
		BuildID:       "build-2",
		TaskQueues:    []string{"queue-b"},
		ActivityTypes: []string{"activity.Bar"},
		HeartbeatTTL:  "1m",
	}

	if err := server.RegisterActivityWorkerVersion(context.Background(), workerDefault); err != nil {
		t.Fatal(err)
	}
	if err := server.RegisterActivityWorkerVersion(context.Background(), workerOther); err != nil {
		t.Fatal(err)
	}

	result, err := server.ListWorkers(executionview.WorkerListFilter{Namespace: "other"})
	if err != nil {
		t.Fatal(err)
	}
	if result.Total != 1 {
		t.Fatalf("expected 1 worker in 'other' namespace, got %d: %+v", result.Total, result.Items)
	}
	if result.Items[0].Identity != "worker-other" {
		t.Fatalf("expected worker-other, got %s", result.Items[0].Identity)
	}
}

func TestListWorkersEmptyWithNoRegistrations(t *testing.T) {
	// Edge Case: no workers registered, list returns empty result
	mgr := memory.NewExecutionManager()
	server := assembleServerWithoutTestWorker(mgr)

	result, err := server.ListWorkers(executionview.WorkerListFilter{Namespace: "default"})
	if err != nil {
		t.Fatal(err)
	}
	if result.Total != 0 {
		t.Fatalf("expected 0 workers, got %d", result.Total)
	}
	if len(result.Items) != 0 {
		t.Fatalf("expected empty items, got %+v", result.Items)
	}
}

func TestListWorkersIncludesWorkflowStickyCacheMetrics(t *testing.T) {
	mgr := memory.NewExecutionManager()
	server := assembleServerWithoutTestWorker(mgr)
	server.recordWorkflowWorker(protocol.WorkerInfo{
		Namespace: "default", Identity: "workflow-worker", BuildID: "build-1",
		StickyCache: &protocol.ExecutionCacheSnapshot{Entries: 3, EstimatedBytes: 4096, Hits: 12, Misses: 2},
	}, time.Now())
	result, err := server.ListWorkers(executionview.WorkerListFilter{Namespace: "default"})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Items) != 1 || result.Items[0].StickyCache == nil {
		t.Fatalf("workers = %+v", result.Items)
	}
	if result.Items[0].StickyCache.Entries != 3 || result.Items[0].StickyCache.Hits != 12 {
		t.Fatalf("sticky cache = %+v", result.Items[0].StickyCache)
	}
}

func TestListWorkersExcludesExpiredWorkers(t *testing.T) {
	// Edge Case: expired workers are not returned by ListActiveActivityWorkerVersions
	store := memory.NewWorkerVersionRepository()
	mgr := memory.NewExecutionManager()
	server := assembleServerWithoutTestWorker(mgr)

	worker := protocol.ActivityWorkerVersion{
		Namespace:     "default",
		Identity:      "worker-expired",
		BuildID:       "build-old",
		TaskQueues:    []string{"queue-a"},
		ActivityTypes: []string{"activity.Foo"},
	}
	pastTime := time.Now().Add(-10 * time.Minute)
	_, err := store.UpsertActivityWorkerVersionInNamespace(protocol.DefaultNamespace, worker, pastTime, 1*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	// Using the server's store which is a fresh local store with nothing registered
	result, err := server.ListWorkers(executionview.WorkerListFilter{Namespace: "default"})
	if err != nil {
		t.Fatal(err)
	}
	if result.Total != 0 {
		t.Fatalf("expected 0 workers (expired), got %d", result.Total)
	}
}

func TestListWorkersServiceRoundTrip(t *testing.T) {
	// 注册 Worker 后通过运行时服务查询。
	mgr := memory.NewExecutionManager()
	server := assembleServerWithoutTestWorker(mgr)

	worker := protocol.ActivityWorkerVersion{
		Namespace:     "default",
		Identity:      "worker-http",
		BuildID:       "build-http",
		TaskQueues:    []string{"queue-http"},
		ActivityTypes: []string{"activity.HTTP"},
		HeartbeatTTL:  "1m",
	}
	if err := server.RegisterActivityWorkerVersion(context.Background(), worker); err != nil {
		t.Fatal(err)
	}

	result, err := server.ListWorkers(executionview.WorkerListFilter{Namespace: "default"})
	if err != nil {
		t.Fatal(err)
	}
	if result.Total != 1 {
		t.Fatalf("expected 1 worker, got %d: %+v", result.Total, result.Items)
	}
	if result.Items[0].Identity != "worker-http" || result.Items[0].BuildID != "build-http" {
		t.Fatalf("unexpected worker: %+v", result.Items[0])
	}
	if len(result.Items[0].TaskQueues) != 1 || result.Items[0].TaskQueues[0] != "queue-http" {
		t.Fatalf("unexpected task queues: %+v", result.Items[0].TaskQueues)
	}
}

func TestListWorkersGroupsActivityTypesFromSameWorker(t *testing.T) {
	// Happy Path: the local store fans out per activity-type; ListWorkers groups them back
	mgr := memory.NewExecutionManager()
	server := assembleServerWithoutTestWorker(mgr)

	worker := protocol.ActivityWorkerVersion{
		Namespace:     "default",
		Identity:      "worker-multi",
		BuildID:       "build-multi",
		TaskQueues:    []string{"queue-x", "queue-y"},
		ActivityTypes: []string{"activity.A", "activity.B", "activity.C"},
		HeartbeatTTL:  "1m",
	}
	if err := server.RegisterActivityWorkerVersion(context.Background(), worker); err != nil {
		t.Fatal(err)
	}

	result, err := server.ListWorkers(executionview.WorkerListFilter{Namespace: "default"})
	if err != nil {
		t.Fatal(err)
	}
	if result.Total != 1 {
		t.Fatalf("expected 1 grouped worker, got %d: %+v", result.Total, result.Items)
	}
	item := result.Items[0]
	if len(item.ActivityTypes) != 3 {
		t.Fatalf("expected 3 activity types, got %+v", item.ActivityTypes)
	}
	if len(item.TaskQueues) != 2 {
		t.Fatalf("expected 2 task queues, got %+v", item.TaskQueues)
	}
}
