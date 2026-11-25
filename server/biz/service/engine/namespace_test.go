package engine

import (
	"context"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/program/programtest"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/server/biz/domain/dispatch"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/infra/memory"
	"github.com/donutnomad/tenuto/server/infra/systemclock"
)

func TestServerIsolatesWorkflowConfigsByNamespace(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	server := assembleServer(mgr, newNamespaceTaskQueue(16), newNamespaceTaskQueue(16))

	wfA := namespaceWorkflowConfig("tenant-flow", "tenant-a-value")
	refA, err := server.Definitions().RegisterWorkflowInNamespace("tenant-a", wfA)
	if err != nil {
		t.Fatal(err)
	}
	wfB := namespaceWorkflowConfig("tenant-flow", "tenant-b-value")
	refB, err := server.Definitions().RegisterWorkflowInNamespace("tenant-b", wfB)
	if err != nil {
		t.Fatal(err)
	}
	if refA.Namespace != "tenant-a" || refB.Namespace != "tenant-b" {
		t.Fatalf("refs should carry namespace: %+v %+v", refA, refB)
	}
	if refA.Version == refB.Version {
		t.Fatalf("test setup expected distinct config versions")
	}

	runAID, err := startedRunID(server.StartWorkflow(ctx, "tenant-a", "tenant-flow", "tenant-flow", "", map[string]any{"request": "a"}))
	if err != nil {
		t.Fatal(err)
	}
	runBID, err := startedRunID(server.StartWorkflow(ctx, "tenant-b", "tenant-flow", "tenant-flow", "", map[string]any{"request": "b"}))
	if err != nil {
		t.Fatal(err)
	}

	runA, err := mgr.Get(runAID)
	if err != nil {
		t.Fatal(err)
	}
	runB, err := mgr.Get(runBID)
	if err != nil {
		t.Fatal(err)
	}
	if runA.Namespace != "tenant-a" || runB.Namespace != "tenant-b" {
		t.Fatalf("runs should carry namespace: %+v %+v", runA, runB)
	}
	if runA.WorkflowConfigVersion != refA.Version || runB.WorkflowConfigVersion != refB.Version {
		t.Fatalf("runs should pin namespace latest versions: %+v %+v", runA, runB)
	}

	gotA, err := server.GetWorkflowConfig(context.Background(), runAID, "")
	if err != nil {
		t.Fatal(err)
	}
	gotB, err := server.GetWorkflowConfig(context.Background(), runBID, "")
	if err != nil {
		t.Fatal(err)
	}
	if gotA.Name != "tenant-a-value" {
		t.Fatalf("tenant-a config leaked: %+v", gotA)
	}
	if gotB.Name != "tenant-b-value" {
		t.Fatalf("tenant-b config leaked: %+v", gotB)
	}

	listA, err := server.View().ListRuns(execution.RunListFilter{Namespace: "tenant-a", IncludeTerminal: true})
	if err != nil {
		t.Fatal(err)
	}
	if listA.Total != 1 || listA.Items[0].RunID != runAID || listA.Items[0].Namespace != "tenant-a" {
		t.Fatalf("tenant-a visibility leaked runs: %+v", listA)
	}
	listB, err := server.View().ListRuns(execution.RunListFilter{Namespace: "tenant-b", IncludeTerminal: true})
	if err != nil {
		t.Fatal(err)
	}
	if listB.Total != 1 || listB.Items[0].RunID != runBID || listB.Items[0].Namespace != "tenant-b" {
		t.Fatalf("tenant-b visibility leaked runs: %+v", listB)
	}
}

type namespaceTaskQueue struct {
	ch chan protocol.TaskLease
}

func newNamespaceTaskQueue(size int) *namespaceTaskQueue {
	return &namespaceTaskQueue{ch: make(chan protocol.TaskLease, size)}
}

func (q *namespaceTaskQueue) Enqueue(ctx context.Context, task protocol.Task) error {
	select {
	case q.ch <- protocol.TaskLease{Task: task}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// sticky 任务在这个测试队列里与普通任务走同一通道，任何 Worker 都能领取。
func (q *namespaceTaskQueue) EnqueueSticky(ctx context.Context, _ string, task protocol.Task) error {
	return q.Enqueue(ctx, task)
}

func (q *namespaceTaskQueue) DequeueForWorker(ctx context.Context, _ string) (protocol.TaskLease, error) {
	return q.Dequeue(ctx)
}

func (*namespaceTaskQueue) HandlesStickyRoutingDeadline() bool { return true }
func (*namespaceTaskQueue) Durable() bool                      { return false }

func (q *namespaceTaskQueue) Dequeue(ctx context.Context) (protocol.TaskLease, error) {
	select {
	case lease := <-q.ch:
		return lease, nil
	case <-ctx.Done():
		return protocol.TaskLease{}, ctx.Err()
	}
}

func (q *namespaceTaskQueue) Ack(ctx context.Context, lease protocol.TaskLease) error {
	return nil
}

func (q *namespaceTaskQueue) Nack(ctx context.Context, lease protocol.TaskLease) error {
	return q.Enqueue(ctx, lease.Task)
}

func TestActivityRoutesAreScopedByNamespace(t *testing.T) {
	catalog := dispatch.NewActivityRouteCatalog(systemclock.Clock{})
	if err := catalog.RegisterActivityWorkerVersion(protocol.ActivityWorkerVersion{
		Namespace:     "tenant-a",
		Identity:      "worker-a",
		BuildID:       "build-a",
		TaskQueues:    []string{"queue-a"},
		ActivityTypes: []string{"payments.Capture"},
		Default:       true,
		ExpiresAt:     time.Now().Add(time.Minute),
	}); err != nil {
		t.Fatal(err)
	}
	if err := catalog.RegisterActivityWorkerVersion(protocol.ActivityWorkerVersion{
		Namespace:     "tenant-b",
		Identity:      "worker-b",
		BuildID:       "build-b",
		TaskQueues:    []string{"queue-b"},
		ActivityTypes: []string{"payments.Capture"},
		Default:       true,
		ExpiresAt:     time.Now().Add(time.Minute),
	}); err != nil {
		t.Fatal(err)
	}

	routeA, err := catalog.ResolveActivityRoute(dispatch.ActivityRouteRef{Namespace: "tenant-a", ActivityID: "capture", Type: "payments.Capture"})
	if err != nil {
		t.Fatal(err)
	}
	routeB, err := catalog.ResolveActivityRoute(dispatch.ActivityRouteRef{Namespace: "tenant-b", ActivityID: "capture", Type: "payments.Capture"})
	if err != nil {
		t.Fatal(err)
	}
	if routeA.TaskQueue != "queue-a" || routeA.BuildID != "build-a" || routeA.Namespace != "tenant-a" {
		t.Fatalf("tenant-a route leaked: %+v", routeA)
	}
	if routeB.TaskQueue != "queue-b" || routeB.BuildID != "build-b" || routeB.Namespace != "tenant-b" {
		t.Fatalf("tenant-b route leaked: %+v", routeB)
	}
}

func TestMemoryActivityQueueRouterPollsByNamespace(t *testing.T) {
	ctx := context.Background()
	router := memory.NewActivityQueueRouter(16)
	if err := router.Enqueue(ctx, "custodian", protocol.Task{Namespace: "tenant-a", Type: protocol.TaskTypeActivity, RunID: "run-a", ScheduledEventID: 1}); err != nil {
		t.Fatal(err)
	}
	if err := router.Enqueue(ctx, "custodian", protocol.Task{Namespace: "tenant-b", Type: protocol.TaskTypeActivity, RunID: "run-b", ScheduledEventID: 1}); err != nil {
		t.Fatal(err)
	}

	leaseB, err := router.Dequeue(protocol.WithNamespace(ctx, "tenant-b"), "custodian")
	if err != nil {
		t.Fatal(err)
	}
	if leaseB.Task.Namespace != "tenant-b" || leaseB.Task.RunID != "run-b" {
		t.Fatalf("tenant-b poll got wrong task: %+v", leaseB.Task)
	}
	leaseA, err := router.Dequeue(protocol.WithNamespace(ctx, "tenant-a"), "custodian")
	if err != nil {
		t.Fatal(err)
	}
	if leaseA.Task.Namespace != "tenant-a" || leaseA.Task.RunID != "run-a" {
		t.Fatalf("tenant-a poll got wrong task: %+v", leaseA.Task)
	}
}

func namespaceWorkflowConfig(id string, value string) *program.WorkflowConfig {
	return &program.WorkflowConfig{
		Type:    id,
		Name:    value,
		Program: programtest.Noop(),
		Activities: map[string]program.ActivityRef{
			"step": {Symbol: "step", Type: "complete"},
		},
	}
}
