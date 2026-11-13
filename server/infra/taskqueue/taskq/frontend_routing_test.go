package taskq

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	taskqfrontend "github.com/donutnomad/taskq/frontend"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/server/biz/domain/dispatch"
)

func TestQueueRoutesStickyTasksToOwningWorkerOnOneClient(t *testing.T) {
	client := &routingClientFake{}
	queue := NewQueue(client)
	var _ dispatch.TaskQueue = queue

	task := protocol.Task{Type: protocol.TaskTypeWorkflow, RunID: "run-1", ScheduledEventID: 3}
	if err := queue.EnqueueSticky(context.Background(), "worker-a", task); err != nil {
		t.Fatal(err)
	}
	if _, err := queue.DequeueForWorker(context.Background(), "worker-b"); !errors.Is(err, errNoMatchingRoute) {
		t.Fatalf("worker-b dequeue error = %v", err)
	}

	lease, err := queue.DequeueForWorker(context.Background(), "worker-a")
	if err != nil {
		t.Fatal(err)
	}
	if lease.Task.WorkflowTaskRoute != protocol.WorkflowTaskRouteSticky || lease.Task.StickyTarget != "worker-a" {
		t.Fatalf("sticky lease = %+v", lease)
	}
	if err := queue.Ack(context.Background(), lease); err != nil {
		t.Fatal(err)
	}
	if client.acked != lease.Token {
		t.Fatalf("ack token = %q, want %q", client.acked, lease.Token)
	}
}

func TestQueueMakesNormalTasksVisibleToStickyPollers(t *testing.T) {
	client := &routingClientFake{}
	queue := NewQueue(client)
	task := protocol.Task{Type: protocol.TaskTypeWorkflow, RunID: "run-normal"}
	if err := queue.Enqueue(context.Background(), task); err != nil {
		t.Fatal(err)
	}

	lease, err := queue.DequeueForWorker(context.Background(), "worker-a")
	if err != nil {
		t.Fatal(err)
	}
	if lease.Task.RunID != task.RunID || lease.Task.WorkflowTaskRoute != protocol.WorkflowTaskRouteNormal {
		t.Fatalf("normal lease = %+v", lease)
	}
}

func TestQueueWiresStickyDeadlineAndNormalizesFallbackLease(t *testing.T) {
	client := &routingClientFake{}
	queue := NewQueue(client)
	deadline := time.Date(2026, 7, 17, 15, 4, 5, 0, time.UTC)
	task := protocol.Task{
		Type:                          protocol.TaskTypeWorkflow,
		RunID:                         "run-fallback",
		ScheduledEventID:              42,
		StickyScheduleToStartDeadline: deadline,
	}
	if err := queue.EnqueueSticky(context.Background(), "owner-worker", task); err != nil {
		t.Fatal(err)
	}
	if !client.lastEnqueueOptions.RoutingDeadline.Equal(deadline) {
		t.Fatalf("routing deadline = %s, want %s", client.lastEnqueueOptions.RoutingDeadline, deadline)
	}

	client.forceRoutingFallback = true
	lease, err := queue.DequeueForWorker(context.Background(), "takeover-worker")
	if err != nil {
		t.Fatal(err)
	}
	if lease.Task.WorkflowTaskRoute != protocol.WorkflowTaskRouteNormal || lease.Task.StickyTarget != "" {
		t.Fatalf("fallback task route = %q target = %q", lease.Task.WorkflowTaskRoute, lease.Task.StickyTarget)
	}
	if lease.Task.RunID != task.RunID || lease.Task.ScheduledEventID != task.ScheduledEventID || lease.Token == "" {
		t.Fatalf("fallback lease lost scheduled identity: %+v", lease)
	}
}

func TestQueueNormalDequeueNormalizesFallbackLease(t *testing.T) {
	client := &routingClientFake{}
	queue := NewQueue(client)
	if err := queue.EnqueueSticky(context.Background(), "owner-worker", protocol.Task{
		Type: protocol.TaskTypeWorkflow, RunID: "run-empty-worker", ScheduledEventID: 7,
	}); err != nil {
		t.Fatal(err)
	}
	client.forceRoutingFallback = true
	lease, err := queue.Dequeue(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if lease.Task.WorkflowTaskRoute != protocol.WorkflowTaskRouteNormal || lease.Task.StickyTarget != "" {
		t.Fatalf("fallback task route = %q target = %q", lease.Task.WorkflowTaskRoute, lease.Task.StickyTarget)
	}
}

func TestActivityRouterPreservesTargetWorkerAcrossNack(t *testing.T) {
	client := &routingClientFake{}
	router := newStaticActivityQueueRouter(t, map[string]*Queue{"payments": NewQueue(client)})
	task := protocol.Task{
		Type:                 protocol.TaskTypeActivity,
		Namespace:            "tenant-a",
		RunID:                "run-target-v2",
		TaskQueue:            "payments",
		TargetWorkerIdentity: "worker-v2",
	}
	if err := router.Enqueue(context.Background(), "payments", task); err != nil {
		t.Fatal(err)
	}

	v1Context := activityWorkerPollContext("tenant-a", "worker-v1", "payments-v1")
	if _, err := router.Dequeue(v1Context, "payments"); !errors.Is(err, errNoMatchingRoute) {
		t.Fatalf("v1 worker dequeue error = %v", err)
	}
	v2Context := activityWorkerPollContext("tenant-a", "worker-v2", "payments-v2")
	lease, err := router.Dequeue(v2Context, "payments")
	if err != nil {
		t.Fatal(err)
	}
	if lease.Task.RunID != task.RunID || lease.Task.TargetWorkerIdentity != "worker-v2" {
		t.Fatalf("v2 lease = %+v", lease)
	}
	if err := router.Nack(context.Background(), lease); err != nil {
		t.Fatal(err)
	}
	if _, err := router.Dequeue(v1Context, "payments"); !errors.Is(err, errNoMatchingRoute) {
		t.Fatalf("v1 worker received nacked v2 task: %v", err)
	}
	if _, err := router.Dequeue(v2Context, "payments"); err != nil {
		t.Fatalf("v2 worker did not receive nacked task: %v", err)
	}
}

func TestActivityRouterMakesUntargetedTasksVisibleToVersionedWorkers(t *testing.T) {
	client := &routingClientFake{}
	router := newStaticActivityQueueRouter(t, map[string]*Queue{"payments": NewQueue(client)})
	if err := router.Enqueue(context.Background(), "payments", protocol.Task{
		Type:      protocol.TaskTypeActivity,
		Namespace: "tenant-a",
		RunID:     "run-unversioned",
		TaskQueue: "payments",
	}); err != nil {
		t.Fatal(err)
	}
	lease, err := router.Dequeue(activityWorkerPollContext("tenant-a", "worker-v2", "payments-v2"), "payments")
	if err != nil {
		t.Fatal(err)
	}
	if lease.Task.RunID != "run-unversioned" {
		t.Fatalf("untargeted lease = %+v", lease)
	}
}

func TestActivityRouterIsolatesNamespacesOnSharedPhysicalQueue(t *testing.T) {
	client := &routingClientFake{}
	router := newStaticActivityQueueRouter(t, map[string]*Queue{"payments": NewQueue(client)})
	for _, task := range []protocol.Task{
		{Type: protocol.TaskTypeActivity, Namespace: "tenant-a", RunID: "run-a", TaskQueue: "payments"},
		{Type: protocol.TaskTypeActivity, Namespace: "tenant-b", RunID: "run-b", TaskQueue: "payments"},
	} {
		if err := router.Enqueue(context.Background(), "payments", task); err != nil {
			t.Fatal(err)
		}
	}

	tenantB, err := router.Dequeue(activityWorkerPollContext("tenant-b", "worker-b", ""), "payments")
	if err != nil {
		t.Fatal(err)
	}
	if tenantB.Task.RunID != "run-b" || tenantB.Task.Namespace != "tenant-b" {
		t.Fatalf("tenant-b lease = %+v", tenantB)
	}
	tenantA, err := router.Dequeue(activityWorkerPollContext("tenant-a", "worker-a", ""), "payments")
	if err != nil {
		t.Fatal(err)
	}
	if tenantA.Task.RunID != "run-a" || tenantA.Task.Namespace != "tenant-a" {
		t.Fatalf("tenant-a lease = %+v", tenantA)
	}
}

func TestActivityRouterIsolatesLogicalQueuesOnSharedPhysicalQueue(t *testing.T) {
	client := &routingClientFake{}
	shared := NewQueue(client)
	router := newStaticActivityQueueRouter(t, map[string]*Queue{"payments": shared, "emails": shared})
	for _, task := range []protocol.Task{
		{Type: protocol.TaskTypeActivity, Namespace: "tenant-a", RunID: "run-payment", TaskQueue: "payments"},
		{Type: protocol.TaskTypeActivity, Namespace: "tenant-a", RunID: "run-email", TaskQueue: "emails"},
	} {
		if err := router.Enqueue(context.Background(), task.TaskQueue, task); err != nil {
			t.Fatal(err)
		}
	}

	email, err := router.Dequeue(activityWorkerPollContext("tenant-a", "email-worker", ""), "emails")
	if err != nil {
		t.Fatal(err)
	}
	if email.Task.RunID != "run-email" {
		t.Fatalf("email lease = %+v", email)
	}
	payment, err := router.Dequeue(activityWorkerPollContext("tenant-a", "payment-worker", ""), "payments")
	if err != nil {
		t.Fatal(err)
	}
	if payment.Task.RunID != "run-payment" {
		t.Fatalf("payment lease = %+v", payment)
	}
}

// newStaticActivityQueueRouter 返回一个把裸 route 名解析到给定 *Queue 的 Router，
// 供单元测试注入 fake client。
func newStaticActivityQueueRouter(t *testing.T, queues map[string]*Queue) *ActivityQueueRouter {
	t.Helper()
	overrides := make(map[string]string, len(queues))
	for name := range queues {
		overrides[name] = name
	}
	router := newLazyActivityQueueRouter(overrides, func(_ context.Context, physical string) (*Queue, interface{ Close() }, error) {
		queue := queues[physical]
		if queue == nil {
			return nil, nil, fmt.Errorf("no queue for physical name %s", physical)
		}
		return queue, noopCloser{}, nil
	})
	t.Cleanup(router.Close)
	return router
}

type noopCloser struct{}

func (noopCloser) Close() {}

func activityWorkerPollContext(namespace, identity, buildID string) context.Context {
	ctx := protocol.WithNamespace(context.Background(), namespace)
	return protocol.WithWorkerInfo(ctx, protocol.WorkerInfo{Namespace: namespace, Identity: identity, BuildID: buildID})
}

var errNoMatchingRoute = errors.New("no matching route")

type routingClientFake struct {
	messages             []routedClientMessage
	inflight             map[string]routedClientMessage
	nextID               int
	acked                string
	nacked               string
	lastEnqueueOptions   taskqfrontend.EnqueueOptions
	forceRoutingFallback bool
}

type routedClientMessage struct {
	data       []byte
	token      string
	routingKey string
}

func (c *routingClientFake) Enqueue(ctx context.Context, data []byte) error {
	return c.EnqueueWithOptions(ctx, data, taskqfrontend.EnqueueOptions{})
}

func (c *routingClientFake) EnqueueWithOptions(_ context.Context, data []byte, opts taskqfrontend.EnqueueOptions) error {
	c.nextID++
	c.lastEnqueueOptions = opts
	c.messages = append(c.messages, routedClientMessage{
		data:       append([]byte(nil), data...),
		token:      fmt.Sprintf("lease-%d", c.nextID),
		routingKey: opts.RoutingKey,
	})
	return nil
}

func (c *routingClientFake) Dequeue(ctx context.Context) ([]byte, string, error) {
	lease, err := c.DequeueWithOptions(ctx, taskqfrontend.PollOptions{})
	return lease.Data, lease.Token, err
}

func (c *routingClientFake) DequeueWithOptions(_ context.Context, opts taskqfrontend.PollOptions) (taskqfrontend.Lease, error) {
	for i, message := range c.messages {
		if message.routingKey != "" && !c.forceRoutingFallback && !slices.Contains(opts.AcceptedRoutingKeys, message.routingKey) {
			continue
		}
		c.messages = append(c.messages[:i], c.messages[i+1:]...)
		if c.inflight == nil {
			c.inflight = make(map[string]routedClientMessage)
		}
		c.inflight[message.token] = message
		return taskqfrontend.Lease{
			Data: message.data, Token: message.token, RoutingKey: message.routingKey,
			RoutingFallback: c.forceRoutingFallback,
		}, nil
	}
	return taskqfrontend.Lease{}, errNoMatchingRoute
}

func (c *routingClientFake) Ack(_ context.Context, token string) error {
	c.acked = token
	delete(c.inflight, token)
	return nil
}

func (c *routingClientFake) Nack(_ context.Context, token string) error {
	c.nacked = token
	message, ok := c.inflight[token]
	if ok {
		delete(c.inflight, token)
		c.messages = append(c.messages, message)
	}
	return nil
}
