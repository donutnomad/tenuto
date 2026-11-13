package taskq

import (
	"context"
	"errors"
	"testing"

	"github.com/donutnomad/tenuto/protocol"
)

func TestActivityTargetIsolatedAcrossWorkersAndRedelivery(t *testing.T) {
	client := &routingClientFake{}
	router := newStaticActivityQueueRouter(t, map[string]*Queue{"payments": NewQueue(client)})
	task := protocol.Task{Type: protocol.TaskTypeActivity, Namespace: "tenant", TaskQueue: "payments", RunID: "target-a", TargetWorkerIdentity: "A"}
	if err := router.Enqueue(context.Background(), "payments", task); err != nil {
		t.Fatal(err)
	}
	a := activityWorkerPollContext("tenant", "A", "same-build")
	b := activityWorkerPollContext("tenant", "B", "same-build")
	for attempt := 0; attempt < 2; attempt++ {
		if _, err := router.Dequeue(b, "payments"); !errors.Is(err, errNoMatchingRoute) {
			t.Fatalf("non-target consumed task: %v", err)
		}
		lease, err := router.Dequeue(a, "payments")
		if err != nil {
			t.Fatal(err)
		}
		if lease.Task.TargetWorkerIdentity != "A" {
			t.Fatalf("target lost: %+v", lease)
		}
		if err := router.Nack(context.Background(), lease); err != nil {
			t.Fatal(err)
		}
	}
}

func TestDefaultActivityTargetKeepsSharedScheduling(t *testing.T) {
	client := &routingClientFake{}
	router := newStaticActivityQueueRouter(t, map[string]*Queue{"payments": NewQueue(client)})
	if err := router.Enqueue(context.Background(), "payments", protocol.Task{Namespace: "tenant", TaskQueue: "payments"}); err != nil {
		t.Fatal(err)
	}
	if _, err := router.Dequeue(activityWorkerPollContext("tenant", "B", "same-build"), "payments"); err != nil {
		t.Fatal(err)
	}
}

// 发给目标 Worker 的任务不绑定 build：目标 Worker 换了 build 之后仍能领取。
func TestActivityTargetIgnoresWorkerBuild(t *testing.T) {
	client := &routingClientFake{}
	router := newStaticActivityQueueRouter(t, map[string]*Queue{"payments": NewQueue(client)})
	task := protocol.Task{Namespace: "tenant", TaskQueue: "payments", TargetWorkerIdentity: "A"}
	if err := router.Enqueue(context.Background(), "payments", task); err != nil {
		t.Fatal(err)
	}
	if _, err := router.Dequeue(activityWorkerPollContext("tenant", "B", "v1"), "payments"); !errors.Is(err, errNoMatchingRoute) {
		t.Fatalf("another worker received the target task: %v", err)
	}
	if _, err := router.Dequeue(activityWorkerPollContext("tenant", "A", "v2"), "payments"); err != nil {
		t.Fatal(err)
	}
}
