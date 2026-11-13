package memory

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol"
)

// Edge Case: 定向给 Worker A 的 Activity Task 只交给 A；Nack 之后仍在 A 的通道里。
func TestActivityTargetWaitsForItsWorker(t *testing.T) {
	router := NewActivityQueueRouter(10)
	if err := router.Enqueue(context.Background(), "queue", protocol.Task{Namespace: "ns", TargetWorkerIdentity: "A", RunID: "target-a"}); err != nil {
		t.Fatal(err)
	}
	b := protocol.WithWorkerRoutingInfo(protocol.WithNamespace(context.Background(), "ns"), protocol.WorkerRoutingInfo{Identity: "B"})
	ctx, cancel := context.WithTimeout(b, 20*time.Millisecond)
	defer cancel()
	if _, err := router.Dequeue(ctx, "queue"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("worker B took target A: %v", err)
	}
	a := protocol.WithWorkerRoutingInfo(protocol.WithNamespace(context.Background(), "ns"), protocol.WorkerRoutingInfo{Identity: "A"})
	ctxA, cancelA := context.WithTimeout(a, time.Second)
	defer cancelA()
	lease, err := router.Dequeue(ctxA, "queue")
	if err != nil {
		t.Fatal(err)
	}
	if lease.Task.RunID != "target-a" {
		t.Fatal(lease)
	}
	if err := router.Nack(context.Background(), lease); err != nil {
		t.Fatal(err)
	}
	if _, err := router.Dequeue(ctxA, "queue"); err != nil {
		t.Fatal(err)
	}
}
