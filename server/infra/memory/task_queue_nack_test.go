package memory

import (
	"context"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol"
)

// Nack 重投队列出队时保存的任务：调用方交回的 lease.Task 被改过（Worker 交回的载荷没有签名）也不影响下一次投递；
// 已确认或已退回的租约再退回什么也不做。
func TestTaskQueueNackRedeliversTheDequeuedTask(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	queue := NewTaskQueue(4)
	original := protocol.Task{Type: protocol.TaskTypeActivity, RunID: "run-1", ScheduledEventID: 5, Input: map[string]any{"amount": int64(10)}}
	if err := queue.Enqueue(ctx, original); err != nil {
		t.Fatal(err)
	}
	lease, err := queue.Dequeue(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if lease.Token == "" {
		t.Fatal("dequeued lease has no token")
	}
	tampered := lease
	tampered.Task.Input = map[string]any{"amount": int64(1_000_000)}
	tampered.Task.LastHeartbeatDetails = map[string]any{"offset": int64(99)}
	if err := queue.Nack(ctx, tampered); err != nil {
		t.Fatal(err)
	}
	again, err := queue.Dequeue(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if again.Task.Input["amount"] != int64(10) || again.Task.LastHeartbeatDetails != nil || again.Token == lease.Token {
		t.Fatalf("redelivered task = %+v (token %q), want the original payload under a new lease", again.Task, again.Token)
	}
	if err := queue.Ack(ctx, again); err != nil {
		t.Fatal(err)
	}
	if err := queue.Nack(ctx, again); err != nil {
		t.Fatal(err)
	}
	if err := queue.Nack(ctx, lease); err != nil {
		t.Fatal(err)
	}
	if pending := len(queue.pending); pending != 0 {
		t.Fatalf("settled leases were redelivered: %d pending", pending)
	}
}

// 没有 Ack 或 Nack 的租约副本到期后在下一次出队时清理；到期后的 Nack 不再重投。
func TestTaskQueueDropsExpiredLeases(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	now := time.Unix(1000, 0)
	queue := NewTaskQueue(4)
	queue.now = func() time.Time { return now }
	for _, runID := range []string{"run-1", "run-2", "run-3"} {
		if err := queue.Enqueue(ctx, protocol.Task{Type: protocol.TaskTypeWorkflow, RunID: runID}); err != nil {
			t.Fatal(err)
		}
	}
	abandoned, err := queue.Dequeue(ctx)
	if err != nil {
		t.Fatal(err)
	}
	nackedLate, err := queue.Dequeue(ctx)
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(taskLeaseTimeout - time.Millisecond)
	if len(queue.leased) != 2 {
		t.Fatalf("leased = %d before expiry, want 2", len(queue.leased))
	}
	now = now.Add(time.Millisecond)
	if err := queue.Nack(ctx, nackedLate); err != nil {
		t.Fatal(err)
	}
	fresh, err := queue.Dequeue(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if fresh.Task.RunID != "run-3" {
		t.Fatalf("dequeued %q, want run-3: an expired nack must not redeliver", fresh.Task.RunID)
	}
	if _, ok := queue.leased[abandoned.Token]; ok || len(queue.leased) != 1 || len(queue.pending) != 0 {
		t.Fatalf("leased = %v, pending = %d; want only the fresh lease", queue.leased, len(queue.pending))
	}
}
