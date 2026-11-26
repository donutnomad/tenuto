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
