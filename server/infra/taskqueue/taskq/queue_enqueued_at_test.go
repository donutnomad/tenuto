package taskq

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol"
)

// TestQueueRoundTripsEnqueuedAt 断言入队时间戳能跨 taskq 边界存活。
//
// 这是 schedule-to-start 指标的命脉：投递侧与消费侧可能是不同进程，中间只有 JSON。
// 字段丢在序列化里的话，消费侧永远看到零值，指标恒为空——正是上一轮不做这个指标的原因。
func TestQueueRoundTripsEnqueuedAt(t *testing.T) {
	client := &fakeClient{}
	queue := NewQueue(client)

	enqueuedAt := time.Now().Add(-4 * time.Second).UTC().Truncate(time.Millisecond)
	task := protocol.Task{
		Type:       protocol.TaskTypeWorkflow,
		RunID:      "run-1",
		EnqueuedAt: enqueuedAt,
	}

	if err := queue.Enqueue(context.Background(), task); err != nil {
		t.Fatal(err)
	}
	client.nextData = client.enqueued[0]
	client.nextToken = "token-1"

	lease, err := queue.Dequeue(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !lease.Task.EnqueuedAt.Equal(enqueuedAt) {
		t.Fatalf("EnqueuedAt did not survive the queue round trip: got %v, want %v — "+
			"schedule-to-start would be permanently unrecordable", lease.Task.EnqueuedAt, enqueuedAt)
	}
}

// TestQueueOmitsZeroEnqueuedAtFromPayload 断言零值不会被写进 JSON。
//
// time.Time 是 struct，`omitempty` 对它**不生效**（struct 永远不会被 omit），
// 必须用 `omitzero`。若 tag 被改回 omitempty，payload 里会出现
// "enqueued_at":"0001-01-01T00:00:00Z"——虽然消费侧的零值保护仍能挡住它，
// 但那是白白撑大每条消息、且让 payload 里出现一个纯噪音字段。
func TestQueueOmitsZeroEnqueuedAtFromPayload(t *testing.T) {
	client := &fakeClient{}
	queue := NewQueue(client)

	if err := queue.Enqueue(context.Background(), protocol.Task{Type: protocol.TaskTypeWorkflow, RunID: "run-1"}); err != nil {
		t.Fatal(err)
	}
	payload := string(client.enqueued[0])
	if strings.Contains(payload, "enqueued_at") {
		t.Fatalf("zero EnqueuedAt leaked into the payload (omitzero not in effect): %s", payload)
	}
}

// TestQueueRoundTripsEnqueuedAtOnStickyPath 断言 sticky 投递路径同样带戳。
//
// sticky 走的是 EnqueueWithOptions（另一条编码路径），且 Task 里另有一个
// StickyScheduleToStartDeadline 字段——两者必须互不干扰：一个是入队时刻，
// 一个是路由截止时刻。
func TestQueueRoundTripsEnqueuedAtOnStickyPath(t *testing.T) {
	client := &fakeClient{}
	queue := NewQueue(client)

	enqueuedAt := time.Now().Add(-2 * time.Second).UTC().Truncate(time.Millisecond)
	deadline := time.Now().Add(30 * time.Second).UTC().Truncate(time.Millisecond)

	err := queue.EnqueueSticky(context.Background(), "worker-1", protocol.Task{
		Type:                          protocol.TaskTypeWorkflow,
		RunID:                         "run-sticky",
		EnqueuedAt:                    enqueuedAt,
		StickyScheduleToStartDeadline: deadline,
	})
	if err != nil {
		t.Fatal(err)
	}
	client.nextData = client.enqueued[0]
	client.nextToken = "token-sticky"

	lease, err := queue.DequeueForWorker(context.Background(), "worker-1")
	if err != nil {
		t.Fatal(err)
	}
	if !lease.Task.EnqueuedAt.Equal(enqueuedAt) {
		t.Fatalf("sticky path lost EnqueuedAt: got %v, want %v", lease.Task.EnqueuedAt, enqueuedAt)
	}
	// 两个时间字段不能互相串味。
	if !lease.Task.StickyScheduleToStartDeadline.Equal(deadline) {
		t.Fatalf("sticky deadline was clobbered: got %v, want %v", lease.Task.StickyScheduleToStartDeadline, deadline)
	}
	if lease.Task.EnqueuedAt.Equal(lease.Task.StickyScheduleToStartDeadline) {
		t.Fatal("EnqueuedAt and StickyScheduleToStartDeadline must stay distinct fields")
	}
}
