package taskq

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	taskqfrontend "github.com/donutnomad/taskq/frontend"
	"github.com/donutnomad/tenuto/protocol"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestQueueEncodesTasksAndAcksLeases(t *testing.T) {
	client := &fakeClient{}
	queue := NewQueue(client)

	task := protocol.Task{
		Type:                protocol.TaskTypeActivity,
		RunID:               "run-1",
		ActivityID:          "step-a",
		ScheduledEventID:    7,
		Attempt:             2,
		ActivityType:        "send_email",
		Input:               map[string]any{"to": "a@example.com"},
		StartToCloseTimeout: 2 * time.Second,
	}

	if err := queue.Enqueue(context.Background(), task); err != nil {
		t.Fatal(err)
	}
	if len(client.enqueued) != 1 {
		t.Fatalf("expected 1 enqueued payload, got %d", len(client.enqueued))
	}

	client.nextData = client.enqueued[0]
	client.nextToken = "token-1"

	lease, err := queue.Dequeue(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if lease.Task.RunID != task.RunID {
		t.Fatalf("expected run id %q, got %q", task.RunID, lease.Task.RunID)
	}
	if lease.Task.ScheduledEventID != task.ScheduledEventID {
		t.Fatalf("expected scheduled event id %d, got %d", task.ScheduledEventID, lease.Task.ScheduledEventID)
	}
	if lease.Task.Attempt != task.Attempt {
		t.Fatalf("expected attempt %d, got %d", task.Attempt, lease.Task.Attempt)
	}
	if lease.Task.StartToCloseTimeout != task.StartToCloseTimeout {
		t.Fatalf("expected timeout %s, got %s", task.StartToCloseTimeout, lease.Task.StartToCloseTimeout)
	}

	if err := queue.Ack(context.Background(), lease); err != nil {
		t.Fatal(err)
	}
	if client.acked != "token-1" {
		t.Fatalf("expected ack token token-1, got %q", client.acked)
	}

	if err := queue.Nack(context.Background(), lease); err != nil {
		t.Fatal(err)
	}
	if client.nacked != "token-1" {
		t.Fatalf("expected nack token token-1, got %q", client.nacked)
	}
}

type fakeClient struct {
	enqueued  [][]byte
	nextData  []byte
	nextToken string
	acked     string
	nacked    string
}

func (c *fakeClient) Enqueue(ctx context.Context, data []byte) error {
	c.enqueued = append(c.enqueued, data)
	return nil
}

func (c *fakeClient) EnqueueWithOptions(ctx context.Context, data []byte, _ taskqfrontend.EnqueueOptions) error {
	return c.Enqueue(ctx, data)
}

func (c *fakeClient) Dequeue(ctx context.Context) ([]byte, string, error) {
	return c.nextData, c.nextToken, nil
}

func (c *fakeClient) DequeueWithOptions(ctx context.Context, _ taskqfrontend.PollOptions) (taskqfrontend.Lease, error) {
	data, token, err := c.Dequeue(ctx)
	return taskqfrontend.Lease{Data: data, Token: token}, err
}

func (c *fakeClient) Ack(ctx context.Context, token string) error {
	c.acked = token
	return nil
}

func (c *fakeClient) Nack(ctx context.Context, token string) error {
	c.nacked = token
	return nil
}

// TaskQ 用完重试次数的错误是暂时性失败（Runtime API 应答 503 / 502）；其余 TaskQ 错误不是。
func TestIsTransientRecognisesRetriesExhausted(t *testing.T) {
	exhausted := fmt.Errorf("frontend: nack: %w: %w", taskqfrontend.ErrRetriesExhausted, status.Error(codes.FailedPrecondition, "not owner"))
	for err, want := range map[error]bool{
		exhausted:                         true,
		fmt.Errorf("nack: %w", exhausted): true,
		status.Error(codes.InvalidArgument, "stale ack token: token epoch 3 does not match queue epoch 4"): false,
		errors.New("max retries exceeded"): false,
	} {
		if got := IsTransient(err); got != want {
			t.Errorf("IsTransient(%v) = %v, want %v", err, got, want)
		}
	}
	if IsTransient(nil) {
		t.Error("IsTransient(nil) = true")
	}
}
