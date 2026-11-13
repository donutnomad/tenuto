package taskq

import (
	"context"
	"errors"
	"strings"
	"testing"

	taskqfrontend "github.com/donutnomad/taskq/frontend"
	"github.com/donutnomad/tenuto/protocol"
)

// decode 失败时 lease 必须归还：taskq 侧已派发任务并签发 token，直接返回会让任务
// 隐形到 visibility timeout（默认 60s）才被重投。
type undecodableClient struct {
	token   string
	acked   string
	nacked  string
	nackErr error
}

func (c *undecodableClient) Enqueue(context.Context, []byte) error { return nil }

func (c *undecodableClient) EnqueueWithOptions(context.Context, []byte, taskqfrontend.EnqueueOptions) error {
	return nil
}

func (c *undecodableClient) Dequeue(context.Context) ([]byte, string, error) {
	return []byte("{not json"), c.token, nil
}

func (c *undecodableClient) DequeueWithOptions(ctx context.Context, _ taskqfrontend.PollOptions) (taskqfrontend.Lease, error) {
	data, token, err := c.Dequeue(ctx)
	return taskqfrontend.Lease{Data: data, Token: token}, err
}

func (c *undecodableClient) Ack(_ context.Context, token string) error {
	c.acked = token
	return nil
}

func (c *undecodableClient) Nack(_ context.Context, token string) error {
	c.nacked = token
	return c.nackErr
}

func TestDequeueReturnsLeaseWhenDecodeFails(t *testing.T) {
	client := &undecodableClient{token: "token-bad"}
	queue := NewQueue(client)

	_, err := queue.Dequeue(context.Background())
	if err == nil {
		t.Fatal("expected decode error, got nil")
	}
	// 错误必须保留原始 decode 根因。
	if !strings.Contains(err.Error(), "unmarshal workflow task") {
		t.Fatalf("expected error to carry the decode cause, got %v", err)
	}
	// 归还必须发生，否则任务隐形到 visibility timeout。
	if client.nacked != "token-bad" {
		t.Fatalf("undecodable lease was not returned: nacked=%q acked=%q", client.nacked, client.acked)
	}
	if client.acked != "" {
		t.Fatalf("undecodable lease must not be acked, got acked=%q", client.acked)
	}
}

// 归还本身失败时，原始 decode 错误不能被掩盖——两者都要带出来。
func TestDequeueJoinsDecodeAndReturnErrors(t *testing.T) {
	returnFailure := errors.New("matching unreachable")
	client := &undecodableClient{token: "token-bad", nackErr: returnFailure}
	queue := NewQueue(client)

	_, err := queue.Dequeue(context.Background())
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, returnFailure) {
		t.Fatalf("expected error to wrap the lease-return failure, got %v", err)
	}
	if !strings.Contains(err.Error(), "unmarshal workflow task") {
		t.Fatalf("lease-return failure masked the decode root cause: %v", err)
	}
}

func TestDecodeTaskLeaseKeepsExactNumbers(t *testing.T) {
	// 队列行里的 Activity 输入与心跳详情：超过 2^53 的整数原样保留，整数是 int64，与内存队列一致。
	task := protocol.Task{
		RunID:                "run-1",
		Input:                map[string]any{"amount": int64(9007199254740993), "rate": 0.5, "items": []any{int64(1)}},
		LastHeartbeatDetails: map[string]any{"offset": int64(9007199254740995)},
	}
	data, err := marshalTask(task)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := decodeTaskLease(data, "token")
	if err != nil {
		t.Fatal(err)
	}
	if got := lease.Task.Input["amount"]; got != int64(9007199254740993) {
		t.Fatalf("amount = %#v", got)
	}
	if got := lease.Task.Input["rate"]; got != 0.5 {
		t.Fatalf("rate = %#v", got)
	}
	if got := lease.Task.Input["items"].([]any)[0]; got != int64(1) {
		t.Fatalf("items[0] = %#v", got)
	}
	if got := lease.Task.LastHeartbeatDetails["offset"]; got != int64(9007199254740995) {
		t.Fatalf("heartbeat offset = %#v", got)
	}
}

// noTaskClient 的长轮询总是到期没有任务。
type noTaskClient struct{ undecodableClient }

func (*noTaskClient) DequeueWithOptions(context.Context, taskqfrontend.PollOptions) (taskqfrontend.Lease, error) {
	return taskqfrontend.Lease{}, taskqfrontend.ErrNoTask
}

func TestDequeueReportsEmptyPollAsPollTimeout(t *testing.T) {
	// 空轮询是 poll timeout 信号：HTTP 应答 204，进程内 Worker 不当作错误记日志。
	if _, err := NewQueue(&noTaskClient{}).Dequeue(context.Background()); !errors.Is(err, protocol.ErrTaskPollTimeout) {
		t.Fatalf("err = %v, want ErrTaskPollTimeout", err)
	}
}
