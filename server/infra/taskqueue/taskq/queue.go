package taskq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	taskqfrontend "github.com/donutnomad/taskq/frontend"
	"github.com/donutnomad/tenuto/internal/jsonvalue"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/server/biz/domain/dispatch"
)

type Client interface {
	Enqueue(ctx context.Context, data []byte) error
	EnqueueWithOptions(ctx context.Context, data []byte, opts taskqfrontend.EnqueueOptions) error
	Dequeue(ctx context.Context) ([]byte, string, error)
	DequeueWithOptions(ctx context.Context, opts taskqfrontend.PollOptions) (taskqfrontend.Lease, error)
	Ack(ctx context.Context, token string) error
	Nack(ctx context.Context, token string) error
}

type Queue struct {
	client Client
}

var _ dispatch.TaskQueue = (*Queue)(nil)

func (*Queue) HandlesStickyRoutingDeadline() bool { return true }

// Durable 返回 true：任务持久化在 TaskQ 的存储里。
func (*Queue) Durable() bool { return true }

// IsTransient 报告 err 是否是 TaskQ 的暂时性失败：Enqueue、Ack、Nack 每次都遇到可重试的错误（分区换主、owner
// 不可达）并用完了重试次数。gRPC Unavailable 由 runtimehttp 直接识别，这里只补上重试用完的情形。
func IsTransient(err error) bool {
	return errors.Is(err, taskqfrontend.ErrRetriesExhausted)
}

// NewQueue 将 taskq frontend 风格客户端适配为 workflow TaskQueue。
func NewQueue(client Client) *Queue {
	return &Queue{client: client}
}

func (q *Queue) Enqueue(ctx context.Context, task protocol.Task) error {
	task.WorkflowTaskRoute = protocol.WorkflowTaskRouteNormal
	task.StickyTarget = ""
	data, err := marshalTask(task)
	if err != nil {
		return err
	}
	return q.client.Enqueue(ctx, data)
}

func (q *Queue) Dequeue(ctx context.Context) (protocol.TaskLease, error) {
	return q.dequeueWithRoutingKeys(ctx, nil)
}

func (q *Queue) Ack(ctx context.Context, lease protocol.TaskLease) error {
	return q.client.Ack(ctx, lease.Token)
}

func (q *Queue) Nack(ctx context.Context, lease protocol.TaskLease) error {
	return q.client.Nack(ctx, lease.Token)
}

func (q *Queue) EnqueueSticky(ctx context.Context, workerIdentity string, task protocol.Task) error {
	routingKey, err := stickyRoutingKey(workerIdentity)
	if err != nil {
		return err
	}
	task.WorkflowTaskRoute = protocol.WorkflowTaskRouteSticky
	task.StickyTarget = workerIdentity
	return q.enqueueWithRoutingOptions(ctx, task, taskqfrontend.EnqueueOptions{
		RoutingKey:      routingKey,
		RoutingDeadline: task.StickyScheduleToStartDeadline,
	})
}

func (q *Queue) DequeueForWorker(ctx context.Context, workerIdentity string) (protocol.TaskLease, error) {
	if workerIdentity == "" {
		return q.Dequeue(ctx)
	}
	routingKey, err := stickyRoutingKey(workerIdentity)
	if err != nil {
		return protocol.TaskLease{}, err
	}
	return q.dequeueWithRoutingKeys(ctx, []string{routingKey})
}

func (q *Queue) enqueueWithRoutingKey(ctx context.Context, task protocol.Task, routingKey string) error {
	return q.enqueueWithRoutingOptions(ctx, task, taskqfrontend.EnqueueOptions{RoutingKey: routingKey})
}

func (q *Queue) enqueueWithRoutingOptions(ctx context.Context, task protocol.Task, opts taskqfrontend.EnqueueOptions) error {
	data, err := marshalTask(task)
	if err != nil {
		return err
	}
	return q.client.EnqueueWithOptions(ctx, data, opts)
}

func (q *Queue) dequeueWithRoutingKeys(ctx context.Context, routingKeys []string) (protocol.TaskLease, error) {
	lease, err := q.client.DequeueWithOptions(ctx, taskqfrontend.PollOptions{AcceptedRoutingKeys: routingKeys})
	if errors.Is(err, taskqfrontend.ErrNoTask) {
		// 长轮询到期没有任务：与 HTTP 的 204 是同一个信号。
		return protocol.TaskLease{}, protocol.ErrTaskPollTimeout
	}
	if err != nil {
		return protocol.TaskLease{}, err
	}
	taskLease, err := decodeTaskLease(lease.Data, lease.Token)
	if err != nil {
		// taskq 已派发任务并签发 token：直接返回会让它隐形到 visibility timeout（默认 60s）
		// 才被重投。语义上 decode 失败更接近「从未交付给消费者」（ReturnLease），但
		// frontend.Frontend 只导出 Ack/Nack，Undelivered 没有导出入口，故用 Nack 立即重投。
		// decode 失败通常是持久性的，消耗 attempt 反而能让坏消息最终被丢弃而非无限复活。
		if nackErr := q.client.Nack(ctx, lease.Token); nackErr != nil {
			return protocol.TaskLease{}, errors.Join(err, fmt.Errorf("nack undecodable workflow task lease: %w", nackErr))
		}
		return protocol.TaskLease{}, err
	}
	if lease.RoutingFallback {
		taskLease.Task.WorkflowTaskRoute = protocol.WorkflowTaskRouteNormal
		taskLease.Task.StickyTarget = ""
	}
	return taskLease, nil
}

func marshalTask(task protocol.Task) ([]byte, error) {
	data, err := json.Marshal(task)
	if err != nil {
		return nil, fmt.Errorf("marshal workflow task: %w", err)
	}
	return data, nil
}

// decodeTaskLease 解码队列里的任务；Activity 输入、心跳详情等载荷保留数字的精确值，与内存队列一致。
func decodeTaskLease(data []byte, token string) (protocol.TaskLease, error) {
	var task protocol.Task
	if err := jsonvalue.Unmarshal(data, &task); err != nil {
		return protocol.TaskLease{}, fmt.Errorf("unmarshal workflow task: %w", err)
	}
	return protocol.TaskLease{Task: task, Token: token}, nil
}
