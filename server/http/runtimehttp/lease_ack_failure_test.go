package runtimehttp

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	taskqfrontend "github.com/donutnomad/taskq/frontend"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/runtimeapi"
	"github.com/donutnomad/tenuto/server/app/assembly"
	"github.com/donutnomad/tenuto/server/biz/domain/dispatch"
	"github.com/donutnomad/tenuto/server/biz/service/engine"
	"github.com/donutnomad/tenuto/server/infra/memory"
	"github.com/donutnomad/tenuto/server/infra/taskqueue/taskq"
	tenutotel "github.com/donutnomad/tenuto/telemetry"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// failingLeaseQueue 的 Ack 与 Nack 返回固定的错误，模拟 TaskQ 分区换主期间的确认失败。
type failingLeaseQueue struct {
	*runtimeHTTPTestQueue
	ackErr, nackErr error
}

func (q *failingLeaseQueue) Ack(context.Context, protocol.TaskLease) error  { return q.ackErr }
func (q *failingLeaseQueue) Nack(context.Context, protocol.TaskLease) error { return q.nackErr }

type failingLeaseRouter struct {
	runtimeHTTPActivityRouter
	queue *failingLeaseQueue
}

func (r failingLeaseRouter) Ack(ctx context.Context, lease protocol.TaskLease) error {
	return r.queue.Ack(ctx, lease)
}

func (r failingLeaseRouter) Nack(ctx context.Context, lease protocol.TaskLease) error {
	return r.queue.Nack(ctx, lease)
}

// lockedBuffer 是并发安全的日志输出：Server 在 HTTP 处理 goroutine 里写日志，测试在应答之后读。
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// soak 里的两种确认失败：分区换主期间重试用完（TaskQ 返回 ErrRetriesExhausted 与最后一次的 FailedPrecondition），
// 以及租约属于旧的 ownership epoch（InvalidArgument）。
var (
	errAckRetriesExhausted = fmt.Errorf("frontend: ack: %w: %w", taskqfrontend.ErrRetriesExhausted,
		status.Error(codes.FailedPrecondition, "matching: not owner of partition"))
	errAckStaleToken = fmt.Errorf("frontend: ack: %w",
		status.Error(codes.InvalidArgument, "stale ack token: token epoch 3 does not match queue epoch 4"))
)

// Worker 只在结局落库之后确认租约（Activity 的 attempt started、WFT 的结果上报），确认失败不改变结局：
// /runtime/activity/ack 与 /runtime/workflow-task/ack 应答 200，Server 记 WARN。修复前两者都是 500。
func TestRuntimeHTTPAckFailureAfterSettledOutcomeIsNotAnError(t *testing.T) {
	for name, ackErr := range map[string]error{"retries exhausted": errAckRetriesExhausted, "stale token": errAckStaleToken} {
		t.Run(name, func(t *testing.T) {
			httpServer, codec, logs := newFailingLeaseHTTPServer(t, ackErr, nil)

			for _, ack := range []struct {
				path  string
				lease protocol.TaskLease
				req   func(protocol.TaskLease) any
			}{
				{
					path:  runtimeapi.ActivityRuntimeAckPath,
					lease: protocol.TaskLease{Task: protocol.Task{Type: protocol.TaskTypeActivity, RunID: "run-ack", ScheduledEventID: 7, Attempt: 1}, Token: "activity-lease"},
					req:   func(lease protocol.TaskLease) any { return runtimeapi.ActivityRuntimeLeaseRequest{Lease: lease} },
				},
				{
					path:  runtimeapi.WorkflowTaskRuntimeAckPath,
					lease: protocol.TaskLease{Task: protocol.Task{Type: protocol.TaskTypeWorkflow, RunID: "run-ack", ScheduledEventID: 3}, Token: "workflow-lease"},
					req:   func(lease protocol.TaskLease) any { return runtimeapi.WorkflowTaskRuntimeLeaseRequest{Lease: lease} },
				},
			} {
				signed, err := signRuntimeTaskLease(codec, ack.lease)
				if err != nil {
					t.Fatal(err)
				}
				if err := wirePost(context.Background(), httpServer.URL, ack.path, ack.req(signed), nil); err != nil {
					t.Fatalf("%s: %v, want 200", ack.path, err)
				}
			}
			if got := strings.Count(logs.String(), "task queue ack failed"); got != 2 {
				t.Fatalf("ack failure WARN logged %d times, want 2; logs:\n%s", got, logs)
			}
		})
	}
}

// 归还租约（Nack）发生在结局落库之前：TaskQ 重试用完是暂时性失败，应答 503 与 Retry-After，Worker 按既有策略重试。
// 修复前错误不带原因，是 500。
func TestRuntimeHTTPNackRetriesExhaustedIsUnavailable(t *testing.T) {
	nackErr := fmt.Errorf("frontend: nack: %w: %w", taskqfrontend.ErrRetriesExhausted,
		status.Error(codes.FailedPrecondition, "matching: not owner of partition"))
	httpServer, codec, _ := newFailingLeaseHTTPServer(t, nil, nackErr)

	for _, nack := range []struct {
		path  string
		lease protocol.TaskLease
		req   func(protocol.TaskLease) any
	}{
		{
			path:  runtimeapi.ActivityRuntimeNackPath,
			lease: protocol.TaskLease{Task: protocol.Task{Type: protocol.TaskTypeActivity, RunID: "run-nack", ScheduledEventID: 7, Attempt: 1}, Token: "activity-lease"},
			req:   func(lease protocol.TaskLease) any { return runtimeapi.ActivityRuntimeLeaseRequest{Lease: lease} },
		},
		{
			path:  runtimeapi.WorkflowTaskRuntimeNackPath,
			lease: protocol.TaskLease{Task: protocol.Task{Type: protocol.TaskTypeWorkflow, RunID: "run-nack", ScheduledEventID: 3}, Token: "workflow-lease"},
			req:   func(lease protocol.TaskLease) any { return runtimeapi.WorkflowTaskRuntimeLeaseRequest{Lease: lease} },
		},
	} {
		signed, err := signRuntimeTaskLease(codec, nack.lease)
		if err != nil {
			t.Fatal(err)
		}
		var statusErr *wireStatusError
		if err := wirePost(context.Background(), httpServer.URL, nack.path, nack.req(signed), nil); !errors.As(err, &statusErr) || statusErr.code != http.StatusServiceUnavailable {
			t.Fatalf("%s: %v, want 503", nack.path, err)
		}
	}
}

func newFailingLeaseHTTPServer(t *testing.T, ackErr, nackErr error) (*httptest.Server, *dispatch.TaskTokenCodec, *lockedBuffer) {
	t.Helper()
	logs := &lockedBuffer{}
	providers := tenutotel.NoopProviders()
	providers.Logger = slog.New(slog.NewTextHandler(logs, nil))
	workflowQueue := &failingLeaseQueue{runtimeHTTPTestQueue: newRuntimeHTTPTestQueue(), ackErr: ackErr, nackErr: nackErr}
	activityQueue := &failingLeaseQueue{runtimeHTTPTestQueue: newRuntimeHTTPTestQueue(), ackErr: ackErr, nackErr: nackErr}
	codec := testTaskTokens()
	server := assembly.NewServer(assembly.Options{
		Stores:         assembly.Stores{Executions: memory.NewExecutionManager()},
		WorkflowQueue:  workflowQueue,
		ActivityRouter: failingLeaseRouter{runtimeHTTPActivityRouter: runtimeHTTPActivityRouter{queue: activityQueue.runtimeHTTPTestQueue}, queue: activityQueue},
		TaskTokens:     codec,
	}, engine.WithTelemetry(providers))
	t.Cleanup(server.Stop)
	// 组合根传入的分类：TaskQ 重试用完是暂时性失败。
	httpServer := httptest.NewServer(NewRuntimeHTTPHandler(server, WithTransientErrors(taskq.IsTransient)))
	t.Cleanup(httpServer.Close)
	return httpServer, codec, logs
}
