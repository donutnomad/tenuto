package engine

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/program/programtest"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

// 没有 Worker 领取的任务（Worker 全部下线、积压）由 redeliverUnclaimedTask 补投，等待按派发次数翻倍、
// 不在 16×grace 处封顶：队列里的份数随等待时长对数增长。
func TestUnclaimedTaskCopiesGrowLogarithmically(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	workflowTasks := memory.NewTaskQueue(4096)
	srv := assembleServer(mgr, workflowTasks, memory.NewTaskQueue(16))
	t.Cleanup(srv.Stop)
	startReconcileTestRun(t, srv, &program.WorkflowConfig{Type: "redelivery-growth", Program: programtest.Noop()})

	// grace 取 2ms（生产 30s），没有任何 Worker 轮询，等待 2s（1000×grace）。
	const grace = 2 * time.Millisecond
	const waited = 2 * time.Second
	deadline := time.Now().Add(waited)
	for time.Now().Before(deadline) {
		if _, _, err := srv.Reconcile(ctx, grace, 0); err != nil {
			t.Fatal(err)
		}
		time.Sleep(grace)
	}
	copies := 0
	for {
		lease, ok := takeQueued(workflowTasks)
		if !ok || copies > 1000 {
			break
		}
		_ = workflowTasks.Ack(ctx, lease)
		copies++
	}
	// 第 k 次补投发生在累计等待 grace·(2^k−1) 之后：份数不超过 log2(waited/grace)+2。
	limit := int(math.Log2(float64(waited/grace))) + 2
	if copies == 0 || copies > limit {
		t.Fatalf("queued copies of one unclaimed workflow task after %v = %d, want 1..%d (logarithmic in the wait)", waited, copies, limit)
	}
}
