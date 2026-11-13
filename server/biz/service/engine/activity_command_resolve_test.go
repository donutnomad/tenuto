package engine

import (
	"sync/atomic"
	"testing"

	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

// runReadCounter 统计事务外读取 Run 行的次数（MySQL 上每次是一条 SELECT 加 JSON 解码）。
type runReadCounter struct {
	*memory.ExecutionManager
	gets atomic.Int64
}

func (m *runReadCounter) Get(runID string) (*execution.Run, error) {
	m.gets.Add(1)
	return m.ExecutionManager.Get(runID)
}

func newResolveActivityFixture(t testing.TB) (*Server, *runReadCounter, string) {
	mgr := &runReadCounter{ExecutionManager: memory.NewExecutionManager()}
	server := assembleServer(mgr, newTestTaskQueue(), newTestTaskQueue())
	t.Cleanup(server.Stop)
	registerChargeWorkerVersion(t, server, "pay-worker-v1", "pay@v1", "v1", "ChargeInput.v1", "Receipt.v1")
	runID, _ := startContractRunWithTarget(t, mgr, server, chargeWorkflowDeclaring("resolve-activity", "v1", "ChargeInput.v1", "Receipt.v1"), "pay-worker-v1")
	return server, mgr, runID
}

// 解析一个 ScheduleActivity 命令只读一次 Run：Namespace、目标 Worker 与配置引用取自同一次读取，路由结果不变。
func TestResolveActivityCommandReadsRunOnce(t *testing.T) {
	server, mgr, runID := newResolveActivityFixture(t)
	mgr.gets.Store(0)
	cmd, err := server.resolveActivityCommand(runID, "", scheduleChargeCommand()[0])
	if err != nil {
		t.Fatal(err)
	}
	if cmd.TaskQueue != "payments" || cmd.TargetWorkerIdentity != "pay-worker-v1" {
		t.Fatalf("route = %q/%q, want payments/pay-worker-v1", cmd.TaskQueue, cmd.TargetWorkerIdentity)
	}
	if got := mgr.gets.Load(); got != 1 {
		t.Fatalf("run reads = %d, want 1", got)
	}
	// Run 不存在：与此前相同，按缺省 Namespace、无目标 Worker、无声明契约解析。
	if _, err := server.resolveActivityCommand("missing-run", "", scheduleChargeCommand()[0]); err != nil {
		t.Fatalf("resolve for a missing run: %v", err)
	}
}

// BenchmarkResolveActivityCommand 是 Workflow Task 提交时解析每个 ScheduleActivity 命令的开销，run-reads/op 是事务外
// 读取 Run 行的次数。
func BenchmarkResolveActivityCommand(b *testing.B) {
	server, mgr, runID := newResolveActivityFixture(b)
	cmd := scheduleChargeCommand()[0]
	mgr.gets.Store(0)
	b.ReportAllocs()
	for b.Loop() {
		if _, err := server.resolveActivityCommand(runID, "", cmd); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(mgr.gets.Load())/float64(b.N), "run-reads/op")
}
