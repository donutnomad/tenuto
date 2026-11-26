package engine

import (
	"context"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

// Happy Path（崩溃恢复）: 写入 ActivityState 后"重启"（新建 Server 复用同一 mgr，内存 timer 清空），
// recovery 直接从 state 表读回并重建 timer/重派——不依赖事件历史。
func TestRecoveryRebuildsFromState(t *testing.T) {
	mgr := memory.NewExecutionManager() // 同一持久化 mgr 跨"重启"复用（engine 包内真原子）
	srv := assembleServer(mgr, newTestTaskQueue(), newTestTaskQueue())
	runID, schedID := scheduleRetryableActivity2(t, srv, mgr)
	_ = srv.HandleActivityStarted(runID, schedID, 1, 1, time.Second, 0)
	_ = srv.HandleActivityFailed(context.Background(), runID, schedID, 1, 1, protocol.Failure{Message: "boom"}) // attempt→2 scheduled
	srv.Stop()                                                                                                  // 模拟崩溃：内存 timer 清空

	// "重启"：同一持久化 mgr，新 Server（内存全空）。
	srv2 := assembleServer(mgr, newTestTaskQueue(), newTestTaskQueue())
	t.Cleanup(srv2.Stop)
	count, err := srv2.recoverActivityRetries(context.Background(), runID, true, testRecoveryHorizon)
	if err != nil {
		t.Fatal(err)
	}
	// recovery 不应丢失状态：activity 仍在 attempt=2 scheduled（直读 state，非扫历史）。
	st, ok, _ := mgr.GetActivityState(runID, schedID)
	if !ok || st.Attempt != 2 || st.IsTerminal() {
		t.Fatalf("recovered state wrong: %+v", st)
	}
	_ = count
}
