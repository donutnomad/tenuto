package engine

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
)

// 本文件覆盖 sticky 回落扫描从 "列出全部 run + N 次 GetWorkflowExecutionState"
// 换成 "1 次定向查询" 之后的行为。
//
// 原实现的问题：列出全部 run 无 WHERE 无 LIMIT，扫**所有历史 run**（该表从不清理），
// 且对每个 run 还逐个 GetWorkflowExecutionState —— 共 1+N 次查询，
// 而这段代码由每个空闲 worker 每秒触发一次。实测天花板约 5k-10k **累计** run。

// countingStickyScanManager 在 stickyDeadlineTestManager 之上：
//   - 统计 List / GetWorkflowExecutionState / ListExpiredStickyExecutions 的调用次数；
//   - 额外塞入 extraRuns 条"无 sticky 的历史 run"，模拟从不清理的 run 表。
//
// 定向查询的实现刻意**只返回真正过期的那条**，与 gormstore 的 WHERE 语义对齐；
// 若引擎侧仍走老路径，List/GetWorkflowExecutionState 的计数就会暴露出来。
type countingStickyScanManager struct {
	*stickyDeadlineTestManager

	mu            sync.Mutex
	listCalls     int
	getStateCalls int
	scanCalls     int
	lastScanLimit int
}

func (m *countingStickyScanManager) List(filter execution.StoreListFilter) (execution.StoreListResult, error) {
	m.mu.Lock()
	m.listCalls++
	m.mu.Unlock()
	return m.stickyDeadlineTestManager.List(filter)
}

func (m *countingStickyScanManager) GetWorkflowExecutionState(runID string) (*execution.WorkflowExecutionState, error) {
	m.mu.Lock()
	m.getStateCalls++
	m.mu.Unlock()
	return m.stickyDeadlineTestManager.GetWorkflowExecutionState(runID)
}

func (m *countingStickyScanManager) ListExpiredStickyExecutions(now time.Time, limit int) ([]*execution.WorkflowExecutionState, error) {
	m.mu.Lock()
	m.scanCalls++
	m.lastScanLimit = limit
	m.mu.Unlock()

	return m.stickyDeadlineTestManager.ListExpiredStickyExecutions(now, limit)
}

func (m *countingStickyScanManager) counts() (list, getState, scan int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.listCalls, m.getStateCalls, m.scanCalls
}

// newCountingStickyScanManager 建一个带 1 条过期 sticky run + extraRuns 条无关历史 run 的 mgr。
func newCountingStickyScanManager(t *testing.T, extraRuns int) (*countingStickyScanManager, *execution.Run) {
	t.Helper()
	run := &execution.Run{ID: "run-sticky-scan", WorkflowID: "wf-sticky-scan", WorkflowType: "wf-sticky-scan", Status: history.StatusRunning}
	expired := time.Now().Add(-time.Second)
	base := newStickyDeadlineTestManager(run, &execution.WorkflowExecutionState{
		RunID:                   run.ID,
		Status:                  history.StatusRunning,
		ExecutionGeneration:     1,
		StickyGeneration:        3,
		StickyWorkerIdentity:    "owner-worker",
		StickyWorkerBuildID:     "workflow-v1",
		StickyExpiresAt:         &expired,
		OpenWorkflowTaskEventID: 42,
	})
	// 历史 run：它们没有 sticky，老路径仍会对每一条发一次 GetWorkflowExecutionState。
	for i := range extraRuns {
		_ = base.RunInTx(func(tx execution.ExecutionTx) error {
			return tx.CreateRun(WithRootBranch(&execution.Run{
				ID: fmt.Sprintf("historical-run-%d", i), WorkflowID: "wf-old", WorkflowType: "wf-old", Status: history.StatusCompleted,
			}))
		})
	}
	return &countingStickyScanManager{stickyDeadlineTestManager: base}, run
}

// TestStickyFallbackUsesTargetedScan 是问题 3 的核心断言：
// 一轮回落必须**恰好 1 次**定向查询，
// 且 List 与 GetWorkflowExecutionState 的调用次数为 **0** —— 即不再是 1+N。
//
// 只断言"结果正确"是不够的：老实现的结果也正确，正是它的 1+N 次查询造成了瓶颈。
func TestStickyFallbackUsesTargetedScan(t *testing.T) {
	const historicalRuns = 50
	mgr, run := newCountingStickyScanManager(t, historicalRuns)
	queue := &nonDeadlineStickyQueue{}
	server := assembleServer(mgr, queue, newTestTaskQueue())

	if err := server.FallbackExpiredStickyWorkflowTasks(context.Background()); err != nil {
		t.Fatalf("FallbackExpiredStickyWorkflowTasks: %v", err)
	}

	list, getState, scan := mgr.counts()
	if scan != 1 {
		t.Fatalf("ListExpiredStickyExecutions 调用次数 = %d, want 1", scan)
	}
	if list != 0 {
		t.Fatalf("List 调用次数 = %d, want 0——仍在全表扫所有历史 run（问题 3 回归）", list)
	}
	// 扫描阶段必须 0 次单查。仍会有 1 次，来自 dispatchWorkflowTask 为**被重投的那条**
	// 读 sticky 路由信息（server.go 中 wfQueue 是 StickyWorkflowTaskQueue 时的分支）——
	// 那是 O(真正过期数)，不是 O(累计 run 数)，与本问题无关。
	// 故上界取"过期条数"（本用例 1 条），而不是 0：>1 即说明又在按 run 逐个单查。
	const expiredRuns = 1
	if getState > expiredRuns {
		t.Fatalf("GetWorkflowExecutionState 调用次数 = %d, want <= %d——仍在逐个单查（%d 条历史 run，问题 3 回归）",
			getState, expiredRuns, historicalRuns)
	}
	if mgr.lastScanLimit != stickyFallbackScanLimit {
		t.Fatalf("扫描 limit = %d, want %d——单轮必须有封顶", mgr.lastScanLimit, stickyFallbackScanLimit)
	}

	// 语义未被优化破坏：sticky 确实被清除，且 task 被重投（不能因为"没查到"就静默跳过）。
	// 经 base 读，避免这次校验自身污染上面的调用计数。
	state, err := mgr.stickyDeadlineTestManager.GetWorkflowExecutionState(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if state.StickyWorkerIdentity != "" {
		t.Fatalf("sticky 未被清除: identity=%q", state.StickyWorkerIdentity)
	}
	if queue.normalEnqueues != 1 {
		t.Fatalf("task 重投次数 = %d, want 1——sticky 已被不可逆清除，本轮不重投即永久孤立",
			queue.normalEnqueues)
	}
}

// TestStickyFallbackScanScalesWithExpiredNotTotalRuns 直接钉住"不再随累计 run 数增长"：
// 把历史 run 从 10 条加到 500 条，查询次数必须**完全不变**。
// 老实现下 GetWorkflowExecutionState 会从 11 次涨到 501 次。
func TestStickyFallbackScanScalesWithExpiredNotTotalRuns(t *testing.T) {
	queryCountFor := func(extraRuns int) (list, getState, scan int) {
		mgr, _ := newCountingStickyScanManager(t, extraRuns)
		server := assembleServer(mgr, &nonDeadlineStickyQueue{}, newTestTaskQueue())
		if err := server.FallbackExpiredStickyWorkflowTasks(context.Background()); err != nil {
			t.Fatalf("FallbackExpiredStickyWorkflowTasks(extraRuns=%d): %v", extraRuns, err)
		}
		return mgr.counts()
	}

	smallList, smallGetState, smallScan := queryCountFor(10)
	largeList, largeGetState, largeScan := queryCountFor(500)

	if smallList != largeList || smallGetState != largeGetState || smallScan != largeScan {
		t.Fatalf("查询次数随累计 run 数增长（问题 3 回归）:\n"+
			"  10 条历史 run: list=%d getState=%d scan=%d\n"+
			" 500 条历史 run: list=%d getState=%d scan=%d",
			smallList, smallGetState, smallScan, largeList, largeGetState, largeScan)
	}
	if largeScan != 1 {
		t.Fatalf("定向查询次数 = %d, want 1", largeScan)
	}
}

// TestStickyFallbackSkipsWhenStickyStillValid 锁定筛选判据没有被放宽：
// sticky **仍然有效**时不得回落（否则会把活跃的 sticky 会话打断）。
func TestStickyFallbackSkipsWhenStickyStillValid(t *testing.T) {
	run := &execution.Run{ID: "run-sticky-valid", WorkflowID: "wf-sticky-valid", WorkflowType: "wf-sticky-valid", Status: history.StatusRunning}
	valid := time.Now().Add(time.Hour)
	base := newStickyDeadlineTestManager(run, &execution.WorkflowExecutionState{
		RunID:                   run.ID,
		Status:                  history.StatusRunning,
		ExecutionGeneration:     1,
		StickyGeneration:        3,
		StickyWorkerIdentity:    "owner-worker",
		StickyWorkerBuildID:     "workflow-v1",
		StickyExpiresAt:         &valid,
		OpenWorkflowTaskEventID: 42,
	})
	mgr := &countingStickyScanManager{stickyDeadlineTestManager: base}
	queue := &nonDeadlineStickyQueue{}
	server := assembleServer(mgr, queue, newTestTaskQueue())

	if err := server.FallbackExpiredStickyWorkflowTasks(context.Background()); err != nil {
		t.Fatalf("FallbackExpiredStickyWorkflowTasks: %v", err)
	}

	state, err := base.GetWorkflowExecutionState(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if state.StickyWorkerIdentity != "owner-worker" {
		t.Fatalf("有效的 sticky 被误清: identity=%q", state.StickyWorkerIdentity)
	}
	if queue.normalEnqueues != 0 || queue.stickyEnqueues != 0 {
		t.Fatalf("有效 sticky 不应触发重投: normal=%d sticky=%d", queue.normalEnqueues, queue.stickyEnqueues)
	}
}
