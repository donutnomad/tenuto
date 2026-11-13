package integration

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	retentionpkg "github.com/donutnomad/tenuto/server/biz/usecase/retention"
	"github.com/donutnomad/tenuto/server/infra/persistence/executionrepo"
	"github.com/donutnomad/tenuto/server/infra/systemclock"
)

// ============================================================================
// retention GC 吞吐（TENUTO_TEST_MYSQL=1）
// ============================================================================
// soak（三副本、每小时约 11 500 个 Run）里 GC 每轮只处理 Batch 棵、每分钟一轮，每分钟回收 100 棵，结束时
// 7 693 个删除任务过期未处理。本用例在 MySQL 上造 gcThroughputTrees 棵已到期的单 Run tree（每棵
// gcThroughputEvents 个事件），跑一轮 RunOnce，断言一轮全部删完，并在日志里给出每分钟回收的 tree 数与每棵
// tree 的语句数。
//
//	TENUTO_TEST_MYSQL=1 go test ./server/integration -run TestRetentionGCThroughput -count=1 -v
// ============================================================================

const (
	gcThroughputTrees  = 1000
	gcThroughputEvents = 8
	// gcThroughputFloorPerMinute 是吞吐下限：soak 的创建速度约每分钟 190 个 Run，下限取它的 5 倍。
	gcThroughputFloorPerMinute = 1000
)

type noopWorkflowLocker struct{}

func (noopWorkflowLocker) LockWorkflow(context.Context, string, string) (func(), error) {
	return func() {}, nil
}

type noopGCMetrics struct{}

func (noopGCMetrics) RecordGCReclaimed(context.Context, string, int)      {}
func (noopGCMetrics) RecordGCFailure(context.Context, string)             {}
func (noopGCMetrics) RecordGCBacklog(context.Context, int, time.Duration) {}

func TestRetentionGCThroughput(t *testing.T) {
	store, db := newGCStore(t, gcTestRetention)

	started := time.Now()
	// 8 个 goroutine 并行建树。
	trees := make(chan int)
	errs := make(chan error, gcThroughputTrees)
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for i := range trees {
				if err := createClosedGCTree(store.Executions, i); err != nil {
					errs <- err
				}
			}
		})
	}
	for i := range gcThroughputTrees {
		trees <- i
	}
	close(trees)
	wg.Wait()
	close(errs)
	if err := <-errs; err != nil {
		t.Fatal(err)
	}
	expireDeletionTasks(t, db)
	t.Logf("created %d closed trees in %s", gcThroughputTrees, time.Since(started).Round(time.Millisecond))

	counter := newStatementCounter()
	counter.attach(t, "gc", db)
	var logs syncBuffer
	task := retentionpkg.NewTask(store.Executions, noopWorkflowLocker{}, noopGCMetrics{}, systemclock.Clock{},
		retentionpkg.Config{ScanInterval: 10 * time.Minute}, slog.New(slog.NewTextHandler(&logs, nil)))
	counter.enabled.Store(true)
	scanStarted := time.Now()
	task.RunOnce(t.Context())
	elapsed := time.Since(scanStarted)
	counter.enabled.Store(false)

	var remaining int64
	if err := db.Model(&executionrepo.DeletionTaskPO{}).Count(&remaining).Error; err != nil {
		t.Fatal(err)
	}
	var runs int64
	if err := db.Model(&executionrepo.RunPO{}).Count(&runs).Error; err != nil {
		t.Fatal(err)
	}
	if remaining != 0 || runs != 0 {
		t.Fatalf("after one scan %d deletion tasks and %d runs remain, want 0; logs:\n%s", remaining, runs, logs.String())
	}
	counts, _ := counter.reset()
	perMinute := float64(gcThroughputTrees) / elapsed.Minutes()
	logStatementBreakdown(t, fmt.Sprintf("GC statements per tree (%d trees in %s, %.0f trees/min)",
		gcThroughputTrees, elapsed.Round(time.Millisecond), perMinute), counts, gcThroughputTrees)
	if perMinute < gcThroughputFloorPerMinute {
		t.Fatalf("GC reclaimed %.0f trees/min, want at least %d", perMinute, gcThroughputFloorPerMinute)
	}
}

// createClosedGCTree 建一棵单 Run 的 tree：gcThroughputEvents-1 个 Marker 加 WorkflowCompleted，关闭时投递删除任务。
func createClosedGCTree(executions execution.ExecutionManager, i int) error {
	runID, treeID, branchID := fmt.Sprintf("gc-run-%05d", i), fmt.Sprintf("gc-tree-%05d", i), fmt.Sprintf("gc-branch-%05d", i)
	token, err := execution.SerializeBranchToken(&execution.HistoryBranch{Version: 1, TreeID: treeID, BranchID: branchID})
	if err != nil {
		return err
	}
	run := &execution.Run{
		ID: runID, Namespace: "default", WorkflowID: "gc-wf-" + runID,
		WorkflowConfigVersion: "v1", Status: history.StatusRunning, BranchToken: token,
	}
	if err := executions.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(run) }); err != nil {
		return err
	}
	for e := range gcThroughputEvents - 1 {
		if _, err := executions.Append(runID, treeID, branchID, history.NewMarkerRecordedEvent(fmt.Sprint("m", e), "x", nil)); err != nil {
			return err
		}
	}
	_, err = executions.Append(runID, treeID, branchID, history.NewWorkflowCompletedEvent(nil))
	return err
}

// syncBuffer 是可并发写的日志缓冲。
type syncBuffer struct {
	mu  sync.Mutex
	buf []byte
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.buf = append(b.buf, p...)
	return len(p), nil
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return string(b.buf)
}
