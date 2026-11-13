package persistence

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/infra/persistence/executionrepo"
)

// dedupeKeyOf 返回事件的 DedupeKey（测试断言 DB 里该 key 的落库条数用）。
func dedupeKeyOf(t *testing.T, event history.Event) string {
	t.Helper()
	key, ok := history.DedupeKey(event)
	if !ok {
		t.Fatalf("event type %s has no dedupe key", event.EventType())
	}
	return key
}

// countEventsByDedupeKey 直查底层 workflow_events 表，统计某 (branchID, dedupeKey) 的落库条数。
func countEventsByDedupeKey(t *testing.T, store *Store, branchID, dedupeKey string) int64 {
	t.Helper()
	var count int64
	if err := store.db.Model(&executionrepo.EventPO{}).
		Where("branch_id = ? AND dedupe_key = ?", branchID, dedupeKey).
		Count(&count).Error; err != nil {
		t.Fatal(err)
	}
	return count
}

// TestGormAppendIdempotent_ConcurrentSameDedupeKey_ReturnsSingleEvent
// Integration (-race): N 个 goroutine 用同一 DedupeKey（同 runID/branch）并发调 AppendIdempotent，
// 验证 ON DUPLICATE KEY UPDATE 的撞键路径：DB 里恰好落一条事件，恰好一个 inserted=true，
// 其余 inserted=false 且都返回同一条既有 NodeID（非 0）。把"撞键返回既有 NodeID"从
// schema 唯一索引的静态保证升级为回归测试。goroutine 在 MySQL 连接池上物理并行，
// 在 execution state 行锁上排队。
func TestGormAppendIdempotent_ConcurrentSameDedupeKey_ReturnsSingleEvent(t *testing.T) {
	store := newTestStore(t)
	const runID = "run-idem-concurrent"
	createTestRun(t, store, runID)

	const n = 20
	const updateID = "up-idem-concurrent"
	event := history.NewWorkflowUpdateAcceptedEvent("set_priority", updateID, nil)
	dedupeKey := dedupeKeyOf(t, event)

	var (
		wg          sync.WaitGroup
		insertedCnt atomic.Int64
		nodeIDs     = make([]int64, n)
		errs        = make([]error, n)
		start       = make(chan struct{})
	)
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start // 尽量同时起跑，制造并发窗口
			var (
				nodeID   int64
				inserted bool
			)
			err := store.Executions.RunInTx(func(tx execution.ExecutionTx) error {
				id, ins, e := tx.AppendIdempotent(runID, runID, runID,
					history.NewWorkflowUpdateAcceptedEvent("set_priority", updateID, nil))
				nodeID, inserted = id, ins
				return e
			})
			errs[i] = err
			nodeIDs[i] = nodeID
			if err == nil && inserted {
				insertedCnt.Add(1)
			}
		}()
	}
	close(start)
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("goroutine %d AppendIdempotent failed: %v", i, err)
		}
	}

	// ① DB 里该 DedupeKey 恰好一条事件。
	if got := countEventsByDedupeKey(t, store, runID, dedupeKey); got != 1 {
		t.Fatalf("concurrent same dedupe key must persist exactly ONE event, got %d", got)
	}

	// ③ 恰好一个 inserted=true，其余 inserted=false。
	if got := insertedCnt.Load(); got != 1 {
		t.Fatalf("exactly one caller must observe inserted=true, got %d", got)
	}

	// ② 所有 goroutine 拿到的 NodeID 相同且非 0（inserted=true 那条与 inserted=false 那些都返回同一 NodeID）。
	want := nodeIDs[0]
	if want == 0 {
		t.Fatalf("returned NodeID must be non-zero, got 0")
	}
	for i, id := range nodeIDs {
		if id != want {
			t.Fatalf("goroutine %d returned NodeID %d, want %d (all callers must share the single existing NodeID)", i, id, want)
		}
	}
}

// TestGormAppendIdempotent_SecondSameDedupeKey_ReturnsExistingNodeID
// Integration: 顺序两次以同一 DedupeKey 调 AppendIdempotent（确定性，不依赖调度时序）。
// 第一次 inserted=true 落库；第二次命中 OnConflict(DoNothing)，inserted=false 且必须返回
// 第一次那条的既有非零 NodeID——直接钉住"冲突回查 existing.NodeID"这条返回路径。
func TestGormAppendIdempotent_SecondSameDedupeKey_ReturnsExistingNodeID(t *testing.T) {
	store := newTestStore(t)
	const runID = "run-idem-sequential"
	createTestRun(t, store, runID)

	const updateID = "up-idem-sequential"
	newEvent := func() history.Event {
		return history.NewWorkflowUpdateAcceptedEvent("set_priority", updateID, nil)
	}
	dedupeKey := dedupeKeyOf(t, newEvent())

	var firstNodeID int64
	if err := store.Executions.RunInTx(func(tx execution.ExecutionTx) error {
		id, inserted, e := tx.AppendIdempotent(runID, runID, runID, newEvent())
		if e != nil {
			return e
		}
		if !inserted {
			t.Fatalf("first append must insert, got inserted=false")
		}
		if id == 0 {
			t.Fatalf("first append must return non-zero NodeID, got 0")
		}
		firstNodeID = id
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	var secondNodeID int64
	if err := store.Executions.RunInTx(func(tx execution.ExecutionTx) error {
		id, inserted, e := tx.AppendIdempotent(runID, runID, runID, newEvent())
		if e != nil {
			return e
		}
		if inserted {
			t.Fatalf("second append with same dedupe key must NOT insert, got inserted=true")
		}
		secondNodeID = id
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	// OnConflict 冲突时必须回查并返回既有非零 NodeID，且与第一次落库那条一致。
	if secondNodeID == 0 {
		t.Fatalf("conflicting append must return existing non-zero NodeID, got 0")
	}
	if secondNodeID != firstNodeID {
		t.Fatalf("conflicting append returned NodeID %d, want existing %d", secondNodeID, firstNodeID)
	}

	// DB 里恰好一条，确认第二次没落库。
	if got := countEventsByDedupeKey(t, store, runID, dedupeKey); got != 1 {
		t.Fatalf("duplicate dedupe key must persist exactly ONE event, got %d", got)
	}
}
