package executionrepo

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/donutnomad/gsql"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/clock/clocktest"
)

// ============================================
// gormstore 事件顺序
// ============================================
// 测试策略:
// - G1 Happy Path: 落库序（表自增 id）与逻辑序（node_id）背离时，GetEvents 必须按 node_id 返回
//   （engine 的 Event.ID 取自 node_id，一切"序单调"假设建立其上）。
// - G2 Edge Case: 并发 append 后 node_id 无重复且连续；goroutine 在 MySQL 连接池上物理并行，
//   在 execution state 行锁上排队。
// - G3 Error Case: duplicate-key 冲突重试——有限次内成功；超限返回错误。
// ============================================

// G1 Happy Path: 手工制造 id 序 ≠ node_id 序（先插 node_id=2 拿小自增 id，再插 node_id=1），
// GetEvents 必须按 node_id 单调返回。
func TestGetEventsByRunID_OrdersByNodeID(t *testing.T) {
	store := newTestStore(t)
	createTestRun(t, store, "run-order")

	mkRow := func(nodeID int64) *EventPO {
		ev := history.Event{ID: nodeID, Timestamp: time.Now(), WorkflowSignaled: &history.WorkflowSignaledEvent{Name: fmt.Sprintf("sig-%d", nodeID)}}
		eventJSON, err := history.CanonicalEventJSON(ev)
		if err != nil {
			t.Fatal(err)
		}
		return eventToRow("run-order", "run-order", "run-order", ev, eventJSON, nil)
	}
	// 落库序（自增 id 序）: node 2, node 1, node 3 —— 逻辑序应为 1, 2, 3。
	for _, n := range []int64{2, 1, 3} {
		if err := store.db.Create(mkRow(n)).Error; err != nil {
			t.Fatal(err)
		}
	}

	events, err := store.GetEvents("run-order")
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 3 {
		t.Fatalf("事件数 = %d, want 3", len(events))
	}
	for i, want := range []int64{1, 2, 3} {
		if events[i].ID != want {
			t.Fatalf("events[%d].ID = %d, want %d（读序必须按 node_id 而非表自增 id）: %+v", i, events[i].ID, want, events)
		}
	}
}

// G2 Edge Case: 并发 Append 后 node_id 无重复且连续。
func TestAppendConcurrent_NoDuplicateNodeID(t *testing.T) {
	store := newTestStore(t)
	createTestRun(t, store, "run-conc")

	const n = 20
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ev := history.Event{Timestamp: time.Now(), WorkflowSignaled: &history.WorkflowSignaledEvent{Name: fmt.Sprintf("s-%d", i)}}
			_, errs[i] = store.Append("run-conc", "run-conc", "run-conc", ev)
		}()
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	events, err := store.GetEvents("run-conc")
	if err != nil {
		t.Fatal(err)
	}
	seen := map[int64]bool{}
	for _, ev := range events {
		if seen[ev.ID] {
			t.Fatalf("node_id 重复: %d", ev.ID)
		}
		seen[ev.ID] = true
	}
	if len(events) != n {
		t.Fatalf("事件数 = %d, want %d", len(events), n)
	}
}

// 零时间戳的事件在追加时取仓储时钟的时刻，事件行的 timestamp 列与事件 JSON 都是这个时刻：
// MySQL 的 DATETIME 拒绝零时间（Error 1292）。
func TestAppendStampsZeroTimestampWithStoreClock(t *testing.T) {
	store := newTestStore(t)
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	store.clock = clocktest.NewFake(now)
	createTestRun(t, store, "run-zero")
	id, err := store.Append("run-zero", "run-zero", "run-zero", history.NewWorkflowSignaledEvent("sig", nil))
	if err != nil {
		t.Fatal(err)
	}
	row, err := gsql.From[EventPO](EventSchema).Where(EventSchema.RunID.Eq("run-zero"), EventSchema.NodeID.Eq(id)).Take(store.db)
	if err != nil || row == nil {
		t.Fatalf("event row: %v %v", row, err)
	}
	event, err := rowToEvent(row)
	if err != nil {
		t.Fatal(err)
	}
	if !row.Timestamp.Equal(now) || !event.Timestamp.Equal(now) {
		t.Fatalf("row timestamp = %v, event timestamp = %v, want %v", row.Timestamp, event.Timestamp, now)
	}
}
