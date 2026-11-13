package engine

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/biz/usecase/retention"
	"github.com/donutnomad/tenuto/server/infra/systemclock"
)

// 回收编排的单元测试在 biz/usecase/retention；本文件只保留需要 Server 的部分：
// *Server 作为 retention.WorkflowLocker 时取的是 keyedMu，与用户操作共用同一把锁。

// fakeGCStore 是 RetentionGCStore 的可编程测试替身。
type fakeGCStore struct {
	mu sync.Mutex

	due []execution.DeletionTask
	// deleteErr 按 treeID 决定 DeleteTree 的返回值。
	deleteErr map[string]error
	// deleted 记录被成功删除的 tree，按顺序。
	deleted []string
	// keys 决定 TreeWorkflowKeys 的返回值。
	keys map[string][]execution.WorkflowKey
	// deferred 记录推后调用。
	deferred []deferCall
	// onDelete 在 DeleteTree 内部回调，用于观察持锁期间的状态。
	onDelete func(treeID string)
}

type deferCall struct {
	treeID       string
	fireAt       time.Time
	countAttempt bool
	cause        error
}

var errNotTerminalFake = errors.New("fake: tree not terminal")

func (f *fakeGCStore) ScavengeOrphans(time.Time, time.Duration, int) (execution.ScavengeResult, error) {
	return execution.ScavengeResult{}, nil
}

func (f *fakeGCStore) ListDueDeletionTasks(now time.Time, limit int) ([]execution.DeletionTask, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if limit > len(f.due) {
		limit = len(f.due)
	}
	return append([]execution.DeletionTask(nil), f.due[:limit]...), nil
}

func (f *fakeGCStore) DeleteTree(treeID string) error {
	if f.onDelete != nil {
		f.onDelete(treeID)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if err, ok := f.deleteErr[treeID]; ok && err != nil {
		return err
	}
	f.deleted = append(f.deleted, treeID)
	return nil
}

func (f *fakeGCStore) DeferDeletionTask(treeID string, fireAt time.Time, countAttempt bool, cause error) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deferred = append(f.deferred, deferCall{treeID, fireAt, countAttempt, cause})
	return nil
}

func (f *fakeGCStore) DeletionBacklog(time.Time) (execution.DeletionBacklog, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.due) == 0 {
		return execution.DeletionBacklog{}, nil
	}
	return execution.DeletionBacklog{Overdue: len(f.due), OldestFireAt: f.due[0].FireAt}, nil
}

func (f *fakeGCStore) TreeWorkflowKeys(treeID string) ([]execution.WorkflowKey, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.keys[treeID], nil
}

func (f *fakeGCStore) IsTreeNotTerminal(err error) bool {
	return errors.Is(err, errNotTerminalFake)
}

func (f *fakeGCStore) snapshotDeleted() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.deleted...)
}

func (f *fakeGCStore) snapshotDeferred() []deferCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]deferCall(nil), f.deferred...)
}

// fakeScavengeStore 记录 ScavengeOrphans 的调用，
// 供 telemetry_metrics_test.go 驱动兜底扫描的指标。
type fakeScavengeStore struct {
	fakeGCStore

	scavengeMu sync.Mutex
	calls      []scavengeCall
	result     execution.ScavengeResult
	err        error
}

type scavengeCall struct {
	now    time.Time
	minAge time.Duration
	limit  int
}

func (f *fakeScavengeStore) ScavengeOrphans(now time.Time, minAge time.Duration, limit int) (execution.ScavengeResult, error) {
	f.scavengeMu.Lock()
	defer f.scavengeMu.Unlock()
	f.calls = append(f.calls, scavengeCall{now, minAge, limit})
	return f.result, f.err
}

func (f *fakeScavengeStore) snapshotCalls() []scavengeCall {
	f.scavengeMu.Lock()
	defer f.scavengeMu.Unlock()
	return append([]scavengeCall(nil), f.calls...)
}

func newGCTestServer(t *testing.T) *Server {
	t.Helper()
	return &Server{keyedMu: NewKeyedLock()}
}

// ── GC 必须拿 keyedMu ──
//
// Temporal 的 PriorityLow 是**任务调度优先级**，不是「不加锁」。删除期间必须与该
// workflowID 的用户操作互斥，否则并发的 signal/query/reset 会读到删了一半的状态。
func TestRetentionGCHoldsWorkflowLockDuringDeletion(t *testing.T) {
	server := newGCTestServer(t)
	key := workflowLockKey("default", "wf-lock")

	lockedDuringDelete := make(chan bool, 1)
	store := &fakeGCStore{
		keys: map[string][]execution.WorkflowKey{
			"tree-1": {{Namespace: "default", WorkflowID: "wf-lock"}},
		},
		due: []execution.DeletionTask{{TreeID: "tree-1"}},
	}
	store.onDelete = func(string) {
		// 删除进行中，另一方必须拿不到同一把锁。
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		_, err := server.keyedMu.Lock(ctx, key)
		lockedDuringDelete <- err == nil
	}

	task := retention.NewTask(store, server, server, systemclock.Clock{}, retention.Config{}, slog.New(slog.DiscardHandler))
	task.RunOnce(t.Context())

	if acquired := <-lockedDuringDelete; acquired {
		t.Fatal("a concurrent workflow operation acquired the keyed lock while GC was deleting; " +
			"signal/query/reset could observe a half-deleted state")
	}
	if got := store.snapshotDeleted(); len(got) != 1 || got[0] != "tree-1" {
		t.Fatalf("deleted = %v, want [tree-1]", got)
	}

	// 删除结束后锁必须已释放，否则该 workflowID 被永久锁死。
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	release, err := server.keyedMu.Lock(ctx, key)
	if err != nil {
		t.Fatalf("keyed lock was not released after GC finished: %v", err)
	}
	release()
	if n := server.keyedMu.EntryCount(); n != 0 {
		t.Fatalf("keyed lock entries leaked: %d", n)
	}
}

// ── 一棵 tree 横跨多个 workflowID 时全部上锁 ──

func TestRetentionGCLocksEveryWorkflowOnTheTree(t *testing.T) {
	server := newGCTestServer(t)
	store := &fakeGCStore{
		keys: map[string][]execution.WorkflowKey{
			"tree-multi": {
				{Namespace: "default", WorkflowID: "wf-a"},
				{Namespace: "default", WorkflowID: "wf-b"},
			},
		},
		due: []execution.DeletionTask{{TreeID: "tree-multi"}},
	}
	store.onDelete = func(string) {
		for _, wf := range []string{"wf-a", "wf-b"} {
			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			_, err := server.keyedMu.Lock(ctx, workflowLockKey("default", wf))
			cancel()
			if err == nil {
				t.Errorf("workflow %s was NOT locked during collection", wf)
			}
		}
	}

	retention.NewTask(store, server, server, systemclock.Clock{}, retention.Config{}, slog.New(slog.DiscardHandler)).RunOnce(t.Context())
	if got := store.snapshotDeleted(); len(got) != 1 {
		t.Fatalf("deleted = %v, want one tree", got)
	}
}

// ── 验收 20：多副本——同一 tree 不被删两次（幂等兜底）──
//
// dbleader 是租约制，切换瞬间可能双 leader，故互斥最终靠幂等。
// 两个扫描器并发处理同一 tree 时，keyedMu 把它们串行化，DeleteTree 的幂等
// 保证第二次是 no-op。
func TestRetentionGCConcurrentScannersAreSerializedByKeyedLock(t *testing.T) {
	server := newGCTestServer(t)
	var concurrent int
	var maxConcurrent int
	var mu sync.Mutex

	store := &fakeGCStore{
		keys: map[string][]execution.WorkflowKey{
			"tree-x": {{Namespace: "default", WorkflowID: "wf-x"}},
		},
		due: []execution.DeletionTask{{TreeID: "tree-x"}},
	}
	store.onDelete = func(string) {
		mu.Lock()
		concurrent++
		maxConcurrent = max(maxConcurrent, concurrent)
		mu.Unlock()
		time.Sleep(20 * time.Millisecond)
		mu.Lock()
		concurrent--
		mu.Unlock()
	}

	// 两个扫描器共用同一个 Server（即同一把 keyedMu），模拟双 leader。
	var wg sync.WaitGroup
	for range 2 {
		wg.Go(func() {
			retention.NewTask(store, server, server, systemclock.Clock{}, retention.Config{}, slog.New(slog.DiscardHandler)).RunOnce(context.Background())
		})
	}
	wg.Wait()

	mu.Lock()
	peak := maxConcurrent
	mu.Unlock()
	if peak > 1 {
		t.Fatalf("two scanners deleted the same tree concurrently (peak=%d); "+
			"keyedMu did not serialize them", peak)
	}
	if n := server.keyedMu.EntryCount(); n != 0 {
		t.Fatalf("keyed lock entries leaked: %d", n)
	}
}

// ── ctx 取消时干净退出，不留锁 ──

func TestRetentionGCStopsOnContextCancellation(t *testing.T) {
	server := newGCTestServer(t)
	store := &fakeGCStore{
		keys: map[string][]execution.WorkflowKey{},
		due:  []execution.DeletionTask{{TreeID: "tree-1"}, {TreeID: "tree-2"}},
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	retention.NewTask(store, server, server, systemclock.Clock{}, retention.Config{}, slog.New(slog.DiscardHandler)).RunOnce(ctx)

	if got := store.snapshotDeleted(); len(got) != 0 {
		t.Fatalf("deleted %v after cancellation, want none", got)
	}
	if n := server.keyedMu.EntryCount(); n != 0 {
		t.Fatalf("keyed lock entries leaked after cancellation: %d", n)
	}
}
