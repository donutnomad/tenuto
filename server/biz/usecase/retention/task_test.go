package retention

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/server/biz/domain/clock/clocktest"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/infra/systemclock"
)

// fakeGCStore 是 run.RetentionGCStore 的可编程测试替身。
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

// fakeLocker 是 WorkflowLocker 的测试替身：每个 (namespace, workflowID) 一把可被 ctx 打断的互斥锁。
type fakeLocker struct {
	mu    sync.Mutex
	locks map[string]chan struct{}
}

func newFakeLocker() *fakeLocker {
	return &fakeLocker{locks: make(map[string]chan struct{})}
}

func (l *fakeLocker) LockWorkflow(ctx context.Context, namespace, workflowID string) (func(), error) {
	key := namespace + "/" + workflowID
	l.mu.Lock()
	lock, ok := l.locks[key]
	if !ok {
		lock = make(chan struct{}, 1)
		l.locks[key] = lock
	}
	l.mu.Unlock()
	select {
	case lock <- struct{}{}:
		return func() { <-lock }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// heldCount 返回当前被持有的锁数。
func (l *fakeLocker) heldCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	held := 0
	for _, lock := range l.locks {
		held += len(lock)
	}
	return held
}

// fakeMetrics 是 Metrics 的测试替身，按维度累计。
type fakeMetrics struct {
	mu        sync.Mutex
	reclaimed map[string]int
	failures  map[string]int
	// backlog 与 backlogAge 是最近一次 RecordGCBacklog 的参数，backlogCalls 是调用次数。
	backlog      int
	backlogAge   time.Duration
	backlogCalls int
}

func newFakeMetrics() *fakeMetrics {
	return &fakeMetrics{reclaimed: make(map[string]int), failures: make(map[string]int)}
}

func (m *fakeMetrics) RecordGCReclaimed(_ context.Context, object string, count int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.reclaimed[object] += count
}

func (m *fakeMetrics) RecordGCFailure(_ context.Context, outcome string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failures[outcome]++
}

func (m *fakeMetrics) RecordGCBacklog(_ context.Context, overdue int, oldestAge time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.backlog, m.backlogAge = overdue, oldestAge
	m.backlogCalls++
}

func (m *fakeMetrics) snapshot() (reclaimed, failures map[string]int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	reclaimed, failures = make(map[string]int), make(map[string]int)
	for key, value := range m.reclaimed {
		reclaimed[key] = value
	}
	for key, value := range m.failures {
		failures[key] = value
	}
	return reclaimed, failures
}

func newTestTask(store execution.RetentionGCStore, locker *fakeLocker, cfg Config) *Task {
	return NewTask(store, locker, newFakeMetrics(), systemclock.Clock{}, cfg, slog.New(slog.DiscardHandler))
}

// ── GC 必须拿 workflow 锁 ──
//
// Temporal 的 PriorityLow 是**任务调度优先级**，不是「不加锁」。删除期间必须与该
// workflowID 的用户操作互斥，否则并发的 signal/query/reset 会读到删了一半的状态。
func TestRetentionGCHoldsWorkflowLockDuringDeletion(t *testing.T) {
	locker := newFakeLocker()

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
		_, err := locker.LockWorkflow(ctx, "default", "wf-lock")
		lockedDuringDelete <- err == nil
	}

	task := newTestTask(store, locker, Config{})
	task.RunOnce(t.Context())

	if acquired := <-lockedDuringDelete; acquired {
		t.Fatal("a concurrent workflow operation acquired the workflow lock while GC was deleting; " +
			"signal/query/reset could observe a half-deleted state")
	}
	if got := store.snapshotDeleted(); len(got) != 1 || got[0] != "tree-1" {
		t.Fatalf("deleted = %v, want [tree-1]", got)
	}

	// 删除结束后锁必须已释放，否则该 workflowID 被永久锁死。
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	release, err := locker.LockWorkflow(ctx, "default", "wf-lock")
	if err != nil {
		t.Fatalf("workflow lock was not released after GC finished: %v", err)
	}
	release()
	if n := locker.heldCount(); n != 0 {
		t.Fatalf("workflow locks still held: %d", n)
	}
}

// ── 一棵 tree 横跨多个 workflowID 时全部上锁 ──

func TestRetentionGCLocksEveryWorkflowOnTheTree(t *testing.T) {
	locker := newFakeLocker()
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
			_, err := locker.LockWorkflow(ctx, "default", wf)
			cancel()
			if err == nil {
				t.Errorf("workflow %s was NOT locked during collection", wf)
			}
		}
	}

	newTestTask(store, locker, Config{}).RunOnce(t.Context())
	if got := store.snapshotDeleted(); len(got) != 1 {
		t.Fatalf("deleted = %v, want one tree", got)
	}
}

// ── 验收 16：失败退避，坏任务不得饿死正常任务 ──

func TestRetentionGCDefersFailingTaskAndKeepsProcessingOthers(t *testing.T) {
	locker := newFakeLocker()
	store := &fakeGCStore{
		keys: map[string][]execution.WorkflowKey{},
		due: []execution.DeletionTask{
			{TreeID: "tree-bad", Attempts: 0},
			{TreeID: "tree-good"},
		},
		deleteErr: map[string]error{"tree-bad": errors.New("disk on fire")},
	}

	newTestTask(store, locker, Config{}).RunOnce(t.Context())

	// 坏任务被推后并计入失败次数。
	deferred := store.snapshotDeferred()
	if len(deferred) != 1 || deferred[0].treeID != "tree-bad" {
		t.Fatalf("deferred = %+v, want one defer for tree-bad", deferred)
	}
	if !deferred[0].countAttempt {
		t.Error("a genuine failure must count as an attempt")
	}
	if !deferred[0].fireAt.After(time.Now()) {
		t.Error("failing task was not pushed into the future; it will hog the scan window (livelock)")
	}
	// 同一轮里正常任务照常被处理 —— 坏任务没有阻断后续。
	if got := store.snapshotDeleted(); len(got) != 1 || got[0] != "tree-good" {
		t.Fatalf("deleted = %v, want [tree-good]", got)
	}
}

// ── 「整树尚有 run 未终态」不计入失败次数 ──
//
// 树上还有活着的 run 是完全正常的状态，不是删除失败。若计入 attempts，
// 一棵长期运行的 tree 会被误判为坏任务而推到 24h 之外。
func TestRetentionGCNonTerminalTreeDoesNotCountAsFailure(t *testing.T) {
	locker := newFakeLocker()
	store := &fakeGCStore{
		keys:      map[string][]execution.WorkflowKey{},
		due:       []execution.DeletionTask{{TreeID: "tree-live"}},
		deleteErr: map[string]error{"tree-live": execution.ErrTreeNotTerminal},
	}

	newTestTask(store, locker, Config{}).RunOnce(t.Context())

	deferred := store.snapshotDeferred()
	if len(deferred) != 1 {
		t.Fatalf("deferred = %+v, want exactly one", deferred)
	}
	if deferred[0].countAttempt {
		t.Fatal("a still-running tree must NOT count as a failed attempt")
	}
	if len(store.snapshotDeleted()) != 0 {
		t.Fatal("a non-terminal tree must not be collected")
	}
}

// ── 超过阈值后推到很远，但不静默删除任务 ──

func TestRetentionGCParksRepeatedlyFailingTaskWithoutDropping(t *testing.T) {
	locker := newFakeLocker()
	store := &fakeGCStore{
		keys:      map[string][]execution.WorkflowKey{},
		due:       []execution.DeletionTask{{TreeID: "tree-cursed", Attempts: MaxAttempts}},
		deleteErr: map[string]error{"tree-cursed": errors.New("permanent")},
	}

	before := time.Now()
	newTestTask(store, locker, Config{}).RunOnce(t.Context())

	deferred := store.snapshotDeferred()
	if len(deferred) != 1 {
		t.Fatalf("deferred = %+v, want one", deferred)
	}
	// 推到很远（约 24h），但任务仍在表里 —— 静默删除会把「删不掉的数据」
	// 变成「没人知道删不掉的数据」。
	if deferred[0].fireAt.Before(before.Add(parkDuration - time.Minute)) {
		t.Fatalf("parked fire_at = %s, want ~%s out", deferred[0].fireAt, parkDuration)
	}
	if deferred[0].cause == nil {
		t.Error("the failure cause must be recorded for operators")
	}
}

// ── 退避是指数且有上界 ──

func TestRetentionGCBackoffIsExponentialAndCapped(t *testing.T) {
	prev := time.Duration(0)
	for attempts := 1; attempts <= 20; attempts++ {
		got := backoff(attempts)
		if got < prev {
			t.Fatalf("backoff decreased at attempt %d: %s < %s", attempts, got, prev)
		}
		if got > time.Hour {
			t.Fatalf("backoff at attempt %d = %s, exceeds the 1h cap", attempts, got)
		}
		prev = got
	}
	if backoff(1) != time.Minute {
		t.Fatalf("first backoff = %s, want 1m", backoff(1))
	}
	if backoff(20) != time.Hour {
		t.Fatalf("backoff never reached the cap: %s", backoff(20))
	}
}

// ── 验收 20：多副本——同一 tree 不被删两次（幂等兜底）──
//
// dbleader 是租约制，切换瞬间可能双 leader，故互斥最终靠幂等。
// 两个扫描器并发处理同一 tree 时，workflow 锁把它们串行化，DeleteTree 的幂等
// 保证第二次是 no-op。
func TestRetentionGCConcurrentScannersAreSerializedByWorkflowLock(t *testing.T) {
	locker := newFakeLocker()
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

	// 两个扫描器共用同一个 WorkflowLocker，模拟双 leader。
	var wg sync.WaitGroup
	for range 2 {
		wg.Go(func() {
			newTestTask(store, locker, Config{}).RunOnce(context.Background())
		})
	}
	wg.Wait()

	mu.Lock()
	peak := maxConcurrent
	mu.Unlock()
	if peak > 1 {
		t.Fatalf("two scanners deleted the same tree concurrently (peak=%d); "+
			"the workflow lock did not serialize them", peak)
	}
	if n := locker.heldCount(); n != 0 {
		t.Fatalf("workflow locks still held: %d", n)
	}
}

// ── ctx 取消时干净退出，不留锁 ──

func TestRetentionGCStopsOnContextCancellation(t *testing.T) {
	locker := newFakeLocker()
	store := &fakeGCStore{
		keys: map[string][]execution.WorkflowKey{},
		due:  []execution.DeletionTask{{TreeID: "tree-1"}, {TreeID: "tree-2"}},
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	newTestTask(store, locker, Config{}).RunOnce(ctx)

	if got := store.snapshotDeleted(); len(got) != 0 {
		t.Fatalf("deleted %v after cancellation, want none", got)
	}
	if n := locker.heldCount(); n != 0 {
		t.Fatalf("workflow locks still held after cancellation: %d", n)
	}
}

// ── 配置默认值 ──

func TestConfigDefaults(t *testing.T) {
	got := Config{}.withDefaults()
	if got.ScanInterval != DefaultScanInterval {
		t.Errorf("ScanInterval = %s, want %s", got.ScanInterval, DefaultScanInterval)
	}
	if got.Batch != DefaultBatch {
		t.Errorf("Batch = %d, want %d", got.Batch, DefaultBatch)
	}
	custom := Config{ScanInterval: time.Minute, Batch: 7}.withDefaults()
	if custom.ScanInterval != time.Minute || custom.Batch != 7 {
		t.Errorf("explicit config was overridden: %+v", custom)
	}
}

// fakeScavengeStore 记录 ScavengeOrphans 的调用，用来验证兜底扫描的参数与指标。
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

// ScavengeOnce 把配置里的 minAge / batch 原样传给存储。
func TestScavengeOnceInvokesStoreWithConfiguredBounds(t *testing.T) {
	locker := newFakeLocker()
	store := &fakeScavengeStore{}
	cfg := Config{ScavengeMinAge: 48 * time.Hour, ScavengeBatch: 33}

	newTestTask(store, locker, cfg).ScavengeOnce(context.Background())

	calls := store.snapshotCalls()
	if len(calls) != 1 {
		t.Fatalf("ScavengeOrphans called %d times, want 1", len(calls))
	}
	if calls[0].minAge != 48*time.Hour {
		t.Errorf("minAge = %s, want 48h", calls[0].minAge)
	}
	if calls[0].limit != 33 {
		t.Errorf("limit = %d, want 33", calls[0].limit)
	}
}

// ctx 已取消时不发起扫描：leader 失去租约后必须立刻停手。
func TestScavengeOnceRespectsCancelledContext(t *testing.T) {
	locker := newFakeLocker()
	store := &fakeScavengeStore{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	newTestTask(store, locker, Config{}).ScavengeOnce(ctx)

	if calls := store.snapshotCalls(); len(calls) != 0 {
		t.Fatalf("ScavengeOrphans called %d times after cancellation, want 0", len(calls))
	}
}

// 兜底扫描失败只记日志、不影响主路径：它是尽力而为的清理，下一轮自然会再来。
func TestScavengeOnceSwallowsStoreError(t *testing.T) {
	locker := newFakeLocker()
	store := &fakeScavengeStore{err: errors.New("boom")}

	// 不 panic、不阻塞即为通过。
	newTestTask(store, locker, Config{}).ScavengeOnce(context.Background())

	if calls := store.snapshotCalls(); len(calls) != 1 {
		t.Fatalf("ScavengeOrphans called %d times, want 1", len(calls))
	}
}

// 兜底扫描的默认值：低频 + 7 天下界 + 单轮上限。
func TestScavengeConfigDefaults(t *testing.T) {
	got := Config{}.withDefaults()
	if got.ScavengeInterval != DefaultScavengeInterval {
		t.Errorf("ScavengeInterval = %s, want %s", got.ScavengeInterval, DefaultScavengeInterval)
	}
	if got.ScavengeMinAge != DefaultScavengeMinAge {
		t.Errorf("ScavengeMinAge = %s, want %s", got.ScavengeMinAge, DefaultScavengeMinAge)
	}
	if got.ScavengeBatch != DefaultScavengeBatch {
		t.Errorf("ScavengeBatch = %d, want %d", got.ScavengeBatch, DefaultScavengeBatch)
	}
	custom := Config{
		ScavengeInterval: time.Hour, ScavengeMinAge: time.Minute, ScavengeBatch: 5,
	}.withDefaults()
	if custom.ScavengeInterval != time.Hour || custom.ScavengeMinAge != time.Minute || custom.ScavengeBatch != 5 {
		t.Errorf("explicit scavenge config was overridden: %+v", custom)
	}
}

// ── 指标：回收数、失败与搁置各记各的维度 ──

func TestRetentionGCRecordsReclaimedDeferredAndParkedMetrics(t *testing.T) {
	metrics := newFakeMetrics()
	store := &fakeGCStore{
		keys: map[string][]execution.WorkflowKey{},
		due: []execution.DeletionTask{
			{TreeID: "tree-ok"},
			{TreeID: "tree-fail"},
			{TreeID: "tree-stuck", Attempts: MaxAttempts - 1},
			{TreeID: "tree-live"},
		},
		deleteErr: map[string]error{
			"tree-fail":  errors.New("disk on fire"),
			"tree-stuck": errors.New("permanent"),
			"tree-live":  execution.ErrTreeNotTerminal,
		},
	}

	NewTask(store, newFakeLocker(), metrics, systemclock.Clock{}, Config{}, slog.New(slog.DiscardHandler)).RunOnce(t.Context())

	reclaimed, failures := metrics.snapshot()
	if len(reclaimed) != 1 || reclaimed["tree"] != 1 {
		t.Fatalf("reclaimed = %v, want tree=1", reclaimed)
	}
	// 未终态的树不是失败，不进任何失败维度。
	if len(failures) != 2 || failures["deferred"] != 1 || failures["parked"] != 1 {
		t.Fatalf("failures = %v, want deferred=1 parked=1", failures)
	}
}

func TestScavengeOnceRecordsOrphanMetrics(t *testing.T) {
	metrics := newFakeMetrics()
	store := &fakeScavengeStore{
		result: execution.ScavengeResult{OrphanEventRuns: 3, OrphanTreeRows: 4},
	}

	NewTask(store, newFakeLocker(), metrics, systemclock.Clock{}, Config{}, slog.New(slog.DiscardHandler)).ScavengeOnce(t.Context())

	reclaimed, failures := metrics.snapshot()
	want := map[string]int{"orphan_event_run": 3, "orphan_tree_row": 4}
	if len(reclaimed) != len(want) {
		t.Fatalf("reclaimed = %v, want %v", reclaimed, want)
	}
	for object, count := range want {
		if reclaimed[object] != count {
			t.Fatalf("reclaimed[%s] = %d, want %d", object, reclaimed[object], count)
		}
	}
	if len(failures) != 0 {
		t.Fatalf("failures = %v, want none", failures)
	}
}

// 空结果不打指标：常态是什么都没清。
func TestScavengeOnceRecordsNothingForEmptyResult(t *testing.T) {
	metrics := newFakeMetrics()

	NewTask(&fakeScavengeStore{}, newFakeLocker(), metrics, systemclock.Clock{}, Config{}, slog.New(slog.DiscardHandler)).ScavengeOnce(t.Context())

	if reclaimed, failures := metrics.snapshot(); len(reclaimed) != 0 || len(failures) != 0 {
		t.Fatalf("metrics = %v / %v, want none for an empty scavenge", reclaimed, failures)
	}
}

// queueStore 像 tenuto_deletion_tasks 一样维护到期任务：删除成功或推后后任务离开到期集合。
type queueStore struct {
	fakeGCStore
	// stuck 中的 tree 删除失败且推后写入失败，一直留在到期集合最前面。
	stuck map[string]bool
	// lists 是 ListDueDeletionTasks 的调用次数；onList 在每次调用时回调。
	lists  int
	onList func()
}

func newQueueStore(n int, fireAt time.Time) *queueStore {
	store := &queueStore{stuck: map[string]bool{}}
	for i := range n {
		store.due = append(store.due, execution.DeletionTask{TreeID: fmt.Sprintf("tree-%04d", i), FireAt: fireAt})
	}
	return store
}

func (s *queueStore) ListDueDeletionTasks(now time.Time, limit int) ([]execution.DeletionTask, error) {
	if s.onList != nil {
		s.onList()
	}
	s.mu.Lock()
	s.lists++
	s.mu.Unlock()
	return s.fakeGCStore.ListDueDeletionTasks(now, limit)
}

func (s *queueStore) removeDue(treeID string) {
	s.due = slices.DeleteFunc(s.due, func(task execution.DeletionTask) bool { return task.TreeID == treeID })
}

func (s *queueStore) DeleteTree(treeID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stuck[treeID] {
		return errors.New("delete failed")
	}
	s.deleted = append(s.deleted, treeID)
	s.removeDue(treeID)
	return nil
}

func (s *queueStore) DeferDeletionTask(treeID string, fireAt time.Time, countAttempt bool, cause error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deferred = append(s.deferred, deferCall{treeID, fireAt, countAttempt, cause})
	if s.stuck[treeID] {
		return errors.New("defer failed")
	}
	s.removeDue(treeID)
	return nil
}

// ── 一轮扫描处理完全部到期任务 ──
//
// 每轮只处理 Batch 个时，回收速度被钉在 Batch / ScanInterval；soak 里每小时创建约 11 500 个 Run，
// GC 每分钟只删 100 棵，到期任务无界堆积。一轮扫描必须与 Batch 无关地处理完全部到期任务。
func TestRunOnceDrainsAllDueTasksRegardlessOfBatch(t *testing.T) {
	const due = 1000
	for _, batch := range []int{1, 7, 100, due, 5000} {
		t.Run(fmt.Sprint("batch=", batch), func(t *testing.T) {
			store := newQueueStore(due, time.Now().Add(-time.Hour))
			metrics := newFakeMetrics()
			task := NewTask(store, newFakeLocker(), metrics, systemclock.Clock{}, Config{Batch: batch}, slog.New(slog.DiscardHandler))
			task.RunOnce(t.Context())

			if got := len(store.snapshotDeleted()); got != due {
				t.Fatalf("deleted %d trees in one scan, want %d", got, due)
			}
			if wantLists := due/batch + 1; store.lists > wantLists {
				t.Fatalf("listed due tasks %d times, want at most %d", store.lists, wantLists)
			}
			reclaimed, _ := metrics.snapshot()
			if reclaimed["tree"] != due {
				t.Fatalf("reclaimed metric = %d, want %d", reclaimed["tree"], due)
			}
			if metrics.backlogCalls != 1 || metrics.backlog != 0 || metrics.backlogAge != 0 {
				t.Fatalf("backlog recorded %d times, last = %d / %s; want once, 0 / 0", metrics.backlogCalls, metrics.backlog, metrics.backlogAge)
			}
		})
	}
}

// 删除与推后都失败的任务一直排在最前面：本轮每棵 tree 只处理一次，不在它们上面空转，
// 也不因为它们挡住后面的任务（只要一批里还有新任务）。
func TestRunOnceProcessesEachTreeOnceWhenDeferFails(t *testing.T) {
	store := newQueueStore(10, time.Now().Add(-time.Hour))
	store.stuck["tree-0000"] = true
	store.stuck["tree-0001"] = true
	metrics := newFakeMetrics()
	task := NewTask(store, newFakeLocker(), metrics, systemclock.Clock{}, Config{Batch: 3}, slog.New(slog.DiscardHandler))
	task.RunOnce(t.Context())

	if got := len(store.snapshotDeleted()); got != 8 {
		t.Fatalf("deleted %d trees, want the 8 healthy ones", got)
	}
	attempts := map[string]int{}
	for _, call := range store.snapshotDeferred() {
		attempts[call.treeID]++
	}
	if attempts["tree-0000"] != 1 || attempts["tree-0001"] != 1 {
		t.Fatalf("stuck trees were processed %v times in one scan, want once each", attempts)
	}
	if metrics.backlog != 2 {
		t.Fatalf("backlog = %d, want the 2 stuck tasks", metrics.backlog)
	}
}

// 一轮扫描的时长不超过 ScanInterval：到期任务源源不断时本轮按时结束，积压与 WARN 由 recordBacklog 给出。
func TestRunOnceStopsAtScanInterval(t *testing.T) {
	start := time.Date(2026, 11, 12, 0, 0, 0, 0, time.UTC)
	clk := clocktest.NewFake(start)
	store := newQueueStore(100, start.Add(-10*time.Minute))
	// 每读一批时钟走 20s：第二批之后用满 30s 的 ScanInterval。
	store.onList = func() { clk.Advance(20 * time.Second) }
	metrics := newFakeMetrics()
	var logs strings.Builder
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	task := NewTask(store, newFakeLocker(), metrics, clk, Config{Batch: 10, ScanInterval: 30 * time.Second}, logger)
	task.RunOnce(t.Context())

	if got := len(store.snapshotDeleted()); got != 20 {
		t.Fatalf("deleted %d trees, want 2 batches of 10 before the scan interval ran out", got)
	}
	if metrics.backlog != 80 || metrics.backlogAge != 10*time.Minute+40*time.Second {
		t.Fatalf("backlog = %d / %s, want 80 / 10m40s", metrics.backlog, metrics.backlogAge)
	}
	if !strings.Contains(logs.String(), "deletion backlog is growing") {
		t.Fatalf("no backlog warning in logs:\n%s", logs.String())
	}
}
