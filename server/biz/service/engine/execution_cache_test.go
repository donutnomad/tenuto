package engine

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/internal/executor"
	"github.com/donutnomad/tenuto/protocol"
)

type cacheTestSession struct {
	size      int64
	closeErr  error
	closed    atomic.Int32
	closeHook func()
}

func (s *cacheTestSession) EstimatedSize() int64 { return s.size }

func (s *cacheTestSession) Close() error {
	s.closed.Add(1)
	if s.closeHook != nil {
		s.closeHook()
	}
	return s.closeErr
}

func cacheTestKey(runID string) executor.ExecutionCacheKey {
	return executor.ExecutionCacheKey{
		Namespace:           "demo",
		RunID:               runID,
		WorkflowType:        "flow",
		ArtifactFingerprint: "artifact-v1",
		ExecutorABIVersion:  "executor-v1",
	}
}

func newCacheForTest(t *testing.T, config executor.ExecutionCacheConfig) *executor.ExecutionCache {
	t.Helper()
	cache, err := executor.NewExecutionCache(config)
	if err != nil {
		t.Fatal(err)
	}
	return cache
}

func TestExecutionCacheAcquirePinsAndSerializesEntry(t *testing.T) {
	cache := newCacheForTest(t, executor.ExecutionCacheConfig{MaxEntries: 2, MaxEstimatedBytes: 200, IdleTTL: time.Minute})
	session := &cacheTestSession{size: 80}
	if err := cache.Admit(cacheTestKey("run-1"), session, executor.ExecutionCacheEntryMetadata{ExecutionGeneration: 1}); err != nil {
		t.Fatal(err)
	}
	first, err := cache.Acquire(cacheTestKey("run-1"))
	if err != nil {
		t.Fatal(err)
	}

	acquired := make(chan *executor.ExecutionCacheLease, 1)
	go func() {
		lease, acquireErr := cache.Acquire(cacheTestKey("run-1"))
		if acquireErr == nil {
			acquired <- lease
		}
	}()
	select {
	case <-acquired:
		t.Fatal("second acquire entered while first activation held the entry")
	case <-time.After(20 * time.Millisecond):
	}
	first.Release()
	select {
	case second := <-acquired:
		second.Release()
	case <-time.After(time.Second):
		t.Fatal("second acquire did not resume")
	}
	if session.closed.Load() != 0 {
		t.Fatal("active cache entry was closed")
	}
}

func TestExecutionCacheCapacityEvictsIdleLRU(t *testing.T) {
	cache := newCacheForTest(t, executor.ExecutionCacheConfig{MaxEntries: 2, MaxEstimatedBytes: 100, IdleTTL: time.Minute})
	first := &cacheTestSession{size: 40}
	second := &cacheTestSession{size: 40}
	third := &cacheTestSession{size: 40}
	if err := cache.Admit(cacheTestKey("run-1"), first, executor.ExecutionCacheEntryMetadata{}); err != nil {
		t.Fatal(err)
	}
	if err := cache.Admit(cacheTestKey("run-2"), second, executor.ExecutionCacheEntryMetadata{}); err != nil {
		t.Fatal(err)
	}
	lease, err := cache.Acquire(cacheTestKey("run-1"))
	if err != nil {
		t.Fatal(err)
	}
	lease.Release()
	if err := cache.Admit(cacheTestKey("run-3"), third, executor.ExecutionCacheEntryMetadata{}); err != nil {
		t.Fatal(err)
	}
	if first.closed.Load() != 0 || second.closed.Load() != 1 || third.closed.Load() != 0 {
		t.Fatalf("unexpected close counts first=%d second=%d third=%d", first.closed.Load(), second.closed.Load(), third.closed.Load())
	}
	if _, err := cache.Acquire(cacheTestKey("run-2")); !errors.Is(err, executor.ErrExecutionCacheEntryMissing) {
		t.Fatalf("expected LRU entry miss, got %v", err)
	}
}

func TestExecutionCacheRejectsAdmissionWhenPinnedEntriesOwnCapacity(t *testing.T) {
	cache := newCacheForTest(t, executor.ExecutionCacheConfig{MaxEntries: 1, MaxEstimatedBytes: 100, IdleTTL: time.Minute})
	first := &cacheTestSession{size: 80}
	if err := cache.Admit(cacheTestKey("run-1"), first, executor.ExecutionCacheEntryMetadata{}); err != nil {
		t.Fatal(err)
	}
	lease, err := cache.Acquire(cacheTestKey("run-1"))
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release()
	second := &cacheTestSession{size: 20}
	if err := cache.Admit(cacheTestKey("run-2"), second, executor.ExecutionCacheEntryMetadata{}); !errors.Is(err, executor.ErrExecutionCacheCapacity) {
		t.Fatalf("expected capacity rejection, got %v", err)
	}
	if second.closed.Load() != 0 {
		t.Fatal("rejected session ownership transferred to cache")
	}
}

func TestExecutionCacheInvalidateWaitsForActivationAndClosesOnce(t *testing.T) {
	cache := newCacheForTest(t, executor.ExecutionCacheConfig{MaxEntries: 1, MaxEstimatedBytes: 100, IdleTTL: time.Minute})
	closed := make(chan struct{})
	session := &cacheTestSession{size: 40, closeHook: func() { close(closed) }}
	if err := cache.Admit(cacheTestKey("run-1"), session, executor.ExecutionCacheEntryMetadata{}); err != nil {
		t.Fatal(err)
	}
	lease, err := cache.Acquire(cacheTestKey("run-1"))
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		done <- cache.Invalidate(cacheTestKey("run-1"), protocol.ExecutionCacheEvictionInvalid)
	}()
	select {
	case <-closed:
		t.Fatal("session closed during activation")
	case <-time.After(20 * time.Millisecond):
	}
	lease.Release()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if err := cache.Invalidate(cacheTestKey("run-1"), protocol.ExecutionCacheEvictionInvalid); err != nil {
		t.Fatal(err)
	}
	if session.closed.Load() != 1 {
		t.Fatalf("close count = %d", session.closed.Load())
	}
}

func TestExecutionCacheSweepUsesIdleTTL(t *testing.T) {
	now := time.Unix(100, 0)
	cache := newCacheForTest(t, executor.ExecutionCacheConfig{
		MaxEntries:        2,
		MaxEstimatedBytes: 200,
		IdleTTL:           time.Minute,
		Now:               func() time.Time { return now },
	})
	session := &cacheTestSession{size: 40}
	if err := cache.Admit(cacheTestKey("run-1"), session, executor.ExecutionCacheEntryMetadata{}); err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Minute)
	if err := cache.SweepExpired(); err != nil {
		t.Fatal(err)
	}
	if session.closed.Load() != 1 {
		t.Fatalf("close count = %d", session.closed.Load())
	}
}

func TestExecutionCacheCloseDoesNotHoldGlobalLockDuringSessionClose(t *testing.T) {
	cache := newCacheForTest(t, executor.ExecutionCacheConfig{MaxEntries: 2, MaxEstimatedBytes: 200, IdleTTL: time.Minute})
	closeEntered := make(chan struct{})
	closeRelease := make(chan struct{})
	session := &cacheTestSession{size: 40, closeHook: func() {
		close(closeEntered)
		<-closeRelease
	}}
	if err := cache.Admit(cacheTestKey("run-1"), session, executor.ExecutionCacheEntryMetadata{}); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cache.Close() }()
	<-closeEntered
	snapshotDone := make(chan struct{})
	go func() {
		_ = cache.Snapshot()
		close(snapshotDone)
	}()
	select {
	case <-snapshotDone:
	case <-time.After(time.Second):
		t.Fatal("global lock remained held during session close")
	}
	close(closeRelease)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

// TestExecutionCacheReferenceCountNeverSkewsLow pins the Release ordering
// invariant: while a lease is live the entry must never be closed by an evictor.
// The reference count is allowed to read HIGH transiently (Release unlocks
// entry.mu before decrementing), which only delays eviction; it must never read
// LOW, which would evict an in-use entry.
func TestExecutionCacheReferenceCountNeverSkewsLow(t *testing.T) {
	var active atomic.Int64
	var closedWhileActive atomic.Int64

	now := time.Unix(0, 0)
	var nowMu sync.Mutex
	nowFn := func() time.Time {
		nowMu.Lock()
		defer nowMu.Unlock()
		return now
	}

	cache := newCacheForTest(t, executor.ExecutionCacheConfig{
		MaxEntries:        4,
		MaxEstimatedBytes: 400,
		IdleTTL:           time.Nanosecond,
		Now:               nowFn,
	})
	session := &cacheTestSession{size: 40, closeHook: func() {
		if active.Load() > 0 {
			closedWhileActive.Add(1)
		}
	}}
	if err := cache.Admit(cacheTestKey("run-1"), session, executor.ExecutionCacheEntryMetadata{}); err != nil {
		t.Fatal(err)
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			nowMu.Lock()
			now = now.Add(time.Second)
			nowMu.Unlock()
			_ = cache.SweepExpired()
		}
	}()
	go func() {
		defer wg.Done()
		defer close(stop)
		for range 20000 {
			lease, err := cache.Acquire(cacheTestKey("run-1"))
			if err != nil {
				continue
			}
			active.Add(1)
			if lease.Session() == nil {
				t.Error("live lease exposed a nil session")
			}
			active.Add(-1)
			lease.Release()
		}
	}()
	wg.Wait()

	if got := closedWhileActive.Load(); got != 0 {
		t.Fatalf("entry closed while %d lease(s) active; references skewed low", got)
	}
	_ = cache.Close()
}

// TestExecutionCacheEvictionWaitsForLeaseUnderContention asserts that entry.close()
// serializes against a live lease, which is the property that makes the Release
// ordering safe. Invalidate races an active lease that mutates entry state; the
// session must never be closed until the lease is fully released.
func TestExecutionCacheEvictionWaitsForLeaseUnderContention(t *testing.T) {
	var inLease atomic.Bool
	var violations atomic.Int64

	for range 300 {
		cache := newCacheForTest(t, executor.ExecutionCacheConfig{MaxEntries: 4, MaxEstimatedBytes: 400, IdleTTL: time.Minute})
		session := &cacheTestSession{size: 40, closeHook: func() {
			if inLease.Load() {
				violations.Add(1)
			}
		}}
		if err := cache.Admit(cacheTestKey("run-1"), session, executor.ExecutionCacheEntryMetadata{}); err != nil {
			t.Fatal(err)
		}

		leaseHeld := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			lease, err := cache.Acquire(cacheTestKey("run-1"))
			if err != nil {
				close(leaseHeld)
				return
			}
			inLease.Store(true)
			close(leaseHeld)
			// Keep the lease held long enough that a racing evictor must either
			// block on entry.mu or (if that lock is missing) close underneath us.
			for range 200 {
				_ = lease.Session()
				_ = lease.Metadata()
				lease.UpdateMetadata(executor.ExecutionCacheEntryMetadata{ExecutionGeneration: 7})
			}
			// Clear before Release: once Release unlocks entry.mu the entry is
			// legitimately evictable, so a close from that point on is correct.
			inLease.Store(false)
			lease.Release()
		}()
		go func() {
			defer wg.Done()
			<-leaseHeld
			_ = cache.Invalidate(cacheTestKey("run-1"), protocol.ExecutionCacheEvictionInvalid)
		}()
		wg.Wait()
		_ = cache.Close()
	}

	if got := violations.Load(); got != 0 {
		t.Fatalf("session closed %d time(s) while a lease was active", got)
	}
}

func TestExecutionCacheConcurrentAcquireInvalidateAndClose(t *testing.T) {
	cache := newCacheForTest(t, executor.ExecutionCacheConfig{MaxEntries: 8, MaxEstimatedBytes: 800, IdleTTL: time.Minute})
	for i := 0; i < 8; i++ {
		if err := cache.Admit(cacheTestKey(string(rune('a'+i))), &cacheTestSession{size: 50}, executor.ExecutionCacheEntryMetadata{}); err != nil {
			t.Fatal(err)
		}
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		key := cacheTestKey(string(rune('a' + i)))
		wg.Add(2)
		go func() {
			defer wg.Done()
			if lease, err := cache.Acquire(key); err == nil {
				lease.Release()
			}
		}()
		go func() {
			defer wg.Done()
			_ = cache.Invalidate(key, protocol.ExecutionCacheEvictionInvalid)
		}()
	}
	wg.Wait()
	if err := cache.Close(); err != nil {
		t.Fatal(err)
	}
}
