package executor

import (
	"container/list"
	"errors"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/donutnomad/tenuto/protocol"
)

var (
	ErrExecutionCacheClosed       = errors.New("execution cache is closed")
	ErrExecutionCacheEntryExists  = errors.New("execution cache entry already exists")
	ErrExecutionCacheEntryMissing = errors.New("execution cache entry is missing")
	ErrExecutionCacheCapacity     = errors.New("execution cache capacity is exhausted by active entries")
)

type ExecutionCacheKey struct {
	Namespace           string
	RunID               string
	WorkflowType        string
	ArtifactFingerprint string
	ExecutorABIVersion  string
}

func (k ExecutionCacheKey) valid() bool {
	return k.Namespace != "" && k.RunID != "" && k.WorkflowType != "" &&
		k.ArtifactFingerprint != "" && k.ExecutorABIVersion != ""
}

type CacheableExecutionSession interface {
	EstimatedSize() int64
	Close() error
}

type ExecutionCacheEntryMetadata struct {
	ExecutionGeneration int64
	StickyGeneration    int64
	WorkerBuildID       string
	Capabilities        []string
}

type ExecutionCacheConfig struct {
	MaxEntries        int
	MaxEstimatedBytes int64
	IdleTTL           time.Duration
	Now               func() time.Time
}

type executionCacheMetrics struct {
	hits       atomic.Uint64
	misses     atomic.Uint64
	admissions atomic.Uint64
	rejected   atomic.Uint64

	mu        sync.Mutex
	evictions map[protocol.ExecutionCacheEvictionReason]uint64
}

func (m *executionCacheMetrics) recordEvictions(reason protocol.ExecutionCacheEvictionReason, count int) {
	if count == 0 {
		return
	}
	m.mu.Lock()
	m.evictions[reason] += uint64(count)
	m.mu.Unlock()
}

type ExecutionCache struct {
	mu             sync.Mutex
	entries        map[ExecutionCacheKey]*executionCacheEntry
	lru            *list.List
	estimatedBytes int64
	closed         bool
	config         ExecutionCacheConfig
	metrics        executionCacheMetrics
}

func (c *ExecutionCache) IdleTTL() time.Duration { return c.config.IdleTTL }

type executionCacheEntry struct {
	mu         sync.Mutex
	key        ExecutionCacheKey
	session    CacheableExecutionSession
	metadata   ExecutionCacheEntryMetadata
	estimated  int64
	createdAt  time.Time
	lastAccess time.Time
	references int
	indexed    bool
	lruElement *list.Element
	closeOnce  sync.Once
	closeErr   error
}

type ExecutionCacheLease struct {
	cache    *ExecutionCache
	entry    *executionCacheEntry
	released atomic.Bool
}

func NewExecutionCache(config ExecutionCacheConfig) (*ExecutionCache, error) {
	if config.MaxEntries <= 0 {
		return nil, errors.New("execution cache MaxEntries must be positive")
	}
	if config.MaxEstimatedBytes <= 0 {
		return nil, errors.New("execution cache MaxEstimatedBytes must be positive")
	}
	if config.IdleTTL <= 0 {
		return nil, errors.New("execution cache IdleTTL must be positive")
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	return &ExecutionCache{
		entries: make(map[ExecutionCacheKey]*executionCacheEntry),
		lru:     list.New(),
		config:  config,
		metrics: executionCacheMetrics{evictions: make(map[protocol.ExecutionCacheEvictionReason]uint64)},
	}, nil
}

func (c *ExecutionCache) Admit(key ExecutionCacheKey, session CacheableExecutionSession, metadata ExecutionCacheEntryMetadata) error {
	if !key.valid() {
		return errors.New("execution cache key is incomplete")
	}
	if session == nil {
		return errors.New("execution cache session is nil")
	}
	estimated := session.EstimatedSize()
	if estimated < 0 {
		return errors.New("execution cache session estimated size is negative")
	}
	now := c.config.Now()
	entry := &executionCacheEntry{
		key:        key,
		session:    session,
		metadata:   cloneExecutionCacheMetadata(metadata),
		estimated:  estimated,
		createdAt:  now,
		lastAccess: now,
		indexed:    true,
	}

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return ErrExecutionCacheClosed
	}
	if _, exists := c.entries[key]; exists {
		c.mu.Unlock()
		return ErrExecutionCacheEntryExists
	}
	candidates, fits := c.capacityCandidatesLocked(1, estimated)
	if !fits {
		c.metrics.rejected.Add(1)
		c.mu.Unlock()
		return ErrExecutionCacheCapacity
	}
	for _, candidate := range candidates {
		c.removeLocked(candidate)
	}
	entry.lruElement = c.lru.PushFront(entry)
	c.entries[key] = entry
	c.estimatedBytes += estimated
	c.metrics.admissions.Add(1)
	c.metrics.recordEvictions(protocol.ExecutionCacheEvictionCapacity, len(candidates))
	c.mu.Unlock()

	return closeExecutionCacheEntries(candidates)
}

func (c *ExecutionCache) Acquire(key ExecutionCacheKey) (*ExecutionCacheLease, error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, ErrExecutionCacheClosed
	}
	entry, ok := c.entries[key]
	if !ok {
		c.metrics.misses.Add(1)
		c.mu.Unlock()
		return nil, ErrExecutionCacheEntryMissing
	}
	entry.references++
	entry.lastAccess = c.config.Now()
	c.lru.MoveToFront(entry.lruElement)
	c.metrics.hits.Add(1)
	c.mu.Unlock()

	entry.mu.Lock()
	return &ExecutionCacheLease{cache: c, entry: entry}, nil
}

func (l *ExecutionCacheLease) Session() CacheableExecutionSession {
	if l == nil || l.entry == nil || l.released.Load() {
		return nil
	}
	return l.entry.session
}

func (l *ExecutionCacheLease) Metadata() ExecutionCacheEntryMetadata {
	if l == nil || l.entry == nil || l.released.Load() {
		return ExecutionCacheEntryMetadata{}
	}
	return cloneExecutionCacheMetadata(l.entry.metadata)
}

func (l *ExecutionCacheLease) UpdateMetadata(metadata ExecutionCacheEntryMetadata) {
	if l == nil || l.entry == nil || l.released.Load() {
		return
	}
	l.entry.metadata = cloneExecutionCacheMetadata(metadata)
}

// Release ends the lease. The ordering below is deliberate; do not "tidy" it.
//
// Lock order is cache.mu -> entry.mu, and the two are never held simultaneously
// (Acquire drops cache.mu before taking entry.mu). Release unwinds in the mirror
// order, so no cycle exists.
//
// Between the entry.mu unlock and the references decrement, references is
// transiently one HIGHER than the number of live leases. That skew is safe by
// construction, because references is only ever consulted to decide whether an
// entry may be evicted (capacityCandidatesLocked, SweepExpired). An overcount can
// only make eviction skip an entry for one round; it can never evict an entry that
// is still in use. The skew is conservative in the safe direction.
//
// Reversing this (decrementing before unlocking entry.mu) is NOT an improvement.
// It would let references reach 0 while this goroutine still holds entry.mu, so an
// evictor could pull the entry out of the index and call entry.close() the instant
// the decrement lands. Correctness would then rest entirely on entry.close() taking
// entry.mu; the moment anyone adds a close path that does not, the session is closed
// out from under a live lease. Keeping the unlock first means a lease's exclusivity
// always ends before the entry becomes evictable at all.
func (l *ExecutionCacheLease) Release() {
	if l == nil || l.entry == nil || !l.released.CompareAndSwap(false, true) {
		return
	}
	// 激活后 session 会增长：在租约仍独占时重新估算，并同步缓存总量。
	estimated := l.entry.session.EstimatedSize()
	l.entry.mu.Unlock()
	l.cache.mu.Lock()
	l.entry.references--
	if l.entry.indexed && estimated >= 0 {
		l.cache.estimatedBytes += estimated - l.entry.estimated
		l.entry.estimated = estimated
	}
	l.cache.mu.Unlock()
}

func (c *ExecutionCache) Invalidate(key ExecutionCacheKey, reason protocol.ExecutionCacheEvictionReason) error {
	c.mu.Lock()
	entry, ok := c.entries[key]
	if !ok {
		c.mu.Unlock()
		return nil
	}
	c.removeLocked(entry)
	c.metrics.recordEvictions(reason, 1)
	c.mu.Unlock()
	return entry.close()
}

func (c *ExecutionCache) InvalidateRun(namespace, runID string, reason protocol.ExecutionCacheEvictionReason) error {
	c.mu.Lock()
	entries := make([]*executionCacheEntry, 0, 1)
	for key, entry := range c.entries {
		if key.Namespace == namespace && key.RunID == runID {
			c.removeLocked(entry)
			entries = append(entries, entry)
		}
	}
	c.metrics.recordEvictions(reason, len(entries))
	c.mu.Unlock()
	return closeExecutionCacheEntries(entries)
}

func (c *ExecutionCache) SweepExpired() error {
	now := c.config.Now()
	c.mu.Lock()
	entries := make([]*executionCacheEntry, 0)
	for element := c.lru.Back(); element != nil; {
		previous := element.Prev()
		entry := element.Value.(*executionCacheEntry)
		if entry.references == 0 && now.Sub(entry.lastAccess) >= c.config.IdleTTL {
			c.removeLocked(entry)
			entries = append(entries, entry)
		}
		element = previous
	}
	c.metrics.recordEvictions(protocol.ExecutionCacheEvictionTTL, len(entries))
	c.mu.Unlock()
	return closeExecutionCacheEntries(entries)
}

func (c *ExecutionCache) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	entries := make([]*executionCacheEntry, 0, len(c.entries))
	for _, entry := range c.entries {
		c.removeLocked(entry)
		entries = append(entries, entry)
	}
	c.metrics.recordEvictions(protocol.ExecutionCacheEvictionShutdown, len(entries))
	c.mu.Unlock()
	return closeExecutionCacheEntries(entries)
}

func (c *ExecutionCache) Snapshot() protocol.ExecutionCacheSnapshot {
	c.mu.Lock()
	snapshot := protocol.ExecutionCacheSnapshot{
		Entries:        len(c.entries),
		EstimatedBytes: c.estimatedBytes,
		Hits:           c.metrics.hits.Load(),
		Misses:         c.metrics.misses.Load(),
		Admissions:     c.metrics.admissions.Load(),
		Rejected:       c.metrics.rejected.Load(),
		Evictions:      make(map[protocol.ExecutionCacheEvictionReason]uint64),
	}
	c.metrics.mu.Lock()
	for reason, count := range c.metrics.evictions {
		snapshot.Evictions[reason] = count
	}
	c.metrics.mu.Unlock()
	c.mu.Unlock()
	return snapshot
}

func (c *ExecutionCache) capacityCandidatesLocked(addEntries int, addBytes int64) ([]*executionCacheEntry, bool) {
	needEntries := len(c.entries) + addEntries - c.config.MaxEntries
	needBytes := c.estimatedBytes + addBytes - c.config.MaxEstimatedBytes
	if needEntries <= 0 && needBytes <= 0 {
		return nil, true
	}
	candidates := make([]*executionCacheEntry, 0)
	var freedBytes int64
	for element := c.lru.Back(); element != nil; element = element.Prev() {
		entry := element.Value.(*executionCacheEntry)
		if entry.references != 0 {
			continue
		}
		candidates = append(candidates, entry)
		freedBytes += entry.estimated
		if len(candidates) >= needEntries && freedBytes >= needBytes {
			return candidates, true
		}
	}
	return nil, false
}

func (c *ExecutionCache) removeLocked(entry *executionCacheEntry) {
	if !entry.indexed {
		return
	}
	entry.indexed = false
	delete(c.entries, entry.key)
	c.lru.Remove(entry.lruElement)
	c.estimatedBytes -= entry.estimated
}

// close shuts the underlying session down exactly once.
//
// Taking entry.mu here is load-bearing, not incidental: it is what makes eviction
// wait for an in-flight lease to finish instead of closing a session that a caller
// is still using. Removing this lock (or closing the session on any path that skips
// close) reintroduces a use-after-close on Lease.Session(). See Release for how this
// interacts with the reference count.
func (entry *executionCacheEntry) close() error {
	entry.mu.Lock()
	defer entry.mu.Unlock()
	entry.closeOnce.Do(func() {
		entry.closeErr = entry.session.Close()
	})
	return entry.closeErr
}

func closeExecutionCacheEntries(entries []*executionCacheEntry) error {
	if len(entries) == 0 {
		return nil
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].key.RunID < entries[j].key.RunID
	})
	var errs []error
	for _, entry := range entries {
		if err := entry.close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func cloneExecutionCacheMetadata(metadata ExecutionCacheEntryMetadata) ExecutionCacheEntryMetadata {
	metadata.Capabilities = append([]string(nil), metadata.Capabilities...)
	return metadata
}
