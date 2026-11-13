package executor

import (
	"github.com/donutnomad/tenuto/protocol"
	"testing"
	"time"
)

type sizedTestSession struct{ size int64 }

func (s *sizedTestSession) EstimatedSize() int64 { return s.size }
func (s *sizedTestSession) Close() error         { return nil }

// Happy Path：session 在租约期间增长，Release 后缓存总量与逐出核算使用新估算值。
func TestExecutionCacheReleaseRefreshesEstimatedSize(t *testing.T) {
	cache, err := NewExecutionCache(ExecutionCacheConfig{MaxEntries: 4, MaxEstimatedBytes: 100, IdleTTL: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cache.Close() })
	key := func(runID string) ExecutionCacheKey {
		return ExecutionCacheKey{Namespace: "default", RunID: runID, WorkflowType: "flow", ArtifactFingerprint: "a", ExecutorABIVersion: "v1"}
	}
	growing := &sizedTestSession{size: 10}
	if err := cache.Admit(key("run-1"), growing, ExecutionCacheEntryMetadata{}); err != nil {
		t.Fatal(err)
	}

	lease, err := cache.Acquire(key("run-1"))
	if err != nil {
		t.Fatal(err)
	}
	growing.size = 70
	lease.Release()
	if got := cache.Snapshot().EstimatedBytes; got != 70 {
		t.Fatalf("estimated bytes after release = %d, want 70", got)
	}

	// 70 + 40 超出 100：新 Entry 进入时逐出已增长的旧 Entry。
	if err := cache.Admit(key("run-2"), &sizedTestSession{size: 40}, ExecutionCacheEntryMetadata{}); err != nil {
		t.Fatal(err)
	}
	snapshot := cache.Snapshot()
	if snapshot.Entries != 1 || snapshot.EstimatedBytes != 40 || snapshot.Evictions[protocol.ExecutionCacheEvictionCapacity] != 1 {
		t.Fatalf("snapshot = %+v", snapshot)
	}
}
