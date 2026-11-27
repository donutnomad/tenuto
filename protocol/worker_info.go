package protocol

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"time"
)

const (
	workerIdentityHeader     = "X-Workflow-Worker-Identity"
	workerBuildIDHeader      = "X-Workflow-Worker-Build-ID"
	workerCapabilitiesHeader = "X-Workflow-Worker-Capabilities"
	workerStickyCacheHeader  = "X-Workflow-Worker-Sticky-Cache"
	workerNamespaceHeader    = "X-Workflow-Namespace"
	workerTaskTimeoutHeader  = "X-Workflow-Worker-Task-Timeout-Ms"
	workerSessionHeader      = "X-Workflow-Worker-Session"
)

// WorkerInfo 是 Worker 随 runtime 请求上报的元数据。
type WorkerInfo struct {
	Namespace            string
	Identity             string
	BuildID              string
	WorkflowCapabilities []string
	StickyCache          *ExecutionCacheSnapshot
	// WorkflowTaskTimeout 是 Worker 执行一个 Workflow Task 的超时；Server 按它为领取的 Workflow Task 计时。
	WorkflowTaskTimeout time.Duration
	// Session 是 Worker 实例的随机标识，每个进程内的 Worker 各不相同。Worker 停止时按它请求 Server 结束自己的在途长轮询，
	// 重启后复用同一 Identity 的新实例不受影响。
	Session string
}

type workerInfoContextKey struct{}

// WithWorkerInfo 把 worker 元数据放入 runtime 边界，server 用它写入 history。
func WithWorkerInfo(ctx context.Context, info WorkerInfo) context.Context {
	ctx = WithWorkerRoutingInfo(ctx, WorkerRoutingInfo{
		Namespace: info.Namespace,
		Identity:  info.Identity,
		BuildID:   info.BuildID,
	})
	return context.WithValue(ctx, workerInfoContextKey{}, info)
}

func WorkerInfoFrom(ctx context.Context) (WorkerInfo, bool) {
	info, ok := ctx.Value(workerInfoContextKey{}).(WorkerInfo)
	return info, ok
}

func WorkerInfoFromRequest(r *http.Request) WorkerInfo {
	info := WorkerInfo{
		Namespace: r.Header.Get(workerNamespaceHeader),
		Identity:  r.Header.Get(workerIdentityHeader),
		BuildID:   r.Header.Get(workerBuildIDHeader),
		Session:   r.Header.Get(workerSessionHeader),
	}
	_ = json.Unmarshal([]byte(r.Header.Get(workerCapabilitiesHeader)), &info.WorkflowCapabilities)
	if ms, err := strconv.ParseInt(r.Header.Get(workerTaskTimeoutHeader), 10, 64); err == nil && ms > 0 {
		info.WorkflowTaskTimeout = time.Duration(ms) * time.Millisecond
	}
	if encoded := r.Header.Get(workerStickyCacheHeader); encoded != "" {
		var snapshot ExecutionCacheSnapshot
		if json.Unmarshal([]byte(encoded), &snapshot) == nil {
			info.StickyCache = &snapshot
		}
	}
	return info
}

func ApplyWorkerInfoHeaders(r *http.Request, info WorkerInfo) {
	if info.Namespace != "" {
		r.Header.Set(workerNamespaceHeader, NormalizeNamespace(info.Namespace))
	}
	if info.Identity != "" {
		r.Header.Set(workerIdentityHeader, info.Identity)
	}
	if info.BuildID != "" {
		r.Header.Set(workerBuildIDHeader, info.BuildID)
	}
	if info.Session != "" {
		r.Header.Set(workerSessionHeader, info.Session)
	}
	if info.WorkflowCapabilities != nil {
		if encoded, err := json.Marshal(info.WorkflowCapabilities); err == nil {
			r.Header.Set(workerCapabilitiesHeader, string(encoded))
		}
	}
	if info.StickyCache != nil {
		if encoded, err := json.Marshal(info.StickyCache); err == nil {
			r.Header.Set(workerStickyCacheHeader, string(encoded))
		}
	}
	if info.WorkflowTaskTimeout > 0 {
		r.Header.Set(workerTaskTimeoutHeader, strconv.FormatInt(info.WorkflowTaskTimeout.Milliseconds(), 10))
	}
}

// ExecutionCacheSnapshot 是 Worker 上报的 sticky 执行缓存统计。
type ExecutionCacheSnapshot struct {
	Entries        int                                     `json:"entries"`
	EstimatedBytes int64                                   `json:"estimated_bytes"`
	Hits           uint64                                  `json:"hits"`
	Misses         uint64                                  `json:"misses"`
	Admissions     uint64                                  `json:"admissions,omitempty"`
	Rejected       uint64                                  `json:"rejected,omitempty"`
	Evictions      map[ExecutionCacheEvictionReason]uint64 `json:"evictions,omitempty"`
}

type ExecutionCacheEvictionReason string

const (
	ExecutionCacheEvictionCapacity   ExecutionCacheEvictionReason = "capacity"
	ExecutionCacheEvictionTTL        ExecutionCacheEvictionReason = "ttl"
	ExecutionCacheEvictionInvalid    ExecutionCacheEvictionReason = "invalidated"
	ExecutionCacheEvictionShutdown   ExecutionCacheEvictionReason = "shutdown"
	ExecutionCacheEvictionGeneration ExecutionCacheEvictionReason = "generation_mismatch"
)
