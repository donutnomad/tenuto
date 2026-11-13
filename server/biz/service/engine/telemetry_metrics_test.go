package engine

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/biz/usecase/retention"
	"github.com/donutnomad/tenuto/server/infra/systemclock"
	tenutotel "github.com/donutnomad/tenuto/telemetry"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// errGCProbeFailure 是本文件里用来触发 GC 失败分支的哨兵错误。
// 刻意不复用 errNotTerminalFake：那个会走"未终态、不计失败"的分支。
var errGCProbeFailure = errors.New("probe: tree deletion failed")

// newMetricProbe 返回一个只接指标的 Providers 和一个采集函数。
// 用 ManualReader：采集时机由测试控制，不必等导出周期。
func newMetricProbe(t *testing.T) (tenutotel.Providers, func() metricdata.ResourceMetrics) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	providers := tenutotel.Resolve(tenutotel.Providers{MeterProvider: provider})
	return providers, func() metricdata.ResourceMetrics {
		var metrics metricdata.ResourceMetrics
		if err := reader.Collect(context.Background(), &metrics); err != nil {
			t.Fatal(err)
		}
		return metrics
	}
}

// sumInt64 汇总某个 Int64 counter 在指定属性下的值。attrKey 为空时汇总全部数据点。
func sumInt64(t *testing.T, metrics metricdata.ResourceMetrics, name, attrKey, attrValue string) int64 {
	t.Helper()
	var total int64
	for _, scope := range metrics.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != name {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("metric %s is not an int64 sum, got %T", name, m.Data)
			}
			for _, point := range sum.DataPoints {
				if attrKey != "" {
					value, found := point.Attributes.Value(attribute.Key(attrKey))
					if !found || value.AsString() != attrValue {
						continue
					}
				}
				total += point.Value
			}
		}
	}
	return total
}

// gaugeInt64 取某个 Int64 gauge 在指定属性下的值；attrKey 为空时取第一个数据点。
func gaugeInt64(t *testing.T, metrics metricdata.ResourceMetrics, name, attrKey, attrValue string) (int64, bool) {
	t.Helper()
	for _, scope := range metrics.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != name {
				continue
			}
			gauge, ok := m.Data.(metricdata.Gauge[int64])
			if !ok {
				t.Fatalf("metric %s is not an int64 gauge, got %T", name, m.Data)
			}
			for _, point := range gauge.DataPoints {
				if attrKey == "" {
					return point.Value, true
				}
				value, found := point.Attributes.Value(attribute.Key(attrKey))
				if found && value.AsString() == attrValue {
					return point.Value, true
				}
			}
		}
	}
	return 0, false
}

// TestBacklogMetricRecordsPendingDispatches 断言队列积压被记进指标，且按任务类型分维度。
//
// 这个指标回答的运维问题是：「reconciler 是不是在持续替主投递路径擦屁股」。
// 持续非零说明正常投递在丢任务，是必须告警的沉默故障。
func TestBacklogMetricRecordsPendingDispatches(t *testing.T) {
	providers, collect := newMetricProbe(t)
	telemetry := newEngineTelemetry(providers, systemclock.Clock{})

	telemetry.recordBacklog(context.Background(), 3, 5)
	metrics := collect()

	if got := sumInt64(t, metrics, "tenuto.task.backlog", tenutotel.AttrTaskType, string(protocol.TaskTypeWorkflow)); got != 3 {
		t.Fatalf("workflow backlog = %d, want 3", got)
	}
	if got := sumInt64(t, metrics, "tenuto.task.backlog", tenutotel.AttrTaskType, string(protocol.TaskTypeActivity)); got != 5 {
		t.Fatalf("activity backlog = %d, want 5", got)
	}

	// 0 不该产生数据点：每轮 reconciler 常态是 0，记进去只会把"有积压"淹没在噪音里。
	telemetry.recordBacklog(context.Background(), 0, 0)
	metrics = collect()
	if got := sumInt64(t, metrics, "tenuto.task.backlog", tenutotel.AttrTaskType, string(protocol.TaskTypeWorkflow)); got != 3 {
		t.Fatalf("workflow backlog after zero-record = %d, want it unchanged at 3", got)
	}
}

// TestRetentionGCMetricsRecordReclaimedAndFailures 断言 retention GC 有了可观测性。
//
// 运维问题：GC 到底回收了什么、有没有在静默失败。此前 GC 只有日志，零指标。
func TestRetentionGCMetricsRecordReclaimedAndFailures(t *testing.T) {
	providers, collect := newMetricProbe(t)
	server := &Server{keyedMu: NewKeyedLock(), telemetry: newEngineTelemetry(providers, systemclock.Clock{})}

	store := &fakeGCStore{
		keys: map[string][]execution.WorkflowKey{
			"tree-ok":   {{Namespace: "default", WorkflowID: "wf-ok"}},
			"tree-fail": {{Namespace: "default", WorkflowID: "wf-fail"}},
		},
		due: []execution.DeletionTask{{TreeID: "tree-ok"}, {TreeID: "tree-fail"}},
		deleteErr: map[string]error{
			"tree-fail": errGCProbeFailure,
		},
	}

	retention.NewTask(store, server, server, systemclock.Clock{}, retention.Config{}, slog.New(slog.DiscardHandler)).RunOnce(t.Context())
	metrics := collect()

	if got := sumInt64(t, metrics, "tenuto.retention.gc.reclaimed", tenutotel.AttrGCObject, "tree"); got != 1 {
		t.Fatalf("reclaimed trees = %d, want 1", got)
	}
	// 失败的那棵被推后，且计入 deferred——这是"GC 在静默失败"的信号来源。
	if got := sumInt64(t, metrics, "tenuto.retention.gc.failures", tenutotel.AttrOutcome, "deferred"); got != 1 {
		t.Fatalf("deferred failures = %d, want 1", got)
	}
	if got := sumInt64(t, metrics, "tenuto.retention.gc.failures", tenutotel.AttrOutcome, "parked"); got != 0 {
		t.Fatalf("parked failures = %d, want 0 at first attempt", got)
	}
}

// TestRetentionGCBacklogGauges 断言每轮扫描之后记下到期未处理的任务数与最早任务的等待时长。
//
// 运维问题：回收是否跟得上到期速度。soak 里 GC 每分钟只删 100 棵，结束时 7 693 个任务过期未处理，
// 此前没有任何指标能看到这个积压。
func TestRetentionGCBacklogGauges(t *testing.T) {
	providers, collect := newMetricProbe(t)
	server := &Server{keyedMu: NewKeyedLock(), telemetry: newEngineTelemetry(providers, systemclock.Clock{})}

	fireAt := time.Now().Add(-time.Hour)
	store := &fakeGCStore{
		keys:      map[string][]execution.WorkflowKey{},
		due:       []execution.DeletionTask{{TreeID: "tree-stuck", FireAt: fireAt}, {TreeID: "tree-stuck-2", FireAt: fireAt}},
		deleteErr: map[string]error{"tree-stuck": errGCProbeFailure, "tree-stuck-2": errGCProbeFailure},
	}
	retention.NewTask(store, server, server, systemclock.Clock{}, retention.Config{}, slog.New(slog.DiscardHandler)).RunOnce(t.Context())
	metrics := collect()

	if got, ok := gaugeInt64(t, metrics, "tenuto.retention.gc.backlog", "", ""); !ok || got != 2 {
		t.Fatalf("backlog gauge = %d (found %v), want 2", got, ok)
	}
	var age float64
	for _, scope := range metrics.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name == "tenuto.retention.gc.backlog.oldest_age" {
				age = m.Data.(metricdata.Gauge[float64]).DataPoints[0].Value
			}
		}
	}
	if age < time.Hour.Seconds() || age > (time.Hour+time.Minute).Seconds() {
		t.Fatalf("oldest age = %vs, want about 3600s", age)
	}
}

// TestRetentionGCParkedFailureIsMetered 断言反复失败被搁置时打的是 parked 维度。
//
// parked 非零 = 有 tree 反复删不掉已被搁置，数据在无界增长而没人知道。
// 这是这组指标里**最该配告警**的一个。
func TestRetentionGCParkedFailureIsMetered(t *testing.T) {
	providers, collect := newMetricProbe(t)
	server := &Server{keyedMu: NewKeyedLock(), telemetry: newEngineTelemetry(providers, systemclock.Clock{})}

	store := &fakeGCStore{
		keys:      map[string][]execution.WorkflowKey{"tree-stuck": {{Namespace: "default", WorkflowID: "wf-stuck"}}},
		due:       []execution.DeletionTask{{TreeID: "tree-stuck", Attempts: retention.MaxAttempts - 1}},
		deleteErr: map[string]error{"tree-stuck": errGCProbeFailure},
	}

	retention.NewTask(store, server, server, systemclock.Clock{}, retention.Config{}, slog.New(slog.DiscardHandler)).RunOnce(t.Context())
	metrics := collect()

	if got := sumInt64(t, metrics, "tenuto.retention.gc.failures", tenutotel.AttrOutcome, "parked"); got != 1 {
		t.Fatalf("parked failures = %d, want 1 once attempts reach the threshold", got)
	}
}

// TestRetentionGCScavengeMetricsRecordOrphans 断言兜底扫描回收的两类孤儿分别可见。
func TestRetentionGCScavengeMetricsRecordOrphans(t *testing.T) {
	providers, collect := newMetricProbe(t)
	server := &Server{keyedMu: NewKeyedLock(), telemetry: newEngineTelemetry(providers, systemclock.Clock{})}

	store := &fakeScavengeStore{
		result: execution.ScavengeResult{OrphanEventRuns: 3, OrphanTreeRows: 4},
	}
	retention.NewTask(store, server, server, systemclock.Clock{}, retention.Config{}, slog.New(slog.DiscardHandler)).ScavengeOnce(t.Context())
	metrics := collect()

	for _, probe := range []struct {
		object string
		want   int64
	}{
		{"orphan_event_run", 3},
		{"orphan_tree_row", 4},
	} {
		if got := sumInt64(t, metrics, "tenuto.retention.gc.reclaimed", tenutotel.AttrGCObject, probe.object); got != probe.want {
			t.Fatalf("reclaimed %s = %d, want %d", probe.object, got, probe.want)
		}
	}
}

// TestDBPoolMetricsObserveSaturation 断言连接池饱和度可采集。
//
// 运维问题：WaitCount 增长 = goroutine 正在排队等连接，此时加并发只会更慢，
// 该调的是 MaxOpenConns。
func TestDBPoolMetricsObserveSaturation(t *testing.T) {
	providers, collect := newMetricProbe(t)
	stats := &fakePoolStats{stats: sql.DBStats{
		WaitCount:    9,
		WaitDuration: 1500 * time.Millisecond,
		InUse:        7,
		Idle:         3,
	}}

	unregister, err := RegisterDBPoolMetrics(providers, "workflow", stats)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = unregister() })

	metrics := collect()
	if got := sumInt64(t, metrics, "tenuto.persistence.pool.wait.count", tenutotel.AttrOperation, "workflow"); got != 9 {
		t.Fatalf("pool wait count = %d, want 9", got)
	}
	inUse, ok := gaugeInt64(t, metrics, "tenuto.persistence.pool.connections", tenutotel.AttrPoolState, "in_use")
	if !ok || inUse != 7 {
		t.Fatalf("pool in_use = %d (found=%v), want 7", inUse, ok)
	}
	idle, ok := gaugeInt64(t, metrics, "tenuto.persistence.pool.connections", tenutotel.AttrPoolState, "idle")
	if !ok || idle != 3 {
		t.Fatalf("pool idle = %d (found=%v), want 3", idle, ok)
	}

	// 注销后回调不该再产出数据点，否则关池后仍会读到已关闭的池。
	if err := unregister(); err != nil {
		t.Fatal(err)
	}
	if _, found := gaugeInt64(t, collect(), "tenuto.persistence.pool.connections", tenutotel.AttrPoolState, "in_use"); found {
		t.Fatal("pool gauge still reported after unregister")
	}
}

// TestRegisterDBPoolMetricsToleratesNilDB 断言 nil DB 不会炸——
// 可观测性代码绝不该成为新的崩溃源。
func TestRegisterDBPoolMetricsToleratesNilDB(t *testing.T) {
	providers, _ := newMetricProbe(t)
	unregister, err := RegisterDBPoolMetrics(providers, "workflow", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := unregister(); err != nil {
		t.Fatal(err)
	}
}

type fakePoolStats struct {
	stats sql.DBStats
}

func (f *fakePoolStats) Stats() sql.DBStats { return f.stats }
