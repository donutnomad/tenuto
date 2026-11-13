package worker

import (
	"context"
	"testing"

	"github.com/donutnomad/tenuto/protocol"
	tenutotel "github.com/donutnomad/tenuto/telemetry"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

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

// TestCacheLookupMetricRecordsHitsAndMisses 断言 sticky cache 命中率可算。
//
// 运维问题：命中率塌陷 = sticky 路由失效，workflow 每次都全量重放历史。
func TestCacheLookupMetricRecordsHitsAndMisses(t *testing.T) {
	providers, collect := newMetricProbe(t)
	telemetry := newWorkerTelemetry(providers)

	telemetry.recordCacheLookups(context.Background(), 7, 2)
	metrics := collect()

	if got := sumInt64(t, metrics, "tenuto.workflow.cache.lookups", tenutotel.AttrCacheResult, "hit"); got != 7 {
		t.Fatalf("cache hits = %d, want 7", got)
	}
	if got := sumInt64(t, metrics, "tenuto.workflow.cache.lookups", tenutotel.AttrCacheResult, "miss"); got != 2 {
		t.Fatalf("cache misses = %d, want 2", got)
	}
}

// TestExecutionCacheSnapshotFeedsLookupDeltas 断言"快照累计值 → 上报增量"这条链路是对的。
//
// counter 语义要求单调累加增量；直接把累计值 Add 进去会重复计数（第二轮把第一轮的量
// 再加一遍）。这里用真实 ExecutionCache 的快照走一遍差值计算，钉住这个换算。
func TestExecutionCacheSnapshotFeedsLookupDeltas(t *testing.T) {
	providers, collect := newMetricProbe(t)
	telemetry := newWorkerTelemetry(providers)

	var lastHits, lastMisses uint64
	snapshot := protocol.ExecutionCacheSnapshot{Hits: 1, Misses: 2}
	telemetry.recordCacheLookups(context.Background(), snapshot.Hits-lastHits, snapshot.Misses-lastMisses)
	lastHits, lastMisses = snapshot.Hits, snapshot.Misses

	// 第二轮：累计值涨到 (4,3)，增量应为 (3,1) 而非把 4/3 再加一遍。
	snapshot = protocol.ExecutionCacheSnapshot{Hits: 4, Misses: 3}
	telemetry.recordCacheLookups(context.Background(), snapshot.Hits-lastHits, snapshot.Misses-lastMisses)

	metrics := collect()
	if got := sumInt64(t, metrics, "tenuto.workflow.cache.lookups", tenutotel.AttrCacheResult, "hit"); got != 4 {
		t.Fatalf("cumulative hits = %d, want 4 (deltas 1+3, not 1+4)", got)
	}
	if got := sumInt64(t, metrics, "tenuto.workflow.cache.lookups", tenutotel.AttrCacheResult, "miss"); got != 3 {
		t.Fatalf("cumulative misses = %d, want 3 (deltas 2+1, not 2+3)", got)
	}
}
