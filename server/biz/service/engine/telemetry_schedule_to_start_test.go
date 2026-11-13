package engine

import (
	"context"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/server/infra/systemclock"
	telemetrypkg "github.com/donutnomad/tenuto/telemetry"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

const scheduleToStartMetric = "tenuto.task.schedule_to_start"

// histogramPoints 返回某个 Float64 histogram 在指定属性下的全部数据点。
// attrKey 为空时返回全部数据点。返回切片而非聚合值：schedule-to-start 的核心断言
// 是「有没有产生样本」（零值必须一条都不产生），这需要能数出 Count。
func histogramPoints(t *testing.T, metrics metricdata.ResourceMetrics, name, attrKey, attrValue string) []metricdata.HistogramDataPoint[float64] {
	t.Helper()
	var points []metricdata.HistogramDataPoint[float64]
	for _, scope := range metrics.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != name {
				continue
			}
			histogram, ok := m.Data.(metricdata.Histogram[float64])
			if !ok {
				t.Fatalf("metric %s is not a float64 histogram, got %T", name, m.Data)
			}
			for _, point := range histogram.DataPoints {
				if attrKey != "" {
					value, found := point.Attributes.Value(attribute.Key(attrKey))
					if !found || value.AsString() != attrValue {
						continue
					}
				}
				points = append(points, point)
			}
		}
	}
	return points
}

// histogramCount 汇总匹配数据点的样本数。0 表示「一条样本都没记」。
func histogramCount(t *testing.T, metrics metricdata.ResourceMetrics, name, attrKey, attrValue string) uint64 {
	t.Helper()
	var total uint64
	for _, point := range histogramPoints(t, metrics, name, attrKey, attrValue) {
		total += point.Count
	}
	return total
}

// histogramSum 汇总匹配数据点的样本值之和（单位：秒）。
func histogramSum(t *testing.T, metrics metricdata.ResourceMetrics, name, attrKey, attrValue string) float64 {
	t.Helper()
	var total float64
	for _, point := range histogramPoints(t, metrics, name, attrKey, attrValue) {
		total += point.Sum
	}
	return total
}

// TestScheduleToStartRecordsQueueWaitByTaskType 断言正常路径：延迟被记录，且按 task 类型分维度。
//
// 运维问题：worker 够不够。执行时间正常但任务在队列里躺几分钟，只有这个指标能看见。
func TestScheduleToStartRecordsQueueWaitByTaskType(t *testing.T) {
	providers, collect := newMetricProbe(t)
	telemetry := newEngineTelemetry(providers, systemclock.Clock{})

	now := time.Now()
	telemetry.recordScheduleToStart(context.Background(), protocol.Task{
		Type:       protocol.TaskTypeWorkflow,
		Namespace:  "default",
		EnqueuedAt: now.Add(-3 * time.Second),
	}, now)
	telemetry.recordScheduleToStart(context.Background(), protocol.Task{
		Type:       protocol.TaskTypeActivity,
		Namespace:  "default",
		TaskQueue:  "default",
		EnqueuedAt: now.Add(-7 * time.Second),
	}, now)

	metrics := collect()

	if got := histogramCount(t, metrics, scheduleToStartMetric, telemetrypkg.AttrTaskType, string(protocol.TaskTypeWorkflow)); got != 1 {
		t.Fatalf("workflow schedule-to-start samples = %d, want 1", got)
	}
	if got := histogramSum(t, metrics, scheduleToStartMetric, telemetrypkg.AttrTaskType, string(protocol.TaskTypeWorkflow)); got != 3 {
		t.Fatalf("workflow schedule-to-start = %vs, want 3s", got)
	}
	if got := histogramCount(t, metrics, scheduleToStartMetric, telemetrypkg.AttrTaskType, string(protocol.TaskTypeActivity)); got != 1 {
		t.Fatalf("activity schedule-to-start samples = %d, want 1", got)
	}
	if got := histogramSum(t, metrics, scheduleToStartMetric, telemetrypkg.AttrTaskType, string(protocol.TaskTypeActivity)); got != 7 {
		t.Fatalf("activity schedule-to-start = %vs, want 7s", got)
	}
}

// TestScheduleToStartSkipsZeroEnqueuedAt 是这组测试里最重要的一条。
//
// 老消息 / 非投递侧构造的 task 反序列化后 EnqueuedAt 是零值（公元 1 年）。
// 若不显式跳过，now.Sub(零值) ≈ 2000 年会被记进直方图，P99 直接爆表、整个指标不可读。
//
// 断言的是 Count == 0（一条样本都不能有），而不是"值别太大"：
// 记 0 秒同样是错的（会稀释真实分布、掩盖真实排队），所以只有"完全不记"才算对。
//
// 红验证：去掉 recordScheduleToStart 里的 IsZero 早返回，本用例必 FAIL。
func TestScheduleToStartSkipsZeroEnqueuedAt(t *testing.T) {
	providers, collect := newMetricProbe(t)
	telemetry := newEngineTelemetry(providers, systemclock.Clock{})

	// 零值 EnqueuedAt：模拟升级前入队、仍压在队列里的老消息。
	telemetry.recordScheduleToStart(context.Background(), protocol.Task{
		Type:      protocol.TaskTypeWorkflow,
		Namespace: "default",
	}, time.Now())

	metrics := collect()

	if got := histogramCount(t, metrics, scheduleToStartMetric, "", ""); got != 0 {
		samples := histogramSum(t, metrics, scheduleToStartMetric, "", "")
		t.Fatalf("zero EnqueuedAt produced %d sample(s) totalling %vs; want 0 samples — "+
			"a missing timestamp must be skipped entirely, not recorded (as ~2000 years, nor as 0)", got, samples)
	}
}

// TestScheduleToStartZeroValueWouldBeAbsurdWithoutGuard 钉住"为什么必须跳过"这件事本身。
//
// 它先算出没有保护时会被记进去的数值，断言那个数值确实荒谬（> 100 年），
// 再断言实际记录数为 0。这样即使有人把 IsZero 判据改成某种"看起来也能跑"的写法
// （比如误用 delay <= 0 兜底），本用例仍会因为样本数不为 0 而 FAIL。
//
// 之所以单独写这条：`delay <= 0` 是最容易被误当成"已经处理了异常值"的写法，
// 但零值算出来是**巨大正数**，`<= 0` 根本挡不住。
func TestScheduleToStartZeroValueWouldBeAbsurdWithoutGuard(t *testing.T) {
	providers, collect := newMetricProbe(t)
	telemetry := newEngineTelemetry(providers, systemclock.Clock{})

	now := time.Now()
	var zero time.Time
	unguarded := now.Sub(zero)
	if unguarded < 100*365*24*time.Hour {
		t.Fatalf("precondition failed: unguarded delay = %v, expected an absurd (>100y) value; "+
			"if this ever becomes small the whole rationale for the zero guard changes", unguarded)
	}
	// 且 <= 0 这个常见写法确实挡不住它——这正是判据必须分开写的原因。
	if unguarded <= 0 {
		t.Fatal("precondition failed: zero-value delay should be a large positive, not <= 0")
	}

	telemetry.recordScheduleToStart(context.Background(), protocol.Task{Type: protocol.TaskTypeWorkflow, Namespace: "default"}, now)

	if got := histogramCount(t, collect(), scheduleToStartMetric, "", ""); got != 0 {
		t.Fatalf("zero EnqueuedAt produced %d sample(s); want 0 (unguarded value would have been %v)", got, unguarded)
	}
}

// TestScheduleToStartClampsNegativeDelayToZero 断言时钟偏差导致的负延迟被截断到 0 且**仍然记录**。
//
// 投递侧与消费侧可能不在同一台机器，NTP 偏差会让 now - enqueuedAt 为负。
// 负值成因是时钟偏差而非数据缺失，它携带的真实信息是「几乎没排队」，截到 0 正是此意。
//
// 同时断言 Count == 1：若跟着零值一起跳过，会系统性删掉延迟最低的那部分样本，
// 使 P50 被抬高——选择性丢样本造成的偏差比截断更严重。
func TestScheduleToStartClampsNegativeDelayToZero(t *testing.T) {
	providers, collect := newMetricProbe(t)
	telemetry := newEngineTelemetry(providers, systemclock.Clock{})

	now := time.Now()
	// 消费侧时钟比投递侧慢 5 秒：enqueuedAt 落在 now 之后。
	telemetry.recordScheduleToStart(context.Background(), protocol.Task{
		Type:       protocol.TaskTypeWorkflow,
		Namespace:  "default",
		EnqueuedAt: now.Add(5 * time.Second),
	}, now)

	metrics := collect()

	if got := histogramCount(t, metrics, scheduleToStartMetric, telemetrypkg.AttrTaskType, string(protocol.TaskTypeWorkflow)); got != 1 {
		t.Fatalf("negative delay produced %d sample(s), want exactly 1 — "+
			"clock skew must clamp to 0, not drop the sample (dropping biases P50 upward)", got)
	}
	if got := histogramSum(t, metrics, scheduleToStartMetric, telemetrypkg.AttrTaskType, string(protocol.TaskTypeWorkflow)); got != 0 {
		t.Fatalf("negative delay recorded as %vs, want 0s (clamped)", got)
	}
}

// TestScheduleToStartRetryRestampsTimestamp 钉住重试语义：重投**重打**时间戳，测「本次等待」。
//
// 保留原值的话，一个重试 5 次、每次 backoff 30s 的任务会报出 150s "延迟"，
// 但那绝大部分是**故意的 backoff**，不是队列饥饿——会把失败率问题伪装成容量问题。
// 重打之后每次重投独立计时，指标始终回答同一个问题：投出去之后多久被拿走。
func TestScheduleToStartRetryRestampsTimestamp(t *testing.T) {
	providers, collect := newMetricProbe(t)
	telemetry := newEngineTelemetry(providers, systemclock.Clock{})

	now := time.Now()
	// 首次入队在 10 分钟前（含多轮 backoff 等待），本次重投在 2 秒前重新打戳。
	firstEnqueue := now.Add(-10 * time.Minute)
	task := protocol.Task{Type: protocol.TaskTypeActivity, Namespace: "default", TaskQueue: "default", EnqueuedAt: firstEnqueue}

	// 模拟 dispatchActivityTask 的重派：重新打戳。
	task.EnqueuedAt = now.Add(-2 * time.Second)
	telemetry.recordScheduleToStart(context.Background(), task, now)

	metrics := collect()

	got := histogramSum(t, metrics, scheduleToStartMetric, telemetrypkg.AttrTaskType, string(protocol.TaskTypeActivity))
	if got != 2 {
		t.Fatalf("redispatched task recorded %vs, want 2s (this dispatch's wait) — "+
			"600s would mean the original stamp survived and backoff got counted as queue starvation", got)
	}
}

// TestScheduleToStartUsesNamespaceAndQueueDimensions 断言属性维度齐全，
// 否则饥饿只能看到全局一个数，定位不到是哪个 namespace / 哪条队列缺 worker。
func TestScheduleToStartUsesNamespaceAndQueueDimensions(t *testing.T) {
	providers, collect := newMetricProbe(t)
	telemetry := newEngineTelemetry(providers, systemclock.Clock{})

	now := time.Now()
	telemetry.recordScheduleToStart(context.Background(), protocol.Task{
		Type:       protocol.TaskTypeActivity,
		Namespace:  "tenant-a",
		TaskQueue:  "billing",
		EnqueuedAt: now.Add(-time.Second),
	}, now)

	metrics := collect()

	if got := histogramCount(t, metrics, scheduleToStartMetric, telemetrypkg.AttrNamespace, "tenant-a"); got != 1 {
		t.Fatalf("namespace dimension missing: samples for tenant-a = %d, want 1", got)
	}
	if got := histogramCount(t, metrics, scheduleToStartMetric, telemetrypkg.AttrTaskQueue, "billing"); got != 1 {
		t.Fatalf("task queue dimension missing: samples for billing = %d, want 1", got)
	}
}

// TestScheduleToStartNormalizesEmptyNamespace 断言空 namespace 被规范化，
// 与既有指标（recordDispatch 等）的属性取值保持一致，避免同一 namespace 出现两种写法。
func TestScheduleToStartNormalizesEmptyNamespace(t *testing.T) {
	providers, collect := newMetricProbe(t)
	telemetry := newEngineTelemetry(providers, systemclock.Clock{})

	now := time.Now()
	telemetry.recordScheduleToStart(context.Background(), protocol.Task{
		Type:       protocol.TaskTypeWorkflow,
		EnqueuedAt: now.Add(-time.Second),
	}, now)

	if got := histogramCount(t, collect(), scheduleToStartMetric, telemetrypkg.AttrNamespace, protocol.NormalizeNamespace("")); got != 1 {
		t.Fatalf("empty namespace was not normalized: samples = %d, want 1", got)
	}
}
