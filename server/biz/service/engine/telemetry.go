package engine

import (
	"context"
	"database/sql"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/server/biz/domain/clock"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	tenutotel "github.com/donutnomad/tenuto/telemetry"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// ServerOption configures optional Server dependencies.
type ServerOption func(*serverOptions)

type serverOptions struct {
	telemetry        tenutotel.Providers
	queryConcurrency int
}

// DefaultQueryConcurrency 是一个 Server 进程同时在本地重放 History 求值 Query 的默认上限。
const DefaultQueryConcurrency = 8

// WithQueryConcurrency 设定同时在本进程重放 History 求值 Query 的上限；n <= 0 时取 DefaultQueryConcurrency。
// 超过上限的 Query 立即以 protocol.ErrResourceExhausted 失败（HTTP 503 与 Retry-After），由调用方退避重发。
func WithQueryConcurrency(n int) ServerOption {
	return func(options *serverOptions) {
		options.queryConcurrency = n
	}
}

// WithTelemetry injects OpenTelemetry providers into a Server.
func WithTelemetry(providers tenutotel.Providers) ServerOption {
	return func(options *serverOptions) {
		options.telemetry = providers
	}
}

func resolveServerOptions(options []ServerOption) serverOptions {
	resolved := serverOptions{}
	for _, option := range options {
		if option != nil {
			option(&resolved)
		}
	}
	resolved.telemetry = tenutotel.Resolve(resolved.telemetry)
	if resolved.queryConcurrency <= 0 {
		resolved.queryConcurrency = DefaultQueryConcurrency
	}
	return resolved
}

type engineTelemetry struct {
	clock               clock.Clock
	providers           tenutotel.Providers
	tracer              trace.Tracer
	dispatchCounter     metric.Int64Counter
	dispatchDuration    metric.Float64Histogram
	transactionCounter  metric.Int64Counter
	transactionDuration metric.Float64Histogram
	backlogCounter      metric.Int64Counter
	gcReclaimedCounter  metric.Int64Counter
	gcFailureCounter    metric.Int64Counter
	gcBacklog           metric.Int64Gauge
	gcBacklogAge        metric.Float64Gauge
	scheduleToStart     metric.Float64Histogram
}

func newEngineTelemetry(providers tenutotel.Providers, clk clock.Clock) *engineTelemetry {
	providers = tenutotel.Resolve(providers)
	meter := providers.MeterProvider.Meter(tenutotel.InstrumentationName())
	t := &engineTelemetry{
		clock:     clk,
		providers: providers,
		tracer:    providers.TracerProvider.Tracer(tenutotel.InstrumentationName()),
	}
	t.dispatchCounter, _ = meter.Int64Counter("tenuto.task.dispatches", metric.WithUnit("{task}"))
	t.dispatchDuration, _ = meter.Float64Histogram("tenuto.task.dispatch.duration", metric.WithUnit("s"))
	t.transactionCounter, _ = meter.Int64Counter("tenuto.persistence.transactions", metric.WithUnit("{transaction}"))
	t.transactionDuration, _ = meter.Float64Histogram("tenuto.persistence.transaction.duration", metric.WithUnit("s"))
	t.backlogCounter, _ = meter.Int64Counter("tenuto.task.backlog", metric.WithUnit("{task}"))
	t.gcReclaimedCounter, _ = meter.Int64Counter("tenuto.retention.gc.reclaimed", metric.WithUnit("{object}"))
	t.gcFailureCounter, _ = meter.Int64Counter("tenuto.retention.gc.failures", metric.WithUnit("{task}"))
	t.gcBacklog, _ = meter.Int64Gauge("tenuto.retention.gc.backlog", metric.WithUnit("{task}"))
	t.gcBacklogAge, _ = meter.Float64Gauge("tenuto.retention.gc.backlog.oldest_age", metric.WithUnit("s"))
	t.scheduleToStart, _ = meter.Float64Histogram("tenuto.task.schedule_to_start", metric.WithUnit("s"))
	return t
}

// recordScheduleToStart 记录任务从入队到被消费者取走的等待时长，按 task 类型分维度。
//
// 这是唯一能测出**队列饥饿**的指标：workflow.task.duration / activity.duration 测的都是
// start→close（执行时间），worker 不够时执行时间可以完全正常，而任务在队列里排队几分钟。
//
// **精度边界（重要）**：投递侧与消费侧可能不在同一进程/机器（tenutoflow 多副本 + taskq），
// enqueuedAt 与 now 取自两个不同的墙钟，本指标因而受 NTP 同步精度影响。毫秒级抖动属正常，
// 请按趋势与量级使用（排队 10ms 还是 10s），不要当精确值卡 SLA 绝对阈值。
//
// 两类异常输入分开处理，判据不可合并：
//
//   - **零值 enqueuedAt → 跳过，不记录任何样本。** 老消息 / 非投递侧构造的 task 反序列化后
//     是零值（公元 1 年），now.Sub(零值) ≈ 2000 年，记进去 P99 直接爆表、整个指标不可读。
//     注意不能用 `<= 0` 兜住它——零值算出来是**巨大正数**，`<= 0` 根本挡不住。
//     记 0 同样不行：会稀释真实分布、掩盖真实排队。宁可漏记，不可错记。
//   - **负值 delay → 截断到 0 并正常记录。** 负值成因是时钟偏差而非数据缺失，它携带的真实
//     信息是「几乎没排队」，截到 0 正是此意。若跟着跳过，会系统性删掉延迟最低的那部分样本，
//     使 P50 被抬高——选择性丢样本造成的偏差比截断更严重。
func (t *engineTelemetry) recordScheduleToStart(ctx context.Context, task protocol.Task, now time.Time) {
	if task.EnqueuedAt.IsZero() {
		return
	}
	delay := now.Sub(task.EnqueuedAt)
	if delay < 0 {
		delay = 0
	}
	t.scheduleToStart.Record(ctx, delay.Seconds(), metric.WithAttributes(
		attribute.String(tenutotel.AttrNamespace, protocol.NormalizeNamespace(task.Namespace)),
		attribute.String(tenutotel.AttrTaskType, string(task.Type)),
		attribute.String(tenutotel.AttrTaskQueue, task.TaskQueue),
	))
}

// recordBacklog 记录 reconciler 一轮补投的积压任务数。
//
// 这个计数**持续非零本身就是故障信号**：正常路径下任务在调度时即已投递，reconciler
// 只是兜底。它一直在补，说明主投递路径在丢任务。
func (t *engineTelemetry) recordBacklog(ctx context.Context, workflowTasks, activityTasks int) {
	if workflowTasks > 0 {
		t.backlogCounter.Add(ctx, int64(workflowTasks),
			metric.WithAttributes(attribute.String(tenutotel.AttrTaskType, string(protocol.TaskTypeWorkflow))))
	}
	if activityTasks > 0 {
		t.backlogCounter.Add(ctx, int64(activityTasks),
			metric.WithAttributes(attribute.String(tenutotel.AttrTaskType, string(protocol.TaskTypeActivity))))
	}
}

// recordGCReclaimed 记录保留期回收删除的对象数。
func (t *engineTelemetry) recordGCReclaimed(ctx context.Context, object string, count int) {
	if count <= 0 {
		return
	}
	t.gcReclaimedCounter.Add(ctx, int64(count),
		metric.WithAttributes(attribute.String(tenutotel.AttrGCObject, object)))
}

// recordGCFailure 记录一次 GC 失败。outcome=parked 表示任务已被搁置——
// 那意味着有数据删不掉且没人知道，是必须告警的状态。
func (t *engineTelemetry) recordGCFailure(ctx context.Context, outcome string) {
	t.gcFailureCounter.Add(ctx, 1,
		metric.WithAttributes(attribute.String(tenutotel.AttrOutcome, outcome)))
}

// recordGCBacklog 记录 GC leader 一轮扫描之后到期未处理的删除任务数与最早任务的等待时长。
// 两者只由持有 GC 租约的副本上报；持续大于 0 且等待时长增长表示回收跟不上到期速度。
func (t *engineTelemetry) recordGCBacklog(ctx context.Context, overdue int, oldestAge time.Duration) {
	t.gcBacklog.Record(ctx, int64(overdue))
	t.gcBacklogAge.Record(ctx, oldestAge.Seconds())
}

// DBPoolStatsSource 提供连接池统计。由 *sql.DB 直接满足（它的 Stats() 返回
// sql.DBStats），定义成接口是为了让测试不必真的拨一个数据库。
type DBPoolStatsSource interface {
	Stats() sql.DBStats
}

// RegisterDBPoolMetrics 把连接池饱和度接到 OTel 的异步 instrument 上，返回注销函数。
//
// 用 observable（异步）而非同步 counter：池状态是**当前值**，没有"事件发生"的时刻可挂钩，
// 只能在采集时回读。name 区分同一进程内的多个池（workflow / taskq）。
//
// 回调在采集线程上跑，`sql.DB.Stats()` 只取一把内部锁、不碰网络，代价可忽略。
func RegisterDBPoolMetrics(providers tenutotel.Providers, name string, db DBPoolStatsSource) (func() error, error) {
	if db == nil {
		return func() error { return nil }, nil
	}
	providers = tenutotel.Resolve(providers)
	meter := providers.MeterProvider.Meter(tenutotel.InstrumentationName())
	poolAttr := attribute.String(tenutotel.AttrOperation, name)

	// WaitCount/WaitDuration 是单调累加的，故用 ObservableCounter 而非 Gauge：
	// 它们回答"有没有 goroutine 在排队等连接"，增长即说明池已成为瓶颈。
	waitCount, err := meter.Int64ObservableCounter("tenuto.persistence.pool.wait.count", metric.WithUnit("{wait}"))
	if err != nil {
		return nil, err
	}
	waitDuration, err := meter.Float64ObservableCounter("tenuto.persistence.pool.wait.duration", metric.WithUnit("s"))
	if err != nil {
		return nil, err
	}
	connections, err := meter.Int64ObservableGauge("tenuto.persistence.pool.connections", metric.WithUnit("{connection}"))
	if err != nil {
		return nil, err
	}

	registration, err := meter.RegisterCallback(func(_ context.Context, observer metric.Observer) error {
		stats := db.Stats()
		observer.ObserveInt64(waitCount, stats.WaitCount, metric.WithAttributes(poolAttr))
		observer.ObserveFloat64(waitDuration, stats.WaitDuration.Seconds(), metric.WithAttributes(poolAttr))
		observer.ObserveInt64(connections, int64(stats.InUse),
			metric.WithAttributes(poolAttr, attribute.String(tenutotel.AttrPoolState, "in_use")))
		observer.ObserveInt64(connections, int64(stats.Idle),
			metric.WithAttributes(poolAttr, attribute.String(tenutotel.AttrPoolState, "idle")))
		return nil
	}, waitCount, waitDuration, connections)
	if err != nil {
		return nil, err
	}
	return registration.Unregister, nil
}

func (t *engineTelemetry) start(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	return t.tracer.Start(ctx, name, trace.WithAttributes(attrs...))
}

func (t *engineTelemetry) inject(ctx context.Context) map[string]string {
	if !trace.SpanContextFromContext(ctx).IsValid() {
		return nil
	}
	carrier := propagation.MapCarrier{}
	t.providers.Propagator.Inject(ctx, carrier)
	if len(carrier) == 0 {
		return nil
	}
	return cloneStringMap(carrier)
}

func (t *engineTelemetry) extract(ctx context.Context, values map[string]string) context.Context {
	if len(values) == 0 {
		return ctx
	}
	return t.providers.Propagator.Extract(ctx, propagation.MapCarrier(values))
}

func (t *engineTelemetry) recordDispatch(ctx context.Context, task protocol.Task, outcome string, elapsed time.Duration) {
	attrs := metric.WithAttributes(
		attribute.String(tenutotel.AttrNamespace, protocol.NormalizeNamespace(task.Namespace)),
		attribute.String(tenutotel.AttrTaskType, string(task.Type)),
		attribute.String(tenutotel.AttrTaskQueue, task.TaskQueue),
		attribute.String(tenutotel.AttrOutcome, outcome),
	)
	t.dispatchCounter.Add(ctx, 1, attrs)
	t.dispatchDuration.Record(ctx, elapsed.Seconds(), attrs)
}

func (t *engineTelemetry) runInTx(ctx context.Context, operation string, manager execution.ExecutionManager, fn func(execution.ExecutionTx) error) (err error) {
	started := t.clock.Now()
	ctx, span := t.start(ctx, "persistence.transaction "+operation, attribute.String(tenutotel.AttrOperation, operation))
	defer func() {
		outcome := "success"
		if err != nil {
			outcome = "error"
		}
		attrs := metric.WithAttributes(attribute.String(tenutotel.AttrOperation, operation), attribute.String(tenutotel.AttrOutcome, outcome))
		t.transactionCounter.Add(ctx, 1, attrs)
		t.transactionDuration.Record(ctx, t.clock.Now().Sub(started).Seconds(), attrs)
		tenutotel.FinishSpan(span, err)
	}()
	err = manager.RunInTx(fn)
	return err
}

func workflowSpanAttributes(namespace, workflowID, runID, workflowType string) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String(tenutotel.AttrNamespace, protocol.NormalizeNamespace(namespace)),
		attribute.String(tenutotel.AttrWorkflowID, workflowID),
		attribute.String(tenutotel.AttrWorkflowRunID, runID),
		attribute.String(tenutotel.AttrWorkflowType, workflowType),
	}
}
