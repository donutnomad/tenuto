package worker

import (
	"context"
	"log/slog"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	tenutotel "github.com/donutnomad/tenuto/telemetry"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// workerTelemetry 是 Worker 侧的 trace、指标与任务日志。
type workerTelemetry struct {
	providers            tenutotel.Providers
	tracer               trace.Tracer
	workflowTaskCounter  metric.Int64Counter
	workflowTaskDuration metric.Float64Histogram
	activityCounter      metric.Int64Counter
	activityDuration     metric.Float64Histogram
	cacheLookupCounter   metric.Int64Counter
}

func newWorkerTelemetry(providers tenutotel.Providers) *workerTelemetry {
	providers = tenutotel.Resolve(providers)
	meter := providers.MeterProvider.Meter(tenutotel.InstrumentationName())
	t := &workerTelemetry{
		providers: providers,
		tracer:    providers.TracerProvider.Tracer(tenutotel.InstrumentationName()),
	}
	t.workflowTaskCounter, _ = meter.Int64Counter("tenuto.workflow.task.executions", metric.WithUnit("{task}"))
	t.workflowTaskDuration, _ = meter.Float64Histogram("tenuto.workflow.task.duration", metric.WithUnit("s"))
	t.activityCounter, _ = meter.Int64Counter("tenuto.activity.executions", metric.WithUnit("{activity}"))
	t.activityDuration, _ = meter.Float64Histogram("tenuto.activity.duration", metric.WithUnit("s"))
	t.cacheLookupCounter, _ = meter.Int64Counter("tenuto.workflow.cache.lookups", metric.WithUnit("{lookup}"))
	return t
}

func (t *workerTelemetry) start(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	return t.tracer.Start(ctx, name, trace.WithAttributes(attrs...))
}

func (t *workerTelemetry) extract(ctx context.Context, values map[string]string) context.Context {
	if len(values) == 0 {
		return ctx
	}
	return t.providers.Propagator.Extract(ctx, propagation.MapCarrier(values))
}

type taskObservationKey struct{}

type taskObservation struct {
	outcome string
	err     error
}

func (t *workerTelemetry) recordCacheLookups(ctx context.Context, hits, misses uint64) {
	if hits > 0 {
		t.cacheLookupCounter.Add(ctx, int64(hits),
			metric.WithAttributes(attribute.String(tenutotel.AttrCacheResult, "hit")))
	}
	if misses > 0 {
		t.cacheLookupCounter.Add(ctx, int64(misses),
			metric.WithAttributes(attribute.String(tenutotel.AttrCacheResult, "miss")))
	}
}

// recordGCReclaimed 记录 retention GC 回收掉的对象数，按对象类型分维度。
func (t *workerTelemetry) recordWorkflowTask(ctx context.Context, task protocol.Task, outcome string, elapsed time.Duration) {
	attrs := metric.WithAttributes(
		attribute.String(tenutotel.AttrNamespace, protocol.NormalizeNamespace(task.Namespace)),
		attribute.String(tenutotel.AttrWorkflowType, task.WorkflowType),
		attribute.String(tenutotel.AttrOutcome, outcome),
	)
	t.workflowTaskCounter.Add(ctx, 1, attrs)
	t.workflowTaskDuration.Record(ctx, elapsed.Seconds(), attrs)
}

func (t *workerTelemetry) recordActivity(ctx context.Context, task protocol.Task, outcome string, elapsed time.Duration) {
	attrs := metric.WithAttributes(
		attribute.String(tenutotel.AttrNamespace, protocol.NormalizeNamespace(task.Namespace)),
		attribute.String(tenutotel.AttrActivityType, task.ActivityType),
		attribute.String(tenutotel.AttrTaskQueue, task.TaskQueue),
		attribute.String(tenutotel.AttrOutcome, outcome),
	)
	t.activityCounter.Add(ctx, 1, attrs)
	t.activityDuration.Record(ctx, elapsed.Seconds(), attrs)
}

func withTaskObservation(ctx context.Context) (context.Context, *taskObservation) {
	observation := &taskObservation{outcome: "success"}
	return context.WithValue(ctx, taskObservationKey{}, observation), observation
}

func markTaskOutcome(ctx context.Context, outcome string, err error) {
	observation, _ := ctx.Value(taskObservationKey{}).(*taskObservation)
	if observation == nil {
		return
	}
	if outcome != "" {
		observation.outcome = outcome
	}
	if err != nil {
		observation.err = err
	}
}

func (t *workerTelemetry) logTask(ctx context.Context, message string, task protocol.Task, observation *taskObservation, elapsed time.Duration) {
	attrs := []slog.Attr{
		slog.String("namespace", protocol.NormalizeNamespace(task.Namespace)),
		slog.String("run_id", task.RunID),
		slog.Int64("scheduled_event_id", task.ScheduledEventID),
		slog.String("outcome", observation.outcome),
		slog.Duration("duration", elapsed),
	}
	if task.WorkflowType != "" {
		attrs = append(attrs, slog.String("workflow_type", task.WorkflowType))
	}
	if task.ActivityType != "" {
		attrs = append(attrs, slog.String("activity_type", task.ActivityType))
	}
	if task.TaskQueue != "" {
		attrs = append(attrs, slog.String("task_queue", task.TaskQueue))
	}
	level := slog.LevelInfo
	if observation.err != nil {
		level = slog.LevelError
		attrs = append(attrs, slog.Any("err", observation.err))
	}
	t.providers.Logger.LogAttrs(ctx, level, message, attrs...)
}
