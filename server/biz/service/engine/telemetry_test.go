package engine

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/internal/worker"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/sdk/activity"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/infra/memory"
	tenutotel "github.com/donutnomad/tenuto/telemetry"
	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/otel/propagation"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func TestWorkerTelemetryPreservesParentAndEmitsAllSignals(t *testing.T) {
	traceExporter := tracetest.NewInMemoryExporter()
	tracerProvider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(traceExporter))
	metricReader := sdkmetric.NewManualReader()
	meterProvider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(metricReader))
	logExporter := &memoryLogExporter{}
	loggerProvider := sdklog.NewLoggerProvider(sdklog.WithProcessor(sdklog.NewSimpleProcessor(logExporter)))
	propagator := propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{})
	providers := tenutotel.Providers{
		TracerProvider: tracerProvider,
		MeterProvider:  meterProvider,
		LoggerProvider: loggerProvider,
		Propagator:     propagator,
		Logger: slog.New(otelslog.NewHandler(
			tenutotel.InstrumentationName(),
			otelslog.WithLoggerProvider(loggerProvider),
		)),
	}
	t.Cleanup(func() {
		_ = loggerProvider.Shutdown(context.Background())
		_ = meterProvider.Shutdown(context.Background())
		_ = tracerProvider.Shutdown(context.Background())
	})

	rootCtx, root := tracerProvider.Tracer("test").Start(context.Background(), "root")
	rootSpanContext := root.SpanContext()
	carrier := propagation.MapCarrier{}
	propagator.Inject(rootCtx, carrier)

	api := newTimeoutWorkerAPI(t, 0)
	worker := assembleWorker(api, activity.NewRegistry(), worker.WorkerOptions{
		WorkflowTaskTimeout: time.Second,
		Telemetry:           providers,
	})
	worker.HandleWorkflowTask(context.Background(), protocol.TaskLease{
		Task: protocol.Task{
			Type:             protocol.TaskTypeWorkflow,
			RunID:            api.runID,
			WorkflowType:     api.wf.ID,
			ScheduledEventID: 1,
			TraceContext:     cloneStringMap(carrier),
		},
		Token: "lease-token",
	})
	root.End()

	var runSpanFound bool
	for _, span := range traceExporter.GetSpans() {
		if span.Name != "RunWorkflowTask" {
			continue
		}
		runSpanFound = true
		if span.Parent.SpanID() != rootSpanContext.SpanID() || span.SpanContext.TraceID() != rootSpanContext.TraceID() {
			t.Fatalf("RunWorkflowTask parent = %s/%s, root = %s/%s", span.Parent.TraceID(), span.Parent.SpanID(), rootSpanContext.TraceID(), rootSpanContext.SpanID())
		}
	}
	if !runSpanFound {
		t.Fatal("RunWorkflowTask span was not exported")
	}

	var metrics metricdata.ResourceMetrics
	if err := metricReader.Collect(context.Background(), &metrics); err != nil {
		t.Fatal(err)
	}
	if !containsMetric(metrics, "tenuto.workflow.task.executions") || !containsMetric(metrics, "tenuto.workflow.task.duration") {
		t.Fatalf("workflow task metrics missing: %+v", metrics)
	}

	records := logExporter.Records()
	if len(records) == 0 {
		t.Fatal("workflow task log was not exported")
	}
	if records[len(records)-1].TraceID() != rootSpanContext.TraceID() {
		t.Fatalf("log trace ID = %s, want %s", records[len(records)-1].TraceID(), rootSpanContext.TraceID())
	}
}

func TestTraceContextSurvivesHistoryAndTaskTokenSerialization(t *testing.T) {
	carrier := map[string]string{
		"traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
		"baggage":     "tenant=acme",
	}
	event := history.NewActivityScheduledEvent(history.ActivityScheduledEvent{
		ActivityID:   "charge",
		ActivityType: "payments.Charge",
		TraceContext: cloneStringMap(carrier),
	})
	data, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	var decodedEvent history.Event
	if err := json.Unmarshal(data, &decodedEvent); err != nil {
		t.Fatal(err)
	}
	if decodedEvent.ActivityScheduled.TraceContext["traceparent"] != carrier["traceparent"] || decodedEvent.ActivityScheduled.TraceContext["baggage"] != carrier["baggage"] {
		t.Fatalf("history trace context = %#v", decodedEvent.ActivityScheduled.TraceContext)
	}

	task := protocol.Task{
		Type:             protocol.TaskTypeActivity,
		RunID:            "run-otel",
		ScheduledEventID: 9,
		Attempt:          1,
		TraceContext:     cloneStringMap(carrier),
	}
	token, err := testTaskTokens().Encode(task)
	if err != nil {
		t.Fatal(err)
	}
	decodedTask, err := testTaskTokens().Decode(token)
	if err != nil {
		t.Fatal(err)
	}
	if decodedTask.TraceContext["traceparent"] != carrier["traceparent"] || decodedTask.TraceContext["baggage"] != carrier["baggage"] {
		t.Fatalf("task token trace context = %#v", decodedTask.TraceContext)
	}
}

func TestDelayedWorkflowTaskPreservesTraceContext(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tracerProvider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	propagator := propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{})
	providers := tenutotel.Providers{TracerProvider: tracerProvider, Propagator: propagator}
	t.Cleanup(func() { _ = tracerProvider.Shutdown(context.Background()) })

	mgr := memory.NewExecutionManager()
	queue := newTestTaskQueue()
	server := assembleServer(mgr, queue, newTestTaskQueue(), WithTelemetry(providers))
	run := &execution.Run{ID: "run-delayed-trace", WorkflowID: "wf-delayed-trace", WorkflowType: "wf-delayed-trace", Status: history.StatusRunning}
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(run)) }); err != nil {
		t.Fatal(err)
	}

	rootCtx, root := tracerProvider.Tracer("test").Start(context.Background(), "root")
	server.scheduleWorkflowTaskAfter(rootCtx, run.ID, 0)
	root.End()

	events, err := mgr.GetEvents(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].WorkflowTaskScheduled == nil {
		t.Fatalf("history = %+v", events)
	}
	got := traceContextFromCarrier(propagator, events[0].WorkflowTaskScheduled.TraceContext)
	if got.TraceID() != root.SpanContext().TraceID() {
		t.Fatalf("delayed task trace ID = %s, want %s", got.TraceID(), root.SpanContext().TraceID())
	}
}

func TestReusedWorkflowTaskUsesPersistedTraceContext(t *testing.T) {
	tracerProvider := sdktrace.NewTracerProvider()
	propagator := propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{})
	providers := tenutotel.Providers{TracerProvider: tracerProvider, Propagator: propagator}
	t.Cleanup(func() { _ = tracerProvider.Shutdown(context.Background()) })

	mgr := memory.NewExecutionManager()
	queue := newTestTaskQueue()
	server := assembleServer(mgr, queue, newTestTaskQueue(), WithTelemetry(providers))
	run := &execution.Run{ID: "run-reused-trace", WorkflowID: "wf-reused-trace", WorkflowType: "wf-reused-trace", Status: history.StatusRunning}
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(run)) }); err != nil {
		t.Fatal(err)
	}

	firstCtx, first := tracerProvider.Tracer("test").Start(context.Background(), "first-parent")
	firstCarrier := propagation.MapCarrier{}
	propagator.Inject(firstCtx, firstCarrier)
	event := history.NewWorkflowTaskScheduledEvent(worker.DefaultWorkflowTaskTimeout.Milliseconds())
	event.WorkflowTaskScheduled.TraceContext = cloneStringMap(firstCarrier)
	if _, err := mgr.Append(run.ID, run.ID, run.ID, event); err != nil {
		t.Fatal(err)
	}
	first.End()

	secondCtx, second := tracerProvider.Tracer("test").Start(context.Background(), "second-parent")
	if err := server.scheduleWorkflowTask(secondCtx, run.ID); err != nil {
		t.Fatal(err)
	}
	second.End()
	if len(queue.tasks) != 1 {
		t.Fatalf("queued tasks = %d", len(queue.tasks))
	}
	got := traceContextFromCarrier(propagator, queue.tasks[0].Task.TraceContext)
	if got.TraceID() != first.SpanContext().TraceID() {
		t.Fatalf("reused task trace ID = %s, want persisted %s", got.TraceID(), first.SpanContext().TraceID())
	}
}

func TestActivityTimeoutPreservesScheduledTrace(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tracerProvider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	propagator := propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{})
	providers := tenutotel.Providers{TracerProvider: tracerProvider, Propagator: propagator}
	t.Cleanup(func() { _ = tracerProvider.Shutdown(context.Background()) })

	mgr := memory.NewExecutionManager()
	server := assembleServer(mgr, newTestTaskQueue(), newTestTaskQueue(), WithTelemetry(providers))
	run := &execution.Run{ID: "run-activity-timeout-trace", WorkflowID: "wf-activity-timeout-trace", WorkflowType: "wf-activity-timeout-trace", Status: history.StatusRunning}
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(run)) }); err != nil {
		t.Fatal(err)
	}
	rootCtx, root := tracerProvider.Tracer("test").Start(context.Background(), "activity-scheduled")
	carrier := propagation.MapCarrier{}
	propagator.Inject(rootCtx, carrier)
	if err := mgr.UpsertActivityState(&execution.ActivityState{
		RunID: run.ID, ScheduledEventID: 7, ActivityID: "charge", ActivityType: "payments.Charge",
		Attempt: 1, Stamp: 1, Status: execution.ActivityStatusEnums.Scheduled, TraceContext: cloneStringMap(carrier),
	}); err != nil {
		t.Fatal(err)
	}

	server.handleActivityTimeoutAttempt(run.ID, 7, 1, protocol.TimeoutTypeStartToClose)
	root.End()
	for _, span := range exporter.GetSpans() {
		if span.Name == "FailActivity" {
			if span.SpanContext.TraceID() != root.SpanContext().TraceID() {
				t.Fatalf("FailActivity trace ID = %s, want %s", span.SpanContext.TraceID(), root.SpanContext().TraceID())
			}
			return
		}
	}
	t.Fatal("FailActivity span was not exported")
}

func traceContextFromCarrier(propagator propagation.TextMapPropagator, carrier map[string]string) trace.SpanContext {
	return trace.SpanContextFromContext(propagator.Extract(context.Background(), propagation.MapCarrier(carrier)))
}

func containsMetric(metrics metricdata.ResourceMetrics, name string) bool {
	for _, scope := range metrics.ScopeMetrics {
		for _, metric := range scope.Metrics {
			if metric.Name == name {
				return true
			}
		}
	}
	return false
}

type memoryLogExporter struct {
	mu      sync.Mutex
	records []sdklog.Record
}

func (e *memoryLogExporter) Export(_ context.Context, records []sdklog.Record) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, record := range records {
		e.records = append(e.records, record.Clone())
	}
	return nil
}

func (e *memoryLogExporter) Shutdown(context.Context) error { return nil }

func (e *memoryLogExporter) ForceFlush(context.Context) error { return nil }

func (e *memoryLogExporter) Records() []sdklog.Record {
	e.mu.Lock()
	defer e.mu.Unlock()
	records := make([]sdklog.Record, len(e.records))
	for i, record := range e.records {
		records[i] = record.Clone()
	}
	return records
}
