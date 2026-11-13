package telemetry

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	otellog "go.opentelemetry.io/otel/log"
	lognoop "go.opentelemetry.io/otel/log/noop"
	"go.opentelemetry.io/otel/metric"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/propagation"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/semconv/v1.37.0"
	"go.opentelemetry.io/otel/trace"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
)

const instrumentationName = "github.com/donutnomad/tenuto"

// Providers is injected into Server, Worker, SDK clients, and transports.
type Providers struct {
	TracerProvider trace.TracerProvider
	MeterProvider  metric.MeterProvider
	LoggerProvider otellog.LoggerProvider
	Propagator     propagation.TextMapPropagator
	Logger         *slog.Logger
}

func NoopProviders() Providers {
	logger := slog.New(slog.NewJSONHandler(discardWriter{}, nil))
	return Providers{
		TracerProvider: tracenoop.NewTracerProvider(),
		MeterProvider:  metricnoop.NewMeterProvider(),
		LoggerProvider: lognoop.NewLoggerProvider(),
		Propagator: propagation.NewCompositeTextMapPropagator(
			propagation.TraceContext{},
			propagation.Baggage{},
		),
		Logger: logger,
	}
}

// Resolve fills omitted provider fields with OpenTelemetry no-op implementations.
func Resolve(providers Providers) Providers {
	defaults := NoopProviders()
	if providers.TracerProvider == nil {
		providers.TracerProvider = defaults.TracerProvider
	}
	if providers.MeterProvider == nil {
		providers.MeterProvider = defaults.MeterProvider
	}
	if providers.LoggerProvider == nil {
		providers.LoggerProvider = defaults.LoggerProvider
	}
	if providers.Propagator == nil {
		providers.Propagator = defaults.Propagator
	}
	if providers.Logger == nil {
		providers.Logger = defaults.Logger
	}
	return providers
}

type Runtime struct {
	config         Config
	providers      Providers
	tracerProvider *sdktrace.TracerProvider
	meterProvider  *sdkmetric.MeterProvider
	loggerProvider *sdklog.LoggerProvider
}

func New(ctx context.Context, config Config) (*Runtime, error) {
	config = config.WithDefaults()
	if err := config.Validate(); err != nil {
		return nil, err
	}
	stdoutHandler := slog.NewJSONHandler(config.LogOutput, &slog.HandlerOptions{Level: config.LogLevel})
	if !config.Enabled {
		providers := NoopProviders()
		providers.Logger = slog.New(stdoutHandler)
		return &Runtime{config: config, providers: providers}, nil
	}

	res, err := newResource(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("create telemetry resource: %w", err)
	}

	traceExporter, err := otlptracegrpc.New(ctx)
	if err != nil {
		return nil, fmt.Errorf("create OTLP trace exporter: %w", err)
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(config.TraceSampleRatio))),
		sdktrace.WithBatcher(traceExporter),
	)

	metricExporter, err := otlpmetricgrpc.New(ctx)
	if err != nil {
		_ = tp.Shutdown(ctx)
		return nil, fmt.Errorf("create OTLP metric exporter: %w", err)
	}
	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithResource(res),
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExporter, sdkmetric.WithInterval(config.MetricExportInterval))),
	)

	logExporter, err := otlploggrpc.New(ctx)
	if err != nil {
		_ = mp.Shutdown(ctx)
		_ = tp.Shutdown(ctx)
		return nil, fmt.Errorf("create OTLP log exporter: %w", err)
	}
	lp := sdklog.NewLoggerProvider(
		sdklog.WithResource(res),
		sdklog.WithProcessor(sdklog.NewBatchProcessor(logExporter)),
	)

	propagator := propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	)
	otelHandler := minimumLevelHandler{
		level: config.LogLevel,
		next:  otelslog.NewHandler(instrumentationName, otelslog.WithLoggerProvider(lp)),
	}
	logger := slog.New(newFanoutHandler(stdoutHandler, otelHandler))
	providers := Providers{
		TracerProvider: tp,
		MeterProvider:  mp,
		LoggerProvider: lp,
		Propagator:     propagator,
		Logger:         logger,
	}

	otel.SetTracerProvider(tp)
	otel.SetMeterProvider(mp)
	otel.SetTextMapPropagator(propagator)
	return &Runtime{
		config: config, providers: providers,
		tracerProvider: tp, meterProvider: mp, loggerProvider: lp,
	}, nil
}

func (r *Runtime) Providers() Providers {
	return Resolve(r.providers)
}

func (r *Runtime) Shutdown(ctx context.Context) error {
	if r == nil {
		return nil
	}
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, r.config.ShutdownTimeout)
		defer cancel()
	}
	var errs []error
	if r.loggerProvider != nil {
		errs = appendShutdownError(errs, "logs", r.loggerProvider.ForceFlush(ctx))
		errs = appendShutdownError(errs, "logs", r.loggerProvider.Shutdown(ctx))
	}
	if r.meterProvider != nil {
		errs = appendShutdownError(errs, "metrics", r.meterProvider.ForceFlush(ctx))
		errs = appendShutdownError(errs, "metrics", r.meterProvider.Shutdown(ctx))
	}
	if r.tracerProvider != nil {
		errs = appendShutdownError(errs, "traces", r.tracerProvider.ForceFlush(ctx))
		errs = appendShutdownError(errs, "traces", r.tracerProvider.Shutdown(ctx))
	}
	return errors.Join(errs...)
}

func newResource(ctx context.Context, config Config) (*resource.Resource, error) {
	attrs := []resource.Option{
		resource.WithFromEnv(),
		resource.WithProcess(),
		resource.WithOS(),
		resource.WithHost(),
		resource.WithTelemetrySDK(),
		resource.WithAttributes(
			semconv.ServiceName(config.ServiceName),
			semconv.ServiceVersion(config.ServiceVersion),
		),
	}
	if config.ServiceInstanceID != "" {
		attrs = append(attrs, resource.WithAttributes(semconv.ServiceInstanceID(config.ServiceInstanceID)))
	}
	return resource.New(ctx, attrs...)
}

func appendShutdownError(errs []error, signal string, err error) []error {
	if err != nil {
		return append(errs, fmt.Errorf("shutdown telemetry %s: %w", signal, err))
	}
	return errs
}

type discardWriter struct{}

func (discardWriter) Write(p []byte) (int, error) { return len(p), nil }

// InstrumentationName is shared by Tenuto's tracers, meters, and log bridge.
func InstrumentationName() string { return instrumentationName }
