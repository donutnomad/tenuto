package runtimeclient

import (
	"context"
	"errors"
	"fmt"

	"github.com/donutnomad/tenuto/protocol"
	tenutotel "github.com/donutnomad/tenuto/telemetry"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

func RuntimeTaskToken(task protocol.Task) (protocol.TaskToken, error) {
	if task.TaskToken == "" {
		return "", fmt.Errorf("runtime task token is empty")
	}
	return task.TaskToken, nil
}

type clientTelemetry struct {
	tracer trace.Tracer
}

func newClientTelemetry(providers tenutotel.Providers) *clientTelemetry {
	providers = tenutotel.Resolve(providers)
	return &clientTelemetry{tracer: providers.TracerProvider.Tracer(tenutotel.InstrumentationName())}
}

func (t *clientTelemetry) start(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	return t.tracer.Start(ctx, name, trace.WithAttributes(attrs...))
}

func finishSpan(span trace.Span, err error) {
	if err != nil && !errors.Is(err, context.Canceled) {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	span.End()
}

const (
	attrNamespace     = "tenuto.namespace"
	attrWorkflowID    = "tenuto.workflow.id"
	attrWorkflowRunID = "tenuto.workflow.run_id"
)
