package telemetry

import (
	"context"
	"errors"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// Server 与 Worker 的 span、指标共用的属性名。
const (
	AttrNamespace        = "tenuto.namespace"
	AttrWorkflowID       = "tenuto.workflow.id"
	AttrWorkflowRunID    = "tenuto.workflow.run_id"
	AttrWorkflowType     = "tenuto.workflow.type"
	AttrActivityType     = "tenuto.activity.type"
	AttrActivityID       = "tenuto.activity.id"
	AttrScheduledEventID = "tenuto.scheduled_event_id"
	AttrAttempt          = "tenuto.attempt"
	AttrTaskQueue        = "tenuto.task.queue"
	AttrTaskType         = "tenuto.task.type"
	AttrOutcome          = "tenuto.outcome"
	AttrOperation        = "tenuto.operation"
	AttrCacheResult      = "tenuto.cache.result"
	AttrGCObject         = "tenuto.retention.object"
	AttrPoolState        = "tenuto.pool.state"
)

// TaskSpanAttributes 返回一个 task 的 span 属性。
func TaskSpanAttributes(task protocol.Task) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String(AttrNamespace, protocol.NormalizeNamespace(task.Namespace)),
		attribute.String(AttrWorkflowRunID, task.RunID),
		attribute.String(AttrWorkflowType, task.WorkflowType),
		attribute.String(AttrActivityType, task.ActivityType),
		attribute.String(AttrActivityID, task.ActivityID),
		attribute.Int64(AttrScheduledEventID, task.ScheduledEventID),
		attribute.Int(AttrAttempt, history.NormalizeActivityAttempt(task.Attempt)),
		attribute.String(AttrTaskQueue, task.TaskQueue),
	}
}

// FinishSpan 结束 span；err 非空且不是 context.Canceled 时记为错误。
func FinishSpan(span trace.Span, err error) {
	if err != nil && !errors.Is(err, context.Canceled) {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	span.End()
}
