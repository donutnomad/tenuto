package dispatch

import (
	"context"
	"log/slog"
	"time"

	"github.com/donutnomad/tenuto/protocol"
)

// Deliver records a dispatch lease before enqueueing so recovery can retry a
// failed delivery without producing duplicate queue entries.
//
// Once the enqueue succeeds the task is delivered. A failure to mark the record
// dispatched is logged and the delivery still reports success: the record stays
// leased, and after its lease expires the reconciler may enqueue the task again.
// That copy is discarded when it starts, because the attempt or Workflow Task it
// belongs to is already started or closed.
func Deliver(
	ctx context.Context,
	store Repository,
	task protocol.Task,
	leaseTTL time.Duration,
	encode func(protocol.Task) (protocol.Task, error),
	enqueue func(context.Context, protocol.Task) error,
) (bool, error) {
	task = NormalizeTask(task)
	lease, acquired, err := store.AcquireTaskDispatch(task, leaseTTL)
	if err != nil || !acquired {
		return false, err
	}
	queueTask, err := encode(task)
	if err != nil {
		_ = store.MarkTaskDispatchFailed(task, lease.LeaseID, err.Error())
		return false, err
	}
	if err := enqueue(ctx, queueTask); err != nil {
		_ = store.MarkTaskDispatchFailed(task, lease.LeaseID, err.Error())
		return false, err
	}
	if err := store.MarkTaskDispatchDispatched(task, lease.LeaseID); err != nil {
		slog.Error("mark task dispatch delivered failed; the task is already enqueued",
			slog.String("run_id", task.RunID),
			slog.String("task_type", string(task.Type)),
			slog.Int64("scheduled_event_id", task.ScheduledEventID),
			slog.Any("err", err),
		)
	}
	return true, nil
}
