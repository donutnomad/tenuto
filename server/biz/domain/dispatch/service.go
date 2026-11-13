package dispatch

import (
	"context"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/server/biz/domain/clock"
)

type StickyAssignment struct {
	WorkerIdentity      string
	StickyGeneration    int64
	ExecutionGeneration int64
	Deadline            time.Time
}

type StickyAssignmentLookup func(runID string, now time.Time) (StickyAssignment, bool)

// Service owns durable task delivery for both workflow and Activity queues.
// The Run runtime supplies the sticky assignment through a read port.
type Service struct {
	store    Repository
	workflow TaskQueue
	activity ActivityTaskQueueRouter
	encode   func(protocol.Task) (protocol.Task, error)
	sticky   StickyAssignmentLookup
	leaseTTL time.Duration
	clock    clock.Clock
}

func NewService(
	store Repository,
	workflow TaskQueue,
	activity ActivityTaskQueueRouter,
	encode func(protocol.Task) (protocol.Task, error),
	sticky StickyAssignmentLookup,
	leaseTTL time.Duration,
	clk clock.Clock,
) *Service {
	return &Service{
		store: store, workflow: workflow, activity: activity,
		encode: encode, sticky: sticky, leaseTTL: leaseTTL, clock: clk,
	}
}

func (s *Service) DispatchWorkflow(ctx context.Context, task protocol.Task) (bool, error) {
	task.Type = protocol.TaskTypeWorkflow
	if assignment, valid := s.sticky(task.RunID, s.clock.Now()); valid {
		task.WorkflowTaskRoute = protocol.WorkflowTaskRouteSticky
		task.StickyTarget = assignment.WorkerIdentity
		task.StickyGeneration = assignment.StickyGeneration
		task.ExecutionGeneration = assignment.ExecutionGeneration
		task.StickyScheduleToStartDeadline = assignment.Deadline
	}
	return Deliver(ctx, s.store, task, s.leaseTTL, s.encode, func(ctx context.Context, queueTask protocol.Task) error {
		queueTask.EnqueuedAt = s.clock.Now()
		if queueTask.WorkflowTaskRoute == protocol.WorkflowTaskRouteSticky {
			return s.workflow.EnqueueSticky(ctx, queueTask.StickyTarget, queueTask)
		}
		queueTask.WorkflowTaskRoute = protocol.WorkflowTaskRouteNormal
		return s.workflow.Enqueue(ctx, queueTask)
	})
}

func (s *Service) DispatchActivity(ctx context.Context, task protocol.Task) (bool, error) {
	task.Type = protocol.TaskTypeActivity
	return Deliver(ctx, s.store, task, s.leaseTTL, s.encode, func(ctx context.Context, queueTask protocol.Task) error {
		queueTask.EnqueuedAt = s.clock.Now()
		return s.activity.Enqueue(ctx, queueTask.TaskQueue, queueTask)
	})
}
