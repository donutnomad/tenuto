package engine

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/internal/worker"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	wfsdk "github.com/donutnomad/tenuto/sdk/activity"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/program/programtest"
)

func TestWorkerKeepsPollingWorkflowTasksAfterTransientError(t *testing.T) {
	api := &retryWorkflowTaskAPI{secondPoll: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	api.cancel = cancel
	defer cancel()

	worker := worker.New(api, nil, nil, wfsdk.NewRegistry(), worker.WorkerOptions{Role: worker.WorkerRoleWorkflow})
	worker.Start(ctx)
	defer worker.Stop()

	select {
	case <-api.secondPoll:
	case <-time.After(300 * time.Millisecond):
		t.Fatalf("workflow worker stopped after transient poll error")
	}
}

func TestWorkerKeepsPollingActivityTasksAfterTransientError(t *testing.T) {
	queue := &retryActivityTaskQueue{secondPoll: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	queue.cancel = cancel
	defer cancel()

	worker := worker.New(nil, queue, noopActivityRuntime{}, wfsdk.NewRegistry(), worker.WorkerOptions{Role: worker.WorkerRoleActivity})
	worker.Start(ctx)
	defer worker.Stop()

	select {
	case <-queue.secondPoll:
	case <-time.After(300 * time.Millisecond):
		t.Fatalf("activity worker stopped after transient poll error")
	}
}

func TestActivityWorkerPollCarriesWorkerInfoAndNamespace(t *testing.T) {
	queue := &pollMetadataActivityTaskQueue{seen: make(chan activityPollMetadata, 1)}
	ctx, cancel := context.WithCancel(context.Background())
	queue.cancel = cancel
	defer cancel()

	worker := worker.New(nil, queue, noopActivityRuntime{}, wfsdk.NewRegistry(), worker.WorkerOptions{
		Role:                 worker.WorkerRoleActivity,
		ActivityTaskQueues:   []string{"payments"},
		Namespace:            "tenant-a",
		Identity:             "activity-worker-a",
		BuildID:              "activity-build-v2",
		WorkflowCapabilities: []string{"cap-a"},
	})
	worker.Start(ctx)
	defer worker.Stop()

	select {
	case metadata := <-queue.seen:
		if metadata.namespace != "tenant-a" {
			t.Fatalf("poll namespace = %q, want tenant-a", metadata.namespace)
		}
		if !metadata.hasWorkerInfo {
			t.Fatal("activity poll context has no WorkerInfo")
		}
		if metadata.worker.Namespace != "tenant-a" || metadata.worker.Identity != "activity-worker-a" || metadata.worker.BuildID != "activity-build-v2" {
			t.Fatalf("poll WorkerInfo = %+v", metadata.worker)
		}
		if len(metadata.worker.WorkflowCapabilities) != 1 || metadata.worker.WorkflowCapabilities[0] != "cap-a" {
			t.Fatalf("poll capabilities = %v", metadata.worker.WorkflowCapabilities)
		}
	case <-time.After(300 * time.Millisecond):
		t.Fatal("activity worker did not poll")
	}
}

type retryWorkflowTaskAPI struct {
	polls      int
	secondPoll chan struct{}
	cancel     context.CancelFunc
}

func (a *retryWorkflowTaskAPI) PollWorkflowTask(ctx context.Context) (protocol.TaskLease, error) {
	a.polls++
	if a.polls == 1 {
		return protocol.TaskLease{}, errors.New("temporary poll failure")
	}
	close(a.secondPoll)
	if a.cancel != nil {
		a.cancel()
	}
	<-ctx.Done()
	return protocol.TaskLease{}, ctx.Err()
}

func (a *retryWorkflowTaskAPI) AckWorkflowTask(ctx context.Context, lease protocol.TaskLease) error {
	return nil
}

func (a *retryWorkflowTaskAPI) NackWorkflowTask(ctx context.Context, lease protocol.TaskLease) error {
	return nil
}

func (a *retryWorkflowTaskAPI) GetWorkflowConfig(_ context.Context, runID, _ string) (*program.WorkflowConfig, error) {
	return &program.WorkflowConfig{Type: "unused", Program: programtest.Noop()}, nil
}

func (a *retryWorkflowTaskAPI) GetEvents(runID string) ([]history.Event, error) {
	return nil, nil
}

func (a *retryWorkflowTaskAPI) GetRunStatus(runID string) (history.Status, error) {
	return history.StatusRunning, nil
}

func (a *retryWorkflowTaskAPI) HandleWorkflowTaskCompleted(ctx context.Context, runID string, scheduledEventID int64, commands []history.Command, workerIdentity string) error {
	return nil
}

func (a *retryWorkflowTaskAPI) HandleWorkflowTaskTimedOut(ctx context.Context, runID string, scheduledEventID int64, errMsg string) error {
	return nil
}

func (a *retryWorkflowTaskAPI) HandleWorkflowTaskFailed(ctx context.Context, runID string, scheduledEventID int64, cause, reason string) error {
	return nil
}

type retryActivityTaskQueue struct {
	polls      int
	secondPoll chan struct{}
	cancel     context.CancelFunc
}

type activityPollMetadata struct {
	namespace     string
	worker        protocol.WorkerInfo
	hasWorkerInfo bool
}

type pollMetadataActivityTaskQueue struct {
	seen   chan activityPollMetadata
	cancel context.CancelFunc
}

func (q *pollMetadataActivityTaskQueue) PollActivityTask(ctx context.Context, _ string) (protocol.TaskLease, error) {
	info, ok := protocol.WorkerInfoFrom(ctx)
	q.seen <- activityPollMetadata{
		namespace:     protocol.NamespaceFromContext(ctx),
		worker:        info,
		hasWorkerInfo: ok,
	}
	q.cancel()
	return protocol.TaskLease{}, ctx.Err()
}

func (*pollMetadataActivityTaskQueue) AckActivityTask(context.Context, protocol.TaskLease) error {
	return nil
}

func (*pollMetadataActivityTaskQueue) NackActivityTask(context.Context, protocol.TaskLease) error {
	return nil
}

func (q *retryActivityTaskQueue) PollActivityTask(ctx context.Context, taskQueue string) (protocol.TaskLease, error) {
	q.polls++
	if q.polls == 1 {
		return protocol.TaskLease{}, errors.New("temporary poll failure")
	}
	close(q.secondPoll)
	if q.cancel != nil {
		q.cancel()
	}
	<-ctx.Done()
	return protocol.TaskLease{}, ctx.Err()
}

func (q *retryActivityTaskQueue) AckActivityTask(ctx context.Context, lease protocol.TaskLease) error {
	return nil
}

func (q *retryActivityTaskQueue) NackActivityTask(ctx context.Context, lease protocol.TaskLease) error {
	return nil
}

func (*retryWorkflowTaskAPI) GetWorkflowActivation(context.Context, history.WorkflowActivationRequest) (history.WorkflowActivation, error) {
	return history.WorkflowActivation{}, errors.New("incremental activation is not used by this fake")
}

func (*retryWorkflowTaskAPI) CompleteWorkflowActivation(context.Context, history.WorkflowTaskCompletionRequest) (history.WorkflowTaskCompletionAck, error) {
	return history.WorkflowTaskCompletionAck{}, errors.New("incremental activation is not used by this fake")
}
