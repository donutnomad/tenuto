package engine

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/internal/worker"
	"github.com/donutnomad/tenuto/protocol"
	wfsdk "github.com/donutnomad/tenuto/sdk/activity"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

func TestActivityWorkerRegistersVersionOnStart(t *testing.T) {
	versionAPI := newActivityWorkerVersionRegistrySpy()
	activities := wfsdk.NewRegistry()
	activities.Register("custodian.Capture", func(ctx context.Context, input map[string]any) (map[string]any, error) {
		return map[string]any{"ok": true}, nil
	})
	worker := assembleActivityWorker(memory.NewActivityQueueRouter(1), noopActivityRuntime{}, activities, worker.WorkerOptions{
		ActivityTaskQueues:            []string{"custodian"},
		Identity:                      "custodian-worker-1",
		BuildID:                       "custodian@v1",
		ActivityWorkerVersionRegistry: versionAPI,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	defer worker.Stop()

	worker.Start(ctx)
	readyCtx, readyCancel := context.WithTimeout(context.Background(), time.Second)
	defer readyCancel()
	if err := worker.WaitReady(readyCtx); err != nil {
		t.Fatal(err)
	}

	registered := receiveRegisteredWorkerVersion(t, versionAPI)
	if registered.Identity != "custodian-worker-1" || registered.BuildID != "custodian@v1" {
		t.Fatalf("unexpected registered worker identity: %+v", registered)
	}
	if len(registered.TaskQueues) != 1 || registered.TaskQueues[0] != "custodian" {
		t.Fatalf("unexpected registered task queues: %+v", registered)
	}
	if len(registered.ActivityTypes) != 1 || registered.ActivityTypes[0] != "custodian.Capture" {
		t.Fatalf("unexpected registered activity types: %+v", registered)
	}
}

func TestActivityWorkerHeartbeatsRegisteredVersion(t *testing.T) {
	versionAPI := newActivityWorkerVersionRegistrySpy()
	activities := wfsdk.NewRegistry()
	activities.Register("custodian.Capture", func(ctx context.Context, input map[string]any) (map[string]any, error) {
		return map[string]any{"ok": true}, nil
	})
	worker := assembleActivityWorker(memory.NewActivityQueueRouter(1), noopActivityRuntime{}, activities, worker.WorkerOptions{
		ActivityTaskQueues:                     []string{"custodian"},
		Identity:                               "custodian-worker-1",
		BuildID:                                "custodian@v1",
		ActivityWorkerVersionRegistry:          versionAPI,
		ActivityWorkerVersionHeartbeatInterval: 5 * time.Millisecond,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	defer worker.Stop()

	worker.Start(ctx)
	_ = receiveRegisteredWorkerVersion(t, versionAPI)

	heartbeat := receiveWorkerVersionHeartbeat(t, versionAPI)
	if heartbeat.identity != "custodian-worker-1" || heartbeat.buildID != "custodian@v1" {
		t.Fatalf("unexpected heartbeat: %+v", heartbeat)
	}
}

func TestActivityWorkerWaitReadyReturnsAfterRegistrationRetry(t *testing.T) {
	versionAPI := newActivityWorkerVersionRegistrySpy()
	versionAPI.registerErrors <- errors.New("Tenuto Server unavailable")
	versionAPI.registerErrors <- nil
	activities := wfsdk.NewRegistry()
	activities.Register("custodian.Capture", func(ctx context.Context, input map[string]any) (map[string]any, error) {
		return map[string]any{"ok": true}, nil
	})
	worker := assembleActivityWorker(memory.NewActivityQueueRouter(1), noopActivityRuntime{}, activities, worker.WorkerOptions{
		ActivityTaskQueues:                     []string{"custodian"},
		Identity:                               "custodian-worker-1",
		BuildID:                                "custodian@v1",
		ActivityWorkerVersionRegistry:          versionAPI,
		ActivityWorkerVersionHeartbeatInterval: 5 * time.Millisecond,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	defer worker.Stop()

	worker.Start(ctx)
	readyCtx, readyCancel := context.WithTimeout(context.Background(), time.Second)
	defer readyCancel()
	if err := worker.WaitReady(readyCtx); err != nil {
		t.Fatal(err)
	}

	registered := receiveRegisteredWorkerVersion(t, versionAPI)
	if registered.Identity != "custodian-worker-1" || registered.BuildID != "custodian@v1" {
		t.Fatalf("unexpected registered worker version after retry: %+v", registered)
	}
}

type activityWorkerVersionRegistrySpy struct {
	registered     chan protocol.ActivityWorkerVersion
	heartbeats     chan workerVersionHeartbeat
	registerErrors chan error
}

type workerVersionHeartbeat struct {
	identity string
	buildID  string
}

func newActivityWorkerVersionRegistrySpy() *activityWorkerVersionRegistrySpy {
	return &activityWorkerVersionRegistrySpy{
		registered:     make(chan protocol.ActivityWorkerVersion, 4),
		heartbeats:     make(chan workerVersionHeartbeat, 8),
		registerErrors: make(chan error, 8),
	}
}

func (s *activityWorkerVersionRegistrySpy) RegisterActivityWorkerVersion(ctx context.Context, worker protocol.ActivityWorkerVersion) error {
	select {
	case err := <-s.registerErrors:
		if err != nil {
			return err
		}
	default:
	}
	s.registered <- worker
	return nil
}

func (s *activityWorkerVersionRegistrySpy) DeregisterActivityWorkerVersionInNamespace(ctx context.Context, namespace, identity, buildID string) error {
	return nil
}

func (s *activityWorkerVersionRegistrySpy) HeartbeatActivityWorkerVersionInNamespace(ctx context.Context, namespace, identity, buildID string) error {
	s.heartbeats <- workerVersionHeartbeat{identity: identity, buildID: buildID}
	return nil
}

func receiveRegisteredWorkerVersion(t *testing.T, spy *activityWorkerVersionRegistrySpy) protocol.ActivityWorkerVersion {
	t.Helper()
	select {
	case worker := <-spy.registered:
		return worker
	case <-time.After(time.Second):
		t.Fatal("expected activity worker version registration")
		return protocol.ActivityWorkerVersion{}
	}
}

func receiveWorkerVersionHeartbeat(t *testing.T, spy *activityWorkerVersionRegistrySpy) workerVersionHeartbeat {
	t.Helper()
	select {
	case heartbeat := <-spy.heartbeats:
		return heartbeat
	case <-time.After(time.Second):
		t.Fatal("expected activity worker version heartbeat")
		return workerVersionHeartbeat{}
	}
}

type noopActivityRuntime struct{}

func (noopActivityRuntime) BeginActivityTask(ctx context.Context, task protocol.Task) (protocol.ActivityTaskBeginResult, error) {
	return protocol.ActivityTaskBeginResult{}, nil
}

func (noopActivityRuntime) RecordActivityHeartbeat(ctx context.Context, task protocol.Task, details map[string]any) (protocol.ActivityHeartbeatResult, error) {
	return protocol.ActivityHeartbeatResult{}, nil
}

func (noopActivityRuntime) CheckActivityTaskCancellation(ctx context.Context, task protocol.Task) (protocol.ActivityCancellationResult, error) {
	return protocol.ActivityCancellationResult{}, nil
}

func (noopActivityRuntime) CompleteActivityTask(ctx context.Context, task protocol.Task, output map[string]any, failure *protocol.Failure) error {
	return nil
}

func (noopActivityRuntime) FailActivityTask(ctx context.Context, task protocol.Task, failure protocol.Failure) error {
	return nil
}

func (noopActivityRuntime) ReleaseActivityTask(context.Context, protocol.Task) error {
	return nil
}
