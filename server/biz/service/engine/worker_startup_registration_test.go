package engine

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/fstest"
	"time"

	"github.com/donutnomad/tenuto/internal/worker"
	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/sdk/activity"
	"github.com/donutnomad/tenuto/sdk/registration"
)

// recordingWorkflowAPI 记录登记与 poll 的先后，用于验证 Worker 生命周期。
type recordingWorkflowAPI struct {
	mu          sync.Mutex
	registered  int
	polls       int
	polledEarly bool
	registerErr error
}

func (f *recordingWorkflowAPI) RegisterWorkflowJSON(ctx context.Context, namespace string, workflowJSON []byte) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.registerErr != nil {
		return "", f.registerErr
	}
	f.registered++
	return "version-1", nil
}

func (f *recordingWorkflowAPI) PollWorkflowTask(ctx context.Context) (protocol.TaskLease, error) {
	f.mu.Lock()
	f.polls++
	if f.registered == 0 {
		f.polledEarly = true
	}
	f.mu.Unlock()
	<-ctx.Done()
	return protocol.TaskLease{}, ctx.Err()
}

func (f *recordingWorkflowAPI) snapshot() (registered, polls int, polledEarly bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.registered, f.polls, f.polledEarly
}

func (f *recordingWorkflowAPI) AckWorkflowTask(context.Context, protocol.TaskLease) error { return nil }
func (f *recordingWorkflowAPI) NackWorkflowTask(context.Context, protocol.TaskLease) error {
	return nil
}
func (f *recordingWorkflowAPI) GetWorkflowConfig(_ context.Context, _, _ string) (*program.WorkflowConfig, error) {
	return nil, errors.New("not implemented")
}
func (f *recordingWorkflowAPI) GetEvents(string) ([]history.Event, error) { return nil, nil }
func (f *recordingWorkflowAPI) GetRunStatus(string) (history.Status, error) {
	return "", errors.New("not implemented")
}
func (f *recordingWorkflowAPI) HandleWorkflowTaskCompleted(context.Context, string, int64, []history.Command, string) error {
	return nil
}
func (f *recordingWorkflowAPI) HandleWorkflowTaskTimedOut(context.Context, string, int64, string) error {
	return nil
}
func (f *recordingWorkflowAPI) HandleWorkflowTaskFailed(context.Context, string, int64, string, string) error {
	return nil
}

func testWorkflowDefinition() registration.Definition {
	return registration.Definition{FS: fstest.MapFS{
		"workflow.json": &fstest.MapFile{Data: []byte(`{"type":"startup-flow"}`)},
	}}
}

// TestWorkerRegisterFailStopsPoll 锁住「登记失败明确返回」：
// Start 的错误经 WaitReady 返回，且 poll 一次都不启动。
func TestWorkerRegisterFailStopsPoll(t *testing.T) {
	api := &recordingWorkflowAPI{registerErr: errors.New("server rejected definition")}
	worker := worker.NewWorkflowWorker(api, worker.WorkerOptions{
		Namespace: "startup",
		Workflows: []registration.Definition{testWorkflowDefinition()},
	})
	worker.Start(context.Background())
	defer worker.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := worker.WaitReady(ctx)
	if err == nil || !strings.Contains(err.Error(), "server rejected definition") {
		t.Fatalf("WaitReady err = %v", err)
	}
	registered, polls, _ := api.snapshot()
	if registered != 0 {
		t.Fatalf("失败时不应记录成功登记，registered = %d", registered)
	}
	if polls != 0 {
		t.Fatalf("登记失败后不应 poll，polls = %d", polls)
	}
}

// TestWorkerRegisterBeforePoll 锁住「自动注册发生在 poll 之前」。
func TestWorkerRegisterBeforePoll(t *testing.T) {
	api := &recordingWorkflowAPI{}
	worker := worker.NewWorkflowWorker(api, worker.WorkerOptions{
		Namespace: "startup",
		Workflows: []registration.Definition{testWorkflowDefinition()},
	})
	worker.Start(context.Background())
	defer worker.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := worker.WaitReady(ctx); err != nil {
		t.Fatalf("WaitReady: %v", err)
	}
	deadline := time.Now().Add(time.Second)
	for {
		registered, polls, early := api.snapshot()
		if polls > 0 {
			if early {
				t.Fatal("poll 发生在登记之前")
			}
			if registered != 1 {
				t.Fatalf("登记次数 = %d, want 1", registered)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("worker 没有开始 poll")
		}
		time.Sleep(time.Millisecond)
	}
}

// recordingActivityRegistrar 同时充当 Activity 路由 registry 与定义登记传输，记录调用顺序。
type recordingActivityRegistrar struct {
	mu          sync.Mutex
	calls       []string
	registerErr error
}

func (r *recordingActivityRegistrar) record(call string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, call)
}

func (r *recordingActivityRegistrar) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.calls...)
}

func (r *recordingActivityRegistrar) RegisterActivityWorkerVersion(context.Context, protocol.ActivityWorkerVersion) error {
	r.record("route")
	return nil
}

func (r *recordingActivityRegistrar) DeregisterActivityWorkerVersionInNamespace(context.Context, string, string, string) error {
	return nil
}

func (r *recordingActivityRegistrar) HeartbeatActivityWorkerVersionInNamespace(context.Context, string, string, string) error {
	return nil
}

func (r *recordingActivityRegistrar) RegisterWorkflowJSON(context.Context, string, []byte) (string, error) {
	if r.registerErr != nil {
		return "", r.registerErr
	}
	r.record("definition")
	return "version-1", nil
}

func (r *recordingActivityRegistrar) PollActivityTask(ctx context.Context, _ string) (protocol.TaskLease, error) {
	r.record("poll")
	<-ctx.Done()
	return protocol.TaskLease{}, ctx.Err()
}

func (r *recordingActivityRegistrar) AckActivityTask(context.Context, protocol.TaskLease) error {
	return nil
}
func (r *recordingActivityRegistrar) NackActivityTask(context.Context, protocol.TaskLease) error {
	return nil
}

func newDefinitionActivityWorker(queue ActivityWorkerQueueAPI, registry worker.ActivityWorkerVersionRegistryAPI) *worker.Worker {
	activities := activity.NewRegistry()
	activities.Register("orders.finalize", func(context.Context, map[string]any) (map[string]any, error) {
		return nil, nil
	})
	return worker.NewActivityWorkerWithQueue(queue, noopActivityRuntime{}, activities, worker.WorkerOptions{
		Namespace:                     "startup",
		ActivityTaskQueues:            []string{"orders"},
		Identity:                      "activity-1",
		BuildID:                       "activity-v1",
		ActivityWorkerVersionRegistry: registry,
		Workflows:                     []registration.Definition{testWorkflowDefinition()},
	})
}

// Happy Path：Activity-role Worker 先上报路由，再登记定义，最后才 poll。
func TestActivityWorkerRegistersDefinitionsBeforePoll(t *testing.T) {
	registrar := &recordingActivityRegistrar{}
	worker := newDefinitionActivityWorker(registrar, registrar)
	worker.Start(context.Background())
	defer worker.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := worker.WaitReady(ctx); err != nil {
		t.Fatalf("WaitReady: %v", err)
	}
	deadline := time.Now().Add(time.Second)
	for {
		calls := registrar.snapshot()
		definition, poll := slices.Index(calls, "definition"), slices.Index(calls, "poll")
		if poll >= 0 {
			if route := slices.Index(calls, "route"); route < 0 || definition < route || poll < definition {
				t.Fatalf("calls = %v, want route -> definition -> poll", calls)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("worker 没有开始 poll，calls = %v", calls)
		}
		time.Sleep(time.Millisecond)
	}
}

// Error Case：登记失败或没有登记传输时，错误经 WaitReady 返回且不启动 poll。
func TestActivityWorkerRegisterFailStopsPoll(t *testing.T) {
	rejected := &recordingActivityRegistrar{registerErr: errors.New("server rejected definition")}
	cases := map[string]struct {
		queue    *recordingActivityRegistrar
		registry worker.ActivityWorkerVersionRegistryAPI
		wantErr  string
	}{
		"registrar rejects definition": {queue: rejected, registry: rejected, wantErr: "server rejected definition"},
		"registry cannot register":     {queue: &recordingActivityRegistrar{}, registry: newActivityWorkerVersionRegistrySpy(), wantErr: "cannot register workflow definitions"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			worker := newDefinitionActivityWorker(tc.queue, tc.registry)
			worker.Start(context.Background())
			defer worker.Stop()

			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if err := worker.WaitReady(ctx); err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("WaitReady err = %v, want %q", err, tc.wantErr)
			}
			if calls := tc.queue.snapshot(); slices.Contains(calls, "poll") {
				t.Fatalf("登记失败后不应 poll，calls = %v", calls)
			}
		})
	}
}

func (*recordingWorkflowAPI) GetWorkflowActivation(context.Context, history.WorkflowActivationRequest) (history.WorkflowActivation, error) {
	return history.WorkflowActivation{}, errors.New("incremental activation is not used by this fake")
}

func (*recordingWorkflowAPI) CompleteWorkflowActivation(context.Context, history.WorkflowTaskCompletionRequest) (history.WorkflowTaskCompletionAck, error) {
	return history.WorkflowTaskCompletionAck{}, errors.New("incremental activation is not used by this fake")
}
