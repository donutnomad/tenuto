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
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
)

func TestWorkerSkipsActivityTaskForTerminalRun(t *testing.T) {
	api := &activityStatusAPI{status: history.StatusTerminated}
	registry := wfsdk.NewRegistry()
	calls := 0
	registry.Register("must_skip", func(ctx context.Context, input map[string]any) (map[string]any, error) {
		calls++
		return map[string]any{"ok": true}, nil
	})

	worker := assembleWorker(api, registry, worker.WorkerOptions{})
	worker.HandleActivityTask(context.Background(), protocol.TaskLease{
		Task: protocol.Task{
			Type:             protocol.TaskTypeActivity,
			RunID:            "run-terminal",
			ActivityID:       "step",
			ScheduledEventID: 10,
			ActivityType:     "must_skip",
		},
		Token: "activity-token",
	})

	if calls != 0 {
		t.Fatalf("expected terminal run activity to be skipped, got %d calls", calls)
	}
	if api.activityAcks != 1 {
		t.Fatalf("expected activity ack, got %d", api.activityAcks)
	}
	if api.activityStarted != 0 || api.activityCompleted != 0 || api.activityFailed != 0 {
		t.Fatalf("unexpected activity lifecycle calls: started=%d completed=%d failed=%d", api.activityStarted, api.activityCompleted, api.activityFailed)
	}
}

func TestWorkerSkipsWorkflowTaskForTerminalRun(t *testing.T) {
	api := &activityStatusAPI{
		status: history.StatusCompleted,
		wf:     &program.WorkflowConfig{Type: "already-done", Program: programtest.Noop()},
	}
	worker := assembleWorker(api, wfsdk.NewRegistry(), worker.WorkerOptions{})

	worker.HandleWorkflowTask(context.Background(), protocol.TaskLease{
		Task:  protocol.Task{Type: protocol.TaskTypeWorkflow, RunID: "run-completed"},
		Token: "workflow-token",
	})

	if api.workflowAcks != 1 {
		t.Fatalf("expected terminal run workflow task ack, got %d", api.workflowAcks)
	}
	if api.workflowConfigLoads != 0 || api.workflowCompletions != 0 {
		t.Fatalf("unexpected workflow processing: config_loads=%d completions=%d", api.workflowConfigLoads, api.workflowCompletions)
	}
}

func TestWorkerSkipsClosedWorkflowTask(t *testing.T) {
	api := &activityStatusAPI{
		status: history.StatusRunning,
		wf:     &program.WorkflowConfig{Type: "closed-task", Program: programtest.Noop()},
		events: []history.Event{
			{ID: 10, WorkflowTaskScheduled: &history.WorkflowTaskScheduledEvent{}},
			{WorkflowTaskCompleted: &history.WorkflowTaskCompletedEvent{ScheduledEventID: 10}},
		},
	}
	worker := assembleWorker(api, wfsdk.NewRegistry(), worker.WorkerOptions{})

	worker.HandleWorkflowTask(context.Background(), protocol.TaskLease{
		Task: protocol.Task{
			Type:             protocol.TaskTypeWorkflow,
			RunID:            "run-closed-workflow-task",
			ScheduledEventID: 10,
		},
		Token: "workflow-token",
	})

	if api.workflowAcks != 1 {
		t.Fatalf("expected closed workflow task ack, got %d", api.workflowAcks)
	}
	if api.workflowConfigLoads != 0 || api.workflowCompletions != 0 {
		t.Fatalf("unexpected closed workflow task processing: config_loads=%d completions=%d", api.workflowConfigLoads, api.workflowCompletions)
	}
}

func TestWorkerSkipsClosedActivityTask(t *testing.T) {
	api := &activityStatusAPI{
		status: history.StatusRunning,
		events: []history.Event{
			{ID: 10, ActivityScheduled: &history.ActivityScheduledEvent{ActivityID: "step"}},
			{ActivityCompleted: &history.ActivityCompletedEvent{ScheduledEventID: 10, Output: map[string]any{"ok": true}}},
		},
	}
	registry := wfsdk.NewRegistry()
	calls := 0
	registry.Register("must_skip", func(ctx context.Context, input map[string]any) (map[string]any, error) {
		calls++
		return map[string]any{"ok": true}, nil
	})

	worker := assembleWorker(api, registry, worker.WorkerOptions{})
	worker.HandleActivityTask(context.Background(), protocol.TaskLease{
		Task: protocol.Task{
			Type:             protocol.TaskTypeActivity,
			RunID:            "run-running",
			ActivityID:       "step",
			ScheduledEventID: 10,
			ActivityType:     "must_skip",
		},
		Token: "activity-token",
	})

	if calls != 0 {
		t.Fatalf("expected closed activity task to be skipped, got %d calls", calls)
	}
	if api.activityAcks != 1 {
		t.Fatalf("expected closed activity task ack, got %d", api.activityAcks)
	}
	if api.activityStarted != 0 || api.activityCompleted != 0 || api.activityFailed != 0 {
		t.Fatalf("unexpected activity lifecycle calls: started=%d completed=%d failed=%d", api.activityStarted, api.activityCompleted, api.activityFailed)
	}
}

func TestWorkerSkipsFailedActivityAttemptTask(t *testing.T) {
	api := &activityStatusAPI{
		status: history.StatusRunning,
		events: []history.Event{
			{ID: 10, ActivityScheduled: &history.ActivityScheduledEvent{ActivityID: "retry-step"}},
			{ActivityStarted: &history.ActivityStartedEvent{ScheduledEventID: 10, Attempt: 1}},
			{ActivityAttemptFailed: &history.ActivityAttemptFailedEvent{ScheduledEventID: 10, Attempt: 1, Error: "temporary"}},
		},
	}
	registry := wfsdk.NewRegistry()
	calls := 0
	registry.Register("must_skip", func(ctx context.Context, input map[string]any) (map[string]any, error) {
		calls++
		return map[string]any{"ok": true}, nil
	})

	worker := assembleWorker(api, registry, worker.WorkerOptions{})
	worker.HandleActivityTask(context.Background(), protocol.TaskLease{
		Task: protocol.Task{
			Type:             protocol.TaskTypeActivity,
			RunID:            "run-retry",
			ActivityID:       "retry-step",
			ScheduledEventID: 10,
			Attempt:          1,
			ActivityType:     "must_skip",
		},
		Token: "stale-attempt-token",
	})

	if calls != 0 {
		t.Fatalf("expected failed attempt task to be skipped, got %d calls", calls)
	}
	if api.activityAcks != 1 {
		t.Fatalf("expected failed attempt task ack, got %d", api.activityAcks)
	}
	if api.activityCompleted != 0 || api.activityFailed != 0 {
		t.Fatalf("unexpected stale attempt lifecycle calls: completed=%d failed=%d", api.activityCompleted, api.activityFailed)
	}
}

func TestWorkerDiscardsStartedActivityAttemptRedelivery(t *testing.T) {
	api := &activityStatusAPI{
		status: history.StatusRunning,
		events: []history.Event{
			{ID: 10, ActivityScheduled: &history.ActivityScheduledEvent{ActivityID: "running-step"}},
			{ActivityStarted: &history.ActivityStartedEvent{ScheduledEventID: 10, Attempt: 1}},
		},
		// 已 started：Begin 返回 Execute=false，Worker 确认丢弃重投的副本。
		state: &execution.ActivityState{RunID: "run-running", ScheduledEventID: 10, ActivityID: "running-step", Attempt: 1, StartedEventID: 10, Status: execution.ActivityStatusEnums.Started},
	}
	registry := wfsdk.NewRegistry()
	calls := 0
	registry.Register("must_retry", func(ctx context.Context, input map[string]any) (map[string]any, error) {
		calls++
		return map[string]any{"ok": true}, nil
	})

	worker := assembleWorker(api, registry, worker.WorkerOptions{})
	worker.HandleActivityTask(context.Background(), protocol.TaskLease{
		Task: protocol.Task{
			Type:             protocol.TaskTypeActivity,
			RunID:            "run-running",
			ActivityID:       "running-step",
			ScheduledEventID: 10,
			Attempt:          1,
			ActivityType:     "must_retry",
		},
		Token: "duplicate-started-token",
	})

	if calls != 0 {
		t.Fatalf("expected started attempt redelivery to be discarded, got %d calls", calls)
	}
	if api.activityAcks != 1 {
		t.Fatalf("expected started attempt redelivery ack, got %d", api.activityAcks)
	}
	if api.activityStarted != 0 || api.activityCompleted != 0 || api.activityFailed != 0 {
		t.Fatalf("unexpected duplicate attempt lifecycle calls: started=%d completed=%d failed=%d", api.activityStarted, api.activityCompleted, api.activityFailed)
	}
}

func TestWorkerAcksActivityLeaseOnceAttemptStarts(t *testing.T) {
	// Happy Path：Server 记录 started 后、Activity 函数运行前确认租约；结果上报失败时不再退回租约，
	// 由 attempt 的超时负责重试。
	api := &activityStatusAPI{
		status: history.StatusRunning,
		state:  &execution.ActivityState{RunID: "run-ack", ScheduledEventID: 10, ActivityID: "step", Attempt: 1, StartedEventID: execution.EmptyEventID, Status: execution.ActivityStatusEnums.Scheduled},
	}
	registry := wfsdk.NewRegistry()
	acksInBody := -1
	registry.Register("long_running", func(ctx context.Context, input map[string]any) (map[string]any, error) {
		acksInBody = api.activityAcks
		return map[string]any{"ok": true}, nil
	})

	worker := assembleWorker(api, registry, worker.WorkerOptions{})
	worker.HandleActivityTask(context.Background(), protocol.TaskLease{
		Task:  protocol.Task{Type: protocol.TaskTypeActivity, RunID: "run-ack", ActivityID: "step", ScheduledEventID: 10, Attempt: 1, ActivityType: "long_running"},
		Token: "activity-token",
	})

	if acksInBody != 1 {
		t.Fatalf("lease must be acked before the activity body runs, acks seen in body = %d", acksInBody)
	}
	if api.activityStarted != 1 || api.activityAcks != 1 || api.activityCompleted != 1 {
		t.Fatalf("unexpected lifecycle: started=%d acks=%d completed=%d", api.activityStarted, api.activityAcks, api.activityCompleted)
	}
}

func TestWorkerLeavesActivityOpenWhenActivityReturnsPending(t *testing.T) {
	api := &activityStatusAPI{
		status: history.StatusRunning,
		// scheduled（StartedEventID=Empty）→ Begin 走 fresh-start 分支，调 HandleActivityStarted。
		state: &execution.ActivityState{RunID: "run-async", ScheduledEventID: 10, ActivityID: "async-step", Attempt: 1, StartedEventID: execution.EmptyEventID, Status: execution.ActivityStatusEnums.Scheduled},
	}
	registry := wfsdk.NewRegistry()
	registry.Register("async_wait", func(ctx context.Context, input map[string]any) (map[string]any, error) {
		return nil, wfsdk.ErrPending
	})

	worker := assembleWorker(api, registry, worker.WorkerOptions{})
	worker.HandleActivityTask(context.Background(), protocol.TaskLease{
		Task: protocol.Task{
			Type:             protocol.TaskTypeActivity,
			RunID:            "run-async",
			ActivityID:       "async-step",
			ScheduledEventID: 10,
			Attempt:          1,
			ActivityType:     "async_wait",
		},
		Token: "activity-token",
	})

	if api.activityStarted != 1 || api.activityAcks != 1 {
		t.Fatalf("expected started activity to be acked as open, got started=%d ack=%d", api.activityStarted, api.activityAcks)
	}
	if api.activityCompleted != 0 || api.activityFailed != 0 {
		t.Fatalf("pending activity should stay open, got completed=%d failed=%d", api.activityCompleted, api.activityFailed)
	}
}

func TestWorkerInjectsActivityTaskTokenIntoActivityContext(t *testing.T) {
	api := &activityStatusAPI{
		status: history.StatusRunning,
		state:  &execution.ActivityState{RunID: "run-token-context", ScheduledEventID: 10, ActivityID: "async-step", Attempt: 1, StartedEventID: execution.EmptyEventID, Status: execution.ActivityStatusEnums.Scheduled},
	}
	registry := wfsdk.NewRegistry()
	token, err := testTaskTokens().Encode(protocol.Task{
		Type:             protocol.TaskTypeActivity,
		RunID:            "run-token-context",
		ActivityID:       "async-step",
		ScheduledEventID: 10,
		Attempt:          1,
		ActivityType:     "async_wait",
		TaskQueue:        "custodian",
	})
	if err != nil {
		t.Fatal(err)
	}
	var got string
	registry.Register("async_wait", func(ctx context.Context, input map[string]any) (map[string]any, error) {
		got = wfsdk.GetInfo(ctx).TaskToken
		return nil, wfsdk.ErrPending
	})

	worker := assembleWorker(api, registry, worker.WorkerOptions{})
	worker.HandleActivityTask(context.Background(), protocol.TaskLease{
		Task: protocol.Task{
			Type:             protocol.TaskTypeActivity,
			RunID:            "run-token-context",
			ActivityID:       "async-step",
			ScheduledEventID: 10,
			Attempt:          1,
			ActivityType:     "async_wait",
			TaskQueue:        "custodian",
			TaskToken:        token,
		},
		Token: "activity-token",
	})

	if got != string(token) {
		t.Fatalf("expected activity token %q, got %q", token, got)
	}
	if api.activityStarted != 1 || api.activityAcks != 1 {
		t.Fatalf("expected pending activity to be started and acked, got started=%d ack=%d", api.activityStarted, api.activityAcks)
	}
}

func TestWorkerTreatsWrappedPendingAsPending(t *testing.T) {
	api := &activityStatusAPI{status: history.StatusRunning}
	registry := wfsdk.NewRegistry()
	registry.Register("async_wait", func(ctx context.Context, input map[string]any) (map[string]any, error) {
		return nil, errors.Join(wfsdk.ErrPending, context.Canceled)
	})

	worker := assembleWorker(api, registry, worker.WorkerOptions{})
	worker.HandleActivityTask(context.Background(), protocol.TaskLease{
		Task: protocol.Task{
			Type:             protocol.TaskTypeActivity,
			RunID:            "run-async",
			ActivityID:       "async-step",
			ScheduledEventID: 10,
			Attempt:          1,
			ActivityType:     "async_wait",
		},
	})

	if api.activityAcks != 1 || api.activityFailed != 0 || api.activityCompleted != 0 {
		t.Fatalf("wrapped pending should ack without close event, got ack=%d completed=%d failed=%d", api.activityAcks, api.activityCompleted, api.activityFailed)
	}
}

func TestWorkerCancelsActivityContextWhenHeartbeatReportsCancellation(t *testing.T) {
	runtime := &cancelOnHeartbeatRuntime{}
	queue := &activityStatusAPI{status: history.StatusRunning}
	registry := wfsdk.NewRegistry()
	registry.Register("cancelable", func(ctx context.Context, input map[string]any) (map[string]any, error) {
		wfsdk.RecordHeartbeat(ctx, map[string]any{"step": "started"})
		<-ctx.Done()
		return nil, ctx.Err()
	})

	worker := worker.New(nil, queue, runtime, registry, worker.WorkerOptions{Role: worker.WorkerRoleActivity})
	worker.HandleActivityTask(context.Background(), protocol.TaskLease{
		Task: protocol.Task{
			Type:             protocol.TaskTypeActivity,
			RunID:            "run-cancel-heartbeat",
			ActivityID:       "cancelable-step",
			ScheduledEventID: 10,
			Attempt:          1,
			ActivityType:     "cancelable",
		},
	})

	if runtime.heartbeats != 1 {
		t.Fatalf("expected one heartbeat, got %d", runtime.heartbeats)
	}
	if runtime.failed != 0 || runtime.completed != 0 || runtime.released != 0 {
		t.Fatalf("a cancelled activity reports nothing, got failed=%d completed=%d released=%d", runtime.failed, runtime.completed, runtime.released)
	}
	if queue.activityAcks != 1 {
		t.Fatalf("expected activity lease ack, got %d", queue.activityAcks)
	}
}

type activityStatusAPI struct {
	status              history.Status
	events              []history.Event
	state               *execution.ActivityState // mutable-state 架构：BeginActivityTask 权威读 state
	wf                  *program.WorkflowConfig
	workflowAcks        int
	workflowConfigLoads int
	workflowCompletions int
	activityAcks        int
	activityStarted     int
	activityCompleted   int
	activityFailed      int
}

func (a *activityStatusAPI) PollWorkflowTask(ctx context.Context) (protocol.TaskLease, error) {
	return protocol.TaskLease{}, nil
}

func (a *activityStatusAPI) PollActivityTask(ctx context.Context, taskQueue string) (protocol.TaskLease, error) {
	return protocol.TaskLease{}, nil
}

func (a *activityStatusAPI) AckWorkflowTask(ctx context.Context, lease protocol.TaskLease) error {
	a.workflowAcks++
	return nil
}

func (a *activityStatusAPI) NackWorkflowTask(ctx context.Context, lease protocol.TaskLease) error {
	return nil
}

func (a *activityStatusAPI) AckActivityTask(ctx context.Context, lease protocol.TaskLease) error {
	a.activityAcks++
	return nil
}

func (a *activityStatusAPI) NackActivityTask(ctx context.Context, lease protocol.TaskLease) error {
	return nil
}

func (a *activityStatusAPI) GetWorkflowConfig(_ context.Context, runID, _ string) (*program.WorkflowConfig, error) {
	a.workflowConfigLoads++
	return a.wf, nil
}

func (a *activityStatusAPI) GetEvents(runID string) ([]history.Event, error) {
	return a.events, nil
}

func (a *activityStatusAPI) GetRunStatus(runID string) (history.Status, error) {
	return a.status, nil
}

func (a *activityStatusAPI) HandleWorkflowTaskCompleted(ctx context.Context, runID string, scheduledEventID int64, commands []history.Command, workerIdentity string) error {
	a.workflowCompletions++
	return nil
}

func (a *activityStatusAPI) HandleWorkflowTaskTimedOut(ctx context.Context, runID string, scheduledEventID int64, errMsg string) error {
	return nil
}

func (a *activityStatusAPI) HandleWorkflowTaskFailed(ctx context.Context, runID string, scheduledEventID int64, cause, reason string) error {
	return nil
}

func (a *activityStatusAPI) GetActivityState(runID string, scheduledEventID int64) (*execution.ActivityState, bool, error) {
	if a.state == nil {
		return nil, false, nil
	}
	cp := *a.state
	return &cp, true, nil
}

func (a *activityStatusAPI) GetWorkflowExecutionState(runID string) (*execution.WorkflowExecutionState, error) {
	return workerAPIExecutionState(a, runID)
}

func (a *activityStatusAPI) TaskRun(task protocol.Task) (*execution.Run, error) {
	return &execution.Run{ID: task.RunID, Namespace: task.Namespace}, nil
}

func (a *activityStatusAPI) StartActivityAttempt(task protocol.Task, _ *execution.Run, startToCloseTimeout, heartbeatTimeout time.Duration) (time.Time, bool, error) {
	a.activityStarted++
	return time.Time{}, true, nil
}

func (a *activityStatusAPI) CompleteActivityAttempt(ctx context.Context, _ *execution.Run, scheduledEventID int64, attempt int, stamp int64, output map[string]any) error {
	a.activityCompleted++
	return nil
}

func (a *activityStatusAPI) FailActivityAttempt(ctx context.Context, _ *execution.Run, scheduledEventID int64, attempt int, stamp int64, failure protocol.Failure) error {
	a.activityFailed++
	return nil
}

func (a *activityStatusAPI) HandleActivityHeartbeat(runID string, scheduledEventID int64, attempt int, stamp int64, details map[string]any) error {
	return nil
}

func (a *activityStatusAPI) CancelWorkflow(runID, reason string) error {
	return nil
}

func (a *activityStatusAPI) TerminateWorkflow(runID, reason string) error {
	return nil
}

type cancelOnHeartbeatRuntime struct {
	heartbeats int
	failed     int
	completed  int
	released   int
}

func (r *cancelOnHeartbeatRuntime) BeginActivityTask(ctx context.Context, task protocol.Task) (protocol.ActivityTaskBeginResult, error) {
	return protocol.ActivityTaskBeginResult{Execute: true}, nil
}

func (r *cancelOnHeartbeatRuntime) RecordActivityHeartbeat(ctx context.Context, task protocol.Task, details map[string]any) (protocol.ActivityHeartbeatResult, error) {
	r.heartbeats++
	return protocol.ActivityHeartbeatResult{CancelRequested: true, Reason: "run_cancelled"}, nil
}

func (r *cancelOnHeartbeatRuntime) CompleteActivityTask(ctx context.Context, task protocol.Task, output map[string]any, failure *protocol.Failure) error {
	r.completed++
	return nil
}

func (r *cancelOnHeartbeatRuntime) ReleaseActivityTask(ctx context.Context, task protocol.Task) error {
	r.released++
	return nil
}

func (r *cancelOnHeartbeatRuntime) FailActivityTask(ctx context.Context, task protocol.Task, failure protocol.Failure) error {
	r.failed++
	return nil
}

func (r *cancelOnHeartbeatRuntime) CheckActivityTaskCancellation(ctx context.Context, task protocol.Task) (protocol.ActivityCancellationResult, error) {
	return protocol.ActivityCancellationResult{CancelRequested: true, Reason: "run_cancelled"}, nil
}

func (*activityStatusAPI) GetWorkflowActivation(context.Context, history.WorkflowActivationRequest) (history.WorkflowActivation, error) {
	return history.WorkflowActivation{}, errors.New("incremental activation is not used by this fake")
}

func (*activityStatusAPI) CompleteWorkflowActivation(context.Context, history.WorkflowTaskCompletionRequest) (history.WorkflowTaskCompletionAck, error) {
	return history.WorkflowTaskCompletionAck{}, errors.New("incremental activation is not used by this fake")
}

func (*activityStatusAPI) ReleaseActivityAttempt(context.Context, *execution.Run, int64, int, int64) error {
	return nil
}

func (*activityStatusAPI) RefuseIncompatibleActivityClaim(context.Context, protocol.Task, *execution.Run) (bool, error) {
	return false, nil
}
