package runtimehttp

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/protocol/runtimeapi"
	"github.com/donutnomad/tenuto/server/app/assembly"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/biz/service/engine"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

// 心跳与取消查询经 HTTP 带回 Workflow 对这个 Activity 发出的取消请求。
func TestActivityRuntimeHTTPReportsCancellationOnHeartbeat(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	server := assembly.NewServer(assembly.Options{Stores: assembly.Stores{Executions: mgr}, WorkflowQueue: memory.NewTaskQueue(8), ActivityRouter: memory.NewActivityQueueRouter(8)})
	defer server.Stop()

	runtimeServer := httptest.NewServer(NewActivityRuntimeHTTPHandler(engine.NewActivityRuntimeService(server), server.TaskTokens(), server.Logger()))
	defer runtimeServer.Close()

	run := execution.NewRun(execution.NewRunOptions{ID: "run-http-heartbeat-cancel", WorkflowID: "wf"})
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(run) }); err != nil {
		t.Fatal(err)
	}
	scheduledID, err := mgr.Append(run.ID, run.ID, run.ID, history.NewActivityScheduledEvent(history.ActivityScheduledEvent{ActivityID: "slow-step"}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.Append(run.ID, run.ID, run.ID, history.NewActivityStartedEvent(history.ActivityStartedEvent{
		ScheduledEventID: scheduledID,
		Attempt:          1,
	})); err != nil {
		t.Fatal(err)
	}
	// Workflow 已对这个 Activity 发出取消命令。
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error {
		return tx.UpsertActivityState(&execution.ActivityState{
			RunID: run.ID, ScheduledEventID: scheduledID, ActivityID: "slow-step",
			Attempt: 1, Stamp: 1, StartedEventID: execution.TransientEventID,
			Status: execution.ActivityStatusEnums.Started, CancelRequested: true,
			ScheduledTime: time.Now(), CreatedAt: time.Now(), UpdatedAt: time.Now(),
		})
	}); err != nil {
		t.Fatal(err)
	}

	task := protocol.Task{
		Type:             protocol.TaskTypeActivity,
		RunID:            run.ID,
		ActivityID:       "slow-step",
		ScheduledEventID: scheduledID,
		Attempt:          1,
		Stamp:            1,
	}
	token, err := server.TaskTokens().Encode(task)
	if err != nil {
		t.Fatal(err)
	}
	task.TaskToken = token
	var result protocol.ActivityHeartbeatResult
	if err := wirePost(ctx, runtimeServer.URL, runtimeapi.ActivityRuntimeHeartbeatPath, runtimeapi.ActivityRuntimeHeartbeatRequest{
		TaskToken: task.TaskToken, Task: task, Details: map[string]any{"offset": int64(10)},
	}, &result); err != nil {
		t.Fatal(err)
	}
	if !result.CancelRequested || result.Reason != protocol.ActivityCancelRequestedReason {
		t.Fatalf("expected heartbeat cancellation result, got %+v", result)
	}

	var status protocol.ActivityCancellationResult
	if err := wirePost(ctx, runtimeServer.URL, runtimeapi.ActivityRuntimeCancelPath, runtimeapi.ActivityRuntimeTaskRequest{TaskToken: task.TaskToken, Task: task}, &status); err != nil {
		t.Fatal(err)
	}
	if !status.CancelRequested || status.Reason != protocol.ActivityCancelRequestedReason {
		t.Fatalf("expected cancellation status, got %+v", status)
	}
}
