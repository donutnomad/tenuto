// 传输层与真实 runtime HTTP handler 的端到端测试：请求经 HTTP 到达内存后端的 Server。
package runtimeclient_test

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/internal/executor/interpreter"
	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/program/programtest"
	"github.com/donutnomad/tenuto/protocol"
	historypkg "github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/protocol/runtimeapi"
	"github.com/donutnomad/tenuto/sdk/internal/runtimeclient"
	"github.com/donutnomad/tenuto/server/app/assembly"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/biz/service/engine"
	"github.com/donutnomad/tenuto/server/http/runtimehttp"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

func TestWorkflowRuntimeHTTPClientStartsAndControlsRun(t *testing.T) {
	ctx := context.Background()
	server := assembly.NewServer(assembly.Options{Stores: assembly.Stores{Executions: memory.NewExecutionManager()}, WorkflowQueue: memory.NewTaskQueue(8), ActivityRouter: memory.NewActivityQueueRouter(8)})
	defer server.Stop()

	httpServer := httptest.NewServer(runtimehttp.NewWorkflowRuntimeHTTPHandler(server))
	defer httpServer.Close()
	client := runtimeclient.NewWorkflowRuntimeHTTPClient(httpServer.URL, httpServer.Client(), runtimeclient.RuntimeHTTPClientOptions{})

	wf := &program.WorkflowConfig{Type: "workflow-runtime-api", Program: programtest.Noop()}
	if _, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, wf); err != nil {
		t.Fatal(err)
	}

	runID := startRun(t, client, wf.Type, m{"request_id": "r-1"})
	run := getRun(t, client, runID)
	if run.ID != runID || run.WorkflowID != wf.Type || run.Status != historypkg.StatusRunning {
		t.Fatalf("unexpected run from runtime api: %+v", run)
	}

	if err := client.CancelWorkflow(ctx, runtimeapi.WorkflowRuntimeCancelRequest{RunID: runID}); err != nil {
		t.Fatal(err)
	}
	run = getRun(t, client, runID)
	if run.Status != historypkg.StatusRunning {
		t.Fatalf("expected running run after cooperative cancel request, got %+v", run)
	}
	history := allEvents(t, client, runID)
	if countEvent(history, historypkg.EventWorkflowCancellationRequested) != 1 {
		t.Fatalf("expected cancellation requested event in runtime history, got %+v", history)
	}
}

func TestWorkflowRuntimeHTTPClientSignalsQueriesAndUpdatesRun(t *testing.T) {
	ctx := context.Background()
	server := assembly.NewServer(assembly.Options{Stores: assembly.Stores{Executions: memory.NewExecutionManager()}, WorkflowQueue: memory.NewTaskQueue(8), ActivityRouter: memory.NewActivityQueueRouter(8)})
	defer server.Stop()

	httpServer := httptest.NewServer(runtimehttp.NewWorkflowRuntimeHTTPHandler(server))
	defer httpServer.Close()
	client := runtimeclient.NewWorkflowRuntimeHTTPClient(httpServer.URL, httpServer.Client(), runtimeclient.RuntimeHTTPClientOptions{})

	wf := &program.WorkflowConfig{Type: "workflow-interaction-api", Program: programtest.Noop()}
	if _, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, wf); err != nil {
		t.Fatal(err)
	}
	runID := startRun(t, client, wf.Type, nil)

	if err := client.SignalWorkflow(ctx, runtimeapi.WorkflowRuntimeInteractionRequest{RunID: runID, Name: "payment_received", Input: m{"amount": int64(42)}}); err != nil {
		t.Fatal(err)
	}
	priority, err := protocol.NewJSONPayload(m{"priority": "high"})
	if err != nil {
		t.Fatal(err)
	}
	update, err := client.UpdateWorkflow(ctx, runtimeapi.WorkflowRuntimeUpdateRequest{RunID: runID, Name: "set_priority", Args: protocol.Payloads{priority}})
	if err != nil {
		t.Fatal(err)
	}
	if update.UpdateID == "" || update.AcceptedEventID == 0 {
		t.Fatalf("unexpected update result: %+v", update)
	}

	query, err := client.QueryWorkflow(ctx, runtimeapi.WorkflowRuntimeInteractionRequest{RunID: runID, Name: "state"})
	if err != nil {
		t.Fatal(err)
	}
	if query.Run.ID != runID || query.Run.Status != historypkg.StatusRunning {
		t.Fatalf("unexpected query run: %+v", query.Run)
	}
	if len(query.Signals) != 1 || query.Signals[0].Name != "payment_received" || query.Signals[0].EventID == 0 {
		t.Fatalf("unexpected query signals: %+v", query.Signals)
	}
	if len(query.Updates) != 1 || query.Updates[0].Name != "set_priority" || query.Updates[0].UpdateID != update.UpdateID {
		t.Fatalf("unexpected query updates: %+v", query.Updates)
	}
	history := allEvents(t, client, runID)
	if countEvent(history, historypkg.EventWorkflowSignaled) != 1 {
		t.Fatalf("expected one signal event, got %+v", history)
	}
	if countEvent(history, historypkg.EventWorkflowUpdateAccepted) != 1 || countEvent(history, historypkg.EventWorkflowUpdateCompleted) != 0 {
		t.Fatalf("expected accepted-only update event, got %+v", history)
	}
}

func TestWorkflowRuntimeHTTPClientRoundTripsUpdatePayloads(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	server := assembly.NewServer(assembly.Options{Stores: assembly.Stores{Executions: mgr}, WorkflowQueue: memory.NewTaskQueue(8), ActivityRouter: memory.NewActivityQueueRouter(8)})
	defer server.Stop()
	httpServer := httptest.NewServer(runtimehttp.NewWorkflowRuntimeHTTPHandler(server))
	defer httpServer.Close()
	client := runtimeclient.NewWorkflowRuntimeHTTPClient(httpServer.URL, httpServer.Client(), runtimeclient.RuntimeHTTPClientOptions{})
	wf := &program.WorkflowConfig{Type: "workflow-update-payload-api", Program: programtest.Noop()}
	if _, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, wf); err != nil {
		t.Fatal(err)
	}
	runID := startRun(t, client, wf.Type, nil)
	arg, err := protocol.NewJSONPayload(map[string]any{"record_id": "r-1"})
	if err != nil {
		t.Fatal(err)
	}
	accepted, err := client.UpdateWorkflow(ctx, runtimeapi.WorkflowRuntimeUpdateRequest{
		RunID: runID, Name: "create", UpdateID: "update-http", WaitStage: string(protocol.UpdateWaitStageEnums.Accepted), Args: protocol.Payloads{arg},
	})
	if err != nil {
		t.Fatal(err)
	}
	events := allEvents(t, client, runID)
	var workflowTaskID int64
	for _, event := range events {
		if event.WorkflowTaskScheduled != nil {
			workflowTaskID = event.ID
		}
	}
	if workflowTaskID == 0 {
		t.Fatalf("workflow task missing: %+v", events)
	}
	resultPayload, err := protocol.NewJSONPayload(uint64(18446744073709551615))
	if err != nil {
		t.Fatal(err)
	}
	outcome := protocol.UpdateOutcome{Success: &resultPayload}
	if err := server.HandleWorkflowTaskCompleted(workflowTaskContext(t, ctx, server, mgr, runID, workflowTaskID), runID, workflowTaskID, []historypkg.Command{{
		Type: historypkg.CmdCompleteWorkflowUpdate, Name: "create", UpdateID: accepted.UpdateID, UpdateOutcome: &outcome,
	}}, ""); err != nil {
		t.Fatal(err)
	}

	completed, err := client.UpdateWorkflow(ctx, runtimeapi.WorkflowRuntimeUpdateRequest{
		RunID: runID, Name: "create", UpdateID: accepted.UpdateID, WaitStage: string(protocol.UpdateWaitStageEnums.Completed), Args: protocol.Payloads{arg},
	})
	if err != nil {
		t.Fatal(err)
	}
	if completed.Outcome == nil || completed.Outcome.Success == nil || string(completed.Outcome.Success.Data) != "18446744073709551615" {
		t.Fatalf("outcome lost over HTTP: %+v", completed)
	}
	query, err := client.QueryWorkflow(ctx, runtimeapi.WorkflowRuntimeInteractionRequest{RunID: runID})
	if err != nil {
		t.Fatal(err)
	}
	if len(query.Updates) != 1 || query.Updates[0].Args == nil || query.Updates[0].Outcome == nil {
		t.Fatalf("update projection missing: %+v", query.Updates)
	}
}

func TestWorkflowTaskRuntimeHTTPClientCompletesWorkflowWithTaskToken(t *testing.T) {
	// Worker 声明能执行 WLang 程序；轮询时服务端按它校验 Program 要求的能力。
	ctx := protocol.WithWorkerInfo(context.Background(), protocol.WorkerInfo{WorkflowCapabilities: program.CurrentWLangCapabilities()})
	mgr := memory.NewExecutionManager()
	workflowTasks := memory.NewTaskQueue(8)
	server := assembly.NewServer(assembly.Options{Stores: assembly.Stores{Executions: mgr}, WorkflowQueue: workflowTasks, ActivityRouter: memory.NewActivityQueueRouter(8)})
	defer server.Stop()

	httpServer := httptest.NewServer(runtimehttp.NewWorkflowTaskRuntimeHTTPHandler(server, server.TaskTokens(), server.Logger()))
	defer httpServer.Close()
	client := runtimeclient.NewWorkflowTaskRuntimeHTTPClient(httpServer.URL, httpServer.Client(), runtimeclient.RuntimeHTTPClientOptions{})

	wf := &program.WorkflowConfig{Type: "workflow-task-runtime-api", Program: programtest.Noop()}
	if _, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, wf); err != nil {
		t.Fatal(err)
	}
	runID, err := startedRunID(server.StartWorkflow(ctx, "", wf.Type, wf.Type, "", nil))
	if err != nil {
		t.Fatal(err)
	}

	lease, err := client.PollWorkflowTask(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if lease.Task.TaskToken == "" {
		t.Fatalf("expected workflow task token, got lease %+v", lease)
	}
	decoded, err := server.TaskTokens().Decode(lease.Task.TaskToken)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Type != protocol.TaskTypeWorkflow || decoded.RunID != runID || decoded.ScheduledEventID == 0 {
		t.Fatalf("unexpected task token payload: %+v", decoded)
	}
	taskCtx := protocol.WithWorkflowTaskToken(ctx, lease.Task.TaskToken)
	status, err := client.GetRunStatusContext(taskCtx, runID)
	if err != nil {
		t.Fatal(err)
	}
	if status != historypkg.StatusRunning {
		t.Fatalf("expected running run, got %s", status)
	}
	history, err := client.GetEventsContext(taskCtx, runID)
	if err != nil {
		t.Fatal(err)
	}
	wfConfig, err := client.GetWorkflowConfig(taskCtx, runID, "")
	if err != nil {
		t.Fatal(err)
	}
	nodes := wfConfig
	commands, err := interpreter.NewWorkflowExecutor(nodes, history).Execute()
	if err != nil {
		t.Fatal(err)
	}
	completionCtx := protocol.WithWorkflowTaskToken(ctx, lease.Task.TaskToken)
	if err := client.HandleWorkflowTaskCompleted(completionCtx, lease.Task.RunID, lease.Task.ScheduledEventID, commands, ""); err != nil {
		t.Fatal(err)
	}
	if err := client.AckWorkflowTask(ctx, lease); err != nil {
		t.Fatal(err)
	}
	waitForServerRunStatus(t, server, runID, historypkg.StatusCompleted, 3*time.Second)
}

type m = map[string]any

func startRun(t *testing.T, client *runtimeclient.WorkflowRuntimeHTTPClient, workflowType string, input map[string]any) string {
	t.Helper()
	resp, err := client.StartWorkflow(context.Background(), runtimeapi.WorkflowRuntimeStartRequest{WorkflowType: workflowType, WorkflowID: workflowType, Input: input})
	if err != nil {
		t.Fatal(err)
	}
	return resp.RunID
}

func getRun(t *testing.T, client *runtimeclient.WorkflowRuntimeHTTPClient, runID string) *runtimeapi.Run {
	t.Helper()
	resp, err := client.GetRun(context.Background(), runtimeapi.WorkflowRuntimeRunRequest{RunID: runID})
	if err != nil {
		t.Fatal(err)
	}
	return resp.Run
}

// allEvents 逐页读出 Run 的历史。
func allEvents(t *testing.T, client *runtimeclient.WorkflowRuntimeHTTPClient, runID string) []historypkg.Event {
	t.Helper()
	var events []historypkg.Event
	token := ""
	for {
		page, err := client.GetEventPage(context.Background(), runtimeapi.WorkflowRuntimeHistoryPageRequest{
			HistoryPageRequest: runtimeapi.HistoryPageRequest{RunID: runID, PageToken: token},
		})
		if err != nil {
			t.Fatal(err)
		}
		events = append(events, page.Events...)
		if page.NextPageToken == "" {
			return events
		}
		token = page.NextPageToken
	}
}

func countEvent(history []historypkg.Event, typ historypkg.EventType) int {
	count := 0
	for _, event := range history {
		if event.EventType() == typ {
			count++
		}
	}
	return count
}

func startedRunID(run *execution.Run, err error) (string, error) {
	if err != nil {
		return "", err
	}
	return run.ID, nil
}

// workflowTaskContext 代替 Worker poll：像 poll 一样先写下 WorkflowTaskStarted（Server 以它为 Worker 看到的边界），
// 再签发 token。
func workflowTaskContext(t testing.TB, ctx context.Context, server *engine.Server, mgr execution.ExecutionManager, runID string, scheduledEventID int64) context.Context {
	t.Helper()
	run, err := mgr.Get(runID)
	if err != nil {
		t.Fatal(err)
	}
	branch, err := execution.DeserializeBranchToken(run.BranchToken)
	if err != nil {
		t.Fatal(err)
	}
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error {
		state, err := tx.GetWorkflowExecutionStateForUpdate(runID)
		if err != nil {
			return err
		}
		_, err = execution.StartWorkflowTask(tx, execution.WorkflowTaskStart{
			RunID: runID, TreeID: branch.TreeID, BranchID: branch.BranchID, ScheduledEventID: scheduledEventID,
			ExecutionGeneration: state.ExecutionGeneration, StickyGeneration: state.StickyGeneration,
			ProgramVersion: run.WorkflowConfigVersion, Now: time.Now(),
		})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	state, err := server.GetWorkflowExecutionState(runID)
	if err != nil {
		t.Fatal(err)
	}
	token, err := server.TaskTokens().Encode(protocol.Task{
		Type: protocol.TaskTypeWorkflow, RunID: runID, ScheduledEventID: scheduledEventID,
		ExecutionGeneration: state.ExecutionGeneration, StickyGeneration: state.StickyGeneration,
	})
	if err != nil {
		t.Fatal(err)
	}
	return protocol.WithWorkflowTaskToken(ctx, token)
}

func waitForServerRunStatus(t *testing.T, server *engine.Server, runID string, want historypkg.Status, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if status, err := server.GetRunStatus(runID); err == nil && status == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	status, err := server.GetRunStatus(runID)
	t.Fatalf("run %s status = %s (err %v), want %s", runID, status, err, want)
}
