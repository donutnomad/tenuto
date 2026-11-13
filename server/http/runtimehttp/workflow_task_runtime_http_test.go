package runtimehttp

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	programpkg "github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/program/programtest"
	"github.com/donutnomad/tenuto/protocol"
	historypkg "github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/protocol/runtimeapi"
	"github.com/donutnomad/tenuto/server/app/assembly"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

func TestWorkflowTaskRuntimeHTTPRoutesV2TaskToCapableWorker(t *testing.T) {
	mgr := memory.NewExecutionManager()
	workflowTasks := newRuntimeHTTPTestQueue()
	server := assembly.NewServer(assembly.Options{Stores: assembly.Stores{Executions: mgr}, WorkflowQueue: workflowTasks, ActivityRouter: newRuntimeHTTPTestQueue().activities()})
	program := programpkg.WLangProgram{
		Schema:           programpkg.WLangProgramSchemaV2,
		Lang:             programpkg.WLangLanguageV1,
		Program:          []any{map[string]any{"return": nil}},
		HandlerABISchema: programpkg.WLangHandlerABISchemaV1,
	}
	if _, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &programpkg.WorkflowConfig{
		ID:         "http-v2-workflow",
		Program:    program,
		Activities: map[string]programpkg.ActivityRef{},
	}); err != nil {
		t.Fatal(err)
	}
	runID, err := startedRunID(server.StartWorkflow(context.Background(), "", "http-v2-workflow", "http-v2-workflow", "", nil))
	if err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(NewWorkflowTaskRuntimeHTTPHandler(server, server.TaskTokens(), server.Logger()))
	defer httpServer.Close()

	_, err = pollWorkflowTask(protocol.WithWorkerInfo(context.Background(), protocol.WorkerInfo{
		Identity: "old-http-worker", WorkflowCapabilities: []string{},
	}), httpServer.URL, time.Second)
	if err == nil || !strings.Contains(err.Error(), programpkg.WLangProgramV2Capability) {
		t.Fatalf("old HTTP worker error = %v", err)
	}
	history, err := mgr.GetEvents(runID)
	if err != nil {
		t.Fatal(err)
	}
	if countRuntimeEvents(history, historypkg.EventWorkflowTaskStarted) != 0 || len(workflowTasks.tasks) != 1 {
		t.Fatalf("old HTTP worker changed task state: history=%+v queued=%d", history, len(workflowTasks.tasks))
	}

	lease, err := pollWorkflowTask(protocol.WithWorkerInfo(context.Background(), protocol.WorkerInfo{
		Identity: "new-http-worker", WorkflowCapabilities: programpkg.CurrentWLangCapabilities(),
	}), httpServer.URL, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if lease.Task.RunID != runID {
		t.Fatalf("new HTTP worker lease = %+v", lease)
	}
	history, err = mgr.GetEvents(runID)
	if err != nil {
		t.Fatal(err)
	}
	if countRuntimeEvents(history, historypkg.EventWorkflowTaskStarted) != 1 {
		t.Fatalf("new HTTP worker history = %+v", history)
	}
}

func countRuntimeEvents(events []historypkg.Event, typ historypkg.EventType) int {
	count := 0
	for _, event := range events {
		if event.EventType() == typ {
			count++
		}
	}
	return count
}

// Integration: selected candidate config crosses HTTP while current remains pinned to old content.
func TestWorkflowTaskHTTPSelectedProgramConfig(t *testing.T) {
	server := assembly.NewServer(assembly.Options{Stores: assembly.Stores{Executions: memory.NewExecutionManager()}, WorkflowQueue: newRuntimeHTTPTestQueue(), ActivityRouter: newRuntimeHTTPTestQueue().activities()})
	old := &programpkg.WorkflowConfig{ID: "versioned-http", Name: "old", Program: programtest.Noop()}
	if _, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, old); err != nil {
		t.Fatal(err)
	}
	runID, err := startedRunID(server.StartWorkflow(context.Background(), "", old.ID, old.ID, "", nil))
	if err != nil {
		t.Fatal(err)
	}
	candidate := &programpkg.WorkflowConfig{ID: old.ID, Name: "candidate", Program: programtest.Noop()}
	ref, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, candidate)
	if err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(NewWorkflowTaskRuntimeHTTPHandler(server, server.TaskTokens(), server.Logger()))
	defer httpServer.Close()
	token, err := server.TaskTokens().Encode(protocol.Task{Type: protocol.TaskTypeWorkflow, RunID: runID, ScheduledEventID: 2})
	if err != nil {
		t.Fatal(err)
	}
	getConfig := func(token protocol.TaskToken, version string) (*programpkg.WorkflowConfig, error) {
		var config programpkg.WorkflowConfig
		err := wirePost(context.Background(), httpServer.URL, runtimeapi.WorkflowTaskRuntimeConfigPath,
			runtimeapi.WorkflowTaskRuntimeRunRequest{TaskToken: token, ProgramVersion: version}, &config)
		return &config, err
	}
	current, err := getConfig(token, "")
	if err != nil {
		t.Fatal(err)
	}
	selected, err := getConfig(token, ref.Version)
	if err != nil {
		t.Fatal(err)
	}
	if current.Name != "old" || selected.Name != "candidate" {
		t.Fatalf("current=%s selected=%s", current.Name, selected.Name)
	}
	// Error Case：没有 task token 的读取被拒绝。
	if _, err := getConfig("", ""); err == nil {
		t.Fatal("a read without a task token was served")
	}
	if _, err := getConfig(token, "missing"); err == nil {
		t.Fatal("missing candidate silently fell back to current")
	}
}

// Error Case：activation / complete-activation 只接受 Workflow Task 的 token，Activity Task token 被拒。
func TestWorkflowTaskRuntimeHTTPActivationRejectsActivityTaskToken(t *testing.T) {
	server := assembly.NewServer(assembly.Options{Stores: assembly.Stores{Executions: memory.NewExecutionManager()}, WorkflowQueue: newRuntimeHTTPTestQueue(), ActivityRouter: newRuntimeHTTPTestQueue().activities()})
	httpServer := httptest.NewServer(NewWorkflowTaskRuntimeHTTPHandler(server, server.TaskTokens(), server.Logger()))
	defer httpServer.Close()
	token, err := server.TaskTokens().Encode(protocol.Task{Type: protocol.TaskTypeActivity, RunID: "run-activity", ScheduledEventID: 3})
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{runtimeapi.WorkflowTaskRuntimeActivationPath, runtimeapi.WorkflowTaskRuntimeCompleteActivationPath} {
		body, err := json.Marshal(map[string]any{"task_token": token})
		if err != nil {
			t.Fatal(err)
		}
		response, err := httpServer.Client().Post(httpServer.URL+path, "application/json", bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		text, _ := io.ReadAll(response.Body)
		_ = response.Body.Close()
		if response.StatusCode < 400 || !strings.Contains(string(text), "invalid for workflow task") {
			t.Fatalf("%s accepted an activity task token: %d %s", path, response.StatusCode, text)
		}
	}
}

// startedRunID 取 Server 启动结果里的 Run ID。
func startedRunID(run *execution.Run, err error) (string, error) {
	if err != nil {
		return "", err
	}
	return run.ID, nil
}
