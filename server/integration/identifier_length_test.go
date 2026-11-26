package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/protocol"
	historypkg "github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/app/assembly"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/biz/service/engine"
	"github.com/donutnomad/tenuto/server/http/runtimehttp"
	"github.com/donutnomad/tenuto/server/infra/memory"
	"github.com/donutnomad/tenuto/server/infra/persistence/mysqltest"
)

// newRuntimeHTTPServer 在 MySQL 存储上组装 Server 与 Runtime HTTP 端点。
func newRuntimeHTTPServer(t *testing.T) (*engine.Server, string) {
	t.Helper()
	mysqltest.Require(t)
	server := assembly.NewServer(assembly.Options{
		Stores:         newWorkflowStores(t),
		WorkflowQueue:  memory.NewTaskQueue(64),
		ActivityRouter: memory.NewActivityQueueRouter(64),
	})
	t.Cleanup(server.Stop)
	httpServer := httptest.NewServer(runtimehttp.NewRuntimeHTTPHandler(server))
	t.Cleanup(httpServer.Close)
	return server, httpServer.URL
}

// registerReturnWorkflow 登记一个直接返回的 Workflow Type。
func registerReturnWorkflow(t *testing.T, server *engine.Server, namespace, workflowType string) {
	t.Helper()
	if _, err := server.Definitions().RegisterWorkflowInNamespace(namespace, &program.WorkflowConfig{
		Type: workflowType,
		Program: program.WLangProgram{
			Schema:           program.WLangProgramSchemaV2,
			Lang:             program.WLangLanguageV1,
			Program:          []any{map[string]any{"return": nil}},
			HandlerABISchema: program.WLangHandlerABISchemaV1,
		},
		Activities: map[string]program.ActivityRef{},
	}); err != nil {
		t.Fatal(err)
	}
}

func postRuntime(t *testing.T, url, path string, body any) (int, string) {
	t.Helper()
	data, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	response, err := http.Post(url+path, "application/json", bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	text, _ := io.ReadAll(response.Body)
	return response.StatusCode, strings.TrimSpace(string(text))
}

// 超过列宽的标识在入口以 400 拒绝，不落到数据库的 1406（500）。
func TestOverlongIdentifiersAreRejectedWith400(t *testing.T) {
	server, url := newRuntimeHTTPServer(t)
	registerReturnWorkflow(t, server, "default", "length-flow")
	long := strings.Repeat("x", 200)

	for name, request := range map[string]struct {
		path string
		body map[string]any
	}{
		"start workflow_id":             {"/runtime/workflow/start", map[string]any{"workflow_type": "length-flow", "workflow_id": long}},
		"signal-with-start workflow_id": {"/runtime/workflow/signal-with-start", map[string]any{"workflow_type": "length-flow", "workflow_id": long, "signal_name": "s"}},
		"update-with-start update_id": {"/runtime/workflow/update-with-start", map[string]any{
			"workflow_type": "length-flow", "workflow_id": "length-update", "update_name": "u", "update_id": long}},
		"register namespace": {"/runtime/workflow/register", map[string]any{"namespace": long, "workflow": map[string]any{"type": "length-flow"}}},
		"create-schedule description": {"/runtime/workflow/create-schedule", map[string]any{
			"workflow_type": "length-flow", "cron_expression": "* * * * *", "description": long}},
		"start-batch reason": {"/runtime/workflow/start-batch", map[string]any{"req": map[string]any{"type": "cancel", "reason": strings.Repeat("x", 1025)}}},
		"register worker task queue": {"/runtime/workflow/register-activity-worker-version", map[string]any{"worker": map[string]any{
			"identity": "w", "build_id": "b", "task_queues": []string{long}, "activity_types": []string{"work"}}}},
	} {
		if code, text := postRuntime(t, url, request.path, request.body); code != http.StatusBadRequest {
			t.Errorf("%s with an overlong value = %d %q, want 400", name, code, text)
		}
	}
}

// Workflow 命令里超长的 Timer ID、子 Workflow ID、Activity Type、Task Queue：Workflow Task 记为失败，Run 显示
// Blocked，命令不落库。没有校验时提交以 MySQL 1406 失败，Worker 反复重试同一批命令、Run 不显示 Blocked。
func TestOverlongWorkflowCommandIdentifiersBlockRun(t *testing.T) {
	long := strings.Repeat("x", 200)
	for name, tc := range map[string]struct {
		command historypkg.Command
		cause   string
	}{
		"timer id":      {historypkg.Command{Type: historypkg.CmdScheduleTimer, TimerID: long, Delay: time.Hour}, execution.WorkflowTaskFailedCauseBadStartTimerAttributes},
		"child id":      {historypkg.Command{Type: historypkg.CmdStartChildWorkflow, ChildID: long, WorkflowType: "length-flow"}, execution.WorkflowTaskFailedCauseBadStartChildWorkflowAttributes},
		"activity type": {historypkg.Command{Type: historypkg.CmdScheduleActivity, ActivityID: "a", ActivityType: long, StartToCloseTimeout: time.Minute}, execution.WorkflowTaskFailedCauseBadScheduleActivityAttributes},
		"task queue":    {historypkg.Command{Type: historypkg.CmdScheduleActivity, ActivityID: "a", ActivityType: "work", TaskQueue: long, StartToCloseTimeout: time.Minute}, execution.WorkflowTaskFailedCauseBadScheduleActivityAttributes},
	} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			server := assembly.NewServer(assembly.Options{
				Stores: newWorkflowStores(t), WorkflowQueue: memory.NewTaskQueue(16), ActivityRouter: memory.NewActivityQueueRouter(16),
			})
			defer server.Stop()
			registerActivityRoutes(t, server, "work")
			registerReturnWorkflow(t, server, protocol.DefaultNamespace, "length-flow")
			runID, err := startedRunID(server.StartWorkflow(ctx, "", "length-flow", "length-"+strings.ReplaceAll(name, " ", "-"), "", nil))
			if err != nil {
				t.Fatal(err)
			}
			wftID := openWorkflowTaskID(t, server, runID)
			if err := server.HandleWorkflowTaskCompleted(workflowTaskContext(t, ctx, server, runID, wftID), runID, wftID, []historypkg.Command{tc.command}, ""); err != nil {
				t.Fatalf("complete workflow task = %v", err)
			}
			state, err := server.GetWorkflowExecutionState(runID)
			if err != nil {
				t.Fatal(err)
			}
			if blocked := state.BlockedInfo(); !blocked.Blocked || blocked.Cause != tc.cause {
				t.Fatalf("blocked info = %+v, want cause %s", blocked, tc.cause)
			}
			events, err := server.GetEvents(runID)
			if err != nil {
				t.Fatal(err)
			}
			for _, event := range events {
				switch event.EventType() {
				case historypkg.EventTimerScheduled, historypkg.EventActivityScheduled, historypkg.EventChildWorkflowStarted:
					t.Fatalf("rejected command was recorded: %s", event.EventType())
				}
			}
		})
	}
}
