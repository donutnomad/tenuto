package engine

import (
	"encoding/json"
	"testing"

	"github.com/donutnomad/tenuto/compiler/sdkgen"
	"github.com/donutnomad/tenuto/internal/executor/interpreter"
	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
)

func TestTemporalUpdateHandlerCreateResultAwaitEndToEnd(t *testing.T) {
	source := []byte(`package flows

import workflow "github.com/donutnomad/tenuto/sdk/workflow"

type CreateResult struct {
	RecordID string ` + "`json:\"record_id\"`" + `
	Status string ` + "`json:\"status\"`" + `
}

func ApprovalWorkflow(ctx workflow.Context, input map[string]any) error {
	var initResult CreateResult
	createDone := false
	err := workflow.SetUpdateHandler(ctx, "create-record", func(ctx workflow.Context) (CreateResult, error) {
		_ = workflow.Await(ctx, func() bool { return createDone })
		return initResult, nil
	})
	if err != nil {
		return err
	}
	initResult = CreateResult{RecordID: "rec-42", Status: "created"}
	createDone = true
	return nil
}
`)
	generated, err := sdkgen.GenerateFromSource(source, sdkgen.Options{
		WorkflowType: "approval-workflow", FuncName: "ApprovalWorkflow", Filename: "approval.go",
	})
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	node := &program.WorkflowConfig{Type: "approval", Program: generated.Workflow.Program}
	accepted := history.NewWorkflowUpdateAcceptedEvent("create-record", "update-1", protocol.Payloads{})
	accepted.ID = 2
	commands, err := interpreter.NewWorkflowExecutor(node, []history.Event{
		history.NewWorkflowStartedEvent(nil, "", "", "approval-1", "ApprovalWorkflow", 0), accepted,
	}).Execute()
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	completed := findUpdateCompletes(commands)
	if len(completed) != 1 || completed[0].UpdateOutcome == nil || completed[0].UpdateOutcome.Success == nil {
		t.Fatalf("completed commands = %+v", completed)
	}
	var result struct {
		RecordID string `json:"record_id"`
		Status   string `json:"status"`
	}
	if err := completed[0].UpdateOutcome.Success.Decode(&result); err != nil {
		t.Fatalf("decode outcome: %v", err)
	}
	if result.RecordID != "rec-42" || result.Status != "created" {
		t.Fatalf("result = %+v", result)
	}
}

func TestTemporalUpdateHandlerMultipleArguments(t *testing.T) {
	source := []byte(`package flows

import workflow "github.com/donutnomad/tenuto/sdk/workflow"

func MathWorkflow(ctx workflow.Context, input map[string]any) error {
	return workflow.SetUpdateHandler(ctx, "sum", func(ctx workflow.Context, left int64, right int64) (int64, error) {
		return left + right, nil
	})
}
`)
	generated, err := sdkgen.GenerateFromSource(source, sdkgen.Options{
		WorkflowType: "math-workflow", FuncName: "MathWorkflow", Filename: "math.go",
	})
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	left, _ := protocol.NewJSONPayload(int64(19))
	right, _ := protocol.NewJSONPayload(int64(23))
	accepted := history.NewWorkflowUpdateAcceptedEvent("sum", "update-sum", protocol.Payloads{left, right})
	accepted.ID = 2
	commands, err := interpreter.NewWorkflowExecutor(&program.WorkflowConfig{
		Type: "math", Program: generated.Workflow.Program,
	}, []history.Event{history.NewWorkflowStartedEvent(nil, "", "", "math-1", "MathWorkflow", 0), accepted}).Execute()
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	completed := findUpdateCompletes(commands)
	if len(completed) != 1 || completed[0].UpdateOutcome == nil || completed[0].UpdateOutcome.Success == nil {
		t.Fatalf("completed commands = %+v", completed)
	}
	var result int64
	if err := completed[0].UpdateOutcome.Success.Decode(&result); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if result != 42 {
		t.Fatalf("result = %d", result)
	}
}

func TestTemporalUpdateHandlerApplicationErrorDetails(t *testing.T) {
	source := []byte(`package flows

import workflow "github.com/donutnomad/tenuto/sdk/workflow"

func ApprovalWorkflow(ctx workflow.Context, input map[string]any) error {
	return workflow.SetUpdateHandler(ctx, "reject", func(ctx workflow.Context, reason string) error {
		return workflow.NewApplicationError("approval rejected", "ApprovalRejected", reason)
	})
}
`)
	generated, err := sdkgen.GenerateFromSource(source, sdkgen.Options{
		WorkflowType: "approval-errors", FuncName: "ApprovalWorkflow", Filename: "approval_error.go",
	})
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	reason, _ := protocol.NewJSONPayload("risk policy")
	accepted := history.NewWorkflowUpdateAcceptedEvent("reject", "update-reject", protocol.Payloads{reason})
	accepted.ID = 2
	commands, err := interpreter.NewWorkflowExecutor(&program.WorkflowConfig{
		Type: "approval-errors", Program: generated.Workflow.Program,
	}, []history.Event{history.NewWorkflowStartedEvent(nil, "", "", "approval-errors-1", "ApprovalWorkflow", 0), accepted}).Execute()
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	completed := findUpdateCompletes(commands)
	if len(completed) != 1 || completed[0].UpdateOutcome == nil || completed[0].UpdateOutcome.Failure == nil {
		t.Fatalf("completed commands = %+v", completed)
	}
	failure := completed[0].UpdateOutcome.Failure
	if failure.Message != "approval rejected" || failure.Type != "ApprovalRejected" || len(failure.Details) != 1 {
		t.Fatalf("failure = %+v", failure)
	}
	var detail string
	if err := failure.Details[0].Decode(&detail); err != nil {
		t.Fatalf("decode detail: %v", err)
	}
	if detail != "risk policy" {
		t.Fatalf("detail = %q", detail)
	}
}

func TestTemporalUpdateHandlerReturnsTypedActivityResult(t *testing.T) {
	source := []byte(`package flows

import (
	"time"
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/app/activities"
)

type CreateResult struct {
	RecordID string ` + "`json:\"record_id\"`" + `
	Status string ` + "`json:\"status\"`" + `
}

func ApprovalWorkflow(ctx workflow.Context, input map[string]any) error {
	var initResult CreateResult
	createDone := false
	if err := workflow.SetUpdateHandler(ctx, "create-record", func(ctx workflow.Context) (CreateResult, error) {
		_ = workflow.Await(ctx, func() bool { return createDone })
		return initResult, nil
	}); err != nil {
		return err
	}
	if err := workflow.ExecuteActivity(ctx, activities.CreateRecord, input, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &initResult); err != nil {
		return err
	}
	createDone = true
	return nil
}
`)
	generated, err := sdkgen.GenerateFromSource(source, sdkgen.Options{
		WorkflowType: "typed-activity-update", FuncName: "ApprovalWorkflow", Filename: "typed_activity.go",
	})
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	node := generated.Workflow
	started := history.NewWorkflowStartedEvent(map[string]any{"record_id": "rec-activity"}, "", "", "typed-activity-1", "ApprovalWorkflow", 0)
	accepted := history.NewWorkflowUpdateAcceptedEvent("create-record", "typed-activity-update-1", protocol.Payloads{})
	accepted.ID = 2
	commands, err := interpreter.NewWorkflowExecutor(node, []history.Event{started, accepted}).Execute()
	if err != nil {
		t.Fatalf("first execution: %v", err)
	}
	var schedule *history.Command
	for i := range commands {
		if commands[i].Type == history.CmdScheduleActivity {
			schedule = &commands[i]
			break
		}
	}
	if schedule == nil {
		t.Fatalf("schedule command missing: %+v", commands)
	}
	if len(findUpdateCompletes(commands)) != 0 {
		t.Fatalf("update completed before activity result: %+v", commands)
	}
	scheduled := history.Event{ID: 3, ActivityScheduled: &history.ActivityScheduledEvent{
		ActivityID: schedule.ActivityID, ActivityType: schedule.ActivityType, Input: schedule.Input,
	}}
	completed := history.Event{ID: 4, ActivityCompleted: &history.ActivityCompletedEvent{
		ScheduledEventID: 3,
		Output:           map[string]any{"record_id": "rec-activity", "status": "pending_created"},
	}}
	commands, err = interpreter.NewWorkflowExecutor(node, []history.Event{started, accepted, scheduled, completed}).Execute()
	if err != nil {
		t.Fatalf("second execution: %v", err)
	}
	updates := findUpdateCompletes(commands)
	if len(updates) != 1 || updates[0].UpdateOutcome == nil || updates[0].UpdateOutcome.Success == nil {
		t.Fatalf("update completion missing: %+v", commands)
	}
	var result struct {
		RecordID string `json:"record_id"`
		Status   string `json:"status"`
	}
	if err := updates[0].UpdateOutcome.Success.Decode(&result); err != nil {
		t.Fatalf("decode outcome: %v", err)
	}
	if result.RecordID != "rec-activity" || result.Status != "pending_created" {
		t.Fatalf("result = %+v", result)
	}
}

func TestDebugReplayExecutesTemporalUpdateHandler(t *testing.T) {
	source := []byte(`package flows

import workflow "github.com/donutnomad/tenuto/sdk/workflow"

func TraceWorkflow(ctx workflow.Context, input map[string]any) error {
	return workflow.SetUpdateHandler(ctx, "inspect", func(ctx workflow.Context, value string) (string, error) {
		return "trace-" + value, nil
	})
}
`)
	generated, err := sdkgen.GenerateFromSource(source, sdkgen.Options{
		WorkflowType: "typed-trace", FuncName: "TraceWorkflow", Filename: "typed_trace.go",
	})
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	programJSON, err := json.Marshal(generated.Workflow.Program)
	if err != nil {
		t.Fatalf("marshal program: %v", err)
	}
	arg, _ := protocol.NewJSONPayload("handler")
	accepted := history.NewWorkflowUpdateAcceptedEvent("inspect", "trace-update-1", protocol.Payloads{arg})
	accepted.ID = 2
	result, err := interpreter.ReplayWLangForDebug(nil, programJSON, []history.Event{
		history.NewWorkflowStartedEvent(nil, "", "", "typed-trace-1", "TraceWorkflow", 0), accepted,
	}, nil)
	if err != nil {
		t.Fatalf("debug replay: %v", err)
	}
	if result.Blocked {
		t.Fatalf("debug replay remained blocked: %+v", result)
	}
}
