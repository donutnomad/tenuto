package sdkgen

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestGenerateFromSource_TemporalUpdateHandlerSignature(t *testing.T) {
	src := []byte(`package flows

import workflow "github.com/donutnomad/tenuto/sdk/workflow"

type CreateResult struct {
	RecordID string ` + "`json:\"record_id\"`" + `
	Status string ` + "`json:\"status\"`" + `
}

func ApprovalWorkflow(ctx workflow.Context, input map[string]any) error {
	err := workflow.SetUpdateHandler(ctx, "create-record", func(ctx workflow.Context, recordID string) (CreateResult, error) {
		return CreateResult{RecordID: recordID, Status: "created"}, nil
	})
	return err
}
`)

	result, err := GenerateFromSource(src, Options{WorkflowID: "approval-flow", FuncName: "ApprovalWorkflow", Filename: "approval.go"})
	if err != nil {
		t.Fatalf("generate typed update handler: %v", err)
	}
	programBytes, err := json.Marshal(result.Workflow.Program.Program)
	if err != nil {
		t.Fatalf("marshal program: %v", err)
	}
	program := string(programBytes)
	if !strings.Contains(program, `"returns":["flows.CreateResult","error"]`) {
		t.Fatalf("typed handler returns missing from artifact: %s", program)
	}
	if len(result.Workflow.Program.Types) == 0 {
		t.Fatal("typed handler result descriptor missing")
	}
}

func TestGenerateFromSource_RejectsInvalidUpdateHandlerSignatures(t *testing.T) {
	tests := []struct {
		name    string
		handler string
		want    string
	}{
		{name: "missing error result", handler: `func(ctx workflow.Context) string { return "ok" }`, want: "must return error or (result, error)"},
		{name: "error result position", handler: `func(ctx workflow.Context) (error, string) { return nil, "ok" }`, want: "last return value must be error"},
		{name: "missing context", handler: `func(value string) error { return nil }`, want: "first parameter must be workflow.Context"},
		{name: "map result without error", handler: `func(ctx workflow.Context, in map[string]any) map[string]any { return in }`, want: "must return error or (result, error)"},
		{name: "too many results", handler: `func(ctx workflow.Context) (string, string, error) { return "", "", nil }`, want: "must return error or (result, error)"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			src := []byte(`package flows

import workflow "github.com/donutnomad/tenuto/sdk/workflow"

func Flow(ctx workflow.Context, input map[string]any) error {
	return workflow.SetUpdateHandler(ctx, "update", ` + tt.handler + `)
}
`)
			_, err := GenerateFromSource(src, Options{WorkflowID: "invalid-update", FuncName: "Flow", Filename: "invalid.go"})
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error = %v, want substring %q", err, tt.want)
			}
		})
	}
}

// Happy Path: 工作流注册 SetUpdateHandler 后，生成的 WLang 程序保留 SetUpdateHandler 宿主调用
// （闭包登记）；handler 由 engine 协程在其 UpdateAccepted 所属批执行。
func TestGenerateFromSource_SetUpdateHandler_KeepsClosure(t *testing.T) {
	src := []byte(`package flows

import workflow "github.com/donutnomad/tenuto/sdk/workflow"

func CounterWorkflow(ctx workflow.Context, input map[string]any) error {
	return workflow.SetUpdateHandler(ctx, "add", func(ctx workflow.Context, in map[string]any) (map[string]any, error) {
		return in, nil
	})
}
`)

	result, err := GenerateFromSource(src, Options{WorkflowID: "counter-flow", FuncName: "CounterWorkflow", Filename: "counter.go"})
	if err != nil {
		t.Fatalf("generate: %v", err)
	}

	programBytes, err := json.Marshal(result.Workflow.Program.Program)
	if err != nil {
		t.Fatalf("marshal program: %v", err)
	}
	prog := string(programBytes)

	if !strings.Contains(prog, "SetUpdateHandler") {
		t.Fatalf("expected SetUpdateHandler host call kept in program, got:\n%s", prog)
	}
}
