package sdkgen

import (
	"strings"
	"testing"
)

func TestGenerateFromSource_WorkflowInfoTranslatesWithDescriptor(t *testing.T) {
	const source = `package flows

import workflow "github.com/donutnomad/tenuto/sdk/workflow"

func W(ctx workflow.Context, input map[string]any) map[string]any {
	info := workflow.GetInfo(ctx)
	return map[string]any{
		"length": info.GetCurrentHistoryLength(),
		"size": info.GetCurrentHistorySize(),
		"suggested": info.GetContinueAsNewSuggested(),
	}
}
`
	result, err := GenerateFromSource([]byte(source), Options{WorkflowType: "workflow-info", FuncName: "W", Filename: "workflow_info.go"})
	if err != nil {
		t.Fatal(err)
	}
	raw := string(result.ProgramJSON)
	for _, symbol := range []string{"GetInfo", "GetCurrentHistoryLength", "GetCurrentHistorySize", "GetContinueAsNewSuggested"} {
		if !strings.Contains(raw, symbol) {
			t.Fatalf("translated program is missing %s: %s", symbol, raw)
		}
	}
	descriptor, ok := result.Workflow.Program.Types["github.com/donutnomad/tenuto/sdk/workflow.Info"]
	if !ok {
		descriptor, ok = result.Workflow.Program.Types["workflow.Info"]
	}
	if !ok || descriptor.Fingerprint == "" {
		t.Fatalf("workflow.Info descriptor missing: %+v", result.Workflow.Program.Types)
	}
}
