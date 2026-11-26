package engine

import (
	"testing"

	"github.com/donutnomad/tenuto/compiler/sdkgen"
	"github.com/donutnomad/tenuto/internal/executor"
	"github.com/donutnomad/tenuto/internal/executor/interpreter"
	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
)

// Update handler 里的 panic（越界、显式 panic）与主流程一致：Execute 返回 workflow_panic 类错误，Workflow Task
// 失败、Run 保持 Running，不产出这个 Update 的完成命令；修复代码后重放时重新执行 handler。
func TestUpdateHandlerPanicBlocksWorkflowTask(t *testing.T) {
	for name, body := range map[string]string{
		"index out of range": "items := []int64{1, 2, 3}\n\t\treturn items[n], nil",
		"explicit panic":     "if n > 0 {\n\t\t\tpanic(\"boom\")\n\t\t}\n\t\treturn n, nil",
	} {
		t.Run(name, func(t *testing.T) {
			source := []byte(`package flows

import workflow "github.com/donutnomad/tenuto/sdk/workflow"

func PanicWorkflow(ctx workflow.Context, input map[string]any) error {
	return workflow.SetUpdateHandler(ctx, "pick", func(ctx workflow.Context, n int64) (int64, error) {
		` + body + `
	})
}
`)
			generated, err := sdkgen.GenerateFromSource(source, sdkgen.Options{WorkflowType: "panic-workflow", FuncName: "PanicWorkflow", Filename: "panic.go"})
			if err != nil {
				t.Fatalf("generate: %v", err)
			}
			arg, _ := protocol.NewJSONPayload(int64(6))
			accepted := history.NewWorkflowUpdateAcceptedEvent("pick", "update-pick", protocol.Payloads{arg})
			accepted.ID = 2
			commands, err := interpreter.NewWorkflowExecutor(&program.WorkflowConfig{Type: "panic", Program: generated.Workflow.Program},
				[]history.Event{history.NewWorkflowStartedEvent(nil, "", "", "panic-1", "PanicWorkflow", 0), accepted}).Execute()
			if cause := executor.WorkflowTaskBlockedCause(err); cause != history.WorkflowTaskFailedCauseWorkflowPanic {
				t.Fatalf("execute err = %v (cause %q), commands = %+v; want a workflow_panic error", err, cause, commands)
			}
			if completed := findUpdateCompletes(commands); len(completed) != 0 {
				t.Fatalf("panicking handler completed the update: %+v", completed)
			}
		})
	}
}
