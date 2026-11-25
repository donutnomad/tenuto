package sdkgen

import (
	"testing"
)

func TestFutureGetPatternCollectsActivity(t *testing.T) {
	src := []byte(`package flows

import (
	"time"
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/app/activities"
)

func OrderWorkflow(ctx workflow.Context, input map[string]any) error {
	var reserve map[string]any
	future := workflow.ExecuteActivity(ctx, activities.ReserveInventory, input, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})
	err := future.Get(ctx, &reserve)
	if err != nil {
		return err
	}
	return nil
}
`)

	result, err := GenerateFromSource(src, Options{WorkflowType: "order-flow", FuncName: "OrderWorkflow", Filename: "order_workflow.go"})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := result.Workflow.Activities["activities.ReserveInventory"]; !ok || len(result.Workflow.Activities) != 1 {
		t.Fatalf("expected one activity symbol, got %+v", result.Workflow.Activities)
	}
}

// fire-and-forget：不 Get 的 ExecuteActivity 照常生成，符号进 Activities 表。
func TestGenerateFromSourceAcceptsFireAndForgetExecuteActivity(t *testing.T) {
	src := []byte(`package flows

import (
	"time"
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/app/activities"
)

func OrderWorkflow(ctx workflow.Context, input map[string]any) error {
	workflow.ExecuteActivity(ctx, activities.ReserveInventory, input, workflow.ActivityOptions{StartToCloseTimeout: time.Minute, TaskQueue: "inventory"})
	return nil
}
`)

	result, err := GenerateFromSource(src, Options{WorkflowType: "bare-future", FuncName: "OrderWorkflow", Filename: "bare_future.go"})
	if err != nil {
		t.Fatal(err)
	}
	ref, ok := result.Workflow.Activities["activities.ReserveInventory"]
	if !ok || ref.Type != "activities.ReserveInventory" || ref.TaskQueue != "" {
		t.Fatalf("activities = %+v", result.Workflow.Activities)
	}
}
