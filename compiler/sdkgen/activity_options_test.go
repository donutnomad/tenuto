package sdkgen

import "testing"

// 调用点的 TaskQueue 只作用于那一次调用，不写进符号的缺省路由。
func TestGenerateFromSourceKeepsCallSiteRouteOutOfActivityRef(t *testing.T) {
	src := []byte(`package flows

import (
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	activities "example.com/activities"
)

func RoutedWorkflow(ctx workflow.Context, input map[string]any) error {
	var out map[string]any
	return workflow.ExecuteActivity(ctx, activities.Capture, input, workflow.ActivityOptions{
		TaskQueue: "custodian",
	}).Get(ctx, &out)
}
`)
	result, err := GenerateFromSource(src, Options{
		WorkflowID: "routed-flow",
		FuncName:   "RoutedWorkflow",
		Filename:   "routed_workflow.go",
	})
	if err != nil {
		t.Fatal(err)
	}
	activity, ok := result.Workflow.Activities["activities.Capture"]
	if !ok || activity.TaskQueue != "" {
		t.Fatalf("activity ref = %+v, want the symbol without a default route", activity)
	}
}
