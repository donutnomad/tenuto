package engine_test

import (
	"context"
	"testing"

	"github.com/donutnomad/tenuto/compiler/sdkgen"
	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/server/app/assembly"
	"github.com/donutnomad/tenuto/server/biz/domain/definition"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

func TestServerRegistersGeneratedWorkflowAndRunPinsItsVersion(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	server := assembly.NewServer(assembly.Options{Stores: assembly.Stores{Executions: mgr}, WorkflowQueue: memory.NewTaskQueue(8), ActivityRouter: memory.NewActivityQueueRouter(8)})

	src := []byte(`package flows

import (
	"time"
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/app/activities"
)

func GeneratedWorkflow(ctx workflow.Context, input map[string]any) error {
	var out map[string]any
	err := workflow.ExecuteActivity(ctx, activities.Capture, input, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &out)
	if err != nil {
		return err
	}
	return nil
}
`)
	generated, err := sdkgen.GenerateFromSource(src, sdkgen.Options{
		WorkflowType: "generated-program-flow",
		Name:         "生成代码流程",
		FuncName:     "GeneratedWorkflow",
		Filename:     "generated_workflow.go",
	})
	if err != nil {
		t.Fatal(err)
	}

	ref, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, generated.Workflow)
	if err != nil {
		t.Fatal(err)
	}
	// 生成的配置原样保存为内容版本：版本是它的内容 hash，按它能读回配置。
	wantVersion, err := program.WorkflowConfigVersion(generated.Workflow)
	if err != nil {
		t.Fatal(err)
	}
	if ref.Version != wantVersion {
		t.Fatalf("config version = %q, want the content hash of the generated config %q", ref.Version, wantVersion)
	}
	if _, err := mgr.GetWorkflowConfig(definition.WorkflowConfigRef{Namespace: ref.Namespace, WorkflowID: ref.WorkflowID, Version: ref.Version}); err != nil {
		t.Fatal(err)
	}

	runID, err := startedRunID(server.StartWorkflowVersion(ctx, ref, ref.WorkflowID, map[string]any{"order_id": "o-1"}))
	if err != nil {
		t.Fatal(err)
	}
	run, err := mgr.Get(runID)
	if err != nil {
		t.Fatal(err)
	}
	if run.WorkflowConfigVersion != ref.Version {
		t.Fatalf("run should pin the generated config, got %+v want %+v", run, ref)
	}
}
