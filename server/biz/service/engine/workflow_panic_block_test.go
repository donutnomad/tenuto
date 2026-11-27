package engine_test

import (
	"context"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/compiler/sdkgen"
	"github.com/donutnomad/tenuto/internal/worker"
	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/app/assembly"
	"github.com/donutnomad/tenuto/server/biz/service/engine"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

// Workflow 代码的运行时 panic（越界、nil 指针取字段、非 comma-ok 类型断言失败，含赋给 `_` 的表达式）按 Temporal 缺省的 BlockWorkflow 处理：Workflow Task 以 workflow_panic 失败，
// Run 保持 Running 并显示为 Blocked，不写 WorkflowFailed。
func TestWorkflowCodePanicBlocksRun(t *testing.T) {
	for name, statement := range map[string]string{
		"used value":            "x := items[n]\n\t_ = x",
		"discarded":             "_ = items[n]",
		"discarded nil field":   "var item *Item\n\tif n > 0 {\n\t\t_ = item.N\n\t}",
		"failed type assertion": "var v any = n\n\ttext := v.(string)\n\tout[text] = true",
	} {
		t.Run(name, func(t *testing.T) {
			source := `package flows
import (
	"time"
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/activities"
)
type Item struct{ N int }
func W(ctx workflow.Context, input map[string]any) error {
	var out map[string]any
	if err := workflow.ExecuteActivity(ctx, activities.A0, input, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &out); err != nil {
		return err
	}
	items := []int{1, 2, 3}
	n := len(out) + 5
	` + statement + `
	return nil
}
`
			generated, err := sdkgen.GenerateFromSource([]byte(source), sdkgen.Options{WorkflowType: "wf-panic", FuncName: "W", Filename: "w.go"})
			if err != nil {
				t.Fatal(err)
			}
			ref := generated.Workflow.Activities["activities.A0"]
			ref.Type = "activity.first"
			generated.Workflow.Activities["activities.A0"] = ref
			wf := &program.WorkflowConfig{Type: "wf-panic", Program: generated.Workflow.Program, Activities: generated.Workflow.Activities}

			mgr := memory.NewExecutionManager()
			activityTasks := memory.NewTaskQueue(16)
			server := assembly.NewServer(assembly.Options{Stores: assembly.Stores{Executions: mgr}, WorkflowQueue: memory.NewTaskQueue(16), ActivityRouter: engine.QueueActivityRouter{Queue: activityTasks}})
			defer server.Stop()
			registerE2EActivities(t, server)
			if _, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, wf); err != nil {
				t.Fatal(err)
			}
			w := worker.NewWorkflowWorker(server, worker.WorkerOptions{Identity: "wf-worker", BuildID: "b1"})
			ctx, cancel := context.WithCancel(context.Background())
			w.Start(ctx)
			defer func() { cancel(); w.Stop() }()
			runID, err := startedRunID(server.StartWorkflow(ctx, "", wf.Type, wf.Type, "", nil))
			if err != nil {
				t.Fatal(err)
			}
			completeActivityTask(t, server, dequeueActivityTask(t, activityTasks), map[string]any{"ok": true})

			deadline := time.Now().Add(5 * time.Second)
			for {
				events, err := server.GetEvents(runID)
				if err != nil {
					t.Fatal(err)
				}
				var panicFailure *history.WorkflowTaskFailedEvent
				for _, event := range events {
					if event.WorkflowFailed != nil || event.WorkflowCompleted != nil {
						t.Fatalf("run closed after a runtime panic: %+v", event.WorkflowFailed)
					}
					if event.WorkflowTaskFailed != nil && event.WorkflowTaskFailed.Cause == history.WorkflowTaskFailedCauseWorkflowPanic {
						panicFailure = event.WorkflowTaskFailed
					}
				}
				if panicFailure != nil {
					status, err := server.GetRunStatus(runID)
					if err != nil || status != history.StatusRunning {
						t.Fatalf("run status = %s err=%v, want Running", status, err)
					}
					state, err := mgr.GetWorkflowExecutionState(runID)
					if err != nil || !state.Blocked.Blocked {
						t.Fatalf("run not blocked after a workflow panic: %+v err=%v", state.Blocked, err)
					}
					return
				}
				if time.Now().After(deadline) {
					t.Fatalf("no workflow_panic WorkflowTaskFailed within 5s; events: %+v", events)
				}
				time.Sleep(20 * time.Millisecond)
			}
		})
	}
}
