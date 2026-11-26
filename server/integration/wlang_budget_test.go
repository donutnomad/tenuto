package integration

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/compiler/sdkgen"
	"github.com/donutnomad/tenuto/internal/worker"
	"github.com/donutnomad/tenuto/program"
	historypkg "github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/sdk/activity"
	"github.com/donutnomad/tenuto/server/app/assembly"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

// Workflow 代码超出 WLang 预算时（构造超大字符串、一个 Workflow Task 里执行的语句超过上限），解释器以 E_BUDGET
// 让 Workflow Task 以 workflow_panic 失败：Run 保持 Running（Blocked），按退避重试，修复代码后继续。
// 内嵌 Worker 所在的进程照常处理之后的 Run。此前 strings.Repeat("x", 1<<34) 或反复自我拼接会让进程 OOM，
// 空循环体的死循环会让 Worker 永远执行同一个 Workflow Task。
func TestWLangBudgetBlocksRunAndEngineKeepsRunning(t *testing.T) {
	eng := assembly.NewEngine(assembly.Options{
		Stores:         assembly.Stores{Executions: memory.NewExecutionManager()},
		WorkflowQueue:  memory.NewTaskQueue(16),
		ActivityRouter: memory.NewActivityQueueRouter(16),
	}, activity.NewRegistry(), worker.WorkerOptions{WorkflowConcurrency: 1, ActivityConcurrency: 1})
	ctx := context.Background()
	eng.Start(ctx)
	defer eng.Stop()

	cases := []struct {
		id   string
		body string
		want string
	}{
		{"budget-doubling", `
	s := "x"
	for i := 0; i < 40; i++ {
		s += s
	}
	if len(s) == 0 {
		return fmt.Errorf("empty")
	}
	return nil`, "MaxStringBytes exceeded"},
		{"budget-repeat", `
	s := strings.Repeat("x", 17179869184)
	if len(s) == 0 {
		return fmt.Errorf("empty")
	}
	return nil`, "MaxStringBytes exceeded"},
		{"budget-sprintf", `
	s := fmt.Sprintf("%9999999d%9999999d", 1, 2)
	if len(s) == 0 {
		return fmt.Errorf("empty")
	}
	return nil`, "MaxStringBytes exceeded"},
		// for {} 在生成期报错；下面两种死循环能通过生成，循环体为空，生成器插入的语句让每轮计入步数。
		{"budget-spin", `
	for i := 0; i < 1; {
	}
	return nil`, "MaxSteps exceeded"},
		{"budget-range-spin", `
	for range 1 << 62 {
	}
	return nil`, "MaxSteps exceeded"},
	}
	for _, tc := range cases {
		wf := sizeLimitWorkflow(t, tc.id, tc.body)
		if _, err := eng.RegisterWorkflow(wf); err != nil {
			t.Fatal(err)
		}
		runID, err := eng.Run(ctx, wf.Type, nil)
		if err != nil {
			t.Fatal(err)
		}
		waitForEventCount(t, eng, ctx, runID, historypkg.EventWorkflowTaskFailed, 1, 10*time.Second)
		events, err := eng.GetEvents(ctx, runID)
		if err != nil {
			t.Fatal(err)
		}
		for _, event := range events {
			if event.WorkflowFailed != nil {
				t.Fatalf("%s: Run failed with %q, want Blocked", tc.id, event.WorkflowFailed.Error)
			}
			if failed := event.WorkflowTaskFailed; failed != nil {
				if failed.Cause != historypkg.WorkflowTaskFailedCauseWorkflowPanic || !strings.Contains(failed.Reason, "[E_BUDGET]") || !strings.Contains(failed.Reason, tc.want) {
					t.Fatalf("%s: WorkflowTaskFailed cause=%q reason=%q, want workflow_panic with E_BUDGET %s", tc.id, failed.Cause, failed.Reason, tc.want)
				}
			}
		}
		run, err := eng.GetRun(ctx, runID)
		if err != nil {
			t.Fatal(err)
		}
		if run.Status != historypkg.StatusRunning {
			t.Fatalf("%s: Run status = %s, want running (Blocked)", tc.id, run.Status)
		}
	}

	// 同一个引擎（同一个内嵌 Worker）继续完成一个正常的 Run，结果长度在上限之内。
	wf := sizeLimitWorkflow(t, "budget-within-limit", `
	s := strings.Repeat("x", 1<<20)
	if len(s) == 0 {
		return fmt.Errorf("empty")
	}
	return nil`)
	if _, err := eng.RegisterWorkflow(wf); err != nil {
		t.Fatal(err)
	}
	runID, err := eng.Run(ctx, wf.Type, nil)
	if err != nil {
		t.Fatal(err)
	}
	waitForRunStatus(t, eng, ctx, runID, historypkg.StatusCompleted, 10*time.Second)
}

// sizeLimitWorkflow 生成以 body 为函数体的 Workflow，body 可以使用 fmt 与 strings。
func sizeLimitWorkflow(t *testing.T, id, body string) *program.WorkflowConfig {
	t.Helper()
	generated, err := sdkgen.GenerateFromSource([]byte(`package flows

import (
	"fmt"
	"strings"

	workflow "github.com/donutnomad/tenuto/sdk/workflow"
)

func W(ctx workflow.Context, input map[string]any) error {`+body+`
}
`), sdkgen.Options{WorkflowType: id, FuncName: "W", Filename: "w.go"})
	if err != nil {
		t.Fatal(err)
	}
	return generated.Workflow
}
