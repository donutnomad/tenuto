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

// Workflow Program 构造超大字符串时，解释器在分配之前以 E_BUDGET 拒绝：Run 以这个错误失败，
// 内嵌 Worker 所在的进程照常处理之后的 Run。此前 strings.Repeat("x", 1<<34) 或反复自我拼接会让进程 OOM。
func TestOversizedStringFailsRunAndEngineKeepsRunning(t *testing.T) {
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
	}{
		{"budget-doubling", `
	s := "x"
	for i := 0; i < 40; i++ {
		s += s
	}
	if len(s) == 0 {
		return fmt.Errorf("empty")
	}
	return nil`},
		{"budget-repeat", `
	s := strings.Repeat("x", 17179869184)
	if len(s) == 0 {
		return fmt.Errorf("empty")
	}
	return nil`},
		{"budget-sprintf", `
	s := fmt.Sprintf("%9999999d%9999999d", 1, 2)
	if len(s) == 0 {
		return fmt.Errorf("empty")
	}
	return nil`},
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
		waitForRunStatus(t, eng, ctx, runID, historypkg.StatusFailed, 5*time.Second)
		events, err := eng.GetEvents(ctx, runID)
		if err != nil {
			t.Fatal(err)
		}
		var failure string
		taskStarts := 0
		for _, event := range events {
			if event.WorkflowFailed != nil {
				failure = event.WorkflowFailed.Error
			}
			if event.WorkflowTaskStarted != nil {
				taskStarts++
			}
		}
		if !strings.HasPrefix(failure, "[E_BUDGET]") || !strings.Contains(failure, "MaxStringBytes exceeded") {
			t.Fatalf("%s: WorkflowFailed error = %q, want the E_BUDGET MaxStringBytes error", tc.id, failure)
		}
		if taskStarts != 1 {
			t.Fatalf("%s: %d Workflow Tasks started, want 1 (the failure is deterministic, not retried)", tc.id, taskStarts)
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
	waitForRunStatus(t, eng, ctx, runID, historypkg.StatusCompleted, 5*time.Second)
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
