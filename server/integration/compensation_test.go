package integration

import (
	"context"
	"fmt"
	"sync"
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

func TestCompensationRunsBeforeWorkflowFails(t *testing.T) {
	registry := activity.NewRegistry()
	mgr := memory.NewExecutionManager()
	workflowTasks := memory.NewTaskQueue(16)
	activityTasks := memory.NewActivityQueueRouter(16)
	eng := assembly.NewEngine(assembly.Options{Stores: assembly.Stores{Executions: mgr}, WorkflowQueue: workflowTasks, ActivityRouter: activityTasks}, registry, worker.WorkerOptions{
		WorkflowConcurrency: 1,
		ActivityConcurrency: 1,
	})

	var mu sync.Mutex
	var calls []string
	registry.Register("charge", func(ctx context.Context, input m) (m, error) {
		mu.Lock()
		calls = append(calls, "charge")
		mu.Unlock()
		return nil, fmt.Errorf("declined")
	})
	registry.Register("refund", func(ctx context.Context, input m) (m, error) {
		mu.Lock()
		calls = append(calls, "refund")
		mu.Unlock()
		return m{"refunded": true}, nil
	})

	wf := compensationWorkflow(t, "compensation")

	ctx := context.Background()
	eng.Start(ctx)
	defer eng.Stop()

	if _, err := eng.RegisterWorkflow(wf); err != nil {
		t.Fatal(err)
	}
	runID, err := eng.Run(ctx, wf.Type, nil)
	if err != nil {
		t.Fatal(err)
	}
	waitForRunStatus(t, eng, ctx, runID, historypkg.StatusFailed, 3*time.Second)

	mu.Lock()
	gotCalls := append([]string(nil), calls...)
	mu.Unlock()
	if len(gotCalls) != 2 || gotCalls[0] != "charge" || gotCalls[1] != "refund" {
		t.Fatalf("unexpected calls: %v", gotCalls)
	}

	history, err := eng.GetEvents(ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	if countStepScheduled(history, compensationRefundStepID) != 1 {
		t.Fatalf("expected one compensation schedule, got history %+v", history)
	}
}

// compensationRefundStepID 是 compensationWorkflow 里补偿调用的 step ID。
const compensationRefundStepID = "#a2"

// compensationWorkflow 生成扣款失败后先退款、再以扣款错误失败的 Workflow：charge 只尝试一次，
// refund 的入参 reason 是扣款错误的文本。
func compensationWorkflow(t testing.TB, id string) *program.WorkflowConfig {
	t.Helper()
	generated, err := sdkgen.GenerateFromSource([]byte(`package flows

import (
	"time"

	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/activities"
)

func W(ctx workflow.Context, input map[string]any) error {
	var charged map[string]any
	err := workflow.ExecuteActivity(ctx, activities.Charge, input, workflow.ActivityOptions{StartToCloseTimeout: time.Minute, RetryPolicy: workflow.RetryPolicy{MaxAttempts: 1}}).Get(ctx, &charged)
	if err != nil {
		var refunded map[string]any
		if refundErr := workflow.ExecuteActivity(ctx, activities.Refund, map[string]any{"reason": err.Error()}, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &refunded); refundErr != nil {
			return refundErr
		}
		return err
	}
	return nil
}
`), sdkgen.Options{WorkflowType: id, FuncName: "W", Filename: "w.go"})
	if err != nil {
		t.Fatal(err)
	}
	for symbol, activityType := range map[string]string{"activities.Charge": "charge", "activities.Refund": "refund"} {
		ref := generated.Workflow.Activities[symbol]
		ref.Type = activityType
		generated.Workflow.Activities[symbol] = ref
	}
	return generated.Workflow
}

func countStepScheduled(history []historypkg.Event, stepID string) int {
	count := 0
	for _, event := range history {
		if event.EventType() == historypkg.EventActivityScheduled && event.ActivityScheduled.ActivityID == stepID {
			count++
		}
	}
	return count
}
