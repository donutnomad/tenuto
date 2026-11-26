package interpreter

// WLang 的执行步数按决策批（Workflow Task）计数：长期运行、每个 Workflow Task 只做少量计算的 Run 不会因为
// 累计步数失败；单个 Workflow Task 超出上限时以 E_BUDGET 失败。

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/donutnomad/tenuto/internal/executor"
	"github.com/donutnomad/tenuto/protocol/history"
	wlerrors "github.com/donutnomad/wlang/errors"
)

// 实体 Workflow：每条 tick Signal 对 200 个条目做一次累加。每个 Workflow Task 约 800 条语句，
// 按 Run 累计计数时约 1200 个 Workflow Task 后超过 1,000,000。
const perSignalSumSrc = `package flows

import (
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
)

type Out struct {
	Total int
}

func W(ctx workflow.Context, input map[string]any) (Out, error) {
	items := make([]int, 0)
	for k := 0; k < 200; k++ {
		items = append(items, k)
	}
	total := 0
	for i := 0; i < 100000; i++ {
		var v map[string]any
		_ = workflow.WaitSignal(ctx, "tick").Get(ctx, &v)
		sum := 0
		for _, it := range items {
			sum += it
		}
		total += sum
	}
	return Out{Total: total}, nil
}
`

func TestExecutionBudgetCountsPerWorkflowTask(t *testing.T) {
	session := NewExecutionSession(context.Background(), compileSource(t, perSignalSumSrc), 1)
	t.Cleanup(func() { _ = session.Close() })
	if _, err := session.Restore(context.Background(), NewSliceHistoryIterator(nil)); err != nil {
		t.Fatal(err)
	}
	next := int64(1)
	var all []history.Event
	appendEvent := func(e history.Event) history.Event {
		e = executionSessionEvent(next, e)
		next++
		all = append(all, e)
		return e
	}
	// 每个 Workflow Task 看到 1 条 Signal，与生产里「一条 Signal 一个 Workflow Task」相同。
	const workflowTasks = 2500
	for wft := 1; wft <= workflowTasks; wft++ {
		var events []history.Event
		if wft == 1 {
			events = append(events, appendEvent(history.NewWorkflowStartedEvent(nil, "", "", "", "", 0)))
		} else {
			events = append(events, appendEvent(history.NewWorkflowSignaledEvent("tick", map[string]any{})))
		}
		scheduled := appendEvent(history.Event{WorkflowTaskScheduled: &history.WorkflowTaskScheduledEvent{}})
		events = append(events, scheduled, appendEvent(history.NewWorkflowTaskStartedEvent(scheduled.ID, "worker", "build", 0)))
		result, err := session.Activate(context.Background(), history.WorkflowActivation{
			ActivationID: fmt.Sprintf("activation-%d", wft), ExecutionGeneration: 1, Events: events, ThroughEventID: next - 1,
		})
		if err != nil {
			t.Fatalf("Workflow Task #%d（History %d 个事件）失败：%v", wft, len(all), err)
		}
		for _, command := range result.Commands {
			if command.Type == history.CmdFailWorkflow {
				t.Fatalf("Workflow Task #%d（History %d 个事件）产出 FailWorkflow：%s", wft, len(all), command.Error)
			}
		}
		completed := appendEvent(history.NewWorkflowTaskCompletedEvent(scheduled.ID, "worker"))
		if err := session.Commit(history.WorkflowTaskCompletionAck{
			Accepted: true, CommittedThroughEventID: completed.ID,
			CommittedEvents:     []history.Event{completed},
			ActivationID:        result.ActivationID,
			CommandDigest:       result.CommandDigest,
			ExecutionGeneration: 1,
		}); err != nil {
			t.Fatal(err)
		}
	}
	// 冷重放全部批同样不触顶：重放时每批重新计数。
	all = append(all, executionSessionEvent(next, history.NewWorkflowSignaledEvent("tick", map[string]any{})))
	if _, err := NewWorkflowExecutor(compileSource(t, perSignalSumSrc), all).Execute(); err != nil {
		t.Fatalf("冷重放 %d 个 Workflow Task 失败：%v", workflowTasks, err)
	}
}

// 一个 Workflow Task 里执行的语句超过上限时以 E_BUDGET 失败，Workflow Task 按 workflow_panic 失败（Run Blocked）。
func TestExecutionBudgetExceededInOneWorkflowTask(t *testing.T) {
	requireBudgetBlocked(t, `
	total := 0
	for i := 0; i < 2000000; i++ {
		total += i
	}
	return Out{Total: total}, nil`)
}

// 空循环体的死循环每轮也计入步数：生成器给空循环体插入一条语句，触顶后 E_BUDGET，不会让 Worker 永远执行。
func TestEmptyLoopBodyCountsSteps(t *testing.T) {
	for _, body := range []string{
		"for i := 0; i < 1; {\n\t}\n\treturn Out{}, nil",
		"for range 1 << 62 {\n\t}\n\treturn Out{}, nil",
		"for i := 0; i < 1; {\n\t\t{\n\t\t}\n\t}\n\treturn Out{}, nil",
	} {
		requireBudgetBlocked(t, body)
	}
}

func requireBudgetBlocked(t *testing.T, body string) {
	t.Helper()
	src := `package flows

import (
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
)

type Out struct {
	Total int
}

func W(ctx workflow.Context, input map[string]any) (Out, error) {
	` + body + `
}
`
	started := history.NewWorkflowStartedEvent(nil, "", "", "", "", 0)
	started.ID = 1
	_, err := NewWorkflowExecutor(compileSource(t, src), []history.Event{started}).Execute()
	var langErr *wlerrors.LangError
	if !errors.As(err, &langErr) || langErr.Code != wlerrors.CodeBudget || !strings.Contains(langErr.Message, "MaxSteps exceeded") {
		t.Fatalf("%s: err = %v, want E_BUDGET MaxSteps exceeded", body, err)
	}
	if cause := executor.WorkflowTaskBlockedCause(err); cause != history.WorkflowTaskFailedCauseWorkflowPanic {
		t.Fatalf("%s: blocked cause = %q, want workflow_panic", body, cause)
	}
}
