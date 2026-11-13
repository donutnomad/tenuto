package interpreter

import (
	"context"
	"errors"

	"github.com/donutnomad/tenuto/internal/executor"
	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/protocol/history"
	wltypes "github.com/donutnomad/wlang/types"
)

// queryBeforeFirstWorkflowTaskMessage 是 Run 还没有完成任何 Workflow Task 时 query 的拒绝原因：
// main 尚未运行，query handler 还没有注册。
const queryBeforeFirstWorkflowTaskMessage = "workflow has not completed its first workflow task yet; query handlers are not registered"

// errQueryHandlerBlocked 是 query handler 调用阻塞原语或 workflow.Go 时的拒绝原因：handler 在调用方的
// goroutine 上对重放后的只读现场求值一次，没有协程可以挂起或推进。
var errQueryHandlerBlocked = errors.New("query handler must not block or start coroutines (Await, Sleep, Future.Get, channel, Selector, WaitGroup.Wait, workflow.Go)")

// QueryEvaluator 实现服务端的 executions.QueryEvaluator 端口。
type QueryEvaluator struct{}

// EvaluateQuery replays History in read-only mode through the last completed
// Workflow Task and invokes a registered Query handler against that state.
func (QueryEvaluator) EvaluateQuery(ctx context.Context, wf *program.WorkflowConfig, events []history.Event, name string, input map[string]any) (map[string]any, bool, string) {
	interpreter := NewWorkflowExecutorWithContext(ctx, wf, events)
	api, wlExec, err := interpreter.ReplayForQuery()
	if err != nil {
		return nil, true, "query replay failed: " + err.Error()
	}
	if api == nil {
		if len(executor.SplitBatches(events)) == 1 {
			return nil, true, queryBeforeFirstWorkflowTaskMessage
		}
		return nil, true, "unknown queryType: " + name
	}
	handler, ok := api.queryHandlers[name]
	if !ok {
		return nil, true, "unknown queryType: " + name
	}
	interpreter.queryHandler = true
	ctxValue := wltypes.NewValue("workflow.Context", workflowContext{lanePath: "query:" + name, scope: interpreter.rootScope})
	inputValue := wltypes.NewValue("map<string,any>", input)
	output, err := wlExec.InvokeClosure(handler, []wltypes.Value{ctxValue, inputValue})
	if err != nil {
		return nil, true, err.Error()
	}
	// 结果 map 里的 record（Query 结果里的 struct 值）按类型表编码。
	result, err := RecordToMap(api.registry, closureOutputMap(output))
	if err != nil {
		return nil, true, err.Error()
	}
	if failure, ok := result[queryHandlerFailureKey].(string); ok && failure != "" {
		return result, true, failure
	}
	return result, false, ""
}
