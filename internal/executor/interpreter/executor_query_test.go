package interpreter

import (
	"strings"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/compiler/sdkgen"
	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	wlruntime "github.com/donutnomad/wlang/runtime"
	"github.com/donutnomad/wlang/types"
)

// queryReplayResult 承载 queryViaReplay 的求值产物：结果 map、rejected 标记、错误。
// 复刻 QueryWorkflow.evaluateQuery（Layer 3）在 engine 内会做的动作，供步骤 5 独立锁定
// ReplayForQuery 语义（不经 server 接线）。
type queryReplayResult struct {
	result    map[string]any    // handler 返回 map（含 __query_error 时也在此）
	rejected  bool              // 命中 __query_error 或 handler 求值 error
	errMsg    string            // rejected 时的错误详情
	commands  []history.Command // ReplayForQuery 收敛后 executor 的 commands（应恒空——只读现场）
	replayErr error             // ReplayForQuery 返回的 error（非确定性等）
}

// queryViaReplay 翻译 src → 构造 query 专用 executor → ReplayForQuery → 按 name 查 queryHandlers →
// InvokeClosure 求值一次。这是 QueryWorkflow.evaluateQuery 的 engine 内等价物，用于步骤 5 直击
// ReplayForQuery/queryReplay 语义（TC-Q2/Q3/Q4/Q7）。
func queryViaReplay(t *testing.T, src string, events []history.Event, name string, input map[string]any) queryReplayResult {
	t.Helper()
	result, err := sdkgen.GenerateFromSource([]byte(src), sdkgen.Options{WorkflowType: "probe-query", FuncName: "W", Filename: "w.go"})
	if err != nil {
		t.Fatalf("translate: %v", err)
	}
	nodes := result.Workflow

	exec := NewWorkflowExecutor(nodes, events)
	api, wlExec, replayErr := exec.ReplayForQuery()
	out := queryReplayResult{commands: exec.commands, replayErr: replayErr}
	if replayErr != nil {
		out.rejected = true
		out.errMsg = replayErr.Error()
		return out
	}
	if api == nil {
		out.rejected = true
		out.errMsg = queryBeforeFirstWorkflowTaskMessage
		return out
	}
	cl, ok := api.queryHandlers[name]
	if !ok {
		out.rejected = true
		out.errMsg = "unknown queryType: " + name
		return out
	}
	ctxVal := types.NewValue("workflow.Context", workflowContext{lanePath: "query:" + name})
	inputVal := types.NewValue("map<string,any>", input)
	res, err := wlExec.InvokeClosure(cl, []types.Value{ctxVal, inputVal})
	if err != nil {
		out.rejected = true
		out.errMsg = err.Error()
		return out
	}
	out.result = closureOutputMap(res)
	if failure, ok := out.result[queryHandlerFailureKey].(string); ok && failure != "" {
		out.rejected = true
		out.errMsg = failure
	}
	// 复刻收敛后重读 commands（求值不应新增命令）。
	out.commands = exec.commands
	return out
}

// TC-R1
// Happy Path: SetQueryHandler 把闭包登记进 queryHandlers，与 update 注册表隔离；同名后登记的覆盖前者。
func TestSetQueryHandler_RegistersIsolatedFromUpdate(t *testing.T) {
	src := `package flows
import workflow "github.com/donutnomad/tenuto/sdk/workflow"
func W(ctx workflow.Context, input map[string]any) error {
	workflow.SetQueryHandler(ctx, "getCount", func(ctx workflow.Context, in map[string]any) map[string]any {
		return map[string]any{"from": "first"}
	})
	workflow.SetQueryHandler(ctx, "getCount", func(ctx workflow.Context, in map[string]any) map[string]any {
		return map[string]any{"from": "second"}
	})
	var stop map[string]any
	return workflow.WaitSignal(ctx, "stop").Get(ctx, &stop)
}
`
	result, err := sdkgen.GenerateFromSource([]byte(src), sdkgen.Options{WorkflowType: "probe-query", FuncName: "W", Filename: "w.go"})
	if err != nil {
		t.Fatalf("translate: %v", err)
	}
	nodes := result.Workflow
	started := history.NewWorkflowStartedEvent(map[string]any{}, "", "", "", "", 0)
	api, _, err := NewWorkflowExecutor(nodes, []history.Event{started, wftCompletedEvent(2)}).ReplayForQuery()
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if len(api.queryHandlers) != 1 || api.queryHandlers["getCount"] == nil {
		t.Fatalf("queryHandlers = %v, want one getCount handler", api.queryHandlers)
	}
	if len(api.updateHandlers) != 0 {
		t.Fatalf("query 注册不应污染 updateHandlers, got %v", api.updateHandlers)
	}
	got := queryViaReplay(t, src, []history.Event{started, wftCompletedEvent(2)}, "getCount", map[string]any{})
	if got.result["from"] != "second" {
		t.Fatalf("同 name 后注册应覆盖, got %#v", got.result)
	}
}

// Error Case: 签名不是 func(workflow.Context, map[string]any) map[string]any 的闭包在登记时被拒绝。
func TestSetQueryHandler_RejectsClosureWithWrongSignature(t *testing.T) {
	api := &runtimeWorkflowAPI{}
	err := api.SetQueryHandler(workflowContext{}, "getCount", &wlruntime.Closure{})
	if err == nil || !strings.Contains(err.Error(), `"getCount"`) || !strings.Contains(err.Error(), "handler must be func(ctx workflow.Context, input map[string]any) map[string]any") {
		t.Fatalf("wrong signature error = %v", err)
	}
	if len(api.queryHandlers) != 0 {
		t.Fatalf("rejected handler was registered: %v", api.queryHandlers)
	}
}

// ============================================
// B1 动态 Query Handler —— Execute 抽取保护网 + query 专用重放
// ============================================
// 本文件的 TC-Q6 是 M3（replayToConverged 抽取）的回归护栏：在把 Execute() 的
// :533-:606 抽成 replayToConverged() 前后，Execute 对同一输入必须产出逐点一致的
// commands/error。三条路径锚定收敛块的全部行为分支：正常完成 / body 失败 / replay 漂移。
// ============================================

// TC-Q6
// Happy Path: 无历史的单批工作流跑完 body → 收敛块 emit CmdCompleteWorkflow。
// 目的：锚定抽取块「非 replay、非失败」收敛分支行为不变。
// 预期：无 error，commands 含且仅含 CmdCompleteWorkflow。
func TestExecute_Regression_HappyCompletes(t *testing.T) {
	const src = `package flows
import workflow "github.com/donutnomad/tenuto/sdk/workflow"
func W(ctx workflow.Context, input map[string]any) error {
	return nil
}
`
	started := history.NewWorkflowStartedEvent(map[string]any{}, "", "", "", "", 0)
	cmds, err := probeExecFix(t, src, []history.Event{started})
	if err != nil {
		t.Fatalf("happy path 不应报错, got %v", err)
	}
	if !hasCompleteWorkflow(cmds) {
		t.Fatalf("应 emit CmdCompleteWorkflow, got %+v", cmds)
	}
}

// TC-Q6
// Error Case: activity 失败后 body return 该 error → 收敛块 emit CmdFailWorkflow（携带错误消息）。
// 目的：锚定抽取块「非 replay、body 失败」收敛分支行为不变（return 值经 workflowReturnErr 通道）。
// 预期：Execute 本身不返回 err，commands 含 CmdFailWorkflow 且携带非空错误消息。
func TestExecute_Regression_BodyReturnErrFails(t *testing.T) {
	started := history.NewWorkflowStartedEvent(map[string]any{}, "", "", "", "", 0)
	sched := history.Event{ID: 2, Timestamp: time.Now(), ActivityScheduled: &history.ActivityScheduledEvent{ActivityID: "#a1", ActivityType: "activities.A"}}
	failed := history.Event{ID: 3, Timestamp: time.Now(), ActivityFailed: &history.ActivityFailedEvent{ScheduledEventID: 2, Error: "boom", Failure: &protocol.Failure{Message: "boom"}}}

	cmds, err := probeExecFix(t, rmSingleActivitySrc, []history.Event{started, sched, failed, wftc(4)})
	if err != nil {
		t.Fatalf("body return activity err 应经 workflowReturnErr 通道产 CmdFailWorkflow，Execute 本身不返回 err, got %v", err)
	}
	failedCmd, msg := hasFailCmd(cmds)
	if !failedCmd {
		t.Fatalf("应 emit CmdFailWorkflow, got %+v", cmds)
	}
	if msg == "" {
		t.Fatalf("CmdFailWorkflow 应携带错误消息, got empty")
	}
}

// TC-Q6
// Error Case: replay 批历史调 activities.A，代码改调 activities.B → 换 type 漂移。
// 目的：锚定抽取块「matchReplayWithHistory 校验」通道行为不变（非确定性错误照常抛）。
// 预期：Execute 返回 nonDeterministicError。
func TestExecute_Regression_ReplayDriftDetected(t *testing.T) {
	const srcCallsB = `package flows
import (
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/activities"
)
func W(ctx workflow.Context, input map[string]any) error {
	f := workflow.ExecuteActivity(ctx, activities.B, input)
	var out map[string]any
	e := f.Get(ctx, &out)
	if e != nil {
		return e
	}
	return nil
}
`
	started := history.NewWorkflowStartedEvent(map[string]any{}, "", "", "", "", 0)
	sched := history.Event{ID: 2, Timestamp: time.Now(), ActivityScheduled: &history.ActivityScheduledEvent{ActivityID: "#a1", ActivityType: "activities.A"}}
	comp := history.Event{ID: 3, Timestamp: time.Now(), ActivityCompleted: &history.ActivityCompletedEvent{ScheduledEventID: 2, Output: map[string]any{"ok": true}}}

	_, err := probeExecFix(t, srcCallsB, []history.Event{started, sched, comp, wftc(4)})
	assertNonDeterministic(t, err, "history event is")
}

// 第 9 项：SetQueryHandler 返回 error；登记成功时为 nil，作者可以照常检查。
func TestSetQueryHandlerReturnsNilOnSuccess(t *testing.T) {
	src := `package flows
import workflow "github.com/donutnomad/tenuto/sdk/workflow"
func W(ctx workflow.Context, input map[string]any) error {
	if err := workflow.SetQueryHandler(ctx, "q", func(ctx workflow.Context, in map[string]any) map[string]any {
		return map[string]any{"ok": true}
	}); err != nil {
		return err
	}
	var stop map[string]any
	return workflow.WaitSignal(ctx, "stop").Get(ctx, &stop)
}
`
	started := history.NewWorkflowStartedEvent(map[string]any{}, "", "", "", "", 0)
	got := queryViaReplay(t, src, []history.Event{started, wftCompletedEvent(2)}, "q", map[string]any{})
	if got.replayErr != nil || got.rejected {
		t.Fatalf("query failed: replayErr=%v errMsg=%q", got.replayErr, got.errMsg)
	}
	if got.result["ok"] != true {
		t.Fatalf("query result = %#v", got.result)
	}
}

// 第 9 项：宿主在登记时校验 handler；绕过生成器的程序拿到带 query 名的错误。
func TestSetQueryHandlerHostRejectsBadRegistration(t *testing.T) {
	api := &runtimeWorkflowAPI{}
	if err := api.SetQueryHandler(workflowContext{}, "", nil); err == nil || !strings.Contains(err.Error(), "query name is empty") {
		t.Fatalf("empty name error = %v", err)
	}
	if err := api.SetQueryHandler(workflowContext{}, "stage", nil); err == nil || !strings.Contains(err.Error(), `"stage"`) || !strings.Contains(err.Error(), "handler is nil") {
		t.Fatalf("nil handler error = %v", err)
	}
	if len(api.queryHandlers) != 0 {
		t.Fatalf("rejected handlers were registered: %v", api.queryHandlers)
	}
}

// queryCounterSrc：body 设 counter=42，SetQueryHandler 返回它，随后 WaitSignal 阻塞（保持 running）。
// query "getCount" 读回内存态 counter。
const queryCounterSrc = `package flows
import workflow "github.com/donutnomad/tenuto/sdk/workflow"
func W(ctx workflow.Context, input map[string]any) error {
	counter := int64(42)
	workflow.SetQueryHandler(ctx, "getCount", func(ctx workflow.Context, in map[string]any) map[string]any {
		return map[string]any{"count": counter}
	})
	var stop map[string]any
	err := workflow.WaitSignal(ctx, "stop").Get(ctx, &stop)
	if err != nil {
		return err
	}
	return nil
}
`

// TC-Q3
// Happy Path（直击 M2）：running run 经 ReplayForQuery 重放到最后一个已完成的 Workflow Task，query
// handler 读回工作流内存态 counter=42。
// 目的：验证 query 专用重放能收敛并对内存态求值；求值全程只读、不产任何 command。
// 预期：result["count"]==42；未 rejected；replayErr==nil；commands 为空（只读现场）。
func TestReplayForQuery_ReadsMemoryState_AllReplay(t *testing.T) {
	started := history.NewWorkflowStartedEvent(map[string]any{}, "", "", "", "", 0)
	got := queryViaReplay(t, queryCounterSrc, []history.Event{started, wftCompletedEvent(2)}, "getCount", map[string]any{})

	if got.replayErr != nil {
		t.Fatalf("running run query 重放不应报错, got %v", got.replayErr)
	}
	if got.rejected {
		t.Fatalf("query 不应被拒, errMsg=%q", got.errMsg)
	}
	if got.result["count"] != int64(42) {
		t.Fatalf("query 应读回内存态 counter=42, got %v (%T)", got.result["count"], got.result["count"])
	}
	if len(got.commands) != 0 {
		t.Fatalf("query 只读现场不应产任何 command, got %+v", got.commands)
	}
}

// TC-Q2
// Edge Case（直击 M1）：terminal run（历史含 WorkflowCompleted，terminated=true）经 ReplayForQuery
// 【不走】Execute 入口的 terminated 短路——照常重放整段 body 到收敛，query 对最终内存态求值；且
// 收敛处 queryReplay 豁免不产终态命令（无幽灵 CmdComplete/CmdFail）。
// 目的：验证 M1 核心不变式——terminal run 也能被 query，且 query 路径产出的任何终态命令被丢弃。
// 预期：result["count"]==42；commands 为空（既无 ScheduleActivity 也无 CmdComplete）。
func TestReplayForQuery_TerminalRun_NoShortCircuit_NoTerminalCmd(t *testing.T) {
	// body 跑完即 return 的 workflow：counter 设好后直接 return（无阻塞），run 已 Completed。
	const src = `package flows
import workflow "github.com/donutnomad/tenuto/sdk/workflow"
func W(ctx workflow.Context, input map[string]any) error {
	counter := int64(42)
	workflow.SetQueryHandler(ctx, "getCount", func(ctx workflow.Context, in map[string]any) map[string]any {
		return map[string]any{"count": counter}
	})
	return nil
}
`
	started := history.NewWorkflowStartedEvent(map[string]any{}, "", "", "", "", 0)
	completed := history.Event{ID: 3, Timestamp: time.Now(), WorkflowCompleted: &history.WorkflowCompletedEvent{}}
	got := queryViaReplay(t, src, []history.Event{started, wftCompletedEvent(2), completed}, "getCount", map[string]any{})

	if got.replayErr != nil {
		t.Fatalf("terminal run query 不应报错, got %v", got.replayErr)
	}
	if got.result["count"] != int64(42) {
		t.Fatalf("terminal run query 应读回 counter=42（未被 terminated 短路吞掉 body 重放）, got %v", got.result["count"])
	}
	if len(got.commands) != 0 {
		t.Fatalf("query 路径 terminal 收敛不得产终态命令（M1 幽灵命令豁免）, got %+v", got.commands)
	}
}

// TC-Q4
// Error Case（直击 M2）：已完成的 Workflow Task 所在的批里，body 的 SideEffect 在历史缺对应 marker →
// isReplay 护栏判定为非确定性 divergence，ReplayForQuery 返回 nonDeterministicError（不裸 panic）。
// 目的：验证已提交的批缺 marker 即报可辨识非确定性错误（QueryWorkflow 将据此 QueryRejected）。
// 尚未被 Workflow Task 处理的末批不执行，见 query_inflight_task_test.go。
// 预期：replayErr 为 nonDeterministicError；rejected=true。
func TestReplayForQuery_MissingMarker_NonDeterministic(t *testing.T) {
	// body 头即 SideEffect；首个 Workflow Task 已完成而历史无该 marker → 缺 marker divergence。
	const src = `package flows
import workflow "github.com/donutnomad/tenuto/sdk/workflow"
func W(ctx workflow.Context, input map[string]any) error {
	workflow.SetQueryHandler(ctx, "q", func(ctx workflow.Context, in map[string]any) map[string]any {
		return map[string]any{"ok": true}
	})
	var cfg map[string]any
	err := workflow.SideEffect(ctx, func(ctx workflow.Context) any { return map[string]any{"region": "sg"} }).Get(ctx, &cfg)
	if err != nil {
		return err
	}
	return nil
}
`
	started := history.NewWorkflowStartedEvent(map[string]any{}, "", "", "", "", 0)
	got := queryViaReplay(t, src, []history.Event{started, wftCompletedEvent(2)}, "q", map[string]any{})

	if got.replayErr == nil {
		t.Fatalf("query 期缺 marker 应返回非确定性错误, got nil (rejected=%v)", got.rejected)
	}
	assertNonDeterministic(t, got.replayErr, "")
	if !got.rejected {
		t.Fatalf("非确定性 replayErr 应被判定为 rejected")
	}
}

// TC-Q7
// Edge Case（query 越过历史调新 activity）：query 期 isReplay=true，body 越过历史边界调一个历史
// 中不存在的 activity——ExecuteActivity 在 isReplay 下不 panic 而是 emitCommand(ScheduleActivity)，
// 该命令进 replayCommands 被弃（不上报）、lane 在 f.Get park；query 仍对已达内存态求值成功。
// 目的：验证 M4 实施细节——不给 ExecuteActivity 加 queryReplay 特判，越界调新 activity 由既有
// isReplay 路径承担；query 求值对 park 之前已达内存态成立，且越界命令不混入上报通道。
// 预期：query result["stage"]=="before"；commands 中不含上报的 CmdScheduleActivity（被弃）。
func TestReplayForQuery_CrossHistoryNewActivity_CommandDiscarded(t *testing.T) {
	const src = `package flows
import (
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/activities"
)
func W(ctx workflow.Context, input map[string]any) error {
	stage := "before"
	workflow.SetQueryHandler(ctx, "stage", func(ctx workflow.Context, in map[string]any) map[string]any {
		return map[string]any{"stage": stage}
	})
	f := workflow.ExecuteActivity(ctx, activities.A, input)
	var out map[string]any
	e := f.Get(ctx, &out)
	if e != nil {
		return e
	}
	stage = "after"
	return nil
}
`
	// 首个 Workflow Task 已完成而历史无任何 activity scheduled——重放越过历史调新 activity。
	started := history.NewWorkflowStartedEvent(map[string]any{}, "", "", "", "", 0)
	got := queryViaReplay(t, src, []history.Event{started, wftCompletedEvent(2)}, "stage", map[string]any{})

	if got.replayErr != nil {
		t.Fatalf("越界调新 activity 应 park 而非报错, got %v", got.replayErr)
	}
	if got.result["stage"] != "before" {
		t.Fatalf("query 应对 activity park 前已达内存态求值（stage=before）, got %v", got.result["stage"])
	}
	for _, c := range got.commands {
		if c.Type == history.CmdScheduleActivity {
			t.Fatalf("query 越界调的 activity 命令不得混入上报通道（应进 replayCommands 被弃）, got %+v", got.commands)
		}
	}
}

// TC-Q5
// Error Case（M3 双错误通道，直击 :569-585 两条 return）：验证 replayToConverged 抽取后同时保留
// execErr 与 evalErr 两条错误通道，各自都能上抛。
//   - execErr 通道：query 路径 main body 首跑 SideEffect 缺 marker → recover 设 execErr →
//     replayToConverged 第二个错误通道检查上抛 → ReplayForQuery 返回（子测试 execErr）。
//   - evalErr 通道：update handler 协程内 R 类原语（RandomUUID）replay 批缺 marker → 协程 panic
//     被收拢为 e.evalErr → replayToConverged 第一个错误通道检查上抛 → Execute 返回（子测试 evalErr）。
//
// 两通道由 Execute 与 ReplayForQuery 共享的 replayToConverged 承担，任一走通即证抽取保留了该 return。
func TestReplayToConverged_DualErrorChannels(t *testing.T) {
	// 子测试 execErr：query 路径 main body panic 经 execErr 通道上抛（非确定性）。
	t.Run("execErr_channel", func(t *testing.T) {
		const src = `package flows
import workflow "github.com/donutnomad/tenuto/sdk/workflow"
func W(ctx workflow.Context, input map[string]any) error {
	workflow.SetQueryHandler(ctx, "q", func(ctx workflow.Context, in map[string]any) map[string]any {
		return map[string]any{"ok": true}
	})
	var cfg map[string]any
	_ = workflow.SideEffect(ctx, func(ctx workflow.Context) any { return map[string]any{"region": "sg"} }).Get(ctx, &cfg)
	return nil
}
`
		started := history.NewWorkflowStartedEvent(map[string]any{}, "", "", "", "", 0)
		got := queryViaReplay(t, src, []history.Event{started, wftCompletedEvent(2)}, "q", map[string]any{})
		if got.replayErr == nil {
			t.Fatalf("main body 缺 marker 应经 execErr 通道上抛, got nil")
		}
		if !got.rejected {
			t.Fatalf("execErr 应被判定为 rejected")
		}
	})

	// 子测试 evalErr：update handler 协程内 RandomUUID replay 批缺 marker → 协程 panic → evalErr。
	// 手写 program（照 TestReplayMatch_UpdateHandler_RunSkipped），但历史【缺】RandomUUID marker。
	t.Run("evalErr_channel", func(t *testing.T) {
		handlerFn := map[string]any{
			"fn": map[string]any{
				"params": []any{
					[]any{"ctx", "workflow.Context"},
					[]any{"in", "map<string,any>"},
				},
				"returns": []any{"map<string,any>"},
				"do": []any{
					map[string]any{"let": map[string]any{"u": map[string]any{"RandomUUID": []any{
						map[string]any{"pkg": "workflow"},
						map[string]any{"var": "ctx"},
					}}}},
					map[string]any{"let": map[string]any{"out": map[string]any{"zero": "map<string,any>"}}},
					map[string]any{"return": map[string]any{"var": "out"}},
				},
			},
		}
		nodes := &program.WorkflowConfig{
			Type: "handler-uuid",
			Program: program.NewWLangProgram([]any{
				map[string]any{"expr": map[string]any{"SetUpdateHandler": []any{
					map[string]any{"pkg": "workflow"},
					map[string]any{"var": "ctx"},
					map[string]any{"literal": map[string]any{"type": "string", "value": "gen"}},
					handlerFn,
				}}},
			}),
		}
		// replay 批（acceptedEvent 后跟 wftc 使其成为已决策批）——handler 重放到 RandomUUID 缺 marker。
		started := history.NewWorkflowStartedEvent(nil, "", "", "", "", 0)
		events := []history.Event{
			started,
			acceptedEvent(2, "gen", "u-1", map[string]any{"id": "x"}),
			wftc(3),
		}
		_, err := NewWorkflowExecutor(nodes, events).Execute()
		if err == nil {
			t.Fatalf("update handler replay 批缺 RandomUUID marker 应经 evalErr 通道上抛, got nil")
		}
		assertNonDeterministic(t, err, "")
	})
}

// queryCompletedOrderSrc：顺序双 activity（Reserve→Sign）+ query handler，对标 demo OrderWorkflow。
const queryCompletedOrderSrc = `package flows
import (
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/activities"
)
func W(ctx workflow.Context, input map[string]any) error {
	stage := "start"
	workflow.SetQueryHandler(ctx, "stage", func(ctx workflow.Context, in map[string]any) map[string]any {
		return map[string]any{"stage": stage}
	})
	var r1 map[string]any
	e1 := workflow.ExecuteActivity(ctx, activities.Reserve, input).Get(ctx, &r1)
	if e1 != nil {
		return e1
	}
	stage = "reserved"
	var r2 map[string]any
	e2 := workflow.ExecuteActivity(ctx, activities.Sign, input).Get(ctx, &r2)
	if e2 != nil {
		return e2
	}
	stage = "signed"
	return nil
}
`

// TC-BUG1
// Error Case（回归复现，直击 completed-run query bug）：一个【已完成】的 run（历史含两个 activity
// 全部 completed + WorkflowCompleted 终态）经 ReplayForQuery 应把 body 重放【到终态】——两个
// ExecuteActivity.Get 都从历史拿到结果快进、stage 推进到 "signed"，协程不应 park 在 ActivityFuture.Get。
// 目的：复现并锁定「completed run 的 query 却把协程停在第一个 activity.Get park」的缺陷。
// 预期（修复后）：query 返回 stage=="signed"（body 已跑完）；__stack_trace 不含 ActivityFuture.Get park 帧。
func TestReplayForQuery_CompletedRunWithActivities_ReplaysToTerminal(t *testing.T) {
	started := history.NewWorkflowStartedEvent(map[string]any{"order_id": "o-1"}, "", "", "", "", 0)
	events := []history.Event{
		started,
		wftc(2),
		{ID: 3, Timestamp: time.Now(), ActivityScheduled: &history.ActivityScheduledEvent{ActivityID: "#a1", ActivityType: "activities.Reserve"}},
		{ID: 4, Timestamp: time.Now(), ActivityCompleted: &history.ActivityCompletedEvent{ScheduledEventID: 3, Output: map[string]any{"reservation_id": "r-1"}}},
		wftc(5),
		{ID: 6, Timestamp: time.Now(), ActivityScheduled: &history.ActivityScheduledEvent{ActivityID: "#a2", ActivityType: "activities.Sign"}},
		{ID: 7, Timestamp: time.Now(), ActivityCompleted: &history.ActivityCompletedEvent{ScheduledEventID: 6, Output: map[string]any{"signature": "s-1"}}},
		wftc(8),
		{ID: 9, Timestamp: time.Now(), WorkflowCompleted: &history.WorkflowCompletedEvent{}},
	}
	got := queryViaReplay(t, queryCompletedOrderSrc, events, "stage", map[string]any{})

	if got.replayErr != nil {
		t.Fatalf("completed run query 重放不应报错, got %v", got.replayErr)
	}
	if got.rejected {
		t.Fatalf("query 不应被拒, errMsg=%q", got.errMsg)
	}
	// 核心断言：body 应重放到终态，stage 推进到 signed（两个 activity 都从历史快进），而非停在中途。
	if got.result["stage"] != "signed" {
		t.Fatalf("completed run 的 query 应对【终态】内存求值 stage=signed（body 重放到底）, got %v", got.result["stage"])
	}
}
