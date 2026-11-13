package engine

import (
	"context"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/compiler/sdkgen"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

// ============================================
// B1 动态 Query Handler —— QueryWorkflow.evaluateQuery 端到端（service 层）
// ============================================
// 经真实 Server 注册含 SetQueryHandler 的翻译 config、建 running run、
// 写历史，再走 QueryWorkflow 就地重放求值。锚定 Layer 3 接线 + S2/S4/S5/F1/F2/M6 映射。
// 历史手动 Append（不走 worker），完全控制 run 停在何处。
// ============================================

// queryE2EEnv 搭一个含已注册翻译 config 的最小 server + runtime，并建好一个 running run。
type queryE2EEnv struct {
	server  *Server
	runtime *Server
	runID   string
}

// newQueryE2EEnv 翻译 src → 注册 config → 建 running run → 写 started 历史（run 停在首个 park 前）。
func newQueryE2EEnv(t *testing.T, src, workflowID, funcName string) *queryE2EEnv {
	t.Helper()
	mgr := memory.NewExecutionManager()
	server := assembleServer(mgr, newTestTaskQueue(), newTestTaskQueue())

	result, err := sdkgen.GenerateFromSource([]byte(src), sdkgen.Options{WorkflowID: workflowID, FuncName: funcName, Filename: "q.go"})
	if err != nil {
		t.Fatalf("translate: %v", err)
	}
	wf := result.Workflow
	ref, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, wf)
	if err != nil {
		t.Fatalf("register: %v", err)
	}

	run := &execution.Run{
		ID:                    "run-" + workflowID,
		WorkflowID:            ref.WorkflowID,
		WorkflowType:          ref.WorkflowID,
		WorkflowConfigVersion: ref.Version,
		Status:                history.StatusRunning,
		CreatedAt:             time.Now(),
	}
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(run)) }); err != nil {
		t.Fatalf("create run: %v", err)
	}
	if _, err := mgr.Append(run.ID, run.ID, run.ID, history.NewWorkflowStartedEvent(nil, "", "", run.WorkflowID, "", 0)); err != nil {
		t.Fatalf("append started: %v", err)
	}
	// query 读最后一个已完成 Workflow Task 提交的状态：补上首个 Workflow Task 的调度与完成。
	scheduledID, err := mgr.Append(run.ID, run.ID, run.ID, history.NewWorkflowTaskScheduledEvent(0))
	if err != nil {
		t.Fatalf("append workflow task scheduled: %v", err)
	}
	if _, err := mgr.Append(run.ID, run.ID, run.ID, history.NewWorkflowTaskCompletedEvent(scheduledID, "worker")); err != nil {
		t.Fatalf("append workflow task completed: %v", err)
	}
	return &queryE2EEnv{server: server, runtime: server, runID: run.ID}
}

// appendActivityScheduled 追加一条 activity scheduled 事件（供 __stack_trace 逻辑进度端到端测试
// 构造「卡在某 activity」的 running run）。返回该 scheduled 事件的 eventID（供后续 completed 关联）。
func (e *queryE2EEnv) appendActivityScheduled(t *testing.T, stepID, activityType string) int64 {
	t.Helper()
	id, err := e.server.mgr.Append(e.runID, e.runID, e.runID, history.Event{
		Timestamp:         time.Now(),
		ActivityScheduled: &history.ActivityScheduledEvent{ActivityID: stepID, ActivityType: activityType, TaskQueue: "default"},
	})
	if err != nil {
		t.Fatalf("append activity scheduled: %v", err)
	}
	return id
}

// appendActivityCompleted 追加一条 activity completed 事件（关联到给定 scheduledEventID）。
func (e *queryE2EEnv) appendActivityCompleted(t *testing.T, scheduledEventID int64) {
	t.Helper()
	if _, err := e.server.mgr.Append(e.runID, e.runID, e.runID, history.Event{
		Timestamp:         time.Now(),
		ActivityCompleted: &history.ActivityCompletedEvent{ScheduledEventID: scheduledEventID, Output: map[string]any{"ok": true}},
	}); err != nil {
		t.Fatalf("append activity completed: %v", err)
	}
}

// query 是对 runtime.QueryWorkflow 的薄封装（默认 namespace，按 runID）。
func (e *queryE2EEnv) query(t *testing.T, name string, input map[string]any) execution.WorkflowQueryResult {
	t.Helper()
	res, err := e.runtime.QueryWorkflow(context.Background(), protocol.DefaultNamespace, "", e.runID, name, input)
	if err != nil {
		t.Fatalf("QueryWorkflow(%q) 不应返回 top-level error（失败应归 QueryRejected）, got %v", name, err)
	}
	return res
}

// queryCounterE2ESrc：counter=7，query "getCount" 读回；body WaitSignal 阻塞保持 running。
const queryCounterE2ESrc = `package flows
import workflow "github.com/donutnomad/tenuto/sdk/workflow"
func QCounter(ctx workflow.Context, input map[string]any) error {
	counter := int64(7)
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

// TC-Q1
// Happy Path（端到端不产事件）：QueryWorkflow 就地重放求值 query handler，返回内存态 counter=7；
// 且求值全程只读——历史长度在 query 前后不变（query 不产事件、不落库）。
// 目的：验证 Layer 3 端到端接线 + M6 QueryResult 映射 + query 不改历史。
// 预期：QueryResult["count"]==7；QueryRejected=false；query 前后 GetEvents 长度一致。
func TestQueryWorkflow_HappyPath_NoEventsProduced(t *testing.T) {
	env := newQueryE2EEnv(t, queryCounterE2ESrc, "wf-q-happy", "QCounter")

	before, _ := env.server.GetEvents(env.runID)
	res := env.query(t, "getCount", map[string]any{})
	after, _ := env.server.GetEvents(env.runID)

	if res.QueryRejected {
		t.Fatalf("query 不应被拒, QueryError=%q", res.QueryError)
	}
	if res.QueryResult["count"] != int64(7) {
		t.Fatalf("QueryResult 应读回 counter=7, got %v", res.QueryResult["count"])
	}
	if len(after) != len(before) {
		t.Fatalf("query 不应产事件：before=%d after=%d", len(before), len(after))
	}
}

// TC-S4
// Error Case（未注册 name 显式报错，不静默）：query 一个未注册的 name → QueryRejected=true，
// QueryError 含 "unknown queryType"。__stack_trace 之外的内置名也走此路径。
// 目的：验证 S4——未注册 query 类型不静默、给可辨识拒绝原因。
// 预期：QueryRejected=true；QueryError 含 "unknown queryType: nope"。
func TestQueryWorkflow_UnknownName_Rejected(t *testing.T) {
	env := newQueryE2EEnv(t, queryCounterE2ESrc, "wf-q-unknown", "QCounter")
	res := env.query(t, "nope", map[string]any{})

	if !res.QueryRejected {
		t.Fatalf("未注册 name 应被拒")
	}
	if res.QueryError != "unknown queryType: nope" {
		t.Fatalf("QueryError 应为 'unknown queryType: nope', got %q", res.QueryError)
	}
	if res.QueryResult != nil {
		t.Fatalf("被拒时 QueryResult 应为 nil, got %v", res.QueryResult)
	}
}

// TC-F1
// Error Case（handler 业务级失败）：query handler 返回含非空 "__query_error" 的 map →
// QueryRejected=true，QueryError=该值，不经 emitCommand、不落历史。
// 目的：验证 F1 + M6——handler 约定失败键映射到 QueryRejected/QueryError。
// 预期：QueryRejected=true；QueryError=="forbidden"；历史无新增。
func TestQueryWorkflow_HandlerQueryError_Rejected(t *testing.T) {
	const src = `package flows
import workflow "github.com/donutnomad/tenuto/sdk/workflow"
func QReject(ctx workflow.Context, input map[string]any) error {
	workflow.SetQueryHandler(ctx, "guard", func(ctx workflow.Context, in map[string]any) map[string]any {
		return map[string]any{"__query_error": "forbidden"}
	})
	var stop map[string]any
	err := workflow.WaitSignal(ctx, "stop").Get(ctx, &stop)
	if err != nil {
		return err
	}
	return nil
}
`
	env := newQueryE2EEnv(t, src, "wf-q-reject", "QReject")
	before, _ := env.server.GetEvents(env.runID)
	res := env.query(t, "guard", map[string]any{})
	after, _ := env.server.GetEvents(env.runID)

	if !res.QueryRejected {
		t.Fatalf("handler __query_error 应判定 rejected")
	}
	if res.QueryError != "forbidden" {
		t.Fatalf("QueryError 应为 handler 返回的 'forbidden', got %q", res.QueryError)
	}
	if len(after) != len(before) {
		t.Fatalf("query 失败不应落历史：before=%d after=%d", len(before), len(after))
	}
}

// TC-F2
// Edge Case：query handler 返回含 "__error" 但【无】"__query_error" 的 map → 不触发 QueryRejected，
// "__error" 作为普通字段进 QueryResult。
// 目的：验证只有 queryHandlerFailureKey("__query_error") 是 query 失败键。
// 预期：QueryRejected=false；QueryResult["__error"]=="ignored"。
func TestQueryWorkflow_ErrorKeyIsolation_NotRejected(t *testing.T) {
	const src = `package flows
import workflow "github.com/donutnomad/tenuto/sdk/workflow"
func QIsolate(ctx workflow.Context, input map[string]any) error {
	workflow.SetQueryHandler(ctx, "q", func(ctx workflow.Context, in map[string]any) map[string]any {
		return map[string]any{"__error": "ignored", "ok": true}
	})
	var stop map[string]any
	err := workflow.WaitSignal(ctx, "stop").Get(ctx, &stop)
	if err != nil {
		return err
	}
	return nil
}
`
	env := newQueryE2EEnv(t, src, "wf-q-isolate", "QIsolate")
	res := env.query(t, "q", map[string]any{})

	if res.QueryRejected {
		t.Fatalf("__error（非 query 失败键）不应触发 QueryRejected, QueryError=%q", res.QueryError)
	}
	if res.QueryResult["__error"] != "ignored" {
		t.Fatalf("__error 应作为普通字段保留在 QueryResult, got %v", res.QueryResult["__error"])
	}
	if res.QueryResult["ok"] != true {
		t.Fatalf("QueryResult 应含 ok=true, got %v", res.QueryResult)
	}
}

// TC-S5
// Edge Case（旧产物兼容）：workflow 未注册任何 SetQueryHandler（旧翻译产物）——query 任意用户
// name → QueryRejected（走 S4 unknown 路径）；而内置 __stack_trace 仍可用（不查 queryHandlers）。
// 目的：验证 S5——无 query handler 的旧 run 被 query 不崩、给可辨识拒绝；内置栈查询向后可用。
// 预期：用户 name → QueryRejected + "unknown queryType"；__stack_trace → 不 rejected + 含 stack 键。
func TestQueryWorkflow_LegacyNoHandler_S4AndStackTrace(t *testing.T) {
	// 不含 SetQueryHandler 的 workflow：body 仅 WaitSignal 阻塞。
	const src = `package flows
import workflow "github.com/donutnomad/tenuto/sdk/workflow"
func QLegacy(ctx workflow.Context, input map[string]any) error {
	var stop map[string]any
	err := workflow.WaitSignal(ctx, "stop").Get(ctx, &stop)
	if err != nil {
		return err
	}
	return nil
}
`
	env := newQueryE2EEnv(t, src, "wf-q-legacy", "QLegacy")

	// 用户 name 未注册 → S4 拒绝。
	res := env.query(t, "anything", map[string]any{})
	if !res.QueryRejected || res.QueryError != "unknown queryType: anything" {
		t.Fatalf("旧产物 query 用户 name 应走 S4 拒绝, got rejected=%v err=%q", res.QueryRejected, res.QueryError)
	}

	// 内置 __stack_trace 仍可用（即便无 SetQueryHandler）：返回 L1/L2 逻辑进度（summary/steps/waiting）。
	st := env.query(t, "__stack_trace", map[string]any{})
	if st.QueryRejected {
		t.Fatalf("__stack_trace 内置路径不应被拒, QueryError=%q", st.QueryError)
	}
	if _, ok := st.QueryResult["summary"]; !ok {
		t.Fatalf("__stack_trace 应返回含 summary 键的逻辑进度 map, got %v", st.QueryResult)
	}
}

// TC-S2
// Error Case（blocked/非确定性 run）：query 一个「重放会复现非确定性错误」的 run——body 头即
// SideEffect（R 类），running 历史无对应 marker，ReplayForQuery 全程 isReplay=true 缺 marker →
// 返回非确定性 err → QueryRejected=true + 可辨识 QueryError（不裸 panic、不返回 top-level 500）。
// 目的：验证 S2——重放复现非确定性时 query 归一到 QueryRejected，不崩溃、给可辨识原因。
// 预期：QueryWorkflow 无 top-level error；QueryRejected=true；QueryError 含 "query replay failed"。
func TestQueryWorkflow_NonDeterministicReplay_Rejected(t *testing.T) {
	const src = `package flows
import workflow "github.com/donutnomad/tenuto/sdk/workflow"
func QNonDet(ctx workflow.Context, input map[string]any) error {
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
	env := newQueryE2EEnv(t, src, "wf-q-nondet", "QNonDet")
	res := env.query(t, "q", map[string]any{}) // env.query 已断言无 top-level error

	if !res.QueryRejected {
		t.Fatalf("重放复现非确定性应归 QueryRejected")
	}
	if res.QueryError == "" {
		t.Fatalf("QueryError 应给可辨识原因, got empty")
	}
	if !contains(res.QueryError, "query replay failed") {
		t.Fatalf("QueryError 应标注 replay 失败, got %q", res.QueryError)
	}
}

// TC-ST1
// Happy Path（__stack_trace 逻辑位置端到端，卡在 activity）：running run 停在一个 activity 上，
// query __stack_trace 返回 L1 waiting（指向该 activity）+ L2 steps（进度序列）+ 可读 summary。
// 目的：验证选项 2 第一步端到端——__stack_trace 返回 workflow 逻辑位置（而非无用的 Go 栈）。
// 预期：不 rejected；waiting.activity_id 指向 pending activity；steps 非空；summary 描述卡在该 activity。
func TestQueryWorkflow_StackTrace_LogicalPosition(t *testing.T) {
	env := newQueryE2EEnv(t, queryCounterE2ESrc, "wf-q-stack", "QCounter")
	// 构造「卡在 activity」：第 1 个 activity 已完成，第 2 个 scheduled 未完成。
	id1 := env.appendActivityScheduled(t, "#a1", "activities.Reserve")
	env.appendActivityCompleted(t, id1)
	env.appendActivityScheduled(t, "#a2", "activities.Sign")

	res := env.query(t, "__stack_trace", map[string]any{})
	if res.QueryRejected {
		t.Fatalf("__stack_trace 不应被拒, QueryError=%q", res.QueryError)
	}
	waiting, _ := res.QueryResult["waiting"].(map[string]any)
	if waiting["activity_id"] != "#a2" || waiting["activity_type"] != "activities.Sign" {
		t.Fatalf("waiting 应指向当前 pending activity #a2/activities.Sign, got %v", waiting)
	}
	steps, ok := res.QueryResult["steps"].([]map[string]any)
	if !ok || len(steps) != 2 {
		t.Fatalf("steps 应含 2 条（#a1 completed + #a2 running）, got %v", res.QueryResult["steps"])
	}
	summary, _ := res.QueryResult["summary"].(string)
	if !contains(summary, "activities.Sign") {
		t.Fatalf("summary 应描述卡在 activities.Sign, got %q", summary)
	}
}

// TC-ST4
// Edge Case（__stack_trace 逻辑进度多步序列）：run 连续调度多个 activity（前几个完成、最后一个
// 进行中），query __stack_trace 的 steps 应按顺序反映每步状态（completed/running）。
// 目的：验证 L2 进度序列端到端——多 activity 的执行轨迹被正确逐条呈现。
// 预期：steps 长度 3；前 2 条 completed、第 3 条 running；waiting 指向第 3 条。
func TestQueryWorkflow_StackTrace_ProgressSequence(t *testing.T) {
	env := newQueryE2EEnv(t, queryCounterE2ESrc, "wf-q-seq", "QCounter")
	id1 := env.appendActivityScheduled(t, "#a1", "activities.Reserve")
	env.appendActivityCompleted(t, id1)
	id2 := env.appendActivityScheduled(t, "#a2", "activities.Notify")
	env.appendActivityCompleted(t, id2)
	env.appendActivityScheduled(t, "#a3", "activities.Sign")

	res := env.query(t, "__stack_trace", map[string]any{})
	if res.QueryRejected {
		t.Fatalf("__stack_trace 不应被拒, QueryError=%q", res.QueryError)
	}
	steps, ok := res.QueryResult["steps"].([]map[string]any)
	if !ok || len(steps) != 3 {
		t.Fatalf("steps 应含 3 条, got %v", res.QueryResult["steps"])
	}
	if steps[0]["status"] != "completed" || steps[1]["status"] != "completed" || steps[2]["status"] != "running" {
		t.Fatalf("steps 状态应为 [completed, completed, running], got %v", steps)
	}
	waiting, _ := res.QueryResult["waiting"].(map[string]any)
	if waiting["activity_id"] != "#a3" {
		t.Fatalf("waiting 应指向 #a3, got %v", waiting)
	}
}

// ============================================
// __stack_trace 逻辑进度（L1 current_waiting + L2 已执行 step 序列）—— 核心单元
// ============================================
// 选项 2 第一步：__stack_trace 从「Go 解释器栈」改为「workflow 逻辑位置」，纯从事件历史推导
// （不调 ReplayForQuery/不抓 Go 栈）。
// stackTraceProgressFromHistory(events) 返回：
//   waiting: L1 当前阻塞点（type/activity_id/activity_type），无 pending 时为空
//   steps:   L2 已调度 activity 的进度序列（activity_id/activity_type/status）
//   summary: 人类可读摘要
// ============================================

// TC-ST-L1A
// Happy Path（running on activity）：run 停在第 2 个 activity（第 1 个已完成）→ waiting 指向
// step2、steps 含两条（step1=completed, step2=running）、summary 描述卡在 step2。
// 目的：验证 L1/L2 从事件历史正确推导「卡在哪个 activity + 走过哪些 step」。
// 预期：waiting.activity_id=#a2；steps 长度 2；step1 completed、step2 running。
func TestStackTraceProgress_RunningOnActivity(t *testing.T) {
	events := []history.Event{
		history.NewWorkflowStartedEvent(map[string]any{}, "", "", "", "", 0),
		wftc(2),
		{ID: 3, Timestamp: time.Now(), ActivityScheduled: &history.ActivityScheduledEvent{ActivityID: "#a1", ActivityType: "activities.Reserve"}},
		{ID: 4, Timestamp: time.Now(), ActivityCompleted: &history.ActivityCompletedEvent{ScheduledEventID: 3, Output: map[string]any{"ok": true}}},
		wftc(5),
		{ID: 6, Timestamp: time.Now(), ActivityScheduled: &history.ActivityScheduledEvent{ActivityID: "#a2", ActivityType: "activities.Sign"}},
	}
	got := stackTraceProgress(t, events)

	waiting, _ := got["waiting"].(map[string]any)
	if waiting["activity_id"] != "#a2" {
		t.Fatalf("waiting 应指向当前未完成的 #a2, got %v", waiting)
	}
	if waiting["activity_type"] != "activities.Sign" {
		t.Fatalf("waiting.activity_type 应为 activities.Sign, got %v", waiting["activity_type"])
	}
	steps, _ := got["steps"].([]map[string]any)
	if len(steps) != 2 {
		t.Fatalf("steps 应含 2 条（#a1 完成 + #a2 进行中）, got %d: %v", len(steps), got["steps"])
	}
	if steps[0]["activity_id"] != "#a1" || steps[0]["status"] != "completed" {
		t.Fatalf("step1 应为 #a1 completed, got %v", steps[0])
	}
	if steps[1]["activity_id"] != "#a2" || steps[1]["status"] != "running" {
		t.Fatalf("step2 应为 #a2 running, got %v", steps[1])
	}
	if got["summary"] == "" || got["summary"] == nil {
		t.Fatalf("summary 不应为空")
	}
}

// TC-ST-L1B
// Edge Case（completed run）：run 已完成（所有 activity completed + WorkflowCompleted）→ waiting
// 为空、steps 全 completed、summary 表明已完成。
// 目的：验证已完成 run 的逻辑进度——无阻塞点，轨迹全绿（对齐 Temporal completed 无 pending）。
// 预期：waiting 为空 map；steps 全 completed。
func TestStackTraceProgress_CompletedRun(t *testing.T) {
	events := []history.Event{
		history.NewWorkflowStartedEvent(map[string]any{}, "", "", "", "", 0),
		wftc(2),
		{ID: 3, Timestamp: time.Now(), ActivityScheduled: &history.ActivityScheduledEvent{ActivityID: "#a1", ActivityType: "activities.Reserve"}},
		{ID: 4, Timestamp: time.Now(), ActivityCompleted: &history.ActivityCompletedEvent{ScheduledEventID: 3, Output: map[string]any{"ok": true}}},
		wftc(5),
		{ID: 6, Timestamp: time.Now(), WorkflowCompleted: &history.WorkflowCompletedEvent{}},
	}
	got := stackTraceProgress(t, events)

	waiting, _ := got["waiting"].(map[string]any)
	if len(waiting) != 0 {
		t.Fatalf("completed run 不应有 waiting, got %v", waiting)
	}
	steps, _ := got["steps"].([]map[string]any)
	if len(steps) != 1 || steps[0]["status"] != "completed" {
		t.Fatalf("completed run 的 steps 应全 completed, got %v", got["steps"])
	}
}

// TC-ST-L1C
// Edge Case（no pending activity，如卡在 WaitSignal）：run 卡在纯代码 park（信号等待），历史无
// pending activity/timer/child → waiting 为空、steps 为空、summary 诚实说明「无 pending activity」。
// 目的：验证 L1/L2 对「事件历史推不出位置」的场景不编造，诚实降级（不自相矛盾）。
// 预期：waiting 空、steps 空、summary 非空且不谎称卡在某 activity。
func TestStackTraceProgress_NoPendingActivity(t *testing.T) {
	events := []history.Event{
		history.NewWorkflowStartedEvent(map[string]any{}, "", "", "", "", 0),
		wftc(2),
		// WFT 完成后无任何 activity/timer/child pending（run 卡在 WaitSignal 这种纯代码 park）。
	}
	got := stackTraceProgress(t, events)

	waiting, _ := got["waiting"].(map[string]any)
	if len(waiting) != 0 {
		t.Fatalf("无 pending activity 时 waiting 应为空, got %v", waiting)
	}
	steps, _ := got["steps"].([]map[string]any)
	if len(steps) != 0 {
		t.Fatalf("无 activity 调度时 steps 应为空, got %v", got["steps"])
	}
	if got["summary"] == "" || got["summary"] == nil {
		t.Fatalf("summary 应非空（诚实说明无 pending）")
	}
}

// stackTraceProgress 用 execution state 的折叠加 History 计算逻辑执行位置，与 __stack_trace 查询同一路径。
func stackTraceProgress(t *testing.T, events []history.Event) map[string]any {
	t.Helper()
	state, err := execution.BuildWorkflowExecutionState(&execution.Run{ID: "run", Status: history.StatusRunning}, events)
	if err != nil {
		t.Fatal(err)
	}
	return execution.StackTraceProgress(state, events)
}
