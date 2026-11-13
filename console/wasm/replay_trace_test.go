package main

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/donutnomad/tenuto/program"
	wlangtypes "github.com/donutnomad/wlang/types"
)

// ============================================
// buildTrace 测试 (M1/M2/M3: laneID + 前沿 + fan-out)
// ============================================
// 测试策略:
// - completed 单 lane: 全部完成,RunKind=completed,main lane EndKind=completed。
// - running 前沿: activity scheduled 未 completed → main lane pending-frontier,
//   WaitingActivity 指向正确 activityID/type,RunKind=running。
// - foreach fan-out (N≥3): 每个并发 routine 一个独立 laneID(RoutineID),
//   Vars 快照互不污染。
// - lane 首句 pending: 首句即 activity 且 pending,lane 仍出现在 Lanes。
// - error 态: activity failed(真实 error) → EndKind=error,不误判成 pending。
// ============================================

// progFanout3: 三个 body-form routine 并发,各自算一个独立的 local 变量,
// 主线 await 全部三个 handle(保证确定性: 主线阻塞直到 routine 完成)。
// 每个 routine body 是相同结构,唯一区分靠 RoutineID(→ laneID)。
const progFanout3 = `{
  "schema":"wlang/program/v2",
  "lang":"wflang/v1",
  "handler_abi_schema":"workflow/update-handler-abi/v1",
  "program":[
    {"let":{"h1":{"routine":{"do":[
      {"let":{"local":{"literal":{"type":"string","value":"a"}}}},
      {"return":{"var":"local"}}
    ]}}}},
    {"let":{"h2":{"routine":{"do":[
      {"let":{"local":{"literal":{"type":"string","value":"b"}}}},
      {"return":{"var":"local"}}
    ]}}}},
    {"let":{"h3":{"routine":{"do":[
      {"let":{"local":{"literal":{"type":"string","value":"c"}}}},
      {"return":{"var":"local"}}
    ]}}}},
    {"let":{"r1":{"await":{"var":"h1"}}}},
    {"let":{"r2":{"await":{"var":"h2"}}}},
    {"let":{"r3":{"await":{"var":"h3"}}}},
    {"return":{"var":"r1"}}
  ]
}`

func mustTrace(t *testing.T, programJSON, eventsJSON string) traceResult {
	t.Helper()
	res, err := buildTrace([]byte(programJSON), []byte(eventsJSON))
	if err != nil {
		t.Fatalf("buildTrace: %v", err)
	}
	return res
}

func TestBuildTracePreservesProgramArtifactV2Descriptors(t *testing.T) {
	descriptor := wlangtypes.TypeDescriptor{
		ID:           "example.Result",
		LanguageName: "example.Result",
		Kind:         wlangtypes.TypeKindRecord,
	}
	descriptor.Fingerprint = wlangtypes.DescriptorFingerprint(descriptor)
	artifact := program.WLangProgram{
		Schema: program.WLangProgramSchemaV2,
		Lang:   program.WLangLanguageV1,
		Program: []any{
			map[string]any{"let": map[string]any{"result": map[string]any{"zero": "example.Result"}}},
			map[string]any{"return": map[string]any{"literal": map[string]any{"type": "null", "value": nil}}},
		},
		Types:            map[string]program.WLangTypeDescriptor{descriptor.ID: descriptor},
		HandlerABISchema: program.WLangHandlerABISchemaV1,
	}
	raw, err := json.Marshal(artifact)
	if err != nil {
		t.Fatal(err)
	}
	trace, err := buildTrace(raw, []byte(`[]`))
	if err != nil {
		t.Fatal(err)
	}
	if trace.Error != "" {
		t.Fatalf("typed artifact replay failed: %s", trace.Error)
	}
}

func laneByID(res traceResult, id string) *laneInfo {
	for i := range res.Lanes {
		if res.Lanes[i].LaneID == id {
			return &res.Lanes[i]
		}
	}
	return nil
}

func hostWaitGroupProgram(t *testing.T) string {
	t.Helper()
	lane := func(activity, local string) map[string]any {
		execute := map[string]any{"ExecuteActivity": []any{
			map[string]any{"pkg": "workflow"},
			map[string]any{"var": "ctx"},
			map[string]any{"literal": map[string]any{"type": "string", "value": activity}},
			map[string]any{"var": "input"},
		}}
		get := map[string]any{"Get": []any{
			execute,
			map[string]any{"var": "ctx"},
			map[string]any{"out": "out"},
		}}
		return map[string]any{"fn": map[string]any{
			"params":  []any{[]any{"ctx", "workflow.Context"}},
			"returns": []any{},
			"do": []any{
				map[string]any{"let": map[string]any{"local": map[string]any{"literal": map[string]any{"type": "string", "value": local}}}},
				map[string]any{"let": map[string]any{"out": map[string]any{"zero": "map<string,any>"}}},
				map[string]any{"let": map[string]any{"err": get}},
			},
		}}
	}
	program := []any{
		map[string]any{"let": map[string]any{"wg": map[string]any{"NewWaitGroup": []any{
			map[string]any{"pkg": "workflow"},
			map[string]any{"var": "ctx"},
		}}}},
	}
	for _, item := range []map[string]any{
		lane("balance-a", "a"),
		lane("balance-b", "b"),
		lane("balance-c", "c"),
	} {
		program = append(program, map[string]any{"expr": map[string]any{"Go": []any{
			map[string]any{"var": "wg"},
			map[string]any{"var": "ctx"},
			item,
		}}})
	}
	program = append(program,
		map[string]any{"expr": map[string]any{"Wait": []any{
			map[string]any{"var": "wg"},
			map[string]any{"var": "ctx"},
		}}},
		map[string]any{"return": map[string]any{"literal": map[string]any{"type": "null", "value": nil}}},
	)
	raw, err := json.Marshal(map[string]any{"schema": "wlang/program/v2", "lang": "wflang/v1", "handler_abi_schema": "workflow/update-handler-abi/v1", "program": program})
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

const hostWaitGroupEvents = `[
  {"id":1,"workflowStarted":{"input":{"account":"demo"}}},
  {"id":2,"activityScheduled":{"activityId":"/0#a1","activityType":"balance-a"}},
  {"id":3,"activityScheduled":{"activityId":"/1#a1","activityType":"balance-b"}},
  {"id":4,"activityScheduled":{"activityId":"/2#a1","activityType":"balance-c"}},
  {"id":5,"activityCompleted":{"scheduledEventId":2,"output":{"balance":10}}},
  {"id":6,"activityCompleted":{"scheduledEventId":3,"output":{"balance":20}}},
  {"id":7,"activityCompleted":{"scheduledEventId":4,"output":{"balance":30}}},
  {"id":8,"workflowCompleted":{"output":{}}}
]`

func TestBuildTrace_HostWaitGroupFanoutUsesExecutorLanes(t *testing.T) {
	program := hostWaitGroupProgram(t)
	res := mustTrace(t, program, hostWaitGroupEvents)
	if res.Error != "" {
		t.Fatalf("trace error = %q", res.Error)
	}
	routineLanes := 0
	for _, lane := range res.Lanes {
		if lane.LaneKind == "routine" {
			routineLanes++
		}
	}
	if routineLanes != 3 {
		t.Fatalf("routine lanes = %d, want 3: %+v", routineLanes, res.Lanes)
	}
	if len(res.Activities) != 3 {
		t.Fatalf("activities = %d, want 3: %+v", len(res.Activities), res.Activities)
	}
	linesByLane := map[string]map[int]bool{}
	for _, traceStep := range res.Steps {
		if traceStep.LaneID == "main" {
			continue
		}
		if linesByLane[traceStep.LaneID] == nil {
			linesByLane[traceStep.LaneID] = map[int]bool{}
		}
		linesByLane[traceStep.LaneID][traceStep.Line] = true
	}
	for laneID, lines := range linesByLane {
		if len(lines) < 3 {
			t.Fatalf("%s internal steps collapsed to source lines %v", laneID, lines)
		}
	}
}

func TestBuildTrace_ClosureLaneShowsOnlyActiveFrameVariables(t *testing.T) {
	// Edge Case: coroutine closures can read captured workflow variables while
	// their debugger snapshot contains only parameters and closure-local vars.
	res := mustTrace(t, hostWaitGroupProgram(t), hostWaitGroupEvents)
	foundLocal := false
	for _, step := range res.Steps {
		if step.LaneID == "main" {
			continue
		}
		if _, ok := step.Vars["local"]; !ok {
			continue
		}
		foundLocal = true
		if len(step.Frames) < 2 {
			t.Fatalf("%s stack = %#v, want closure and main frames", step.LaneID, step.Frames)
		}
		if step.Frames[0].Name != "closure" || step.Frames[1].Name != "main" {
			t.Fatalf("%s stack order = %#v", step.LaneID, step.Frames)
		}
		for _, captured := range []string{"input", "wg"} {
			if _, ok := step.Vars[captured]; ok {
				t.Fatalf("%s leaked into %s closure frame: %#v", captured, step.LaneID, step.Vars)
			}
		}
	}
	if !foundLocal {
		t.Fatal("closure-local variable snapshot missing")
	}
}

func TestStepTo_HostWaitGroupFanoutCompletes(t *testing.T) {
	program := hostWaitGroupProgram(t)
	h := mustHandle(t, program, hostWaitGroupEvents)
	state := h.stepTo(8)
	if errText, _ := state["error"].(string); errText != "" {
		t.Fatalf("debugger error = %q", errText)
	}
	activities, _ := state["activities"].([]any)
	if len(activities) != 3 {
		t.Fatalf("activities = %d, want 3", len(activities))
	}
}

func TestBuildTrace_ChildWorkflowEffectsCarryReplayAnchors(t *testing.T) {
	program := `{
  "schema":"wlang/program/v2",
  "lang":"wflang/v1",
  "handler_abi_schema":"workflow/update-handler-abi/v1",
  "program":[
    {"let":{"child":{"ExecuteChildWorkflow":[
      {"pkg":"workflow"},
      {"var":"ctx"},
      {"literal":{"type":"string","value":"child-flow"}},
      {"var":"input"}
    ]}}},
    {"let":{"out":{"zero":"map<string,any>"}}},
    {"let":{"err":{"Get":[
      {"var":"child"},
      {"var":"ctx"},
      {"out":"out"}
    ]}}},
    {"return":{"var":"err"}}
  ]
}`
	events := `[
  {"id":1,"workflowStarted":{"input":{"order_id":"o-1"}}},
  {"id":2,"childWorkflowStarted":{"name":"child-flow","childId":"#c1","childRunId":"child-run-1","workflowConfigVersion":"v1","input":{"order_id":"o-1"}}},
  {"id":3,"childWorkflowCompleted":{"childId":"#c1","childRunId":"child-run-1","scheduledEventId":2,"output":{"ok":true}}},
  {"id":4,"workflowCompleted":{"output":{}}}
]`

	res := mustTrace(t, program, events)
	var effects []traceEffect
	for _, step := range res.Steps {
		effects = append(effects, step.Effects...)
	}
	phaseEffects := make(map[string]traceEffect)
	for _, effect := range effects {
		if effect.Kind == "child-workflow" {
			phaseEffects[effect.Phase] = effect
		}
	}
	for _, phase := range []string{"scheduled", "awaited", "resolved"} {
		effect, ok := phaseEffects[phase]
		if !ok {
			t.Fatalf("child effect phase %q missing: %+v", phase, effects)
		}
		if effect.InvocationID != "child-workflow:#c1" || effect.ChildID != "#c1" || effect.ChildRunID != "child-run-1" {
			t.Fatalf("child effect phase %q metadata = %+v", phase, effect)
		}
		if effect.ScheduledEventID != 2 || effect.Status != "completed" {
			t.Fatalf("child effect phase %q state = %+v", phase, effect)
		}
		if effect.CallSeq == nil || effect.WaitSeq == nil || effect.ResumeSeq == nil {
			t.Fatalf("child effect phase %q anchors = %+v", phase, effect)
		}
	}
}

// TestBuildTrace_CompletedSingleLane
// Happy Path: 纯 let/return 程序完成 → 单 main lane,RunKind=completed。
func TestBuildTrace_CompletedSingleLane(t *testing.T) {
	events := `[{"id":1,"workflowStarted":{"input":{}}},{"id":2,"workflowCompleted":{"output":{}}}]`
	res := mustTrace(t, progLetReturn, events)

	if res.RunKind != "completed" {
		t.Fatalf("RunKind = %q, want completed", res.RunKind)
	}
	if len(res.Lanes) != 1 {
		t.Fatalf("expected 1 lane, got %d: %+v", len(res.Lanes), res.Lanes)
	}
	main := laneByID(res, "main")
	if main == nil {
		t.Fatalf("main lane missing")
	}
	if main.EndKind != "completed" {
		t.Fatalf("main EndKind = %q, want completed", main.EndKind)
	}
	if main.LaneKind != "main" {
		t.Fatalf("main LaneKind = %q, want main", main.LaneKind)
	}
	if res.TotalSteps == 0 || len(res.Steps) != res.TotalSteps {
		t.Fatalf("TotalSteps=%d steps=%d mismatch", res.TotalSteps, len(res.Steps))
	}
}

// TestBuildTrace_RunningPendingFrontier
// Edge Case: activity scheduled 未 completed → main lane pending-frontier,
// WaitingActivity 指向 #a1/greet,RunKind=running,Error 空。
func TestBuildTrace_RunningPendingFrontier(t *testing.T) {
	events := `[
	  {"id":1,"workflowStarted":{"input":{"name":"bob"}}},
	  {"id":2,"activityScheduled":{"activityId":"#a1","activityType":"greet"}}
	]`
	res := mustTrace(t, progActivity, events)

	if res.RunKind != "running" {
		t.Fatalf("RunKind = %q, want running", res.RunKind)
	}
	if res.Error != "" {
		t.Fatalf("pending must not surface as Error, got %q", res.Error)
	}
	main := laneByID(res, "main")
	if main == nil {
		t.Fatalf("main lane missing")
	}
	if main.EndKind != "pending-frontier" || !main.PendingFrontier {
		t.Fatalf("main EndKind=%q pending=%v, want pending-frontier/true", main.EndKind, main.PendingFrontier)
	}
	if main.WaitingActivity == nil {
		t.Fatalf("WaitingActivity nil at frontier")
	}
	if main.WaitingActivity.ActivityID != "#a1" {
		t.Fatalf("WaitingActivity.ActivityID = %q, want #a1", main.WaitingActivity.ActivityID)
	}
	if main.WaitingActivity.Type != "greet" {
		t.Fatalf("WaitingActivity.Type = %q, want greet", main.WaitingActivity.Type)
	}
	if main.WaitingActivity.Kind != "activity" {
		t.Fatalf("WaitingActivity.Kind = %q, want activity", main.WaitingActivity.Kind)
	}
	// 前沿 lane 必须真实出现且有步数(不能变成消失或看似正常的短 lane)。
	if main.StepCount == 0 {
		t.Fatalf("pending lane must have recorded steps, got StepCount=0")
	}
	// EndLine 应停在 activity 语句(program[1] → line 1),即前沿处而非末行。
	if main.EndLine != 1 {
		t.Fatalf("pending frontier EndLine = %d, want 1 (activity stmt)", main.EndLine)
	}
}

// TestBuildTrace_PendingLaneVisibleWithFewSteps
// 关键正确性: 首句附近就 pending 的 lane,step 很少,但仍必须出现在 Lanes 且
// EndKind=pending-frontier(绝不能因步数少而被误判为 completed 或消失)。
func TestBuildTrace_PendingLaneVisibleWithFewSteps(t *testing.T) {
	events := `[
	  {"id":1,"workflowStarted":{"input":{"name":"bob"}}},
	  {"id":2,"activityScheduled":{"activityId":"#a1","activityType":"greet"}}
	]`
	res := mustTrace(t, progActivity, events)

	if len(res.Lanes) != 1 {
		t.Fatalf("expected the single pending lane to be present, got %d lanes", len(res.Lanes))
	}
	main := res.Lanes[0]
	if main.EndKind != "pending-frontier" {
		t.Fatalf("short pending lane misclassified as %q", main.EndKind)
	}
}

// TestBuildTrace_CompletedWithActivity
// Happy Path: activity completed → main lane completed,activities 记录 completed。
func TestBuildTrace_CompletedWithActivity(t *testing.T) {
	events := `[
	  {"id":1,"workflowStarted":{"input":{"name":"bob"}}},
	  {"id":2,"activityScheduled":{"activityId":"#a1","activityType":"greet"}},
	  {"id":3,"activityCompleted":{"scheduledEventId":2,"output":{"msg":"hi bob"}}},
	  {"id":4,"workflowCompleted":{"output":{"msg":"hi bob"}}}
	]`
	res := mustTrace(t, progActivity, events)

	if res.RunKind != "completed" {
		t.Fatalf("RunKind = %q, want completed", res.RunKind)
	}
	main := laneByID(res, "main")
	if main == nil || main.EndKind != "completed" {
		t.Fatalf("main lane = %+v, want completed", main)
	}
	if len(res.Activities) != 1 || res.Activities[0].Status != "completed" {
		t.Fatalf("activities = %+v, want 1 completed", res.Activities)
	}
}

// TestBuildTrace_ActivityFailedNotPending
// Error Case(不误判): activity failed 但 workflow 代码用 `let err = ...Get` 捕获,
// workflow 仍跑完。断言: 该 lane 不是 pending-frontier(failed ≠ pending)。
func TestBuildTrace_ActivityFailedNotPending(t *testing.T) {
	events := `[
	  {"id":1,"workflowStarted":{"input":{"name":"bob"}}},
	  {"id":2,"activityScheduled":{"activityId":"#a1","activityType":"greet"}},
	  {"id":3,"activityFailed":{"scheduledEventId":2,"error":"boom","failure":{"message":"boom"}}}
	]`
	res := mustTrace(t, progActivity, events)

	main := laneByID(res, "main")
	if main == nil {
		t.Fatalf("main lane missing")
	}
	if main.PendingFrontier {
		t.Fatalf("activity failure must not be a pending frontier: %+v", main)
	}
	// activity 失败被代码捕获,workflow 跑完 → completed(非 error/非 pending)。
	if main.EndKind == "pending-frontier" {
		t.Fatalf("main EndKind must not be pending-frontier for handled failure")
	}
	if res.Activities[0].Status != "failed" {
		t.Fatalf("activity status = %q, want failed", res.Activities[0].Status)
	}
}

// TestBuildTrace_Fanout3Lanes
// 关键(M2): 三个并发 routine 各得一个独立 laneID(RoutineID),互不相同;
// 各 routine 的 local 变量快照互不污染(a/b/c 分离)。
func TestBuildTrace_Fanout3Lanes(t *testing.T) {
	events := `[{"id":1,"workflowStarted":{"input":{}}},{"id":2,"workflowCompleted":{"output":{}}}]`
	res := mustTrace(t, progFanout3, events)

	// 1 main + 3 routine lanes。
	routineLanes := 0
	laneIDs := map[string]bool{}
	for _, l := range res.Lanes {
		if laneIDs[l.LaneID] {
			t.Fatalf("duplicate laneID %q", l.LaneID)
		}
		laneIDs[l.LaneID] = true
		if l.LaneKind == "routine" {
			routineLanes++
		}
	}
	if routineLanes != 3 {
		t.Fatalf("expected 3 routine lanes, got %d: %+v", routineLanes, res.Lanes)
	}
	if !laneIDs["main"] {
		t.Fatalf("main lane missing: %+v", res.Lanes)
	}

	// 收集每条 routine lane 的 local 值,应为 {a,b,c} 三个不同值(不污染)。
	localsByLane := map[string]string{}
	for _, s := range res.Steps {
		if s.LaneID == "main" {
			continue
		}
		if v, ok := s.Vars["local"]; ok {
			if str, ok := v.(string); ok {
				localsByLane[s.LaneID] = str
			}
		}
	}
	if len(localsByLane) != 3 {
		t.Fatalf("expected 3 routine locals, got %d: %v", len(localsByLane), localsByLane)
	}
	seen := map[string]bool{}
	for lane, val := range localsByLane {
		if seen[val] {
			t.Fatalf("lane %s local value %q polluted (duplicated across lanes)", lane, val)
		}
		seen[val] = true
	}
	for _, want := range []string{"a", "b", "c"} {
		if !seen[want] {
			t.Fatalf("missing routine local %q; got %v", want, localsByLane)
		}
	}
}

// TestBuildTrace_WorkflowFailedIsError
// Error Case: WorkflowFailed 事件 → main lane EndKind=error,ErrorMsg 带原因。
func TestBuildTrace_WorkflowFailedIsError(t *testing.T) {
	events := `[
	  {"id":1,"workflowStarted":{"input":{}}},
	  {"id":2,"workflowFailed":{"error":"boom"}}
	]`
	res := mustTrace(t, progLetReturn, events)

	main := laneByID(res, "main")
	if main == nil {
		t.Fatalf("main lane missing")
	}
	if main.PendingFrontier {
		t.Fatalf("workflow failure must not be a pending frontier: %+v", main)
	}
	if main.EndKind != "error" {
		t.Fatalf("main EndKind = %q, want error", main.EndKind)
	}
	if !strings.Contains(main.ErrorMsg, "boom") {
		t.Fatalf("main ErrorMsg = %q, want contains boom", main.ErrorMsg)
	}
	if !strings.Contains(res.Error, "boom") {
		t.Fatalf("res.Error = %q, want contains boom", res.Error)
	}
}

// progNestedForCond 是嵌在 if 里的三段式 for，伪代码把 post 写在循环体之后的 `} post {` 块里。
const progNestedForCond = `{
  "schema":"wlang/program/v2",
  "lang":"wflang/v1",
  "handler_abi_schema":"workflow/update-handler-abi/v1",
  "program":[
    {"let":{"total":{"literal":{"type":"int64","value":"0"}}}},
    {"if":{"cond":{"==":[{"var":"total"},{"literal":{"type":"int64","value":"0"}}]},"then":[
      {"fori":{"var":"i","from":{"literal":{"type":"int64","value":"0"}},
        "cond":{"<":[{"var":"i"},{"literal":{"type":"int64","value":"3"}}]},
        "post":[{"set":{"i":{"+":[{"var":"i"},{"literal":{"type":"int64","value":"1"}}]}}}],
        "do":[{"set":{"total":{"+":[{"var":"total"},{"var":"i"}]}}}]}}
    ]}},
    {"return":{"var":"total"}}
  ]
}`

// TestBuildTrace_ForPostStepsOnPostLine 断言嵌套 for 的 post 语句每轮停在自己的伪代码行，
// 单步顺序是 for 头、循环体、post、循环体、post……
func TestBuildTrace_ForPostStepsOnPostLine(t *testing.T) {
	events := `[{"id":1,"workflowStarted":{"input":{}}},{"id":2,"workflowCompleted":{"output":{}}}]`
	res := mustTrace(t, progNestedForCond, events)
	if res.Error != "" {
		t.Fatalf("trace error = %q", res.Error)
	}
	lines := strings.Split(res.Pseudocode, "\n")
	lineOf := func(text string) int {
		for i, line := range lines {
			if strings.TrimSpace(line) == text {
				return i
			}
		}
		t.Fatalf("pseudocode has no line %q:\n%s", text, res.Pseudocode)
		return -1
	}
	header, body, post := lineOf("for i := 0; i < 3 {"), lineOf("total = total + i"), lineOf("i = i + 1")
	var got []int
	for _, step := range res.Steps {
		if strings.Contains(step.Path, "/fori") {
			got = append(got, step.Line)
		}
	}
	want := []int{header, body, post, body, post, body, post}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("loop step lines = %v, want %v\n%s", got, want, res.Pseudocode)
	}
}
