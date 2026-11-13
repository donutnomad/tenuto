package main

import (
	"encoding/json"
	"testing"
)

// ============================================
// 黄金锁快照测试 (Golden Snapshot) —— stepTo
// ============================================
// 测试策略:
// - 对 stepTo 的稳定输出做序列化快照比对,锁住现有行为,防止后续重构回归。
// - 覆盖: completed run / running run(pending 前沿) / 带 if(condResults) / 带 activity。
// - 确定性: 这些 program 不调用 Now/Random,快照不含时间与随机输出。
// ============================================

// goldenKeys 是纳入快照比对的稳定字段(排序后序列化)。
// 排除易变或与本测试目的无关的字段以保证确定性。
var goldenKeys = []string{
	"currentLine", "nodePath", "variables", "activities",
	"timers", "signals", "condResults", "error", "totalEvents",
}

// snapshotOf 从 stepTo 返回的 map 中抽取稳定字段并做确定性序列化。
func snapshotOf(t *testing.T, state map[string]any) string {
	t.Helper()
	picked := map[string]any{}
	for _, k := range goldenKeys {
		if v, ok := state[k]; ok {
			picked[k] = v
		}
	}
	// json.Marshal 对 map 键排序,保证确定性。
	b, err := json.Marshal(picked)
	if err != nil {
		t.Fatalf("marshal snapshot: %v", err)
	}
	return string(b)
}

func mustHandle(t *testing.T, programJSON, eventsJSON string) *replayHandle {
	t.Helper()
	h, err := newReplayHandle([]byte(programJSON), []byte(eventsJSON))
	if err != nil {
		t.Fatalf("newReplayHandle: %v", err)
	}
	return h
}

// ── Program fixtures ──

// progLetReturn: 无 activity,纯 let/return,可跑到完成。
const progLetReturn = `{
  "schema":"wlang/program/v2",
  "lang":"wflang/v1",
  "handler_abi_schema":"workflow/update-handler-abi/v1",
  "program":[
    {"let":{"greeting":{"literal":{"type":"string","value":"hello"}}}},
    {"let":{"count":{"literal":{"type":"int64","value":"3"}}}},
    {"return":{"var":"greeting"}}
  ]
}`

// progActivity: 调用一个 activity 并 Get 结果。
//
//	let out = zero(map<string,any>)
//	let err = workflow.ExecuteActivity(ctx, "greet", input).Get(ctx, out)
//	return out
const progActivity = `{
  "schema":"wlang/program/v2",
  "lang":"wflang/v1",
  "handler_abi_schema":"workflow/update-handler-abi/v1",
  "program":[
    {"let":{"out":{"zero":"map<string,any>"}}},
    {"let":{"err":{"Get":[
      {"ExecuteActivity":[
        {"pkg":"workflow"},
        {"var":"ctx"},
        {"literal":{"type":"string","value":"greet"}},
        {"var":"input"}
      ]},
      {"var":"ctx"},
      {"out":"out"}
    ]}}},
    {"return":{"var":"out"}}
  ]
}`

// progIf: 带 if 分支,用于验证 condResults。
//
//	let n = 7
//	if n > 5 { let branch = "high" } else { let branch = "low" }
//	return n
const progIf = `{
  "schema":"wlang/program/v2",
  "lang":"wflang/v1",
  "handler_abi_schema":"workflow/update-handler-abi/v1",
  "program":[
    {"let":{"n":{"literal":{"type":"int64","value":"7"}}}},
    {"if":{
      "cond":{">":[{"var":"n"},{"literal":{"type":"int64","value":"5"}}]},
      "then":[{"let":{"branch":{"literal":{"type":"string","value":"high"}}}}],
      "else":[{"let":{"branch":{"literal":{"type":"string","value":"low"}}}}]
    }},
    {"return":{"var":"n"}}
  ]
}`

// ── Golden cases ──

func TestStepTo_Golden(t *testing.T) {
	tests := []struct {
		// name 含类型标识(Happy Path / Edge Case)。
		name       string
		program    string
		events     string
		eventIndex int
		// want 为期望快照;首次运行用占位符 "" 生成,确认无误后填入固化。
		want string
	}{
		{
			// Happy Path: 纯 let/return 程序完成,变量 greeting/count 可见,无 activity/error。
			name:       "completed let/return run",
			program:    progLetReturn,
			events:     `[{"id":1,"workflowStarted":{"input":{}}},{"id":2,"workflowCompleted":{"output":{"ok":true}}}]`,
			eventIndex: 2,
			want:       `{"activities":null,"currentLine":3,"nodePath":"/program/2/return","totalEvents":2,"variables":{"_result":{"ok":true},"count":3,"greeting":"hello","input":{}}}`,
		},
		{
			// Happy Path: activity 已完成(有 ActivityCompleted),程序跑通,activity 状态 completed,out 被填充。
			name:    "completed run with activity",
			program: progActivity,
			events: `[
			  {"id":1,"workflowStarted":{"input":{"name":"bob"}}},
			  {"id":2,"activityScheduled":{"activityId":"#a1","activityType":"greet"}},
			  {"id":3,"activityCompleted":{"scheduledEventId":2,"output":{"msg":"hi bob"}}},
			  {"id":4,"workflowCompleted":{"output":{"msg":"hi bob"}}}
			]`,
			eventIndex: 4,
			want:       `{"activities":[{"activityID":"#a1","input":{"name":"bob"},"output":{"msg":"hi bob"},"status":"completed","type":"greet"}],"currentLine":3,"nodePath":"/program/2/return","totalEvents":4,"variables":{"_result":{"msg":"hi bob"},"err":null,"input":{"name":"bob"},"out":{"msg":"hi bob"}}}`,
		},
		{
			// Edge Case (running run): activity 已 scheduled 但未 completed,重放到 pending 前沿。
			// 断言: 引擎以 Blocked 表示 pending → error 为空(pending 不算错误),currentLine 停在 activity 语句(前沿)。
			name:    "running run pending frontier",
			program: progActivity,
			events: `[
			  {"id":1,"workflowStarted":{"input":{"name":"bob"}}},
			  {"id":2,"activityScheduled":{"activityId":"#a1","activityType":"greet"}}
			]`,
			eventIndex: 2,
			want:       `{"activities":[{"activityID":"#a1","input":{"name":"bob"},"status":"scheduled","type":"greet"}],"currentLine":1,"nodePath":"/program/1/let","totalEvents":2,"variables":{"input":{"name":"bob"},"out":null}}`,
		},
		{
			// Happy Path: 带 if 分支,cond n>5 为真,condResults 记录 line/result/expr。
			name:       "completed run with if condResults",
			program:    progIf,
			events:     `[{"id":1,"workflowStarted":{"input":{}}},{"id":2,"workflowCompleted":{"output":{}}}]`,
			eventIndex: 2,
			want:       `{"activities":null,"condResults":[{"expr":"{\"P\":\"/program/1/if/cond/\\u003e\",\"Op\":\"\\u003e\",\"Args\":[{\"P\":\"/program/1/if/cond/\\u003e/0/var\",\"Name\":\"n\",\"Default\":null},{\"P\":\"/program/1/if/cond/\\u003e/1/literal\",\"Value\":{}}]}","line":1,"result":true}],"currentLine":7,"nodePath":"/program/2/return","totalEvents":2,"variables":{"_result":{},"input":{},"n":7}}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := mustHandle(t, tt.program, tt.events)
			state := h.stepTo(tt.eventIndex)
			got := snapshotOf(t, state)
			if tt.want == "" {
				t.Fatalf("GOLDEN[%s] =\n%s", tt.name, got)
			}
			if got != tt.want {
				t.Fatalf("snapshot mismatch for %q\n want: %s\n got:  %s", tt.name, tt.want, got)
			}
		})
	}
}

// TestStepTo_PendingIsNotError 显式断言: running run 的 pending 前沿不被误判为 error。
// Edge Case: activity scheduled 未 completed → stepTo 返回的 error 字段应为空/缺失。
func TestStepTo_PendingIsNotError(t *testing.T) {
	events := `[
	  {"id":1,"workflowStarted":{"input":{"name":"bob"}}},
	  {"id":2,"activityScheduled":{"activityId":"#a1","activityType":"greet"}}
	]`
	h := mustHandle(t, progActivity, events)
	state := h.stepTo(2)

	if errVal, ok := state["error"]; ok {
		if s, _ := errVal.(string); s != "" {
			t.Fatalf("pending frontier must not be reported as error, got error=%q", s)
		}
	}
	// activity 应处于 scheduled(前沿),而非 completed/failed。
	acts, _ := state["activities"].([]any)
	if len(acts) != 1 {
		t.Fatalf("expected 1 pending activity, got %d", len(acts))
	}
	first, _ := acts[0].(map[string]any)
	if first["status"] != "scheduled" {
		t.Fatalf("expected activity status scheduled at frontier, got %v", first["status"])
	}
}
