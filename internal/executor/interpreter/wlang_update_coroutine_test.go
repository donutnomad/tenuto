package interpreter

import (
	"errors"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/internal/executor"
	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/protocol/history"
)

// ============================================
// update handler 协程化测试
// ============================================
// 测试策略:
// - Happy Path: 单 update 完成（T1）、handler 内 activity 跨批 park/resume + 专属键（T4）、
//   与 body 并发推进（T6）
// - Edge Case: 等注册竞争（T2）、无 handler 退出（T3）、同批多 update 按 AcceptedEventID 序（T5）、
//   body 全 done 后 live 批新 update 仍被执行（T12）
// - Error Case: handler 失败两形态（T10）、非确定性 panic 升级不降级（T13）
//
// 背景: handler 从「body 尾 drain 同步执行」迁移为「独立协程」（照 Temporal defaultUpdateHandler
// → scheduler.Spawn，internal_update.go:329/:332）。handler 在其 UpdateAccepted 所属批的视界
// 执行（前缀一致性），确定性键迁入 update:<UpdateID> 专属桶。
// ============================================

// bodyActivityWithHandlerProgram：body 注册 handler 后自己调 activity（park 点），
// 锚定 handler 与 body 并发推进（drain 模型下 body park 时 handler 永不执行）。
func bodyActivityWithHandlerProgram(name string) *program.WorkflowConfig {
	handlerFn := map[string]any{
		"fn": map[string]any{
			"params": []any{
				[]any{"ctx", "workflow.Context"},
				[]any{"in", "map<string,any>"},
			},
			"returns": []any{"map<string,any>", "error"},
			"do": []any{
				handlerReturn(map[string]any{"map": map[string]any{
					"type": []any{"string", "any"},
					"entries": []any{
						[]any{
							map[string]any{"literal": map[string]any{"type": "string", "value": "pong"}},
							map[string]any{"literal": map[string]any{"type": "boolean", "value": true}},
						},
					},
				}}),
			},
		},
	}
	return &program.WorkflowConfig{
		ID: "body-activity-handler",
		Activities: map[string]program.ActivityRef{
			"activities.Work": {Symbol: "activities.Work", Type: "activities.Work"},
		},
		Program: program.NewWLangProgram([]any{
			map[string]any{"expr": map[string]any{"SetUpdateHandler": []any{
				map[string]any{"pkg": "workflow"},
				map[string]any{"var": "ctx"},
				map[string]any{"literal": map[string]any{"type": "string", "value": name}},
				handlerFn,
			}}},
			map[string]any{"let": map[string]any{"out": map[string]any{"zero": "map<string,any>"}}},
			map[string]any{"let": map[string]any{"err": map[string]any{"Get": []any{
				map[string]any{"ExecuteActivity": []any{
					map[string]any{"pkg": "workflow"},
					map[string]any{"var": "ctx"},
					map[string]any{"symbol": "activities.Work"},
					map[string]any{"map": map[string]any{"type": []any{"string", "any"}, "entries": []any{}}},
				}},
				map[string]any{"var": "ctx"},
				map[string]any{"out": "out"},
			}}}},
		}),
	}
}

func wftCompletedEvent(id int64) history.Event {
	return history.Event{ID: id, Timestamp: time.Now(), WorkflowTaskCompleted: &history.WorkflowTaskCompletedEvent{}}
}

// T6 Happy Path: body park 于 activity 时 handler 照常推进完成——
// handler 是独立协程与 body 平权（照 Temporal），不再等 body 跑到末尾。
func TestUpdateHandlerCoroutine_ConcurrentWithBody(t *testing.T) {
	nodes := bodyActivityWithHandlerProgram("ping")
	started := history.NewWorkflowStartedEvent(nil, "", "", "", "", 0)
	// 批 1（replay）: live 当年 main 注册 handler 后调度了 body activity（#a1）
	wftc := wftCompletedEvent(2)
	schedBody := history.Event{ID: 3, Timestamp: time.Now(), ActivityScheduled: &history.ActivityScheduledEvent{ActivityID: "#a1", ActivityType: "activities.Work"}}
	// 批 2（live）: update 到达；body activity 仍未完成（main 保持 park）
	accepted := acceptedEvent(4, "ping", "u-6", map[string]any{})

	commands, err := NewWorkflowExecutor(nodes, []history.Event{started, wftc, schedBody, accepted}).Execute()
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	completes := findUpdateCompletes(commands)
	if len(completes) != 1 {
		t.Fatalf("body park 于 activity 时 handler 协程应照常完成 update（与 body 并发，照 Temporal）, got %+v", commands)
	}
	if completes[0].UpdateID != "u-6" {
		t.Fatalf("unexpected update complete: %+v", completes[0])
	}
	if got, _ := updateOutcomeOutput(t, completes[0])["pong"].(bool); !got {
		t.Fatalf("handler output 应上浮, got %+v", updateOutcomeOutput(t, completes[0]))
	}
	for _, c := range commands {
		if c.Type == history.CmdCompleteWorkflow {
			t.Fatalf("body 未结束不得完成工作流, got %+v", commands)
		}
	}
}

// T4 Happy Path（键迁移锚）: handler 内 ExecuteActivity 的确定性键在 update:<UpdateID> 专属桶
// （update:u-1#a1），不再混用 main root 桶——两个 handler 各调 activity 不再互相挤占 #aN 序。
func TestUpdateHandlerCoroutine_ActivityInside_ParksAndResumes(t *testing.T) {
	nodes := activityHandlerProgram()
	started := history.NewWorkflowStartedEvent(nil, "", "", "", "", 0)
	accepted := acceptedEvent(2, "enrich", "u-1", map[string]any{"id": "x"})

	// 轮 1: handler 协程内 ExecuteActivity 首见 → 发真命令（键 = update:u-1#a1）→ park。
	commands, err := NewWorkflowExecutor(nodes, []history.Event{started, accepted}).Execute()
	if err != nil {
		t.Fatalf("first round: %v", err)
	}
	var schedule *history.Command
	for i := range commands {
		if commands[i].Type == history.CmdScheduleActivity {
			schedule = &commands[i]
		}
	}
	if schedule == nil {
		t.Fatalf("expected ScheduleActivity from handler, got %+v", commands)
	}
	if schedule.ActivityID != "update:u-1#a1" {
		t.Fatalf("handler 内 activity 键应在 update:<id> 专属桶, got %q", schedule.ActivityID)
	}
	if len(findUpdateCompletes(commands)) != 0 {
		t.Fatalf("activity 未完成 update 不得先完成, got %+v", commands)
	}

	// 轮 2: activity 完成（历史含 WFTC 分界 → 轮 1 决策批 replay，handler 重放跨批命中）。
	wftc := wftCompletedEvent(3)
	scheduled := history.Event{ID: 4, Timestamp: time.Now(), ActivityScheduled: &history.ActivityScheduledEvent{ActivityID: schedule.ActivityID, ActivityType: schedule.ActivityType, Input: schedule.Input}}
	completed := history.Event{ID: 5, Timestamp: time.Now(), ActivityCompleted: &history.ActivityCompletedEvent{ScheduledEventID: 4, Output: map[string]any{"enriched": true}}}

	commands2, err := NewWorkflowExecutor(nodes, []history.Event{started, accepted, wftc, scheduled, completed}).Execute()
	if err != nil {
		t.Fatalf("second round: %v", err)
	}
	completes := findUpdateCompletes(commands2)
	if len(completes) != 1 {
		t.Fatalf("expected 1 complete after activity resolves, got %+v", commands2)
	}
	if got, _ := updateOutcomeOutput(t, completes[0])["enriched"].(bool); !got {
		t.Fatalf("expected handler to return activity output, got %+v", updateOutcomeOutput(t, completes[0]))
	}
}

// T13 Error Case（v2 必改 2）: handler 内非确定性 panic 升级为 Execute 错误
// （errors.As 命中 nonDeterministicError），绝不降级为 completed-with-failure。
// 构造: handler 内 SideEffect 在 replay 批读不到 marker → panicNonDeterministic 经 wlang
// safeCall 上浮（typed 传播已落地，LangError.Cause 保留错误链）。
func TestUpdateHandler_NonDetPanic_EscalatesNotDowngrades(t *testing.T) {
	handlerFn := map[string]any{
		"fn": map[string]any{
			"params": []any{
				[]any{"ctx", "workflow.Context"},
				[]any{"in", "map<string,any>"},
			},
			"returns": []any{"map<string,any>", "error"},
			"do": []any{
				// handler 内 SideEffect：replay 批中 marker 未命中 → panicNonDeterministic
				map[string]any{"let": map[string]any{"v": map[string]any{"Get": []any{
					map[string]any{"SideEffect": []any{
						map[string]any{"pkg": "workflow"},
						map[string]any{"var": "ctx"},
						map[string]any{"literal": map[string]any{"type": "string", "value": "cfg"}},
					}},
					map[string]any{"var": "ctx"},
				}}}},
				handlerReturn(map[string]any{"map": map[string]any{"type": []any{"string", "any"}, "entries": []any{}}}),
			},
		},
	}
	nodes := &program.WorkflowConfig{
		ID: "nondet-handler",
		Program: program.NewWLangProgram([]any{
			map[string]any{"expr": map[string]any{"SetUpdateHandler": []any{
				map[string]any{"pkg": "workflow"},
				map[string]any{"var": "ctx"},
				map[string]any{"literal": map[string]any{"type": "string", "value": "cfg-upd"}},
				handlerFn,
			}}},
		}),
	}
	started := history.NewWorkflowStartedEvent(nil, "", "", "", "", 0)
	accepted := acceptedEvent(2, "cfg-upd", "u-9", map[string]any{})
	// WFTC 分界使 accepted 落 replay 批 → handler 在 replay 批执行 → SideEffect marker 未命中 → 非确定性。
	wftc := wftCompletedEvent(3)
	// 批 2（live）随便一个外部事件开批（signal 占位）。
	sig := history.Event{ID: 4, Timestamp: time.Now(), WorkflowSignaled: &history.WorkflowSignaledEvent{Name: "wake", Input: map[string]any{}}}

	_, err := NewWorkflowExecutor(nodes, []history.Event{started, accepted, wftc, sig}).Execute()
	if err == nil {
		t.Fatalf("handler 内非确定性 panic 应升级为 Execute 错误（不降级为 update 失败）")
	}
	var nd executor.NonDeterministicError
	if !errors.As(err, &nd) {
		t.Fatalf("错误应可 errors.As 出 nonDeterministicError, got %T: %v", err, err)
	}
}
