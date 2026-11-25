package interpreter

import (
	"testing"
	"time"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/protocol/history"
)

// activityHandlerProgram 手工构造一个 handler 内调 activity 的 WLang 程序：
//
//	workflow.SetUpdateHandler(ctx, "enrich", func(ctx, in map<string,any>) (map<string,any>, error) {
//	    let out = zero(map<string,any>)
//	    let err = workflow.ExecuteActivity(ctx, activities.Enrich, in).Get(ctx, out)
//	    return out, nil
//	})
func activityHandlerProgram() *program.WorkflowConfig {
	handlerFn := map[string]any{
		"fn": map[string]any{
			"params": []any{
				[]any{"ctx", "workflow.Context"},
				[]any{"in", "map<string,any>"},
			},
			"returns": []any{"map<string,any>", "error"},
			"do": []any{
				map[string]any{"let": map[string]any{"out": map[string]any{"zero": "map<string,any>"}}},
				map[string]any{"let": map[string]any{"err": map[string]any{"Get": []any{
					map[string]any{"ExecuteActivity": []any{
						map[string]any{"pkg": "workflow"},
						map[string]any{"var": "ctx"},
						map[string]any{"symbol": "activities.Enrich"},
						map[string]any{"var": "in"},
					}},
					map[string]any{"var": "ctx"},
					map[string]any{"out": "out"},
				}}}},
				handlerReturn(map[string]any{"var": "out"}),
			},
		},
	}
	return &program.WorkflowConfig{
		Type: "handler-activity",
		Activities: map[string]program.ActivityRef{
			"activities.Enrich": {Symbol: "activities.Enrich", Type: "activities.Enrich"},
		},
		Program: program.NewWLangProgram([]any{
			map[string]any{"expr": map[string]any{"SetUpdateHandler": []any{
				map[string]any{"pkg": "workflow"},
				map[string]any{"var": "ctx"},
				map[string]any{"literal": map[string]any{"type": "string", "value": "enrich"}},
				handlerFn,
			}}},
		}),
	}
}

// handler 内调 activity：首轮 handler 协程停在 ExecuteActivity.Get，
// 产 ScheduleActivity；activity 完成后次轮 WFT 重放，handler 快进越过 activity、产 completed。
func TestSetUpdateHandler_HandlerCallsActivity_ReplaysAcrossWFT(t *testing.T) {
	nodes := activityHandlerProgram()
	started := history.NewWorkflowStartedEvent(nil, "", "", "", "", 0)
	accepted := acceptedEvent(2, "enrich", "u-1", map[string]any{"id": "x"})

	// 首轮：handler 调 activity → park → ScheduleActivity，update 未完成。
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
		t.Fatalf("expected ScheduleActivity from handler activity, got %+v", commands)
	}
	if len(findUpdateCompletes(commands)) != 0 {
		t.Fatalf("update must not complete before activity resolves, got %+v", commands)
	}

	// 次轮：activity 完成事件入历史，handler 快进越过 activity、产 CompleteWorkflowUpdate。
	scheduled := history.Event{ID: 3, Timestamp: time.Now(), ActivityScheduled: &history.ActivityScheduledEvent{ActivityID: schedule.ActivityID, Input: schedule.Input}}
	completed := history.Event{ID: 4, Timestamp: time.Now(), ActivityCompleted: &history.ActivityCompletedEvent{ScheduledEventID: 3, Output: map[string]any{"enriched": true}}}

	commands2, err := NewWorkflowExecutor(nodes, []history.Event{started, accepted, scheduled, completed}).Execute()
	if err != nil {
		t.Fatalf("second round: %v", err)
	}
	completes := findUpdateCompletes(commands2)
	if len(completes) != 1 {
		t.Fatalf("expected 1 complete after activity resolves, got %+v", commands2)
	}
	if completes[0].UpdateID != "u-1" {
		t.Fatalf("unexpected complete: %+v", completes[0])
	}
	if got, _ := updateOutcomeOutput(t, completes[0])["enriched"].(bool); !got {
		t.Fatalf("expected handler to return activity output, got %+v", updateOutcomeOutput(t, completes[0]))
	}
}
