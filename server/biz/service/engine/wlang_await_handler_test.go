package engine

import (
	"context"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/internal/executor/interpreter"
	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
)

// ============================================
// handler 内 Await 端到端（驱动链闭合）测试
// ============================================
// 验证「handler 内 Await 阻塞等主流程条件、结果同步回传」这条驱动链：
//   第一轮：handler 协程在 Await(createDone) 上 park，主流程产出 ScheduleActivity
//   → 主流程 activity 完成事件触发下一轮 WFT
//   → replay：主流程快进过 activity、置 createDone=true
//   → handler 协程的 Await(createDone) 满足 → handler 返回 initResult
//   → 产 CmdCompleteWorkflowUpdate 带 initResult
// ============================================

// awaitDriverProgram 构造驱动用例的等价 WLang 节点（手工构造，非 go2wlang）：
//
//	let createDone = false
//	let initResult = zero(map<string,any>)
//	workflow.SetUpdateHandler(ctx, "create", func(ctx, in) (map<string,any>, error) {
//	    workflow.Await(ctx, func() bool { return createDone })  // handler 内 Await 等主流程条件
//	    return initResult, nil                                   // 满足后回传 initResult
//	})
//	let r = zero(map<string,any>)
//	workflow.ExecuteActivity(ctx, activities.CreateRecord, ...).Get(ctx, r)  // 主流程建记录
//	set initResult = r
//	set createDone = true          // 置条件 → handler 协程的 Await 满足
//
// 这里手工构造 WLang 节点是为了在 executor 层精确控制历史事件、隔离验证驱动链的每一轮；
// 手工节点与 sdkgen 真实产物同构。用户实际写代码时无需手工 WLang——sdkgen 已能干净翻译
// 此模式（handler 内 workflow.Await + 主流程 activity），端到端翻译由
// sdkgen/await_handler_demo_test.go 锁定。
func awaitDriverProgram() *program.WorkflowConfig {
	condFn := map[string]any{
		"fn": map[string]any{
			"params":  []any{},
			"returns": []any{"boolean"},
			"do": []any{
				map[string]any{"return": map[string]any{"var": "createDone"}},
			},
		},
	}
	handlerFn := map[string]any{
		"fn": map[string]any{
			"params": []any{
				[]any{"ctx", "workflow.Context"},
				[]any{"in", "map<string,any>"},
			},
			"returns": []any{"map<string,any>", "error"},
			"do": []any{
				map[string]any{"expr": map[string]any{"Await": []any{
					map[string]any{"pkg": "workflow"},
					map[string]any{"var": "ctx"},
					condFn,
				}}},
				handlerReturn(map[string]any{"var": "initResult"}),
			},
		},
	}
	return &program.WorkflowConfig{
		ID: "await-driver-wf",
		Activities: map[string]program.ActivityRef{
			"activities.CreateRecord": {Symbol: "activities.CreateRecord", Type: "activities.CreateRecord"},
		},
		Program: program.NewWLangProgram([]any{
			map[string]any{"let": map[string]any{"createDone": map[string]any{"literal": map[string]any{"type": "boolean", "value": "false"}}}},
			map[string]any{"let": map[string]any{"initResult": map[string]any{"zero": "map<string,any>"}}},
			map[string]any{"expr": map[string]any{"SetUpdateHandler": []any{
				map[string]any{"pkg": "workflow"},
				map[string]any{"var": "ctx"},
				map[string]any{"literal": map[string]any{"type": "string", "value": "create"}},
				handlerFn,
			}}},
			map[string]any{"let": map[string]any{"r": map[string]any{"zero": "map<string,any>"}}},
			map[string]any{"expr": map[string]any{"Get": []any{
				map[string]any{"ExecuteActivity": []any{
					map[string]any{"pkg": "workflow"},
					map[string]any{"var": "ctx"},
					map[string]any{"symbol": "activities.CreateRecord"},
					map[string]any{"zero": "map<string,any>"},
					map[string]any{"struct": []any{"workflow.ActivityOptions", map[string]any{
						"StartToCloseTimeout": map[string]any{"literal": map[string]any{"type": "int64", "value": "60000000000"}},
					}}},
				}},
				map[string]any{"var": "ctx"},
				map[string]any{"out": "r"},
			}}},
			map[string]any{"set": map[string]any{"initResult": map[string]any{"var": "r"}}},
			map[string]any{"set": map[string]any{"createDone": map[string]any{"literal": map[string]any{"type": "boolean", "value": "true"}}}},
		}),
	}
}

// activityCompletedRound 从第一轮的 ScheduleActivity 命令构造第二轮历史所需的
// ActivityScheduled + ActivityCompleted 事件（output 作为 activity 返回值）。
func activityCompletedRound(schedule *history.Command, output map[string]any) (history.Event, history.Event) {
	scheduled := history.Event{ID: 3, ActivityScheduled: &history.ActivityScheduledEvent{ActivityID: schedule.ActivityID, Input: schedule.Input}}
	completed := history.Event{ID: 4, ActivityCompleted: &history.ActivityCompletedEvent{ScheduledEventID: 3, Output: output}}
	return scheduled, completed
}

// 🔴 核心驱动用例：handler 内 Await 阻塞等主流程条件，主流程 activity 完成后置条件，handler 协程的 Await 满足、回传 initResult。
//
// 第一轮(started + accepted)——handler 协程 park 在 Await 上，
//
//	Await 未求值，只产 CmdScheduleActivity，无 CmdCompleteWorkflowUpdate。
//
// 第二轮(+ ActivityCompleted)——主流程快进过 activity、置 createDone=true，handler 的
//
//	Await(createDone) 满足、handler 返回 initResult(=activity output)，产 CmdCompleteWorkflowUpdate 带 initResult。
func TestHandlerAwait_EndToEnd_DrivenByMainFlow(t *testing.T) {
	nodes := awaitDriverProgram()
	started := history.NewWorkflowStartedEvent(nil, "", "", "", "", 0)
	accepted := acceptedEvent(2, "create", "u-1", map[string]any{"id": "x"})

	// —— 第一轮：handler 内 Await 不满足（甚至未进 handler），主流程 activity 未完成。
	commands1, err := interpreter.NewWorkflowExecutor(nodes, []history.Event{started, accepted}).Execute()
	if err != nil {
		t.Fatalf("first round: %v", err)
	}
	var schedule *history.Command
	for i := range commands1 {
		if commands1[i].Type == history.CmdScheduleActivity {
			schedule = &commands1[i]
		}
	}
	if schedule == nil {
		t.Fatalf("第一轮应产 ScheduleActivity(主流程 CreateRecord)，got %+v", commands1)
	}
	if len(findUpdateCompletes(commands1)) != 0 {
		t.Fatalf("第一轮 handler 内 Await 未满足，禁止产 CmdCompleteWorkflowUpdate，got %+v", commands1)
	}

	// —— 第二轮：activity 完成 → 主流程置 createDone=true → handler 的 Await 满足 → 回传 initResult。
	initResult := map[string]any{"record_id": "rec-42"}
	scheduled, completed := activityCompletedRound(schedule, initResult)
	commands2, err := interpreter.NewWorkflowExecutor(nodes, []history.Event{started, accepted, scheduled, completed}).Execute()
	if err != nil {
		t.Fatalf("second round: %v", err)
	}
	completes := findUpdateCompletes(commands2)
	if len(completes) != 1 {
		t.Fatalf("第二轮应产 1 个 CmdCompleteWorkflowUpdate，got %+v", commands2)
	}
	c := completes[0]
	if c.UpdateID != "u-1" || c.Name != "create" {
		t.Fatalf("update 标识错误: %+v", c)
	}
	if got, _ := updateOutcomeOutput(t, c)["record_id"].(string); got != "rec-42" {
		t.Fatalf("handler 应回传主流程 initResult(record_id=rec-42)，got %+v", updateOutcomeOutput(t, c))
	}
}

// 幂等：handler 每轮 replay 都会重新执行（第一轮 Await 不满足、第二轮满足）。
// 幂等语义迁移：旧模型靠 updateCompleted 幂等护栏跳过；
// 协程化后 replay 批重放 handler 全程，Complete 进 replayCommands 与历史 WorkflowUpdateCompleted
// 按 UpdateID 配对（比对通过 = 幂等成立），上报侧无重复。历史改真实 server 形态（命令事件归其
// 决策批：Sched 随 WFTC₁、updCompleted 随 WFTC₂）。
func TestHandlerAwait_Idempotent_MultiRoundDrainNoDuplicate(t *testing.T) {
	nodes := awaitDriverProgram()
	started := history.NewWorkflowStartedEvent(nil, "", "", "", "", 0)
	accepted := acceptedEvent(2, "create", "u-1", map[string]any{"id": "x"})

	commands1, err := interpreter.NewWorkflowExecutor(nodes, []history.Event{started, accepted}).Execute()
	if err != nil {
		t.Fatalf("first round: %v", err)
	}
	var schedule *history.Command
	for i := range commands1 {
		if commands1[i].Type == history.CmdScheduleActivity {
			schedule = &commands1[i]
		}
	}
	if schedule == nil {
		t.Fatalf("第一轮应产 ScheduleActivity")
	}
	initResult := map[string]any{"record_id": "rec-42"}

	// 第三轮（全 replay）：批 1 = 轮 1 决策（Sched 归位批尾），批 2 = 轮 2 决策（updCompleted 归位），
	// 末批空 live。handler 重放全程，Complete 进 replayCommands 与 updCompleted 配对。
	wftc1 := wftCompletedEvent(3)
	scheduled := history.Event{ID: 4, ActivityScheduled: &history.ActivityScheduledEvent{ActivityID: schedule.ActivityID, ActivityType: schedule.ActivityType, Input: schedule.Input}}
	completed := history.Event{ID: 5, ActivityCompleted: &history.ActivityCompletedEvent{ScheduledEventID: 4, Output: initResult}}
	wftc2 := wftCompletedEvent(6)
	updCompleted := completedEvent(t, 7, "create", "u-1", initResult)
	commands3, err := interpreter.NewWorkflowExecutor(nodes, []history.Event{started, accepted, wftc1, scheduled, completed, wftc2, updCompleted}).Execute()
	if err != nil {
		t.Fatalf("third round: %v", err)
	}
	if len(findUpdateCompletes(commands3)) != 0 {
		t.Fatalf("replay 批重放的 Complete 不得上报（进 replayCommands 只比对），got %+v", commands3)
	}
}

// 确定性重放：两轮各自的命令产出跨多次重放稳定（cond 求值 + handler 协程调度顺序确定）。
func TestHandlerAwait_ReplayDeterminism(t *testing.T) {
	nodes := awaitDriverProgram()
	started := history.NewWorkflowStartedEvent(nil, "", "", "", "", 0)
	accepted := acceptedEvent(2, "create", "u-1", map[string]any{"id": "x"})

	// 先取一次 schedule 命令构造第二轮历史。
	c1, err := interpreter.NewWorkflowExecutor(nodes, []history.Event{started, accepted}).Execute()
	if err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	var schedule *history.Command
	for i := range c1 {
		if c1[i].Type == history.CmdScheduleActivity {
			schedule = &c1[i]
		}
	}
	if schedule == nil {
		t.Fatalf("bootstrap 应产 ScheduleActivity")
	}
	initResult := map[string]any{"record_id": "rec-42"}
	scheduled, completed := activityCompletedRound(schedule, initResult)
	round2Events := []history.Event{started, accepted, scheduled, completed}
	round1Events := []history.Event{started, accepted}

	for i := 0; i < 50; i++ {
		// 第一轮：始终只产 1 个 ScheduleActivity、0 个 completion。
		r1, err := interpreter.NewWorkflowExecutor(nodes, round1Events).Execute()
		if err != nil {
			t.Fatalf("replay %d round1: %v", i, err)
		}
		if len(findUpdateCompletes(r1)) != 0 {
			t.Fatalf("replay %d round1 drift: 出现 completion %+v", i, r1)
		}
		schedCount := 0
		for _, c := range r1 {
			if c.Type == history.CmdScheduleActivity {
				schedCount++
			}
		}
		if schedCount != 1 {
			t.Fatalf("replay %d round1 drift: ScheduleActivity 数=%d，want 1", i, schedCount)
		}
		// 第二轮：始终产 1 个 completion(u-1, record_id=rec-42)。
		r2, err := interpreter.NewWorkflowExecutor(nodes, round2Events).Execute()
		if err != nil {
			t.Fatalf("replay %d round2: %v", i, err)
		}
		completes := findUpdateCompletes(r2)
		if len(completes) != 1 {
			t.Fatalf("replay %d round2 drift: completion 数=%d，want 1", i, len(completes))
		}
		if completes[0].UpdateID != "u-1" {
			t.Fatalf("replay %d round2 drift: UpdateID=%s，want u-1", i, completes[0].UpdateID)
		}
		if got, _ := updateOutcomeOutput(t, completes[0])["record_id"].(string); got != "rec-42" {
			t.Fatalf("replay %d round2 drift: outcome=%+v", i, completes[0].UpdateOutcome)
		}
	}
}

// driveHandlerAwait 是接 server long-poll 的真实 driver：不硬编码 output，而是用真实 WLang executor
// （driverProgram，含 handler 内 Await）计算 CmdCompleteWorkflowUpdate 的 output，闭合驱动链：
//   - 轮询 history 找到 accepted 但未 completed 的 update + 当前 WFT scheduled event id。
//   - 用当前 history 跑 executor：
//   - 若产 CmdScheduleActivity（handler 内 Await 未满足、主流程 activity 未完成）→ 模拟主流程推进：
//     往 history 追加 ActivityScheduled + ActivityCompleted(initResult)，触发"下一轮 WFT"（重新循环）。
//   - 若产 CmdCompleteWorkflowUpdate（第二轮 handler 的 Await 满足）→ HandleWorkflowTaskCompleted
//     落库，Signal waiter，driver 退出。
//
// 这把「Await host + handler 协程 + 真实 executor + server long-poll」端到端串起来——driver 用的是
// 生产 executor，不是硬编码 output。
func driveHandlerAwait(t *testing.T, env *updateWaitTestEnv, nodes *program.WorkflowConfig, initResult map[string]any) <-chan struct{} {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		ctx := context.Background()
		deadline := time.After(5 * time.Second)
		for {
			select {
			case <-deadline:
				return
			default:
			}
			events, err := env.server.GetEvents(env.run.ID)
			if err != nil {
				time.Sleep(time.Millisecond)
				continue
			}
			// 找 accepted 未 completed 的 update + 当前 WFT scheduled event id。
			completed := make(map[string]bool)
			for _, ev := range events {
				if ev.WorkflowUpdateCompleted != nil {
					completed[ev.WorkflowUpdateCompleted.UpdateID] = true
				}
			}
			var pendingUpdateID string
			var scheduledEventID int64
			for _, ev := range events {
				if ev.WorkflowTaskScheduled != nil {
					scheduledEventID = ev.ID
				}
				if ev.WorkflowUpdateAccepted != nil && !completed[ev.WorkflowUpdateAccepted.UpdateID] {
					pendingUpdateID = ev.WorkflowUpdateAccepted.UpdateID
				}
			}
			if pendingUpdateID == "" || scheduledEventID == 0 {
				time.Sleep(time.Millisecond)
				continue
			}

			// 用真实 executor 跑当前 history。
			commands, err := interpreter.NewWorkflowExecutor(nodes, events).Execute()
			if err != nil {
				t.Errorf("driver executor: %v", err)
				return
			}

			// 找主流程 activity schedule（handler 内 Await 未满足，第一轮）。
			var schedule *history.Command
			for i := range commands {
				if commands[i].Type == history.CmdScheduleActivity {
					schedule = &commands[i]
				}
			}
			completes := findUpdateCompletes(commands)

			if len(completes) == 0 && schedule != nil {
				// 第一轮：模拟主流程 activity 完成 → 追加 ActivityScheduled + ActivityCompleted 到 history。
				schedID, aerr := env.server.mgr.Append(env.run.ID, env.run.ID, env.run.ID,
					history.NewActivityScheduledEvent(history.ActivityScheduledEvent{ActivityID: schedule.ActivityID, Input: schedule.Input}))
				if aerr != nil {
					t.Errorf("driver append scheduled: %v", aerr)
					return
				}
				if _, aerr := env.server.mgr.Append(env.run.ID, env.run.ID, env.run.ID,
					history.NewActivityCompletedEvent(history.ActivityCompletedEvent{ScheduledEventID: schedID, Output: initResult})); aerr != nil {
					t.Errorf("driver append completed: %v", aerr)
					return
				}
				continue // 下一轮循环重跑 executor（这次主流程快进、置 createDone、handler 的 Await 满足）。
			}

			if len(completes) > 0 {
				// 第二轮：executor 产出 CmdCompleteWorkflowUpdate（handler 内 Await 满足、回传 initResult）。
				if err := env.server.HandleWorkflowTaskCompleted(env.server.WorkflowTaskContext(ctx, env.run.ID, scheduledEventID), env.run.ID, scheduledEventID, commands, ""); err != nil {
					t.Errorf("driver complete WFT: %v", err)
					return
				}
				return
			}

			time.Sleep(time.Millisecond)
		}
	}()
	return done
}

// 🔴 端到端 + long-poll：外部 UpdateWorkflow(stage=Completed) 发 create update，driver 用真实
// executor（handler 内 Await）驱动 activity 完成后，long-poll 拿到 handler 回传的 initResult。
//
// 验证完整链路：UpdateWorkflow(Completed) accept → 调度 WFT → driver 跑真实 executor
// （第一轮 handler 内 Await 未满足只产 ScheduleActivity → 模拟主流程 activity 完成 → 第二轮 handler 的
// Await 满足、产 CmdCompleteWorkflowUpdate(initResult)）→ 落库 Signal waiter → long-poll
// 返回 initResult。initResult 由真实 executor 跑出，非硬编码。
func TestHandlerAwait_LongPollReturnsInitResult(t *testing.T) {
	env := newUpdateWaitTestEnv(t, "run-handler-await-longpoll")
	ctx := context.Background()

	// run 需要 WorkflowStarted 事件，供 executor 执行时环境完整（handler 注册 + activity 步骤 seq）。
	if _, err := env.server.mgr.Append(env.run.ID, env.run.ID, env.run.ID,
		history.NewWorkflowStartedEvent(nil, "", "", env.run.WorkflowID, "", 0)); err != nil {
		t.Fatal(err)
	}

	nodes := awaitDriverProgram()
	initResult := map[string]any{"record_id": "rec-longpoll"}
	driverDone := driveHandlerAwait(t, env, nodes, initResult)

	// 外部发 create update，等到 Completed（long-poll）。driver 会用真实 executor 驱动 activity 完成后回传。
	result, err := env.runtime.UpdateWorkflow(ctx, "", env.run.WorkflowID, env.run.ID, "create", "", protocol.UpdateWaitStageEnums.Completed, updateArgs(map[string]any{"id": "x"}))
	if err != nil {
		t.Fatalf("UpdateWorkflow: %v", err)
	}
	<-driverDone

	if result.Stage != protocol.UpdateReachedStageEnums.Completed {
		t.Fatalf("期望 long-poll 到 Completed 阶段，got %q (result=%+v)", result.Stage, result)
	}
	if result.Outcome == nil || result.Outcome.Success == nil || result.Outcome.Failure != nil {
		t.Fatalf("期望 success，got %+v", result.Outcome)
	}
	var longPollOutput map[string]any
	if err := result.Outcome.Success.Decode(&longPollOutput); err != nil {
		t.Fatalf("decode long-poll outcome: %v", err)
	}
	if got, _ := longPollOutput["record_id"].(string); got != "rec-longpoll" {
		t.Fatalf("long-poll 应拿到 handler 回传的 initResult(record_id=rec-longpoll)，got %+v", longPollOutput)
	}
	if env.server.runWatch.EntryCount() != 0 {
		t.Fatalf("waiter 必须释放，count=%d", env.server.runWatch.EntryCount())
	}
}

// Error Case：update 名没有对应 handler → 真实 executor 产出 Failure.Type=UnknownUpdate 的完成命令，
// 等 Completed 的 long-poll 拿到这个失败 outcome，不会挂到超时。
func TestUnknownUpdate_LongPollReturnsUnknownUpdateFailure(t *testing.T) {
	env := newUpdateWaitTestEnv(t, "run-unknown-update-longpoll")
	ctx := context.Background()
	if _, err := env.server.mgr.Append(env.run.ID, env.run.ID, env.run.ID,
		history.NewWorkflowStartedEvent(nil, "", "", env.run.WorkflowID, "", 0)); err != nil {
		t.Fatal(err)
	}
	// 程序只注册了 "create"；主流程阻塞在 activity 上，run 保持 running。
	driverDone := driveHandlerAwait(t, env, awaitDriverProgram(), nil)

	result, err := env.runtime.UpdateWorkflow(ctx, "", env.run.WorkflowID, env.run.ID, "missing", "up-unknown", protocol.UpdateWaitStageEnums.Completed, nil)
	if err != nil {
		t.Fatalf("UpdateWorkflow: %v", err)
	}
	<-driverDone

	if result.Stage != protocol.UpdateReachedStageEnums.Completed {
		t.Fatalf("stage = %q, want completed (result=%+v)", result.Stage, result)
	}
	if result.Outcome == nil || result.Outcome.Failure == nil || result.Outcome.Success != nil {
		t.Fatalf("outcome = %+v, want failure", result.Outcome)
	}
	failure := result.Outcome.Failure
	if failure.Type != "UnknownUpdate" || failure.Message != `no update handler registered for "missing"` {
		t.Fatalf("failure = %+v", failure)
	}
	if env.server.runWatch.EntryCount() != 0 {
		t.Fatalf("waiter 必须释放，count=%d", env.server.runWatch.EntryCount())
	}
}
