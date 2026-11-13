package engine

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"

	"github.com/donutnomad/tenuto/server/biz/domain/clock/clocktest"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

// updateWaitTestEnv 搭一个最小 server + runtime + running run，供 UpdateWorkflow(Completed) 测试用。
// Server 用假时钟：Completed 等待的软超时只在 clk.Advance 时到期。
type updateWaitTestEnv struct {
	server  *Server
	runtime *Server
	clk     *clocktest.Fake
	wfQueue *testTaskQueue
	run     *execution.Run
}

func newUpdateWaitTestEnv(t *testing.T, runID string) *updateWaitTestEnv {
	t.Helper()
	mgr := memory.NewExecutionManager()
	wfQueue := newTestTaskQueue()
	server, clk := newFakeClockServer(mgr, wfQueue, newTestTaskQueue())
	run := &execution.Run{ID: runID, WorkflowID: "wf-" + runID, WorkflowType: "wf-" + runID, Namespace: protocol.DefaultNamespace, Status: history.StatusRunning}
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(run)) }); err != nil {
		t.Fatal(err)
	}
	return &updateWaitTestEnv{
		server:  server,
		runtime: server,
		clk:     clk,
		wfQueue: wfQueue,
		run:     run,
	}
}

// driveUpdateComplete 后台驱动：等待 accepted 事件出现的 updateID，用给定 output 落 completed（提交后 runWatch.Notify 唤醒等待方）。
// 模拟 handler 在下一次 WFT 里完成 update。返回一个可等待 driver 退出的 channel。
func (e *updateWaitTestEnv) driveUpdateComplete(t *testing.T, ctx context.Context, output map[string]any) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		deadline := time.After(5 * time.Second)
		for {
			select {
			case <-ctx.Done():
				return
			case <-deadline:
				return
			default:
			}
			// 找到一个 accepted 但未 completed 的 update，并拿到当前 WFT scheduled event id。
			events, err := e.server.GetEvents(e.run.ID)
			if err != nil {
				time.Sleep(time.Millisecond)
				continue
			}
			var pendingUpdateID string
			var scheduledEventID int64
			completed := make(map[string]bool)
			for _, ev := range events {
				if ev.WorkflowUpdateCompleted != nil {
					completed[ev.WorkflowUpdateCompleted.UpdateID] = true
				}
			}
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
			err = e.server.HandleWorkflowTaskCompleted(e.server.WorkflowTaskContext(ctx, e.run.ID, scheduledEventID), e.run.ID, scheduledEventID, []history.Command{{
				Type:          history.CmdCompleteWorkflowUpdate,
				Name:          "set_priority",
				UpdateID:      pendingUpdateID,
				Output:        output,
				UpdateOutcome: updateSuccessOutcome(t, output),
			}}, "")
			if err == nil {
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()
	return done
}

// TestUpdateResult_HasStageAndOutput
// Happy Path: 验证 WorkflowUpdateResult 新增 Stage/Output/Outcome/Failure 字段存在且 JSON tag 正确。
func TestUpdateResult_HasStageAndOutcome(t *testing.T) {
	payload, err := protocol.NewJSONPayload(true)
	if err != nil {
		t.Fatal(err)
	}
	r := protocol.WorkflowUpdateResult{
		UpdateID:        "u1",
		AcceptedEventID: 7,
		Stage:           protocol.UpdateReachedStageEnums.Completed,
		Outcome:         &protocol.UpdateOutcome{Success: &payload},
	}
	data, err := json.Marshal(r)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if m["update_id"] != "u1" {
		t.Fatalf("update_id tag wrong: %v", m)
	}
	if m["accepted_event_id"].(float64) != 7 {
		t.Fatalf("accepted_event_id tag wrong: %v", m)
	}
	if m["stage"] != "Completed" {
		t.Fatalf("stage tag wrong: %v", m)
	}
	outcome, ok := m["outcome"].(map[string]any)
	if !ok || outcome["success"] == nil || outcome["failure"] != nil {
		t.Fatalf("outcome tag wrong: %v", m)
	}
	if _, ok := m["output"]; ok {
		t.Fatalf("the result carries its value only in outcome: %v", m)
	}
}

// TestUpdateWaitStageEnums
// Happy Path: 枚举值符合项目字符串枚举规范。
func TestUpdateWaitStageEnums(t *testing.T) {
	if protocol.UpdateWaitStageEnums.Accepted != "Accepted" || protocol.UpdateWaitStageEnums.Completed != "Completed" {
		t.Fatalf("UpdateWaitStageEnums wrong: %+v", protocol.UpdateWaitStageEnums)
	}
	if protocol.UpdateReachedStageEnums.Accepted != "Accepted" || protocol.UpdateReachedStageEnums.Completed != "Completed" {
		t.Fatalf("UpdateReachedStageEnums wrong: %+v", protocol.UpdateReachedStageEnums)
	}
}

// TestFillCompleted
// Happy Path + Error Case: fillCompleted 分别处理 success 与 failure outcome。
func TestFillCompleted(t *testing.T) {
	base := protocol.WorkflowUpdateResult{UpdateID: "u1", AcceptedEventID: 3, Stage: protocol.UpdateReachedStageEnums.Accepted}

	successPayload, err := protocol.NewJSONPayload(map[string]any{"v": 1})
	if err != nil {
		t.Fatalf("NewJSONPayload: %v", err)
	}
	okOutcome := protocol.UpdateOutcome{Success: &successPayload}
	ok := execution.CompleteUpdateResult(base, &okOutcome)
	if ok.Stage != protocol.UpdateReachedStageEnums.Completed || ok.Outcome == nil || updateOutput(ok)["v"] != float64(1) {
		t.Fatalf("success fill wrong: %+v", ok)
	}

	failOutcome := protocol.UpdateOutcome{Failure: &protocol.Failure{Message: "boom"}}
	fail := execution.CompleteUpdateResult(base, &failOutcome)
	if fail.Stage != protocol.UpdateReachedStageEnums.Completed || fail.Outcome == nil || fail.Outcome.Failure == nil || fail.Outcome.Failure.Message != "boom" {
		t.Fatalf("failure fill wrong: %+v", fail)
	}
	if updateOutput(fail) != nil {
		t.Fatalf("failure fill must not carry output: %+v", updateOutput(fail))
	}

	payload, err := protocol.NewJSONPayload([]any{"created", nil})
	if err != nil {
		t.Fatal(err)
	}
	successOutcome := protocol.UpdateOutcome{Success: &payload}
	completed := execution.CompleteUpdateResult(base, &successOutcome)
	if completed.Stage != protocol.UpdateReachedStageEnums.Completed || completed.Outcome == nil || completed.Outcome.Success == nil {
		t.Fatalf("success outcome fill wrong: %+v", completed)
	}
}

// TestUpdateWorkflow_WaitAccepted_IsOldBehavior
// Happy Path: stage=Accepted（或空）只返回 accepted，无 Output——回归老 accept-only 语义。
func TestUpdateWorkflow_WaitAccepted_IsOldBehavior(t *testing.T) {
	env := newUpdateWaitTestEnv(t, "run-accept-old")
	ctx := context.Background()

	result, err := env.runtime.UpdateWorkflow(ctx, "", env.run.WorkflowID, env.run.ID, "set_priority", "", "", updateArgs(map[string]any{"priority": "high"}))
	if err != nil {
		t.Fatal(err)
	}
	if result.UpdateID == "" || result.AcceptedEventID == 0 {
		t.Fatalf("unexpected result: %+v", result)
	}
	if result.Stage != protocol.UpdateReachedStageEnums.Accepted {
		t.Fatalf("stage should be Accepted, got %q", result.Stage)
	}
	if len(updateOutput(result)) != 0 || result.Outcome != nil {
		t.Fatalf("accept-only must not carry output/outcome: %+v", result)
	}
	if env.server.runWatch.EntryCount() != 0 {
		t.Fatalf("Accepted stage must not register waiter, count=%d", env.server.runWatch.EntryCount())
	}
}

func TestUpdateWorkflowRejectsInvalidEncodingBeforeHistory(t *testing.T) {
	env := newUpdateWaitTestEnv(t, "run-invalid-payload")
	_, err := env.runtime.UpdateWorkflow(
		context.Background(), "", env.run.WorkflowID, env.run.ID, "create", "u-invalid",
		protocol.UpdateWaitStageEnums.Accepted, protocol.Payloads{{Encoding: "unknown/v1", Data: []byte(`{}`)}},
	)
	if err == nil {
		t.Fatal("invalid payload encoding must fail")
	}
	events, getErr := env.server.GetEvents(env.run.ID)
	if getErr != nil {
		t.Fatal(getErr)
	}
	for _, event := range events {
		if event.WorkflowUpdateAccepted != nil {
			t.Fatalf("invalid payload wrote history: %+v", events)
		}
	}
}

// TestUpdateWorkflow_WaitCompleted_ReturnsOutput
// Happy Path: stage=Completed，driver 完成 WFT 落 completed → waiter 命中 → Output 返回。
func TestUpdateWorkflow_WaitCompleted_ReturnsOutput(t *testing.T) {
	env := newUpdateWaitTestEnv(t, "run-complete-output")
	ctx := context.Background()

	driverDone := env.driveUpdateComplete(t, ctx, map[string]any{"accepted": true})

	result, err := env.runtime.UpdateWorkflow(ctx, "", env.run.WorkflowID, env.run.ID, "set_priority", "", protocol.UpdateWaitStageEnums.Completed, updateArgs(map[string]any{"priority": "high"}))
	if err != nil {
		t.Fatal(err)
	}
	<-driverDone

	if result.Stage != protocol.UpdateReachedStageEnums.Completed {
		t.Fatalf("expected Completed stage, got %q (result=%+v)", result.Stage, result)
	}
	if result.Outcome == nil || result.Outcome.Success == nil || result.Outcome.Failure != nil {
		t.Fatalf("expected success outcome, got %+v", result.Outcome)
	}
	if updateOutput(result)["accepted"] != true {
		t.Fatalf("expected output accepted=true, got %+v", updateOutput(result))
	}
	if env.server.runWatch.EntryCount() != 0 {
		t.Fatalf("waiter must be released, count=%d", env.server.runWatch.EntryCount())
	}
}

// TestUpdateWorkflow_SignalWakeMatchesHistory
// Integration: waiter 拿到的 Output == QueryWorkflow 历史里的 Output。
func TestUpdateWorkflow_SignalWakeMatchesHistory(t *testing.T) {
	env := newUpdateWaitTestEnv(t, "run-wake-matches-history")
	ctx := context.Background()

	driverDone := env.driveUpdateComplete(t, ctx, map[string]any{"v": "x"})

	result, err := env.runtime.UpdateWorkflow(ctx, "", env.run.WorkflowID, env.run.ID, "set_priority", "up-fixed", protocol.UpdateWaitStageEnums.Completed, updateArgs(map[string]any{"priority": "high"}))
	if err != nil {
		t.Fatal(err)
	}
	<-driverDone

	q, err := env.runtime.QueryWorkflow(ctx, "", env.run.WorkflowID, env.run.ID, "", nil)
	if err != nil {
		t.Fatal(err)
	}
	var histOutput map[string]any
	for _, rec := range q.Updates {
		if rec.UpdateID != "up-fixed" {
			continue
		}
		if rec.Outcome == nil || rec.Outcome.Success == nil {
			t.Fatalf("history record must carry the success outcome: %+v", rec)
		}
		if err := rec.Outcome.Success.Decode(&histOutput); err != nil {
			t.Fatalf("decode history outcome: %v", err)
		}
	}
	if histOutput["v"] != updateOutput(result)["v"] {
		t.Fatalf("waiter output %v != history output %v", updateOutput(result), histOutput)
	}
}

// TestUpdateWorkflow_WaitCompleted_SoftTimeoutFallsBackToPoll
// Edge Case: completed 直接写入历史、没有唤醒任何等待方时，等待方读取的数据源（lookupUpdateFromHistory）
// 给出完成的结果。
func TestUpdateWorkflow_WaitCompleted_SoftTimeoutFallsBackToPoll(t *testing.T) {
	env := newUpdateWaitTestEnv(t, "run-poll-fallback")

	// 预置一个已 accepted+completed 的 update（直接进历史，不经 Workflow Task 完成路径）。
	updateID := "up-poll"
	if _, err := env.server.mgr.Append(env.run.ID, env.run.ID, env.run.ID,
		history.NewWorkflowUpdateAcceptedEvent("set_priority", updateID, updateArgs(map[string]any{"priority": "high"}))); err != nil {
		t.Fatal(err)
	}
	payload, err := protocol.NewJSONPayload(map[string]any{"done": true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := env.server.mgr.Append(env.run.ID, env.run.ID, env.run.ID,
		history.Event{WorkflowUpdateCompleted: &history.WorkflowUpdateCompletedEvent{
			Name: "set_priority", UpdateID: updateID,
			Outcome: &protocol.UpdateOutcome{Success: &payload},
		}}); err != nil {
		t.Fatal(err)
	}

	// lookupUpdateFromHistory 直接验证等待方的数据源正确（复用 QueryWorkflow 组装逻辑）：
	// CompletedEventID>0 的完成历史必须带 Outcome。
	rec, ok := env.runtime.lookupUpdateFromHistory("", env.run.WorkflowID, env.run.ID, updateID)
	if !ok || rec.CompletedEventID == 0 || rec.Outcome == nil || rec.Outcome.Success == nil {
		t.Fatalf("poll fallback source wrong: ok=%v rec=%+v", ok, rec)
	}
	var done map[string]any
	if err := rec.Outcome.Success.Decode(&done); err != nil || done["done"] != true {
		t.Fatalf("poll fallback outcome payload wrong: %+v (err=%v)", done, err)
	}
}

// TestUpdateWorkflow_WaitCompleted_TimeoutReturnsAcceptedStage
// Edge Case: Run 没有变化、历史无 completed → 软超时返回 Stage=Accepted 不报错。
func TestUpdateWorkflow_WaitCompleted_TimeoutReturnsAcceptedStage(t *testing.T) {
	env := newUpdateWaitTestEnv(t, "run-timeout-accepted")
	ctx := context.Background()

	// 不驱动完成：Run 不再变化、历史无 completed。
	type updateReturn struct {
		result protocol.WorkflowUpdateResult
		err    error
	}
	returned := make(chan updateReturn, 1)
	go func() {
		result, err := env.runtime.UpdateWorkflow(ctx, "", env.run.WorkflowID, env.run.ID, "set_priority", "", protocol.UpdateWaitStageEnums.Completed, updateArgs(map[string]any{"priority": "high"}))
		returned <- updateReturn{result, err}
	}()
	// 时钟停住时等待不会结束：Run 没有变化，软超时也没到期。
	waitForEngineEventCount(t, env.server.mgr, env.run.ID, history.EventWorkflowUpdateAccepted, 1, 3*time.Second)
	select {
	case got := <-returned:
		t.Fatalf("update returned before its soft timeout: %+v err=%v", got.result, got.err)
	default:
	}
	got := advanceUntilReturned(t, env.clk, env.server.updateSoftTimeout, returned)
	result, err := got.result, got.err
	if err != nil {
		t.Fatalf("soft timeout must not error: %v", err)
	}
	if result.Stage != protocol.UpdateReachedStageEnums.Accepted {
		t.Fatalf("expected Accepted stage on soft timeout, got %q", result.Stage)
	}
	if len(updateOutput(result)) != 0 {
		t.Fatalf("expected no output on soft timeout, got %+v", updateOutput(result))
	}
	if env.server.runWatch.EntryCount() != 0 {
		t.Fatalf("waiter must be released after timeout, count=%d", env.server.runWatch.EntryCount())
	}
}

// TestUpdateWaiter_DeadlockRegression
// Edge Case (-race): 并发发起 N 个 WaitStage=Completed 的 update，driver 逐个完成，
// 断言无死锁（watchdog 超时即失败）——验证"等在锁外、Notify 非阻塞"。
func TestUpdateWaiter_DeadlockRegression(t *testing.T) {
	const n = 30
	envs := make([]*updateWaitTestEnv, n)
	for i := range n {
		envs[i] = newUpdateWaitTestEnv(t, "run-deadlock-"+string(rune('a'+i%26))+string(rune('0'+i/26)))
	}

	ctx := context.Background()
	watchdog := time.AfterFunc(30*time.Second, func() {
		panic("TestUpdateWaiter_DeadlockRegression: deadlock detected (watchdog fired)")
	})
	defer watchdog.Stop()

	var wg sync.WaitGroup
	for i := range n {
		env := envs[i]
		driverDone := env.driveUpdateComplete(t, ctx, map[string]any{"i": i})
		wg.Add(1)
		go func() {
			defer wg.Done()
			result, err := env.runtime.UpdateWorkflow(ctx, "", env.run.WorkflowID, env.run.ID, "set_priority", "", protocol.UpdateWaitStageEnums.Completed, updateArgs(map[string]any{"priority": "high"}))
			if err != nil {
				t.Errorf("update failed: %v", err)
				return
			}
			if result.Stage != protocol.UpdateReachedStageEnums.Completed {
				t.Errorf("expected Completed, got %q", result.Stage)
			}
			<-driverDone
		}()
	}
	wg.Wait()

	for _, env := range envs {
		if c := env.server.runWatch.EntryCount(); c != 0 {
			t.Fatalf("waiter leak: count=%d", c)
		}
	}
}

// advanceUntilReturned 每次把时钟推进 step，直到 returned 收到值。等待方在登记 waiter 之后才排定软超时，
// 推进可能早于排定，因此重复推进，直到它返回。
func advanceUntilReturned[T any](t *testing.T, clk *clocktest.Fake, step time.Duration, returned <-chan T) T {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		clk.Advance(step)
		select {
		case got := <-returned:
			return got
		case <-deadline:
			t.Fatal("call did not return after its timeout elapsed")
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// updateSuccessOutcome 把测试 output 编成 Success outcome：complete 命令现在必须带 UpdateOutcome。
func updateSuccessOutcome(tb testing.TB, output map[string]any) *protocol.UpdateOutcome {
	tb.Helper()
	payload, err := protocol.NewJSONPayload(output)
	if err != nil {
		tb.Fatalf("encode update output: %v", err)
	}
	return &protocol.UpdateOutcome{Success: &payload}
}

// updateOutput 解出成功 Update 结果的 map 载荷；失败或未完成时返回 nil。
func updateOutput(result protocol.WorkflowUpdateResult) map[string]any {
	if result.Outcome == nil || result.Outcome.Success == nil {
		return nil
	}
	var output map[string]any
	if err := json.Unmarshal(result.Outcome.Success.Data, &output); err != nil {
		return nil
	}
	return output
}
