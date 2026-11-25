package engine

import (
	"context"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/program/programtest"
	"github.com/donutnomad/tenuto/protocol"
	historypkg "github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
)

// ============================================
// 取消命令的 Server 规则（Temporal：workflowTaskCompletedHandler 与 MutableStateImpl）
// ============================================
// 测试策略：内存后端与假时钟，Worker 的决策直接以命令提交，Activity Worker 的领取与上报直接调用 Server。
// - Happy Path: 未开始的 Activity 被取消时立即 ActivityCancelled 并调度 Workflow Task
// - Happy Path: 已开始的 Activity 经心跳得知取消，确认取消时 ActivityCancelled 带 details
// - Edge Case: 取消确认之前完成 → ActivityCompleted；失败与超时不再重试；交还的 attempt 以取消结束
// - Edge Case: 重试退避中取消 → 立即 ActivityCancelled，退避到期后没有新的 attempt
// - Happy Path: CancelTimer → TimerCancelled，到期不触发
// - Happy Path: 子 Workflow 取消请求送到子 Run，与父关闭策略组合
// - Edge Case: 重复的 Workflow 取消请求幂等；已关闭的 Run 拒绝取消请求
// ============================================

const cancelQueue = "payments"

// cancelHarness 启动一个 Run 并以 "charge" 调度 payments.charge。
func newCancelHarness(t *testing.T, id string, cmd historypkg.Command) (*buildRoutingHarness, string, int64) {
	t.Helper()
	h := newBuildRoutingHarness(t)
	h.register("pay-1", "pay@1", cancelQueue, chargeContract("v1"))
	runID := h.startRun(id, execution.RunPolicyAutoUpgrade, cancelQueue, "v1", cmd)
	events := h.events(runID)
	for _, event := range events {
		if event.ActivityScheduled != nil {
			return h, runID, event.ID
		}
	}
	t.Fatal("activity was not scheduled")
	return nil, "", 0
}

func (h *buildRoutingHarness) events(runID string) []historypkg.Event {
	h.t.Helper()
	events, err := h.mgr.GetEvents(runID)
	if err != nil {
		h.t.Fatal(err)
	}
	return events
}

// decide 以 commands 完成 runID 当前打开的 Workflow Task；没有打开的就先调度一个。
func (h *buildRoutingHarness) decide(runID string, commands ...historypkg.Command) {
	h.t.Helper()
	wftID := openWorkflowTaskScheduledIDInEvents(h.events(runID))
	if wftID == 0 {
		wftID = appendOpenWorkflowTaskForTest(h.t, h.mgr, runID)
	}
	if err := h.server.HandleWorkflowTaskCompleted(h.server.WorkflowTaskContext(context.Background(), runID, wftID), runID, wftID, commands, ""); err != nil {
		h.t.Fatal(err)
	}
}

func cancelCharge() historypkg.Command {
	return historypkg.Command{Type: historypkg.CmdRequestCancelActivity, ActivityID: "charge"}
}

func lastEvent(events []historypkg.Event, typ historypkg.EventType) *historypkg.Event {
	for i := len(events) - 1; i >= 0; i-- {
		if events[i].EventType() == typ {
			return &events[i]
		}
	}
	return nil
}

// 未开始的 Activity：取消请求与 ActivityCancelled 同事务写入，调度一个 Workflow Task；排在队列里的投递不再执行。
func TestCancelActivityNotStartedIsCancelledImmediately(t *testing.T) {
	h, runID, scheduledID := newCancelHarness(t, "cancel-not-started", historypkg.Command{})
	h.decide(runID, cancelCharge())

	events := h.events(runID)
	requested := lastEvent(events, historypkg.EventActivityCancelRequested)
	cancelled := lastEvent(events, historypkg.EventActivityCancelled)
	if requested == nil || requested.ActivityCancelRequested.ScheduledEventID != scheduledID {
		t.Fatalf("ActivityCancelRequested = %+v, want scheduled event %d", requested, scheduledID)
	}
	if cancelled == nil || cancelled.ActivityCancelled.CancelRequestedEventID != requested.ID || cancelled.ID < requested.ID {
		t.Fatalf("ActivityCancelled = %+v, want it after the request", cancelled)
	}
	if state := h.state(runID, scheduledID); state.Status != execution.ActivityStatusEnums.Cancelled || !state.CancelRequested {
		t.Fatalf("activity state = %+v, want cancelled", state)
	}
	if openWorkflowTaskScheduledIDInEvents(h.events(runID)) == 0 {
		t.Fatal("no workflow task scheduled for the cancellation result")
	}
	task, ok := h.poll("pay-1", "pay@1", cancelQueue, time.Second)
	if !ok {
		t.Fatal("queued delivery missing")
	}
	if result := h.begin(task); result.Execute || result.Reason != "activity_closed" {
		t.Fatalf("begin after cancel = %+v, want activity_closed", result)
	}
}

// 已开始的 Activity：只记录请求，心跳返回 activity_cancel_requested；Worker 确认取消（CanceledError 失败上报）
// 时以带 details 的 ActivityCancelled 结束，并调度 Workflow Task。
func TestCancelStartedActivityIsAcknowledgedWithDetails(t *testing.T) {
	h, runID, scheduledID := newCancelHarness(t, "cancel-started", historypkg.Command{HeartbeatTimeout: time.Minute})
	task := h.mustClaim("pay-1", "pay@1", cancelQueue, 1)
	h.decide(runID, cancelCharge())

	if lastEvent(h.events(runID), historypkg.EventActivityCancelled) != nil {
		t.Fatal("started activity cancelled before the worker acknowledged")
	}
	heartbeat, err := h.tasks.RecordActivityHeartbeat(context.Background(), task, map[string]any{"progress": 40})
	if err != nil {
		t.Fatal(err)
	}
	if !heartbeat.CancelRequested || heartbeat.Reason != protocol.ActivityCancelRequestedReason {
		t.Fatalf("heartbeat = %+v, want activity cancel requested", heartbeat)
	}
	detail, err := protocol.NewJSONPayload("stopped at 40%")
	if err != nil {
		t.Fatal(err)
	}
	if err := h.tasks.FailActivityTask(context.Background(), task, protocol.Failure{
		Message: "canceled", Type: protocol.CanceledFailureType, NonRetryable: true, Details: protocol.Payloads{detail},
	}); err != nil {
		t.Fatal(err)
	}
	events := h.events(runID)
	cancelled := lastEvent(events, historypkg.EventActivityCancelled)
	if cancelled == nil || len(cancelled.ActivityCancelled.Details) != 1 || cancelled.ActivityCancelled.ScheduledEventID != scheduledID {
		t.Fatalf("ActivityCancelled = %+v, want the acknowledged details", cancelled)
	}
	if lastEvent(events, historypkg.EventActivityFailed) != nil {
		t.Fatal("acknowledged cancellation recorded as a failure")
	}
	if state := h.state(runID, scheduledID); state.Status != execution.ActivityStatusEnums.Cancelled {
		t.Fatalf("activity state = %+v, want cancelled", state)
	}
	if openWorkflowTaskScheduledIDInEvents(events) == 0 {
		t.Fatal("no workflow task scheduled for the cancellation result")
	}
}

// 取消请求之后已开始的 attempt 的结局：完成仍记为完成；失败、超时不再重试；Worker 停止交还时以取消结束。
func TestCancelRequestedActivityOutcomes(t *testing.T) {
	retry := &protocol.ParsedRetryPolicy{MaxAttempts: 5, InitialInterval: time.Second, BackoffCoefficient: 1}
	cases := []struct {
		name   string
		finish func(h *buildRoutingHarness, runID string, task protocol.Task)
		want   historypkg.EventType
	}{
		{"completion wins", func(h *buildRoutingHarness, runID string, task protocol.Task) {
			if err := h.server.HandleActivityCompleted(context.Background(), runID, task.ScheduledEventID, task.Attempt, task.Stamp, map[string]any{"ok": true}); err != nil {
				h.t.Fatal(err)
			}
		}, historypkg.EventActivityCompleted},
		{"failure is not retried", func(h *buildRoutingHarness, runID string, task protocol.Task) {
			if err := h.server.HandleActivityFailed(context.Background(), runID, task.ScheduledEventID, task.Attempt, task.Stamp, protocol.Failure{Message: "temporary"}); err != nil {
				h.t.Fatal(err)
			}
		}, historypkg.EventActivityFailed},
		{"timeout is not retried", func(h *buildRoutingHarness, runID string, task protocol.Task) {
			h.clk.Advance(time.Minute)
		}, historypkg.EventActivityTimedOut},
		{"released attempt is cancelled", func(h *buildRoutingHarness, runID string, task protocol.Task) {
			if err := h.server.HandleActivityReleased(context.Background(), runID, task.ScheduledEventID, task.Attempt, task.Stamp); err != nil {
				h.t.Fatal(err)
			}
		}, historypkg.EventActivityCancelled},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h, runID, scheduledID := newCancelHarness(t, "cancel-outcome", historypkg.Command{RetryPolicy: retry})
			task := h.mustClaim("pay-1", "pay@1", cancelQueue, 1)
			h.decide(runID, cancelCharge())
			tc.finish(h, runID, task)

			deadline := time.Now().Add(2 * time.Second)
			for !h.state(runID, scheduledID).IsTerminal() && time.Now().Before(deadline) {
				time.Sleep(5 * time.Millisecond)
			}
			state := h.state(runID, scheduledID)
			if !state.IsTerminal() || state.Attempt != 1 {
				t.Fatalf("activity state = %+v, want closed on attempt 1", state)
			}
			closed := lastEvent(h.events(runID), tc.want)
			if closed == nil {
				t.Fatalf("no %s event in %+v", tc.want, h.events(runID))
			}
			switch {
			case closed.ActivityFailed != nil && closed.ActivityFailed.RetryState != execution.ActivityCancelRetryState,
				closed.ActivityTimedOut != nil && closed.ActivityTimedOut.RetryState != execution.ActivityCancelRetryState:
				t.Fatalf("close event %+v, want retryState %s", closed, execution.ActivityCancelRetryState)
			}
		})
	}
}

// 重试退避中取消：Activity 立即以 ActivityCancelled 结束，退避到期后没有新的 attempt。
func TestCancelDuringRetryBackoffStopsFurtherAttempts(t *testing.T) {
	h, runID, scheduledID := newCancelHarness(t, "cancel-backoff", historypkg.Command{RetryPolicy: retryAfterSecond})
	first := h.mustClaim("pay-1", "pay@1", cancelQueue, 1)
	if err := h.server.HandleActivityFailed(context.Background(), runID, first.ScheduledEventID, first.Attempt, first.Stamp, protocol.Failure{Message: "temporary"}); err != nil {
		t.Fatal(err)
	}
	h.decide(runID, cancelCharge())
	if state := h.state(runID, scheduledID); state.Status != execution.ActivityStatusEnums.Cancelled || state.Attempt != 2 {
		t.Fatalf("activity state = %+v, want cancelled before attempt 2 started", state)
	}
	h.clk.Advance(time.Minute)
	if task, ok := h.poll("pay-1", "pay@1", cancelQueue, 100*time.Millisecond); ok {
		if result := h.begin(task); result.Execute {
			t.Fatalf("attempt %d started after cancellation", task.Attempt)
		}
	}
	if n := countEngineEvents(h.events(runID), historypkg.EventActivityCancelled); n != 1 {
		t.Fatalf("ActivityCancelled count = %d, want 1", n)
	}
}

// CancelTimer：TimerCancelled 写入，timer 状态为 cancelled，到期不再触发。
func TestCancelTimerStopsFiring(t *testing.T) {
	h := newBuildRoutingHarness(t)
	ref, err := h.server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{Type: "cancel-timer", Program: programtest.Noop()})
	if err != nil {
		t.Fatal(err)
	}
	run := &execution.Run{ID: "run-cancel-timer", Namespace: protocol.DefaultNamespace, WorkflowID: ref.WorkflowID, WorkflowType: ref.WorkflowID,
		WorkflowConfigVersion: ref.Version, Status: historypkg.StatusRunning, CreatedAt: h.clk.Now()}
	createStartedRunForTest(t, h.mgr, run)
	h.decide(run.ID, historypkg.Command{Type: historypkg.CmdScheduleTimer, TimerID: "#t1", Delay: time.Hour})
	h.decide(run.ID, historypkg.Command{Type: historypkg.CmdCancelTimer, TimerID: "#t1"})

	events := h.events(run.ID)
	scheduled := lastEvent(events, historypkg.EventTimerScheduled)
	cancelled := lastEvent(events, historypkg.EventTimerCancelled)
	if cancelled == nil || cancelled.TimerCancelled.ScheduledEventID != scheduled.ID {
		t.Fatalf("TimerCancelled = %+v, want it to reference %d", cancelled, scheduled.ID)
	}
	timer, ok, err := h.mgr.GetTimerState(run.ID, scheduled.ID)
	if err != nil || !ok || timer.Status != execution.TimerStatusEnums.Cancelled {
		t.Fatalf("timer state = %+v ok=%v err=%v, want cancelled", timer, ok, err)
	}
	h.clk.Advance(2 * time.Hour)
	time.Sleep(50 * time.Millisecond)
	if n := countEngineEvents(h.events(run.ID), historypkg.EventTimerFired); n != 0 {
		t.Fatalf("cancelled timer fired %d times", n)
	}
}

// 子 Workflow 的取消请求送到子 Run，子关联记下请求；随后父 Run 以 Cancelled 关闭时按父关闭策略处理：
// Terminate 终止子 Run，RequestCancel 不重复请求，Abandon 不处理。
func TestCancelChildWorkflowComposesWithParentClosePolicy(t *testing.T) {
	policies := []protocol.ParentClosePolicy{protocol.ParentClosePolicyTerminate, protocol.ParentClosePolicyRequestCancel, protocol.ParentClosePolicyAbandon}
	for _, policy := range policies {
		t.Run(string(policy), func(t *testing.T) {
			tree := newPolicyTree(t, "parent-cancel-"+string(policy))
			childRunID := tree.startChild(t, tree.parentRunID, tree.childType, "#c1", policy)
			tree.completeTask(t, tree.parentRunID, historypkg.Command{Type: historypkg.CmdRequestCancelChildWorkflow, ChildID: "#c1"})

			parentEvents, err := tree.mgr.GetEvents(tree.parentRunID)
			if err != nil {
				t.Fatal(err)
			}
			requested := lastEvent(parentEvents, historypkg.EventChildWorkflowCancelRequested)
			if requested == nil || requested.ChildWorkflowCancelRequested.ChildRunID != childRunID {
				t.Fatalf("ChildWorkflowCancelRequested = %+v, want child run %s", requested, childRunID)
			}
			if link := tree.link(t, tree.parentRunID, "#c1"); !link.CancelRequested {
				t.Fatalf("child link = %+v, want CancelRequested", link)
			}
			childState, err := tree.mgr.GetWorkflowExecutionState(childRunID)
			if err != nil || !childState.CancelRequested {
				t.Fatalf("child state = %+v err=%v, want the cancellation request", childState, err)
			}

			// 父 Workflow 不等子 Workflow，返回取消错误：父 Run 以 Cancelled 关闭。
			tree.completeTask(t, tree.parentRunID, historypkg.Command{Type: historypkg.CmdCancelWorkflow})
			child, err := tree.mgr.Get(childRunID)
			if err != nil {
				t.Fatal(err)
			}
			childEvents, err := tree.mgr.GetEvents(childRunID)
			if err != nil {
				t.Fatal(err)
			}
			requests := countEngineEvents(childEvents, historypkg.EventWorkflowCancellationRequested)
			switch policy {
			case protocol.ParentClosePolicyTerminate:
				if child.Status != historypkg.StatusTerminated {
					t.Fatalf("child status = %s, want terminated", child.Status)
				}
			default:
				if child.Status != historypkg.StatusRunning || requests != 1 {
					t.Fatalf("child status = %s with %d cancellation requests, want running with exactly one", child.Status, requests)
				}
			}
		})
	}
}

// 父 Run 记下了取消请求而子 Run 没有收到（提交后、发出请求之前进程退出，或子 Run 在请求之后才建好）：
// 子 Run 的恢复按子关联补发取消请求。
func TestChildCancelRequestIsRecovered(t *testing.T) {
	tree := newPolicyTree(t, "parent-cancel-recover")
	child := &execution.Run{ID: "run-child-cancel-recover", Namespace: protocol.DefaultNamespace, WorkflowID: "child-cancel-recover",
		WorkflowType: tree.childType, ParentRunID: tree.parentRunID, ParentScheduledEventID: 99,
		Status: historypkg.StatusRunning, CreatedAt: time.Now()}
	createStartedRunForTest(t, tree.mgr, child)
	if err := tree.mgr.RunInTx(func(tx execution.ExecutionTx) error {
		return tx.UpsertChildWorkflowState(&execution.ChildWorkflowState{
			RunID: tree.parentRunID, ScheduledEventID: 99, ChildID: "#c9", ChildRunID: child.ID, WorkflowID: tree.childType,
			Status: execution.ChildWorkflowStatusEnums.Started, CancelRequested: true, CreatedAt: time.Now(), UpdatedAt: time.Now(),
		})
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := tree.srv.recoverParentClosePolicy(context.Background(), child); err != nil {
		t.Fatal(err)
	}
	state, err := tree.mgr.GetWorkflowExecutionState(child.ID)
	if err != nil || !state.CancelRequested {
		t.Fatalf("child state after recovery = %+v err=%v, want the cancellation request", state.CancelRequested, err)
	}
}

// 重复的取消请求只写一次 WorkflowCancellationRequested；已关闭的 Run 拒绝取消请求。
func TestCancelWorkflowIsIdempotentAndRejectsClosedRun(t *testing.T) {
	srv, mgr := newServerForTest(t)
	ref, err := srv.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{Type: "cancel-twice", Program: programtest.Noop()})
	if err != nil {
		t.Fatal(err)
	}
	run := &execution.Run{ID: "run-cancel-twice", Namespace: protocol.DefaultNamespace, WorkflowID: ref.WorkflowID, WorkflowType: ref.WorkflowID,
		WorkflowConfigVersion: ref.Version, Status: historypkg.StatusRunning, CreatedAt: time.Now()}
	createStartedRunForTest(t, mgr, run)
	ctx := context.Background()
	for range 2 {
		if err := srv.CancelWorkflow(ctx, "", "", run.ID, "stop"); err != nil {
			t.Fatal(err)
		}
	}
	events, err := mgr.GetEvents(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if n := countEngineEvents(events, historypkg.EventWorkflowCancellationRequested); n != 1 {
		t.Fatalf("WorkflowCancellationRequested count = %d, want 1", n)
	}
	if err := srv.TerminateWorkflow(ctx, "", "", run.ID, "done"); err != nil {
		t.Fatal(err)
	}
	other := &execution.Run{ID: "run-cancel-closed", Namespace: protocol.DefaultNamespace, WorkflowID: ref.WorkflowID + "-closed", WorkflowType: ref.WorkflowID,
		WorkflowConfigVersion: ref.Version, Status: historypkg.StatusRunning, CreatedAt: time.Now()}
	createStartedRunForTest(t, mgr, other)
	if err := srv.TerminateWorkflow(ctx, "", "", other.ID, "done"); err != nil {
		t.Fatal(err)
	}
	if err := srv.CancelWorkflow(ctx, "", "", other.ID, "late"); err == nil {
		t.Fatal("cancel request on a closed run was accepted")
	}
}
