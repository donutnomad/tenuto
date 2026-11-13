package engine

import (
	"context"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/protocol"
	historypkg "github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/sdk/activity"
	"github.com/donutnomad/tenuto/server/biz/domain/clock/clocktest"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/biz/usecase/activitytasks"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

// Activity 任务不绑定 build：每次投递只按队列（与 Run 的目标 Worker）路由，队列上任何 build 的 Worker 都能领取，
// 契约在 Worker 开始 attempt 时按它自己的上报校验。以下场景用内存队列与假时钟驱动 Server，Worker 的领取与开始
// 直接调用 PollActivityTask 与 BeginActivityTask。

type buildRoutingHarness struct {
	t      *testing.T
	mgr    *memory.ExecutionManager
	server *Server
	clk    *clocktest.Fake
	tasks  *activitytasks.UseCase
}

func newBuildRoutingHarness(t *testing.T) *buildRoutingHarness {
	t.Helper()
	clk := clocktest.NewFake(testClockStart)
	mgr := memory.NewExecutionManager()
	mgr.WithClock(clk)
	deps := assembleServerDeps(mgr, newTestTaskQueue(), memory.NewActivityQueueRouter(16))
	deps.Clock = clk
	server := NewServer(deps)
	t.Cleanup(server.Stop)
	return &buildRoutingHarness{t: t, mgr: mgr, server: server, clk: clk, tasks: activitytasks.NewUseCase(server)}
}

// chargeContract 是 payments.charge 在 version 这一版的契约。
func chargeContract(version string) activity.Contract {
	return activity.Contract{Type: "payments.charge", Version: version, Input: "ChargeInput." + version, Output: "Receipt." + version}
}

func (h *buildRoutingHarness) register(identity, build, queue string, contract activity.Contract) {
	h.t.Helper()
	if err := h.server.RegisterActivityWorkerVersion(context.Background(), protocol.ActivityWorkerVersion{
		Identity: identity, BuildID: build, TaskQueues: []string{queue},
		ActivityTypes: []string{contract.Type}, Contracts: []activity.Contract{contract},
	}); err != nil {
		h.t.Fatal(err)
	}
}

func (h *buildRoutingHarness) deregister(identity, build string) {
	h.t.Helper()
	if err := h.server.DeregisterActivityWorkerVersionInNamespace(context.Background(), protocol.DefaultNamespace, identity, build); err != nil {
		h.t.Fatal(err)
	}
}

// startRun 启动一个声明 version 契约的 Run，policy 是 Run 的版本策略，并让它调度 payments.charge。
func (h *buildRoutingHarness) startRun(id, policy, queue, version string, cmd historypkg.Command) string {
	h.t.Helper()
	wf := contractWorkflowConfig(id, map[string]program.ActivityRef{
		"charge": {Type: "payments.charge", TaskQueue: queue, Version: version, Input: "ChargeInput." + version, Output: "Receipt." + version},
	})
	ref, err := h.server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, wf)
	if err != nil {
		h.t.Fatal(err)
	}
	run := &execution.Run{
		ID: "run-" + id, Namespace: protocol.DefaultNamespace, WorkflowID: ref.WorkflowID, WorkflowType: ref.WorkflowID,
		WorkflowConfigVersion: ref.Version, Policy: policy, Status: historypkg.StatusRunning, CreatedAt: h.clk.Now(),
	}
	createStartedRunForTest(h.t, h.mgr, run)
	wftID := appendOpenWorkflowTaskForTest(h.t, h.mgr, run.ID)
	cmd.Type, cmd.ActivityID, cmd.ActivityType = historypkg.CmdScheduleActivity, "charge", "payments.charge"
	if cmd.StartToCloseTimeout == 0 {
		cmd.StartToCloseTimeout = time.Minute
	}
	if err := h.server.HandleWorkflowTaskCompleted(h.server.WorkflowTaskContext(context.Background(), run.ID, wftID), run.ID, wftID, []historypkg.Command{cmd}, ""); err != nil {
		h.t.Fatal(err)
	}
	return run.ID
}

// poll 以 identity/build 的身份从 queue 领取一个 Activity Task；wait 内没有任务时 ok 为 false。
func (h *buildRoutingHarness) poll(identity, build, queue string, wait time.Duration) (protocol.Task, bool) {
	h.t.Helper()
	ctx := protocol.WithWorkerInfo(protocol.WithNamespace(context.Background(), protocol.DefaultNamespace),
		protocol.WorkerInfo{Namespace: protocol.DefaultNamespace, Identity: identity, BuildID: build})
	ctx, cancel := context.WithTimeout(ctx, wait)
	defer cancel()
	lease, err := h.server.PollActivityTask(ctx, queue)
	if err != nil {
		return protocol.Task{}, false
	}
	task := lease.Task
	task.WorkerIdentity, task.WorkerBuildID = identity, build
	return task, true
}

func (h *buildRoutingHarness) begin(task protocol.Task) protocol.ActivityTaskBeginResult {
	h.t.Helper()
	result, err := h.tasks.BeginActivityTask(context.Background(), task)
	if err != nil {
		h.t.Fatal(err)
	}
	return result
}

func (h *buildRoutingHarness) state(runID string, scheduledEventID int64) *execution.ActivityState {
	h.t.Helper()
	state, ok, err := h.server.GetActivityState(runID, scheduledEventID)
	if err != nil || !ok {
		h.t.Fatalf("activity state: ok=%v err=%v", ok, err)
	}
	return state
}

// mustClaim 断言 identity/build 从 queue 领到 attempt 并开始执行。
func (h *buildRoutingHarness) mustClaim(identity, build, queue string, attempt int) protocol.Task {
	h.t.Helper()
	task, ok := h.poll(identity, build, queue, 2*time.Second)
	if !ok {
		h.t.Fatalf("%s/%s received no task on %s", identity, build, queue)
	}
	if task.Attempt != attempt {
		h.t.Fatalf("%s/%s received attempt %d, want %d", identity, build, task.Attempt, attempt)
	}
	if result := h.begin(task); !result.Execute {
		h.t.Fatalf("%s/%s could not begin attempt %d: %+v", identity, build, attempt, result)
	}
	return task
}

var retryAfterSecond = &protocol.ParsedRetryPolicy{MaxAttempts: 5, InitialInterval: time.Second, BackoffCoefficient: 1}

// 滚动发布：v1 与 v2 上报相同的契约。v1 上的 attempt 失败、v1 下线之后，重试由 v2 领取并执行；Run 的策略不影响路由。
func TestRollingUpgradeRetryIsClaimedByNewBuild(t *testing.T) {
	for _, policy := range []string{execution.RunPolicyAutoUpgrade, execution.RunPolicyPinned} {
		t.Run(policy, func(t *testing.T) {
			h := newBuildRoutingHarness(t)
			h.register("pay-v1", "pay@v1", "payments", chargeContract("v1"))
			runID := h.startRun("rolling-retry-"+policy, policy, "payments", "v1", historypkg.Command{RetryPolicy: retryAfterSecond})

			first := h.mustClaim("pay-v1", "pay@v1", "payments", 1)
			if err := h.server.HandleActivityFailed(context.Background(), runID, first.ScheduledEventID, first.Attempt, first.Stamp, protocol.Failure{Message: "temporary"}); err != nil {
				t.Fatal(err)
			}
			h.deregister("pay-v1", "pay@v1")
			h.register("pay-v2", "pay@v2", "payments", chargeContract("v1"))

			h.clk.Advance(time.Second)
			retry := h.mustClaim("pay-v2", "pay@v2", "payments", 2)
			if state := h.state(runID, retry.ScheduledEventID); state.WorkerIdentity != "pay-v2" || state.WorkerBuildID != "pay@v2" {
				t.Fatalf("attempt record = %s/%s, want pay-v2/pay@v2", state.WorkerIdentity, state.WorkerBuildID)
			}
		})
	}
}

// 滚动发布：v1 在线时调度、投递已在队列里，v1 下线后 v2 领取这份投递。
func TestRollingUpgradeQueuedDeliveryIsClaimedByNewBuild(t *testing.T) {
	h := newBuildRoutingHarness(t)
	h.register("pay-v1", "pay@v1", "payments", chargeContract("v1"))
	h.startRun("rolling-queued", execution.RunPolicyAutoUpgrade, "payments", "v1", historypkg.Command{})
	h.deregister("pay-v1", "pay@v1")
	h.register("pay-v2", "pay@v2", "payments", chargeContract("v1"))

	h.mustClaim("pay-v2", "pay@v2", "payments", 1)
}

// 滚动发布：v1 停止时交还已开始的 attempt，重新派发的同一个 attempt 由 v2 领取。
func TestRollingUpgradeReleasedAttemptIsClaimedByNewBuild(t *testing.T) {
	h := newBuildRoutingHarness(t)
	h.register("pay-v1", "pay@v1", "payments", chargeContract("v1"))
	runID := h.startRun("rolling-release", execution.RunPolicyAutoUpgrade, "payments", "v1", historypkg.Command{})
	started := h.mustClaim("pay-v1", "pay@v1", "payments", 1)
	h.deregister("pay-v1", "pay@v1")
	if err := h.server.HandleActivityReleased(context.Background(), runID, started.ScheduledEventID, started.Attempt, started.Stamp); err != nil {
		t.Fatal(err)
	}
	h.register("pay-v2", "pay@v2", "payments", chargeContract("v1"))

	h.mustClaim("pay-v2", "pay@v2", "payments", 1)
}

// 独立 Activity：Run 调用的 Activity 由另一个程序的 Worker 实现。那个程序换了 build 之后，Run 的 Activity 由它的
// 当前 build 领取。
func TestIndependentActivityIsClaimedByOtherProgramsCurrentBuild(t *testing.T) {
	h := newBuildRoutingHarness(t)
	h.register("ledger-1", "ledger@v6", "ledger", chargeContract("v1"))
	h.startRun("independent", execution.RunPolicyPinned, "ledger", "v1", historypkg.Command{})
	h.deregister("ledger-1", "ledger@v6")
	h.register("ledger-1", "ledger@v7", "ledger", chargeContract("v1"))

	h.mustClaim("ledger-1", "ledger@v7", "ledger", 1)
}

// 契约不兼容的 Worker 领到任务时不开始 attempt：attempt 不消耗，这份投递丢弃，Server 在重派延迟之后重新投递。
// 只有不兼容的 Worker 在线、时钟不动时，它只拿到一次；兼容的 build 回来后在下一次投递领取。
func TestIncompatibleClaimIsRefusedWithoutSpinning(t *testing.T) {
	h := newBuildRoutingHarness(t)
	h.register("pay-v1", "pay@v1", "payments", chargeContract("v1"))
	runID := h.startRun("incompatible-claim", execution.RunPolicyPinned, "payments", "v1", historypkg.Command{})
	h.deregister("pay-v1", "pay@v1")
	h.register("pay-v2", "pay@v2", "payments", chargeContract("v2"))

	task, ok := h.poll("pay-v2", "pay@v2", "payments", 2*time.Second)
	if !ok {
		t.Fatal("the queued task was not offered to the v2 worker")
	}
	if result := h.begin(task); result.Execute || result.Reason != activitytasks.ReasonWorkerContractMismatch {
		t.Fatalf("incompatible begin = %+v, want refused with %s", result, activitytasks.ReasonWorkerContractMismatch)
	}
	state := h.state(runID, task.ScheduledEventID)
	if state.Attempt != 1 || state.Stamp != task.Stamp || state.StartedEventID != execution.EmptyEventID || !state.IsScheduled() {
		t.Fatalf("refused claim changed the attempt: %+v", state)
	}
	// 时钟不动：拒绝之后没有新的投递，不兼容的 Worker 不会反复领到同一个任务。
	for range 3 {
		if again, ok := h.poll("pay-v2", "pay@v2", "payments", 50*time.Millisecond); ok {
			t.Fatalf("refused task was redelivered without delay: %+v", again)
		}
	}

	h.register("pay-v1", "pay@v1", "payments", chargeContract("v1"))
	h.clk.Advance(time.Second)
	claimed := h.mustClaim("pay-v1", "pay@v1", "payments", 1)
	if claimed.Stamp != task.Stamp {
		t.Fatalf("redelivered stamp = %d, want %d", claimed.Stamp, task.Stamp)
	}
}

// pinned Run 的 Activity 一直等契约兼容的 build：同一队列上不兼容的 v2 拿不到执行权，ScheduleToStartTimeout 限定等待。
func TestPinnedRunActivityWaitsForCompatibleBuildUntilScheduleToStart(t *testing.T) {
	h := newBuildRoutingHarness(t)
	h.register("pay-v1", "pay@v1", "payments", chargeContract("v1"))
	runID := h.startRun("pinned-wait", execution.RunPolicyPinned, "payments", "v1", historypkg.Command{ScheduleToStartTimeout: 10 * time.Second})
	h.deregister("pay-v1", "pay@v1")
	h.register("pay-v2", "pay@v2", "payments", chargeContract("v2"))

	task, ok := h.poll("pay-v2", "pay@v2", "payments", 2*time.Second)
	if !ok {
		t.Fatal("the queued task was not offered to the v2 worker")
	}
	if result := h.begin(task); result.Execute {
		t.Fatalf("v2 began the pinned run's activity: %+v", result)
	}
	h.clk.Advance(10 * time.Second)
	deadline := time.Now().Add(2 * time.Second)
	for {
		state := h.state(runID, task.ScheduledEventID)
		if state.IsTerminal() {
			if state.Attempt != 1 {
				t.Fatalf("timed out at attempt %d, want 1", state.Attempt)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("activity did not time out on ScheduleToStart: %+v", state)
		}
		time.Sleep(5 * time.Millisecond)
	}
	events, err := h.mgr.GetEvents(runID)
	if err != nil {
		t.Fatal(err)
	}
	timedOut := false
	for _, event := range events {
		if event.ActivityTimedOut != nil && event.ActivityTimedOut.TimeoutType == protocol.TimeoutTypeScheduleToStart {
			timedOut = true
		}
	}
	if !timedOut {
		t.Fatal("history has no ScheduleToStart ActivityTimedOut")
	}
}
