package engine

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/program/programtest"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/definition"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
)

// updateWithStartTestEnv 搭一个带已注册 config 的最小 server + runtime，供 UpdateWithStart 测试用。
// 与 updateWaitTestEnv 不同：这里不预建 run，而是让 UpdateWithStart 自己 start（走 config → WorkflowStarted）。
type updateWithStartTestEnv struct {
	server     *Server
	runtime    *Server
	mgr        *failingEventManager
	wfQueue    *testTaskQueue
	workflowID string
}

type failingActiveConfigStore struct {
	definition.ConfigRepository
	err error
}

func (s failingActiveConfigStore) LatestWorkflowConfigRefInNamespace(string, string) (definition.WorkflowConfigRef, error) {
	return definition.WorkflowConfigRef{}, s.err
}

func newUpdateWithStartTestEnv(t *testing.T, workflowID string) *updateWithStartTestEnv {
	t.Helper()
	mgr := newFailingEventManager()
	wfQueue := newTestTaskQueue()
	server := assembleServer(mgr, wfQueue, newTestTaskQueue())
	if _, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{Type: workflowID, Program: programtest.Noop()}); err != nil {
		t.Fatal(err)
	}
	return &updateWithStartTestEnv{
		server:     server,
		runtime:    server,
		mgr:        mgr,
		wfQueue:    wfQueue,
		workflowID: workflowID,
	}
}

// countStartedEvents 统计该 run 历史里 WorkflowStarted / WorkflowUpdateAccepted 事件条数。
func countStartedEvents(t *testing.T, srv *Server, runID string) (started, accepted int) {
	t.Helper()
	events, err := srv.GetEvents(runID)
	if err != nil {
		t.Fatal(err)
	}
	for _, ev := range events {
		if ev.WorkflowStarted != nil {
			started++
		}
		if ev.WorkflowUpdateAccepted != nil {
			accepted++
		}
	}
	return started, accepted
}

// driveUpdateCompleteForRun 与 updateWaitTestEnv.driveUpdateComplete 同构，但显式传 runID
// （UpdateWithStart 场景里 run 由 start 分支创建，runID 事先不知道，靠轮询 current 发现）。
func (e *updateWithStartTestEnv) driveUpdateCompleteForCurrent(t *testing.T, ctx context.Context, output map[string]any) <-chan struct{} {
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
			runID, err := e.server.mgr.GetCurrentRunID(protocol.DefaultNamespace, e.workflowID)
			if err != nil {
				time.Sleep(time.Millisecond)
				continue
			}
			events, err := e.server.GetEvents(runID)
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
			if err := e.server.HandleWorkflowTaskCompleted(e.server.WorkflowTaskContext(ctx, runID, scheduledEventID), runID, scheduledEventID, []history.Command{{
				Type:          history.CmdCompleteWorkflowUpdate,
				Name:          "set_priority",
				UpdateID:      pendingUpdateID,
				Output:        output,
				UpdateOutcome: updateSuccessOutcome(t, output),
			}}, ""); err == nil {
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()
	return done
}

// TestUpdateWithStart_NewWorkflow_StartsAndAccepts
// Happy Path: workflow 不存在 → UpdateWithStart 先 start（产 WorkflowStarted）再 accept（产 WorkflowUpdateAccepted），
// 历史恰好一条 WorkflowStarted + 一条 WorkflowUpdateAccepted，run 进入 Running。
func TestUpdateWithStart_NewWorkflow_StartsAndAccepts(t *testing.T) {
	env := newUpdateWithStartTestEnv(t, "wf-uws-new")
	ctx := context.Background()

	runID, result, err := env.runtime.UpdateWithStart(ctx, "", env.workflowID, env.workflowID,
		map[string]any{"seed": 1}, "set_priority", "up-new", updateArgs(map[string]any{"priority": "high"}),
		protocol.UpdateWaitStageEnums.Accepted)
	if err != nil {
		t.Fatal(err)
	}
	if runID == "" {
		t.Fatal("expected non-empty runID from start")
	}
	if result.UpdateID != "up-new" || result.AcceptedEventID == 0 {
		t.Fatalf("unexpected result: %+v", result)
	}
	if result.Stage != protocol.UpdateReachedStageEnums.Accepted {
		t.Fatalf("stage should be Accepted, got %q", result.Stage)
	}

	started, accepted := countStartedEvents(t, env.server, runID)
	if started != 1 {
		t.Fatalf("expected exactly 1 WorkflowStarted, got %d", started)
	}
	if accepted != 1 {
		t.Fatalf("expected exactly 1 WorkflowUpdateAccepted, got %d", accepted)
	}

	// current run 指向新建的 run，且状态为 Running。
	cur, err := env.server.mgr.GetCurrentRunID(protocol.DefaultNamespace, env.workflowID)
	if err != nil {
		t.Fatal(err)
	}
	if cur != runID {
		t.Fatalf("current run %q != started run %q", cur, runID)
	}
	status, err := env.server.GetRunStatus(runID)
	if err != nil {
		t.Fatal(err)
	}
	if status != history.StatusRunning {
		t.Fatalf("expected Running, got %q", status)
	}
}

func TestUpdateWithStart_PreservesZeroArgs(t *testing.T) {
	env := newUpdateWithStartTestEnv(t, "wf-uws-payloads")
	runID, result, err := env.runtime.UpdateWithStart(
		context.Background(), "", env.workflowID, env.workflowID, map[string]any{"seed": 1},
		"create", "up-payloads", nil, protocol.UpdateWaitStageEnums.Accepted,
	)
	if err != nil {
		t.Fatal(err)
	}
	if result.Stage != protocol.UpdateReachedStageEnums.Accepted {
		t.Fatalf("unexpected result: %+v", result)
	}
	events, err := env.server.GetEvents(runID)
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range events {
		if accepted := event.WorkflowUpdateAccepted; accepted != nil {
			if accepted.Args == nil || len(*accepted.Args) != 0 {
				t.Fatalf("zero args lost wire presence: %+v", accepted)
			}
			return
		}
	}
	t.Fatalf("accepted event missing: %+v", events)
}

func TestUpdateWithStartSeparatesWorkflowTypeAndExecutionID(t *testing.T) {
	env := newUpdateWithStartTestEnv(t, "approval-workflow")
	ctx := context.Background()
	activeRef, err := definition.ActiveWorkflowRef(env.server.workflowConfigs, protocol.DefaultNamespace, "approval-workflow")
	if err != nil {
		t.Fatal(err)
	}

	runID, result, err := env.runtime.UpdateWithStart(
		ctx, "", "approval-workflow", "create-rec-1", map[string]any{"record_id": "rec-1"},
		"create-record", "update-rec-1", nil, protocol.UpdateWaitStageEnums.Accepted,
	)
	if err != nil {
		t.Fatal(err)
	}
	if result.UpdateID != "update-rec-1" || result.Stage != protocol.UpdateReachedStageEnums.Accepted {
		t.Fatalf("result = %+v", result)
	}
	run, err := env.server.mgr.Get(runID)
	if err != nil {
		t.Fatal(err)
	}
	if run.WorkflowID != "create-rec-1" || run.WorkflowType != "approval-workflow" {
		t.Fatalf("run identity = %+v", run)
	}
	if run.WorkflowConfigVersion != activeRef.Version {
		t.Fatalf("run version = %q, want active %q", run.WorkflowConfigVersion, activeRef.Version)
	}
	loaded, err := env.server.GetWorkflowConfig(context.Background(), runID, "")
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Type != "approval-workflow" {
		t.Fatalf("loaded workflow = %q", loaded.Type)
	}

	sameRunID, _, err := env.runtime.UpdateWithStart(
		ctx, "", "approval-workflow", "create-rec-1", map[string]any{"record_id": "ignored"},
		"create-record", "update-rec-2", nil, protocol.UpdateWaitStageEnums.Accepted,
	)
	if err != nil {
		t.Fatal(err)
	}
	if sameRunID != runID {
		t.Fatalf("UseExisting run = %q, want %q", sameRunID, runID)
	}

	otherRunID, _, err := env.runtime.UpdateWithStart(
		ctx, "", "approval-workflow", "create-rec-2", map[string]any{"record_id": "rec-2"},
		"create-record", "update-rec-3", nil, protocol.UpdateWaitStageEnums.Accepted,
	)
	if err != nil {
		t.Fatal(err)
	}
	if otherRunID == runID {
		t.Fatal("distinct execution workflow IDs shared a run")
	}
}

func TestUpdateWithStartPropagatesActiveConfigStoreFailure(t *testing.T) {
	env := newUpdateWithStartTestEnv(t, "wf-config-store-failure")
	storeErr := errors.New("config store unavailable")
	env.server.workflowConfigs = failingActiveConfigStore{ConfigRepository: env.server.workflowConfigs, err: storeErr}

	_, _, err := env.runtime.UpdateWithStart(
		context.Background(), "", env.workflowID, env.workflowID, nil,
		"update", "update-store-failure", nil, protocol.UpdateWaitStageEnums.Accepted,
	)
	if !errors.Is(err, storeErr) {
		t.Fatalf("error = %v", err)
	}
	if _, err := env.server.mgr.GetCurrentRunID(protocol.DefaultNamespace, env.workflowID); !errors.Is(err, execution.ErrNoCurrentRun) {
		t.Fatalf("store failure left a current run: %v", err)
	}
}

// TestUpdateWithStart_ExistingWorkflow_UseExisting
// Happy Path: workflow 已在跑 → UseExisting 语义：复用 current run，只 accept update，不重复 start
// （历史里无第二个 WorkflowStarted，runID 不变）。
func TestUpdateWithStart_ExistingWorkflow_UseExisting(t *testing.T) {
	env := newUpdateWithStartTestEnv(t, "wf-uws-existing")
	ctx := context.Background()

	// 先手动 start 一个 run（模拟 workflow 已存在）。
	existingRunID, err := startedRunID(env.server.StartWorkflow(ctx, "", env.workflowID, env.workflowID, "", map[string]any{"seed": 0}))
	if err != nil {
		t.Fatal(err)
	}
	startedBefore, _ := countStartedEvents(t, env.server, existingRunID)
	if startedBefore != 1 {
		t.Fatalf("precondition: expected 1 WorkflowStarted, got %d", startedBefore)
	}

	// UpdateWithStart：workflow 已存在 → 复用 current run，不重复 start。
	runID, result, err := env.runtime.UpdateWithStart(ctx, "", env.workflowID, env.workflowID,
		map[string]any{"seed": 99}, "set_priority", "up-existing", updateArgs(map[string]any{"priority": "low"}),
		protocol.UpdateWaitStageEnums.Accepted)
	if err != nil {
		t.Fatal(err)
	}
	if runID != existingRunID {
		t.Fatalf("UseExisting must reuse current run: got %q want %q", runID, existingRunID)
	}
	if result.Stage != protocol.UpdateReachedStageEnums.Accepted {
		t.Fatalf("stage should be Accepted, got %q", result.Stage)
	}

	// 关键断言：历史里仍只有一条 WorkflowStarted（未重复 start），且有一条 accepted。
	started, accepted := countStartedEvents(t, env.server, runID)
	if started != 1 {
		t.Fatalf("UseExisting must not re-start: expected 1 WorkflowStarted, got %d", started)
	}
	if accepted != 1 {
		t.Fatalf("expected 1 WorkflowUpdateAccepted, got %d", accepted)
	}
}

// TestUpdateWithStart_Completed_ReturnsInitResult
// Integration: stage=Completed → 后台 driver 完成 handler → long-poll 拿到 handler 回传的结果。
func TestUpdateWithStart_Completed_ReturnsInitResult(t *testing.T) {
	env := newUpdateWithStartTestEnv(t, "wf-uws-completed")
	ctx := context.Background()

	driverDone := env.driveUpdateCompleteForCurrent(t, ctx, map[string]any{"init": "ok"})

	runID, result, err := env.runtime.UpdateWithStart(ctx, "", env.workflowID, env.workflowID,
		map[string]any{"seed": 1}, "set_priority", "up-completed", updateArgs(map[string]any{"priority": "high"}),
		protocol.UpdateWaitStageEnums.Completed)
	if err != nil {
		t.Fatal(err)
	}
	<-driverDone

	if runID == "" {
		t.Fatal("expected non-empty runID")
	}
	if result.Stage != protocol.UpdateReachedStageEnums.Completed {
		t.Fatalf("expected Completed stage, got %q (result=%+v)", result.Stage, result)
	}
	if result.Outcome == nil || result.Outcome.Success == nil || result.Outcome.Failure != nil {
		t.Fatalf("expected success outcome, got %+v", result.Outcome)
	}
	if updateOutput(result)["init"] != "ok" {
		t.Fatalf("expected output init=ok, got %+v", updateOutput(result))
	}
	if env.server.runWatch.EntryCount() != 0 {
		t.Fatalf("waiter must be released, count=%d", env.server.runWatch.EntryCount())
	}
}

// TestUpdateWithStart_Atomic
// Edge Case: start + accept 在同一 keyed lock 内原子完成——两者之间无别的请求插入窗口。
// 用一个"在 start 后、accept 前无法观察到中间态"的间接断言：并发多个 UpdateWithStart（不同 updateID）
// 到同一个新 workflow，最终只有一条 WorkflowStarted（首个 start 建 run，其余走 UseExisting），
// 且每个 updateID 各产一条 accepted，无重复 start。
func TestUpdateWithStart_Atomic(t *testing.T) {
	env := newUpdateWithStartTestEnv(t, "wf-uws-atomic")
	ctx := context.Background()

	const n = 8
	var wg sync.WaitGroup
	runIDs := make([]string, n)
	for i := range n {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			runID, _, err := env.runtime.UpdateWithStart(ctx, "", env.workflowID, env.workflowID,
				map[string]any{"seed": i}, "set_priority",
				"up-atomic-"+string(rune('a'+i)), updateArgs(map[string]any{"priority": "high"}),
				protocol.UpdateWaitStageEnums.Accepted)
			if err != nil {
				t.Errorf("UpdateWithStart[%d] failed: %v", i, err)
				return
			}
			runIDs[i] = runID
		}(i)
	}
	wg.Wait()

	// 全部指向同一个 run（首个 start 建 run，其余 UseExisting）。
	cur, err := env.server.mgr.GetCurrentRunID(protocol.DefaultNamespace, env.workflowID)
	if err != nil {
		t.Fatal(err)
	}
	for i, rid := range runIDs {
		if rid != cur {
			t.Fatalf("run[%d]=%q must equal current %q (atomic start+UseExisting)", i, rid, cur)
		}
	}

	// 关键原子性断言：只有一条 WorkflowStarted（start 与 accept 同锁，无并发重复 start），
	// 且恰好 n 条 accepted（每个 updateID 一条，无丢失、无重复）。
	started, accepted := countStartedEvents(t, env.server, cur)
	if started != 1 {
		t.Fatalf("atomicity broken: expected exactly 1 WorkflowStarted, got %d", started)
	}
	if accepted != n {
		t.Fatalf("expected %d accepted (one per updateID), got %d", n, accepted)
	}
}

// TestUpdateWithStart_Idempotent
// Happy Path: 重复调用同 updateID → 只一条 accepted（AppendIdempotent 的 DedupeKey 去重）。
func TestUpdateWithStart_Idempotent(t *testing.T) {
	env := newUpdateWithStartTestEnv(t, "wf-uws-idem")
	ctx := context.Background()
	const updateID = "up-idem"

	runID1, _, err := env.runtime.UpdateWithStart(ctx, "", env.workflowID, env.workflowID,
		map[string]any{"seed": 1}, "set_priority", updateID, updateArgs(map[string]any{"priority": "high"}),
		protocol.UpdateWaitStageEnums.Accepted)
	if err != nil {
		t.Fatal(err)
	}
	runID2, _, err := env.runtime.UpdateWithStart(ctx, "", env.workflowID, env.workflowID,
		map[string]any{"seed": 2}, "set_priority", updateID, updateArgs(map[string]any{"priority": "high"}),
		protocol.UpdateWaitStageEnums.Accepted)
	if err != nil {
		t.Fatal(err)
	}
	if runID1 != runID2 {
		t.Fatalf("same workflow must reuse run: %q != %q", runID1, runID2)
	}
	started, accepted := countStartedEvents(t, env.server, runID1)
	if started != 1 {
		t.Fatalf("expected 1 WorkflowStarted, got %d", started)
	}
	if accepted != 1 {
		t.Fatalf("same updateID must produce exactly 1 accepted, got %d", accepted)
	}
}

// TestUpdateWithStart_RejectedInput_NoSideEffect
// Error Case: 无状态 validator 拒绝 input → 返回 ErrUpdateRejected，且零副作用（不 start、不 accept）。
func TestUpdateWithStart_RejectedInput_NoSideEffect(t *testing.T) {
	env := newUpdateWithStartTestEnv(t, "wf-uws-rejected")
	ctx := context.Background()

	env.runtime.RegisterUpdateValidator("set_priority", func(name string, args protocol.Payloads) error {
		input := firstUpdateArg(args)
		if input["priority"] == "" {
			return context.DeadlineExceeded // 任意非 nil error
		}
		return nil
	})

	_, _, err := env.runtime.UpdateWithStart(ctx, "", env.workflowID, env.workflowID,
		map[string]any{"seed": 1}, "set_priority", "up-rej", updateArgs(map[string]any{"priority": ""}),
		protocol.UpdateWaitStageEnums.Accepted)
	if err == nil {
		t.Fatal("expected rejection error")
	}
	// 零副作用：无 current run（未 start）。
	if _, e := env.server.mgr.GetCurrentRunID(protocol.DefaultNamespace, env.workflowID); e == nil {
		t.Fatal("rejected input must not start workflow (no current run expected)")
	}
}

// TestUpdateWithStart_DeadlockRegression
// Edge Case (-race): 并发 N 个 WaitStage=Completed 的 UpdateWithStart（各自新 workflow），
// driver 逐个完成，断言无死锁（watchdog 超时即失败）——验证 start+accept 在锁内、Wait 在锁外。
func TestUpdateWithStart_DeadlockRegression(t *testing.T) {
	const n = 20
	envs := make([]*updateWithStartTestEnv, n)
	for i := range n {
		envs[i] = newUpdateWithStartTestEnv(t, "wf-uws-dl-"+string(rune('a'+i%26))+string(rune('0'+i/26)))
	}

	ctx := context.Background()
	watchdog := time.AfterFunc(30*time.Second, func() {
		panic("TestUpdateWithStart_DeadlockRegression: deadlock detected (watchdog fired)")
	})
	defer watchdog.Stop()

	var wg sync.WaitGroup
	for i := range n {
		env := envs[i]
		driverDone := env.driveUpdateCompleteForCurrent(t, ctx, map[string]any{"i": i})
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, result, err := env.runtime.UpdateWithStart(ctx, "", env.workflowID, env.workflowID,
				map[string]any{"seed": 1}, "set_priority", "", updateArgs(map[string]any{"priority": "high"}),
				protocol.UpdateWaitStageEnums.Completed)
			if err != nil {
				t.Errorf("UpdateWithStart failed: %v", err)
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

func TestUpdateWithStart_StartAndAcceptRollBackAtomically(t *testing.T) {
	env := newUpdateWithStartTestEnv(t, "wf-uws-atomic")
	ctx := context.Background()

	mgr := env.mgr

	const updateID = "up-atomic"

	mgr.failOn(history.EventWorkflowUpdateAccepted)
	_, _, err := env.runtime.UpdateWithStart(ctx, "", env.workflowID, env.workflowID,
		map[string]any{"seed": 1}, "set_priority", updateID, updateArgs(map[string]any{"priority": "high"}),
		protocol.UpdateWaitStageEnums.Accepted)
	if err == nil {
		t.Fatal("expected accept failure error, got nil")
	}
	if _, err := env.server.mgr.GetCurrentRunID(protocol.DefaultNamespace, env.workflowID); !errors.Is(err, execution.ErrNoCurrentRun) {
		t.Fatalf("failed transaction left a current run: %v", err)
	}

	mgr.failOn("")
	runID, result, err := env.runtime.UpdateWithStart(ctx, "", env.workflowID, env.workflowID,
		map[string]any{"seed": 2}, "set_priority", updateID, updateArgs(map[string]any{"priority": "high"}),
		protocol.UpdateWaitStageEnums.Accepted)
	if err != nil {
		t.Fatalf("retry after rollback: %v", err)
	}
	if result.UpdateID != updateID || result.AcceptedEventID == 0 {
		t.Fatalf("retry result = %+v", result)
	}
	if result.Stage != protocol.UpdateReachedStageEnums.Accepted {
		t.Fatalf("retry stage = %q", result.Stage)
	}

	started, accepted := countStartedEvents(t, env.server, runID)
	if started != 1 || accepted != 1 {
		t.Fatalf("committed transaction events: started=%d accepted=%d", started, accepted)
	}
	cur, err := env.server.mgr.GetCurrentRunID(protocol.DefaultNamespace, env.workflowID)
	if err != nil {
		t.Fatal(err)
	}
	if cur != runID {
		t.Fatalf("run %q must be current, got %q", runID, cur)
	}
	if status, e := env.server.GetRunStatus(runID); e != nil || status != history.StatusRunning {
		t.Fatalf("status=%q err=%v", status, e)
	}
}
