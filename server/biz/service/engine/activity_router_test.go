package engine

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/program/programtest"
	"github.com/donutnomad/tenuto/protocol"
	historypkg "github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/dispatch"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/infra/memory"
	"github.com/donutnomad/tenuto/server/infra/systemclock"
)

func TestMemoryActivityQueueRouterKeepsQueuesIsolated(t *testing.T) {
	ctx := context.Background()
	router := memory.NewActivityQueueRouter(8)

	if err := router.Enqueue(ctx, "custodian", protocol.Task{Type: protocol.TaskTypeActivity, ActivityType: "custodian.Capture"}); err != nil {
		t.Fatal(err)
	}
	if err := router.Enqueue(ctx, "wallets", protocol.Task{Type: protocol.TaskTypeActivity, ActivityType: "wallets.Sign"}); err != nil {
		t.Fatal(err)
	}

	custodianLease, err := router.Dequeue(ctx, "custodian")
	if err != nil {
		t.Fatal(err)
	}
	if custodianLease.Task.ActivityType != "custodian.Capture" || custodianLease.Task.TaskQueue != "custodian" {
		t.Fatalf("unexpected custodian task: %+v", custodianLease.Task)
	}

	walletsLease, err := router.Dequeue(ctx, "wallets")
	if err != nil {
		t.Fatal(err)
	}
	if walletsLease.Task.ActivityType != "wallets.Sign" || walletsLease.Task.TaskQueue != "wallets" {
		t.Fatalf("unexpected wallets task: %+v", walletsLease.Task)
	}
}

func TestActivityRouteCatalogResolvesWorkerVersionRoutes(t *testing.T) {
	catalog := dispatch.NewActivityRouteCatalog(systemclock.Clock{})
	if err := catalog.RegisterActivityWorkerVersion(protocol.ActivityWorkerVersion{
		App:           "custodian",
		Identity:      "custodian-worker-1",
		BuildID:       "custodian@v1",
		TaskQueues:    []string{"custodian"},
		ActivityTypes: []string{"custodian.Capture"},
	}); err != nil {
		t.Fatal(err)
	}

	route, err := catalog.ResolveActivityRoute(dispatch.ActivityRouteRef{ActivityID: "capture", Type: "custodian.Capture"})
	if err != nil {
		t.Fatal(err)
	}
	if route.TaskQueue != "custodian" || route.BuildID != "custodian@v1" {
		t.Fatalf("unexpected route: %+v", route)
	}
}

func TestActivityRouteCatalogRegistersAllWorkerTaskQueues(t *testing.T) {
	catalog := dispatch.NewActivityRouteCatalog(systemclock.Clock{})
	if err := catalog.RegisterActivityWorkerVersion(protocol.ActivityWorkerVersion{
		App:           "custodian",
		Identity:      "custodian-worker-1",
		BuildID:       "custodian@v1",
		TaskQueues:    []string{"custodian", "custodian-priority"},
		ActivityTypes: []string{"custodian.Capture"},
	}); err != nil {
		t.Fatal(err)
	}

	route, err := catalog.ResolveActivityRoute(dispatch.ActivityRouteRef{
		ActivityID: "capture",
		Type:       "custodian.Capture",
		TaskQueue:  "custodian-priority",
	})
	if err != nil {
		t.Fatal(err)
	}
	if route.TaskQueue != "custodian-priority" || route.BuildID != "custodian@v1" {
		t.Fatalf("unexpected route: %+v", route)
	}
}

func TestActivityRouteCatalogUsesDefaultBuild(t *testing.T) {
	catalog := dispatch.NewActivityRouteCatalog(systemclock.Clock{})
	if err := catalog.RegisterActivityWorkerVersion(protocol.ActivityWorkerVersion{
		App:           "custodian",
		Identity:      "custodian-worker-v1",
		BuildID:       "custodian@v1",
		TaskQueues:    []string{"custodian"},
		ActivityTypes: []string{"custodian.Capture"},
		Default:       true,
	}); err != nil {
		t.Fatal(err)
	}
	if err := catalog.RegisterActivityWorkerVersion(protocol.ActivityWorkerVersion{
		App:           "custodian",
		Identity:      "custodian-worker-v2",
		BuildID:       "custodian@v2",
		TaskQueues:    []string{"custodian"},
		ActivityTypes: []string{"custodian.Capture"},
	}); err != nil {
		t.Fatal(err)
	}

	route, err := catalog.ResolveActivityRoute(dispatch.ActivityRouteRef{
		ActivityID: "capture",
		Type:       "custodian.Capture",
		RoutingKey: "run-1:capture:custodian.Capture",
	})
	if err != nil {
		t.Fatal(err)
	}
	if route.BuildID != "custodian@v1" {
		t.Fatalf("unexpected route: %+v", route)
	}
}

func TestActivityRouteCatalogUsesWeightedRoute(t *testing.T) {
	catalog := dispatch.NewActivityRouteCatalog(systemclock.Clock{})
	if err := catalog.RegisterActivityWorkerVersion(protocol.ActivityWorkerVersion{
		App:           "custodian",
		Identity:      "custodian-worker-v1",
		BuildID:       "custodian@v1",
		TaskQueues:    []string{"custodian"},
		ActivityTypes: []string{"custodian.Capture"},
		Weight:        0,
	}); err != nil {
		t.Fatal(err)
	}
	if err := catalog.RegisterActivityWorkerVersion(protocol.ActivityWorkerVersion{
		App:           "custodian",
		Identity:      "custodian-worker-v2",
		BuildID:       "custodian@v2",
		TaskQueues:    []string{"custodian"},
		ActivityTypes: []string{"custodian.Capture"},
		Weight:        100,
	}); err != nil {
		t.Fatal(err)
	}

	first, err := catalog.ResolveActivityRoute(dispatch.ActivityRouteRef{
		ActivityID: "capture",
		Type:       "custodian.Capture",
		RoutingKey: "run-1:capture:custodian.Capture",
	})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		route, err := catalog.ResolveActivityRoute(dispatch.ActivityRouteRef{
			ActivityID: "capture",
			Type:       "custodian.Capture",
			RoutingKey: "run-1:capture:custodian.Capture",
		})
		if err != nil {
			t.Fatal(err)
		}
		if route.BuildID != first.BuildID {
			t.Fatalf("expected stable route, got first=%+v next=%+v", first, route)
		}
	}
	if first.BuildID != "custodian@v2" {
		t.Fatalf("unexpected weighted route: %+v", first)
	}
}

func TestActivityRouteCatalogIgnoresExpiredWorkerVersions(t *testing.T) {
	now := time.Now()
	catalog := dispatch.NewActivityRouteCatalog(systemclock.Clock{})
	if err := catalog.RegisterActivityWorkerVersion(protocol.ActivityWorkerVersion{
		App:           "custodian",
		Identity:      "custodian-worker-v1",
		BuildID:       "custodian@v1",
		TaskQueues:    []string{"custodian"},
		ActivityTypes: []string{"custodian.Capture"},
		ExpiresAt:     now.Add(-time.Second),
	}); err != nil {
		t.Fatal(err)
	}
	if err := catalog.RegisterActivityWorkerVersion(protocol.ActivityWorkerVersion{
		App:           "custodian",
		Identity:      "custodian-worker-v2",
		BuildID:       "custodian@v2",
		TaskQueues:    []string{"custodian"},
		ActivityTypes: []string{"custodian.Capture"},
		ExpiresAt:     now.Add(time.Minute),
	}); err != nil {
		t.Fatal(err)
	}

	route, err := catalog.ResolveActivityRoute(dispatch.ActivityRouteRef{
		ActivityID: "capture",
		Type:       "custodian.Capture",
		RoutingKey: "run-1:capture:custodian.Capture",
	})
	if err != nil {
		t.Fatal(err)
	}
	if route.BuildID != "custodian@v2" {
		t.Fatalf("unexpected route: %+v", route)
	}
}

func TestActivityRouteCatalogAllowsConfigWithOnlyActivityType(t *testing.T) {
	catalog := dispatch.NewActivityRouteCatalog(systemclock.Clock{},
		dispatch.ActivityRoute{Type: "custodian.Capture", TaskQueue: "custodian", BuildID: "custodian@v1"},
	)

	err := catalog.ValidateWorkflowConfigInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{
		Type:    "resolved-route",
		Program: programtest.Noop(),
		Activities: map[string]program.ActivityRef{
			"capture": {Symbol: "capture", Type: "custodian.Capture"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
}

// 显式指定 task queue 后不再要求目录里有匹配项（对齐 Temporal 的隐式队列语义）。
// 本用例锁定放宽后的注册期契约：显式指定一律放行，未指定仍依赖目录。
func TestActivityRouteCatalogAcceptsExplicitQueuesRegardlessOfCatalog(t *testing.T) {
	catalog := dispatch.NewActivityRouteCatalog(systemclock.Clock{},
		dispatch.ActivityRoute{Type: "custodian.Capture", TaskQueue: "custodian", BuildID: "custodian@v1"},
	)

	// 目录里没有 wallets 队列，但显式指定即放行（此前是 hard error）。
	err := catalog.ValidateWorkflowConfigInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{
		Type:    "mismatched-route",
		Program: programtest.Noop(),
		Activities: map[string]program.ActivityRef{
			"capture": {Symbol: "capture", Type: "custodian.Capture", TaskQueue: "wallets"},
		},
	})
	if err != nil {
		t.Fatalf("explicit task_queue should be accepted even when absent from catalog: %v", err)
	}

	err = catalog.ValidateWorkflowConfigInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{
		Type:    "missing-build",
		Program: programtest.Noop(),
		Activities: map[string]program.ActivityRef{
			"capture": {Symbol: "capture", Type: "custodian.Capture", TaskQueue: "custodian"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestRegisterWorkflowAllowsWorkerVersionResolvedRoutes(t *testing.T) {
	server := assembleServer(memory.NewExecutionManager(), newTestTaskQueue(), newTestTaskQueue())
	if err := server.RegisterActivityWorkerVersion(context.Background(), protocol.ActivityWorkerVersion{
		App:           "custodian",
		BuildID:       "custodian@v1",
		TaskQueues:    []string{"custodian"},
		ActivityTypes: []string{"custodian.Capture"},
	}); err != nil {
		t.Fatal(err)
	}

	_, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{
		Type:    "worker-version-route",
		Program: programtest.Noop(),
		Activities: map[string]program.ActivityRef{
			"capture": {Symbol: "capture", Type: "custodian.Capture"},
		},
	})

	if err != nil {
		t.Fatal(err)
	}
}

func TestServerRefreshActivityWorkerVersionsDropsStaleRoutes(t *testing.T) {
	server := assembleServer(memory.NewExecutionManager(), newTestTaskQueue(), newTestTaskQueue())
	first := protocol.ActivityWorkerVersion{
		Identity:      "custodian-worker-1",
		BuildID:       "custodian@v1",
		TaskQueues:    []string{"custodian"},
		ActivityTypes: []string{"custodian.Capture", "custodian.Refund"},
	}
	if err := server.RegisterActivityWorkerVersion(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	second := protocol.ActivityWorkerVersion{
		Identity:      "custodian-worker-1",
		BuildID:       "custodian@v1",
		TaskQueues:    []string{"custodian"},
		ActivityTypes: []string{"custodian.Capture"},
	}
	if err := server.RegisterActivityWorkerVersion(context.Background(), second); err != nil {
		t.Fatal(err)
	}

	_, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{
		Type:    "stale-route-removed",
		Program: programtest.Noop(),
		Activities: map[string]program.ActivityRef{
			"refund": {Symbol: "refund", Type: "custodian.Refund"},
		},
	})

	if err != nil {
		t.Fatalf("publishing must not depend on worker routes: %v", err)
	}
	server.mu.Lock()
	routes := server.activityRoutes
	server.mu.Unlock()
	if _, err := routes.ResolveActivityRoute(dispatch.ActivityRouteRef{ActivityID: "refund", Type: "custodian.Refund"}); !errors.Is(err, dispatch.ErrActivityRouteUnavailable) {
		t.Fatalf("stale activity route must be removed, err=%v", err)
	}
}

func TestScheduleActivityCommandUsesResolvedWorkerVersionRoute(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	activityTasks := newTestTaskQueue()
	server := assembleServer(mgr, newTestTaskQueue(), activityTasks)
	if err := server.RegisterActivityWorkerVersion(context.Background(), protocol.ActivityWorkerVersion{
		App:           "custodian",
		BuildID:       "custodian@v1",
		TaskQueues:    []string{"custodian"},
		ActivityTypes: []string{"custodian.Capture"},
	}); err != nil {
		t.Fatal(err)
	}

	ref, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{
		Type:    "schedule-worker-version-route",
		Program: programtest.Noop(),
		Activities: map[string]program.ActivityRef{
			"capture": {Symbol: "capture", Type: "custodian.Capture"},
		},
	})

	if err != nil {
		t.Fatal(err)
	}
	run := &execution.Run{
		ID:                    "run-worker-version-route",
		WorkflowID:            ref.WorkflowType,
		WorkflowType:          ref.WorkflowType,
		WorkflowConfigVersion: ref.Version,
		Status:                historypkg.StatusRunning,
		CreatedAt:             time.Now(),
	}
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(run)) }); err != nil {
		t.Fatal(err)
	}
	workflowTaskID := appendOpenWorkflowTaskForTest(t, mgr, run.ID)

	err = server.HandleWorkflowTaskCompleted(server.WorkflowTaskContext(ctx, run.ID, workflowTaskID), run.ID, workflowTaskID, []historypkg.Command{{
		StartToCloseTimeout: time.Minute,
		Type:                historypkg.CmdScheduleActivity,
		ActivityID:          "capture",
		ActivityType:        "custodian.Capture",
	}}, "")
	if err != nil {
		t.Fatal(err)
	}

	history, err := mgr.GetEvents(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	var scheduled *historypkg.ActivityScheduledEvent
	for _, event := range history {
		if event.ActivityScheduled != nil {
			scheduled = event.ActivityScheduled
			break
		}
	}
	if scheduled == nil || scheduled.TaskQueue != "custodian" {
		t.Fatalf("unexpected scheduled route: %+v", scheduled)
	}
	if len(activityTasks.tasks) != 1 {
		t.Fatalf("expected one activity task, got %d", len(activityTasks.tasks))
	}
	task := activityTasks.tasks[0].Task
	if task.TaskQueue != "custodian" || task.TargetWorkerIdentity != "" {
		t.Fatalf("unexpected task route: %+v", task)
	}
}

// 1. 核心对齐行为：显式指定一个目录里完全没有的队列 → resolve 成功，原样返回该队列。
func TestResolveActivityRouteAllowsUnknownExplicitTaskQueue(t *testing.T) {
	catalog := dispatch.NewActivityRouteCatalog(systemclock.Clock{},
		dispatch.ActivityRoute{Type: "custodian.Capture", TaskQueue: "custodian", BuildID: "custodian@v1"},
	)

	route, err := catalog.ResolveActivityRoute(dispatch.ActivityRouteRef{
		ActivityID: "capture",
		Type:       "custodian.Capture",
		TaskQueue:  "queue-that-no-worker-serves",
	})
	if err != nil {
		t.Fatalf("explicit task_queue must not require an online worker: %v", err)
	}
	if route.TaskQueue != "queue-that-no-worker-serves" {
		t.Fatalf("unexpected task queue: %+v", route)
	}
	if route.Type != "custodian.Capture" {
		t.Fatalf("unexpected activity type: %+v", route)
	}
}

// 5. 防止改过头：未指定 TaskQueue 且目录无候选 → 仍然报错（目录是决定投哪的唯一依据）。
func TestResolveActivityRouteStillFailsWithoutTaskQueue(t *testing.T) {
	catalog := dispatch.NewActivityRouteCatalog(systemclock.Clock{})

	_, err := catalog.ResolveActivityRoute(dispatch.ActivityRouteRef{
		ActivityID: "capture",
		Type:       "custodian.Capture",
	})
	if err == nil {
		t.Fatal("expected an error when no task_queue is given and the catalog has no candidate")
	}
}

// 6. 注册期：含显式队列的 workflow 在无 worker 时能注册成功；不含显式队列的仍注册失败。
func TestRegisterWorkflowDistinguishesExplicitTaskQueue(t *testing.T) {
	server := assembleServer(memory.NewExecutionManager(), newTestTaskQueue(), newTestTaskQueue())
	// 注册一个无关的 worker，让 server 持有一个非空目录（目录为 nil 时校验整体跳过，
	// 那样测不到本用例要覆盖的区分逻辑）。custodian.Capture 依然不在目录里。
	if err := server.RegisterActivityWorkerVersion(context.Background(), protocol.ActivityWorkerVersion{
		Identity:      "unrelated-worker-1",
		BuildID:       "unrelated@v1",
		TaskQueues:    []string{"unrelated"},
		ActivityTypes: []string{"unrelated.Ping"},
	}); err != nil {
		t.Fatal(err)
	}

	_, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{
		Type:    "explicit-queue-no-worker",
		Program: programtest.Noop(),
		Activities: map[string]program.ActivityRef{
			"capture": {Symbol: "capture", Type: "custodian.Capture", TaskQueue: "custodian"},
		},
	})

	if err != nil {
		t.Fatalf("explicit task_queue must register without any online worker: %v", err)
	}

	_, err = server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{
		Type:    "implicit-queue-no-worker",
		Program: programtest.Noop(),
		Activities: map[string]program.ActivityRef{
			"capture": {Symbol: "capture", Type: "custodian.Capture"},
		},
	})

	if err != nil {
		t.Fatalf("workflow without an explicit task_queue must register without any online worker: %v", err)
	}
}

// 7. 打错队列名（显式队列、无任何 worker 订阅）→ 仍照投，但走 warning 分支。
//
// 这是 ScheduleToStartTimeout 默认为 0（不超时）时最危险的形态：任务投进一个没人 poll
// 的队列会永久静默挂起。决策是**不改默认超时**（会破坏滚动升级等合法的长等待场景），
// 而是让「打错」这件事本身可观测。
func TestResolveActivityRouteFlagsTaskQueueWithoutSubscriber(t *testing.T) {
	catalog := dispatch.NewActivityRouteCatalog(systemclock.Clock{},
		dispatch.ActivityRoute{Type: "custodian.Capture", TaskQueue: "custodian", BuildID: "custodian@v1"},
	)

	ref := dispatch.ActivityRouteRef{
		ActivityID: "capture",
		Type:       "custodian.Capture",
		TaskQueue:  "custodain-typo", // 打错的队列名
	}
	route, err := catalog.ResolveActivityRoute(ref)
	if err != nil {
		t.Fatalf("explicit task_queue must not be rejected: %v", err)
	}
	// 行为不变：照原样投递，不改写队列名、不回退到目录里的队列。
	if route.TaskQueue != "custodain-typo" {
		t.Fatalf("task queue must be preserved verbatim, got %q", route.TaskQueue)
	}
}
