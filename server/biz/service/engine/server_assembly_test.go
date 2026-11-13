package engine

import (
	"context"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/internal/executor/interpreter"
	"github.com/donutnomad/tenuto/internal/worker"
	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/sdk/activity"
	"github.com/donutnomad/tenuto/server/biz/domain/clock"
	"github.com/donutnomad/tenuto/server/biz/domain/definition"
	"github.com/donutnomad/tenuto/server/biz/domain/dispatch"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/infra/memory"
	"github.com/donutnomad/tenuto/server/infra/persistence"
	"github.com/donutnomad/tenuto/server/infra/systemclock"
)

// 包内测试不能 import assembly（它依赖本包），这里按同样的规则装配依赖。

func assembleServerDeps(mgr execution.ExecutionManager, wfQueue dispatch.TaskQueue, actQueue dispatch.ActivityTaskQueueRouter) ServerDeps {
	deps := ServerDeps{
		Executions:     mgr,
		WorkflowQueue:  wfQueue,
		ActivityRouter: actQueue,
		TaskTokens:     testTaskTokens(),
		Batches:        memory.NewBatchRepository(),
		QueryEvaluator: interpreter.QueryEvaluator{},
		Clock:          systemclock.Clock{},
	}
	// 内存 ExecutionManager 自带配置仓储（RunStore 内嵌 DefinitionRepository），测试经它读回配置。
	configs, ok := mgr.(definition.ConfigRepository)
	if !ok {
		configs = memory.NewDefinitionRepository()
	}
	deps.Configs = configs
	deps.Dispatches = memory.NewDispatchRepository()
	deps.WorkerVersions = testActivityWorkerVersions()
	deps.Schedules = memory.NewScheduleRepository()
	return deps
}

// testActivityTypes 是本包测试调度、但不检验选路的 Activity 类型。测试 Server 的 Worker 登记里预先有一个同进程测试
// Worker 声明它们，与生产中执行 Activity 的 Worker 登记自己的路由一致；检验选路的测试用别的类型自行登记。
var testActivityTypes = []string{
	"a", "accept", "act", "activities.ChargeCard", "activities.Cleanup", "activities.Create", "activities.CreateRecord",
	"activities.Reserve", "activities.ReserveInventory", "activities.Sign", "activity.any", "activity.first", "activity.late",
	"activity.second", "async_wait", "b", "cancelable", "charge_card", "complete", "demo.Flaky", "demo.Task", "long_running",
	"must_retry", "must_skip", "never-picked", "noop", "probe", "process", "retry-work", "send_email", "Settle", "slow", "T",
	"validate", "work",
	"activities.Step1", "activities.Step2", "activities.Step3", "activities.Step4", "activities.Step5", "activities.Step6",
}

// assembleServerWithoutTestWorker 装配一个 Worker 登记为空的 Server，供检验 Worker 登记视图的测试使用。
func assembleServerWithoutTestWorker(mgr execution.ExecutionManager) *Server {
	deps := assembleServerDeps(mgr, newTestTaskQueue(), QueueActivityRouter{Queue: newTestTaskQueue()})
	deps.WorkerVersions = memory.NewWorkerVersionRepository()
	return NewServer(deps)
}

// RegisterTestActivityWorker 以测试 Worker 的身份在 default namespace、default task queue 登记 activityTypes，供包外测试
// 让它们调度的 Activity 有路由。
func RegisterTestActivityWorker(tb testing.TB, server *Server, activityTypes ...string) {
	tb.Helper()
	if err := server.RegisterActivityWorkerVersion(context.Background(), protocol.ActivityWorkerVersion{
		Identity: testActivityWorkerIdentity, TaskQueues: []string{protocol.DefaultActivityTaskQueue}, ActivityTypes: activityTypes,
	}); err != nil {
		tb.Fatal(err)
	}
}

// testActivityWorkerIdentity 是预先登记 testActivityTypes 的测试 Worker。
const testActivityWorkerIdentity = "engine-test-activity-worker"

// testActivityWorkerVersions 返回预先登记了 testActivityTypes 的 Worker 登记仓储：default namespace、default task queue，
// 有效期足够覆盖假时钟推进。
func testActivityWorkerVersions() *memory.WorkerVersionRepository {
	repo := memory.NewWorkerVersionRepository()
	worker := protocol.ActivityWorkerVersion{
		Identity: testActivityWorkerIdentity, TaskQueues: []string{protocol.DefaultActivityTaskQueue}, ActivityTypes: testActivityTypes,
	}
	if _, err := repo.UpsertActivityWorkerVersionInNamespace(protocol.DefaultNamespace, worker, time.Now(), 100*365*24*time.Hour); err != nil {
		panic(err)
	}
	return repo
}

// assemblePersistentServer 用 GORM Store 的各仓储装配 Server。
func assemblePersistentServer(store *persistence.Store, wfQueue dispatch.TaskQueue, actQueue dispatch.ActivityTaskQueueRouter) *Server {
	deps := assembleServerDeps(store.Executions, wfQueue, actQueue)
	deps.Configs = store.Definitions
	deps.Dispatches, deps.WorkerVersions, deps.Schedules = store.Dispatches, store.Dispatches, store.Schedules
	return NewServer(deps)
}

// assembleServer 把 Activity 任务投进 actQueue 这一条测试队列，测试直接从它观察派出的 Activity Task。
func assembleServer(mgr execution.ExecutionManager, wfQueue, actQueue dispatch.TaskQueue, options ...ServerOption) *Server {
	return assembleServerWithActivityRouter(mgr, wfQueue, QueueActivityRouter{Queue: actQueue}, options...)
}

// QueueActivityRouter 让一条测试队列承接全部 Activity task queue：入队时写上 task queue 与 namespace，
// 出队不区分 task queue 与 Worker。包外测试经 engine.QueueActivityRouter 使用。
type QueueActivityRouter struct {
	Queue dispatch.TaskQueue
}

func (r QueueActivityRouter) Enqueue(ctx context.Context, taskQueue string, task protocol.Task) error {
	task.TaskQueue = dispatch.NormalizeActivityTaskQueue(taskQueue)
	task.Namespace = protocol.NormalizeNamespace(task.Namespace)
	return r.Queue.Enqueue(ctx, task)
}

func (r QueueActivityRouter) Dequeue(ctx context.Context, _ string) (protocol.TaskLease, error) {
	return r.Queue.Dequeue(ctx)
}

func (r QueueActivityRouter) Ack(ctx context.Context, lease protocol.TaskLease) error {
	return r.Queue.Ack(ctx, lease)
}

func (r QueueActivityRouter) Nack(ctx context.Context, lease protocol.TaskLease) error {
	return r.Queue.Nack(ctx, lease)
}

func (r QueueActivityRouter) Durable() bool { return r.Queue.Durable() }

// assembleServerWithClock 用 clk 作为时间来源装配 Server，测试经 clocktest.Fake 手动推进时间。
// 内存存储换成同一个时钟，事件与 execution state 的时间和 Server 一致。
func assembleServerWithClock(mgr execution.ExecutionManager, wfQueue, actQueue dispatch.TaskQueue, clk clock.Clock, options ...ServerOption) *Server {
	if store, ok := mgr.(*memory.ExecutionManager); ok {
		store.WithClock(clk)
	}
	deps := assembleServerDeps(mgr, wfQueue, QueueActivityRouter{Queue: actQueue})
	deps.Clock = clk
	return NewServer(deps, options...)
}

func assembleServerWithActivityRouter(mgr execution.ExecutionManager, wfQueue dispatch.TaskQueue, actQueue dispatch.ActivityTaskQueueRouter, options ...ServerOption) *Server {
	return NewServer(assembleServerDeps(mgr, wfQueue, actQueue), options...)
}

// assembleWorker 创建与 Server 同进程的 Worker。
func assembleWorker(api ServerAPI, activities *activity.Registry, opts worker.WorkerOptions) *worker.Worker {
	return worker.New(api, api, NewActivityRuntimeService(api), activities, opts)
}

func assembleActivityWorker(activityQueue dispatch.ActivityTaskQueueRouter, runtime worker.ActivityRuntimeAPI, activities *activity.Registry, opts worker.WorkerOptions) *worker.Worker {
	return worker.NewActivityWorkerWithQueue(NewActivityQueueWorkerAPI(activityQueue), runtime, activities, opts)
}

// testTaskTokens 返回测试 Server 共用的 task token codec。
func testTaskTokens() *dispatch.TaskTokenCodec {
	codec, err := dispatch.NewTaskTokenCodec([]byte("engine-test-task-token-secret-0123456789"))
	if err != nil {
		panic(err)
	}
	return codec
}

// startedRunID 取 Server 启动结果里的 Run ID。
func startedRunID(run *execution.Run, err error) (string, error) {
	if err != nil {
		return "", err
	}
	return run.ID, nil
}

// getRun 读取请求寻址的 Run，Status 取 execution state 的投影。
func getRun(s *Server, namespace, workflowID, runID string) (*execution.Run, error) {
	run, _, err := s.View().GetRunWithState(namespace, workflowID, runID)
	return run, err
}

// capableWorkerContext 给 ctx 的 Worker 信息补上当前 WLang 能力：轮询 Workflow Task 时服务端按它校验 Program
// 要求的能力。已声明能力的 ctx 原样返回。
func capableWorkerContext(ctx context.Context) context.Context {
	info, _ := protocol.WorkerInfoFrom(ctx)
	if info.WorkflowCapabilities != nil {
		return ctx
	}
	info.WorkflowCapabilities = program.CurrentWLangCapabilities()
	return protocol.WithWorkerInfo(ctx, info)
}
