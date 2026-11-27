package engine

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/server/biz/domain/batch"
	"github.com/donutnomad/tenuto/server/biz/domain/clock"
	"github.com/donutnomad/tenuto/server/biz/domain/definition"
	"github.com/donutnomad/tenuto/server/biz/domain/dispatch"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/biz/domain/schedule"
	"github.com/donutnomad/tenuto/server/biz/usecase/batches"
	"github.com/donutnomad/tenuto/server/biz/usecase/definitions"
	"github.com/donutnomad/tenuto/server/biz/usecase/executions"
	"github.com/donutnomad/tenuto/server/biz/usecase/schedules"
	"github.com/donutnomad/tenuto/server/biz/view/executionview"
)

var _ ServerAPI = (*Server)(nil)

const taskDispatchLeaseTTL = 30 * time.Second

// WorkflowFailed 事件上的 Workflow 超时种类，取值与 SDK 读取的 protocol.TimeoutType 相同。
const (
	TimeoutTypeExecution = string(protocol.TimeoutTypeWorkflowExecution)
	TimeoutTypeRun       = string(protocol.TimeoutTypeWorkflowRun)
)

// Server 负责事件落库、状态推进和任务调度。
type Server struct {
	clock      clock.Clock
	mgr        execution.ExecutionManager // 唯一存储入口（内嵌 EventStore + RunStore + ActivityStateStore + RunInTx）
	runtime    *execution.Runtime
	dispatcher *dispatch.Service
	// tokens 签发与校验 task token；HTTP 边界与引擎使用同一个 codec。
	tokens *dispatch.TaskTokenCodec
	// payloadSizeLimit 是单个载荷的字节上限，见 WithPayloadSizeLimit。
	payloadSizeLimit int

	wfQueue         dispatch.TaskQueue
	actQueue        dispatch.ActivityTaskQueueRouter
	workflowConfigs definition.ConfigRepository
	definitions     *definitions.UseCase
	view            *executionview.View
	dispatches      dispatch.Repository
	workerVersions  dispatch.WorkerVersionRepository
	// activityRoutes 是 Activity Worker 登记的快照，始终存在：没有登记时是空目录，Activity 命令照常按它校验。
	activityRoutes *dispatch.ActivityRouteCatalog
	// activityRoutesLoadedAt 是 activityRoutes 从数据库重建的时刻；activityRoutesLoadMu 让重建串行。
	activityRoutesLoadedAt time.Time
	activityRoutesLoadMu   sync.Mutex
	// payloadCompat 缓存两份 Program 之间的载荷兼容结果；零值可用。
	payloadCompat payloadCompatCache

	schedules       schedule.Repository
	scheduleUseCase *schedules.UseCase
	batches         *batches.UseCase
	queryEvaluator  executions.QueryEvaluator
	// querySlots 限制同时在本进程重放 History 求值 Query 的数量（WithQueryConcurrency）。
	querySlots chan struct{}

	// updateValidators 按 update name 注册无状态校验器，为空则不校验。RegisterUpdateValidator（写）可能与
	// UpdateWorkflow（读）并发，由 updateValidatorsMu 保护。
	updateValidatorsMu sync.RWMutex
	updateValidators   map[string]InputValidator

	// keyedMu 是 per-(namespace, workflowID) 的 keyed lock。
	// 写操作经 withWorkflowLease 拿此锁，覆盖"解析 current → 操作"全窗口，防 current 漂移。
	// WFT 处理（HandleWorkflowTaskCompleted/TimedOut）也经此锁串行化。
	// ⚠️ 锁 key 一律经 workflowLockKey(namespace, workflowID) 构造，禁止裸 workflowID：
	// 裸 key 会让不同 namespace 下的同名 workflowID 争同一把锁（多租户下的意外串行），
	// 而混用两种 key 更会导致同一实体互斥失效。
	// 锁顺序：keyedMu(namespace/workflowID) -> mu / timers 内部锁。
	keyedMu *KeyedLock

	// runWatch 唤醒等待 Run 变化的调用方（Update 结果、Run 结束），覆盖其他副本提交的变化，见 run_watch.go。
	// 等待在 keyed lock 之外；Notify 只争它自己的锁、不阻塞。
	runWatch *runWatch

	// longPolls 在 StopLongPolls 时取消：长轮询（领取 Workflow / Activity Task、等 Run 结束、等 Update 结果）
	// 经 longPollContext 监听它，停机时立即返回；其余请求不受影响，在 HTTP Shutdown 的期限内照常做完。
	longPolls     context.Context
	stopLongPolls context.CancelFunc
	// workerPolls 记录各 Worker 实例在途的领取长轮询，Worker 停止时经 ShutdownWorkerPolls 结束，见 worker_polls.go。
	workerPolls *workerPollSessions

	// timers 保存全部进程内 timer 句柄，到期统一进入 fireTimer。锁顺序：keyedMu -> timers 内部锁。
	timers *timerSet

	// mu 保护 stickyFallbackLast 与 workflowWorkers。
	mu sync.Mutex

	// WFT 失败重调度退避参数。测试可缩短以避免等待默认退避。
	wftBackoffBase time.Duration
	wftBackoffCap  time.Duration
	// updateSoftTimeout 是 Update 等待 Completed 的软超时。
	updateSoftTimeout  time.Duration
	stickyFallbackLast time.Time
	workflowWorkers    map[string]executionview.WorkflowWorkerStatus
	telemetry          *engineTelemetry
	// logger 取自 WithTelemetry 注入的 telemetry.Providers.Logger，未注入时丢弃日志。
	logger *slog.Logger
}

// 编译期断言：Server 满足 schedules 用例消费的 schedule.Firer 与批量操作用例消费的 batches.Actions。
var (
	_ schedule.Firer  = (*Server)(nil)
	_ batches.Actions = (*Server)(nil)
)

// ServerDeps 是 Server 的全部外部依赖，由装配方（assembly、应用入口、测试辅助）选择实现后注入。
type ServerDeps struct {
	Executions     execution.ExecutionManager
	WorkflowQueue  dispatch.TaskQueue
	ActivityRouter dispatch.ActivityTaskQueueRouter
	Configs        definition.ConfigRepository
	Dispatches     dispatch.Repository
	WorkerVersions dispatch.WorkerVersionRepository
	Schedules      schedule.Repository
	Batches        batch.Repository
	QueryEvaluator executions.QueryEvaluator
	// TaskTokens 签发与校验 task token。
	TaskTokens *dispatch.TaskTokenCodec
	// Clock 是引擎、用例与读模型共用的时间来源：当前时间与全部进程内定时器都经它。
	Clock clock.Clock
}

func NewServer(deps ServerDeps, options ...ServerOption) *Server {
	resolved := resolveServerOptions(options)
	mgr, wfQueue, actQueue := deps.Executions, deps.WorkflowQueue, deps.ActivityRouter
	server := &Server{
		clock:             deps.Clock,
		mgr:               mgr,
		wfQueue:           wfQueue,
		actQueue:          actQueue,
		workflowConfigs:   deps.Configs,
		dispatches:        deps.Dispatches,
		workerVersions:    deps.WorkerVersions,
		activityRoutes:    dispatch.NewActivityRouteCatalog(deps.Clock),
		schedules:         deps.Schedules,
		queryEvaluator:    deps.QueryEvaluator,
		querySlots:        make(chan struct{}, resolved.queryConcurrency),
		tokens:            deps.TaskTokens,
		updateSoftTimeout: updateSoftTimeout,
		payloadSizeLimit:  resolved.payloadSizeLimit,
		keyedMu:           NewKeyedLock(),
		workflowWorkers:   make(map[string]executionview.WorkflowWorkerStatus),
		updateValidators:  make(map[string]InputValidator),
		telemetry:         newEngineTelemetry(resolved.telemetry, deps.Clock),
		workerPolls:       newWorkerPollSessions(deps.Clock),
	}
	server.logger = server.telemetry.providers.Logger
	server.longPolls, server.stopLongPolls = context.WithCancel(context.Background())
	server.runWatch = newRunWatch(deps.Clock, mgr.ListWorkflowExecutionStates, server.logger)
	server.timers = newTimerSet(deps.Clock, server.fireTimer)
	server.definitions = definitions.NewUseCase(deps.Configs, definitionRoutes{server}, deps.WorkerVersions, deps.Clock)
	server.view = executionview.New(mgr, deps.Configs, deps.WorkerVersions, deps.Schedules, deps.Clock)
	server.scheduleUseCase = schedules.NewUseCase(deps.Schedules, server, deps.Clock, server.logger)
	server.batches = batches.NewUseCase(deps.Batches, mgr, server, deps.Clock, server.logger)
	server.runtime = execution.NewRuntime(mgr)
	server.dispatcher = dispatch.NewService(deps.Dispatches, wfQueue, actQueue, deps.TaskTokens.Attach,
		func(runID string, now time.Time) (dispatch.StickyAssignment, bool) {
			state, err := mgr.GetWorkflowExecutionState(runID)
			if err != nil || state.StickyWorkerIdentity == "" || !state.StickyValidAt(now) {
				return dispatch.StickyAssignment{}, false
			}
			return dispatch.StickyAssignment{
				WorkerIdentity:      state.StickyWorkerIdentity,
				StickyGeneration:    state.StickyGeneration,
				ExecutionGeneration: state.ExecutionGeneration,
				Deadline:            *state.StickyExpiresAt,
			}, true
		}, taskDispatchLeaseTTL, deps.Clock)
	return server
}

// Logger 返回 Server 的日志记录器，HTTP 边界用它记录服务端故障。
func (s *Server) Logger() *slog.Logger { return s.logger }

// TaskTokens 返回 Server 签发与校验 task token 用的 codec，HTTP 边界用它签发与校验租约。
func (s *Server) TaskTokens() *dispatch.TaskTokenCodec { return s.tokens }

// ScheduleUseCase 返回以本 Server 为 Firer 的 schedules 用例，应用入口用它构造触发任务，HTTP 层经它管理 Schedule。
func (s *Server) ScheduleUseCase() *schedules.UseCase {
	return s.scheduleUseCase
}

// Definitions 返回 Server 使用的 definitions 用例：登记 Workflow 内容与发布目标 Worker 配置。
func (s *Server) Definitions() *definitions.UseCase {
	return s.definitions
}

// Batches 返回以本 Server 执行取消、终止与 Reset 的批量操作用例，应用入口用它构造批量操作驱动，HTTP 层经它受理与查询。
func (s *Server) Batches() *batches.UseCase {
	return s.batches
}

// View 返回 Run、History、配置、Worker 与 namespace 的读模型。
func (s *Server) View() *executionview.View {
	return s.view
}

// StopLongPolls 让进行中与之后的长轮询立即返回（领取 Task 得到超时、等 Run 结束与等 Update 结果按期限到期处理）。
// 应用入口在停机、关闭 HTTP 监听之前调用；写请求不受影响。
func (s *Server) StopLongPolls() {
	s.stopLongPolls()
}

// longPollContext 返回在 ctx 结束或 StopLongPolls 时取消的 context。
func (s *Server) longPollContext(ctx context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(s.longPolls, cancel)
	return ctx, func() {
		stop()
		cancel()
	}
}

type longWaitReleaseKey struct{}

// WithLongWaitRelease 让请求在进入长等待（等 Update 结果）之前调用 release：入口限制在途请求数时，请求在拿锁与
// 写库阶段占用名额，长等待不占用。release 可能被调用多次，由调用方保证幂等。
func WithLongWaitRelease(ctx context.Context, release func()) context.Context {
	return context.WithValue(ctx, longWaitReleaseKey{}, release)
}

// releaseBeforeLongWait 调用 ctx 携带的 WithLongWaitRelease 回调。
func releaseBeforeLongWait(ctx context.Context) {
	if release, ok := ctx.Value(longWaitReleaseKey{}).(func()); ok {
		release()
	}
}

// Stop 停止进程内的定时器与 runWatch 的读取。驱动 Server 的后台任务（schedule 驱动、保留期 GC）归应用入口所有，
// 应用入口先停它们再调用 Stop，停止后不会再有新 Run 挂上定时器。
func (s *Server) Stop() {
	s.stopLongPolls()
	s.timers.StopAll()
	s.runWatch.Stop()
}
