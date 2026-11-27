package worker

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/donutnomad/tenuto/internal/executor"
	"github.com/donutnomad/tenuto/internal/executor/interpreter"
	"github.com/donutnomad/tenuto/internal/runtimectx"
	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/sdk/activity"
	"github.com/donutnomad/tenuto/sdk/registration"
	tenutotel "github.com/donutnomad/tenuto/telemetry"
)

const (
	DefaultWorkflowTaskTimeout = protocol.DefaultWorkflowTaskTimeout
	// DefaultGracefulStopTimeout 是 Stop 等待在途任务结束的缺省上限：覆盖一个缺省超时的
	// Workflow Task，并留在常见编排器（Kubernetes 缺省 30s）的终止宽限期之内。
	DefaultGracefulStopTimeout = 10 * time.Second
)

type WorkerOptions struct {
	Namespace                              string
	WorkflowConcurrency                    int
	ActivityConcurrency                    int
	Role                                   WorkerRole
	ActivityTaskQueues                     []string
	WorkflowTaskTimeout                    time.Duration
	Identity                               string
	BuildID                                string
	WorkflowCapabilities                   []string
	ActivityWorkerVersionDefault           bool
	ActivityWorkerVersionWeight            int
	ActivityWorkerVersionRegistry          ActivityWorkerVersionRegistryAPI
	ActivityWorkerVersionHeartbeatInterval time.Duration
	ActivityWorkerVersionHeartbeatTTL      time.Duration
	// PollShutdown 是 Stop 请求 Server 结束本 Worker 在途长轮询的能力；nil 时（同进程 Worker）Stop 直接取消长轮询。
	PollShutdown WorkerPollShutdownAPI
	// GracefulStopTimeout 是 Stop 等待在途 Activity 与 Workflow Task 结束的上限，<=0 时取
	// DefaultGracefulStopTimeout。到期后仍在运行的 Activity 的 context 被取消，它们和未完成的
	// Workflow Task 都不上报失败，交还 Server 重新派发。
	GracefulStopTimeout time.Duration
	ExecutionCache      *executor.ExecutionCache
	Telemetry           tenutotel.Providers
	// Workflows 是随二进制发布的 Workflow 定义（生成器 embed 的 workflow.json）。
	// Start 在开始 poll 之前自动登记：新内容首次出现时激活 latest，同 hash 重复
	// 启动保持 latest；登记失败不启动 poll，错误由 WaitReady 返回。
	Workflows []registration.Definition
}

type WorkerRole string

const (
	WorkerRoleBoth     WorkerRole = "both"
	WorkerRoleWorkflow WorkerRole = "workflow"
	WorkerRoleActivity WorkerRole = "activity"
)

type Worker struct {
	workflowAPI     WorkflowWorkerAPI
	activityQueue   ActivityWorkerQueueAPI
	activityRuntime ActivityRuntimeAPI
	activities      *activity.Registry
	options         WorkerOptions
	// stopPolling 停止拉取新任务；stopWork 取消在途任务的 context（宽限期到期时）。
	stopPolling context.CancelFunc
	stopWork    context.CancelCauseFunc
	// stopping 在 Stop 开始时关闭，Activity 经 activity.GetWorkerStopChannel 读到它；
	// stopped 在 Stop 完成时关闭，并发的 Stop 调用都等它。
	stopping       chan struct{}
	stopped        chan struct{}
	stopOnce       sync.Once
	wg             sync.WaitGroup
	ready          chan struct{}
	readyOnce      sync.Once
	readyMu        sync.Mutex
	readyErr       error
	executionCache *executor.ExecutionCache
	telemetry      *workerTelemetry
	// logger 取自 WorkerOptions.Telemetry.Logger，未配置时丢弃日志。
	logger *slog.Logger
	// session 是本 Worker 实例的随机标识，随每个 runtime 请求上报，Stop 时按它结束 Server 上的在途长轮询。
	session string
	// polls 统计在途的长轮询；Stop 先关闭它，之后不再发起新的 poll。
	polls pollTracker
	// beginRejections 是 Server 连续以不可重试的 4xx 拒绝 BeginActivityTask 的次数，决定退回投递前的退避。
	beginRejections atomic.Int64
}

const (
	// beginRejectionInitialBackoff 与 beginRejectionMaxBackoff 是 Begin 被 4xx 拒绝后、退回投递之前的退避：
	// 按连续拒绝次数翻倍。
	beginRejectionInitialBackoff = time.Second
	beginRejectionMaxBackoff     = time.Minute
)

func NewWorkflowWorker(api WorkflowWorkerAPI, opts WorkerOptions) *Worker {
	opts.Role = WorkerRoleWorkflow
	return New(api, nil, nil, activity.NewRegistry(), opts)
}

func NewActivityWorkerWithQueue(activityQueue ActivityWorkerQueueAPI, runtime ActivityRuntimeAPI, activities *activity.Registry, opts WorkerOptions) *Worker {
	opts.Role = WorkerRoleActivity
	return New(nil, activityQueue, runtime, activities, opts)
}

// New 创建 Worker。workflowAPI 为 nil 时不消费 Workflow Task；activityQueue 或 activityRuntime 为 nil 时不消费 Activity。
func New(workflowAPI WorkflowWorkerAPI, activityQueue ActivityWorkerQueueAPI, activityRuntime ActivityRuntimeAPI, activities *activity.Registry, opts WorkerOptions) *Worker {
	if opts.Role == "" {
		opts.Role = WorkerRoleBoth
	}
	if opts.WorkflowConcurrency <= 0 {
		opts.WorkflowConcurrency = 1
	}
	if opts.ActivityConcurrency <= 0 {
		opts.ActivityConcurrency = 1
	}
	if len(opts.ActivityTaskQueues) == 0 {
		opts.ActivityTaskQueues = []string{protocol.DefaultActivityTaskQueue}
	}
	if opts.WorkflowTaskTimeout <= 0 {
		opts.WorkflowTaskTimeout = DefaultWorkflowTaskTimeout
	}
	if opts.GracefulStopTimeout <= 0 {
		opts.GracefulStopTimeout = DefaultGracefulStopTimeout
	}
	if opts.WorkflowCapabilities == nil {
		opts.WorkflowCapabilities = program.CurrentWLangCapabilities()
	}
	if opts.ExecutionCache != nil {
		opts.WorkflowCapabilities = executor.AppendWorkflowCapabilities(opts.WorkflowCapabilities,
			history.WorkflowCapabilityIncrementalActivationV1,
			history.WorkflowCapabilityStickyExecutionV1,
		)
	}
	if activities == nil {
		activities = activity.NewRegistry()
	}
	worker := &Worker{
		workflowAPI:     workflowAPI,
		activityQueue:   activityQueue,
		activityRuntime: activityRuntime,
		activities:      activities,
		options:         opts,
		ready:           make(chan struct{}),
		stopping:        make(chan struct{}),
		stopped:         make(chan struct{}),
		executionCache:  opts.ExecutionCache,
		telemetry:       newWorkerTelemetry(opts.Telemetry),
		session:         rand.Text(),
	}
	worker.logger = worker.telemetry.providers.Logger
	if err := executor.ValidateWorkerCache(worker.executionCache, worker.options.Identity); err != nil {
		worker.readyErr = err
	}
	return worker
}

// Start 开始拉取并处理任务。ctx 是在途任务的执行 context：它被取消时在途任务立即中止（与 Stop
// 的宽限期到期相同）；平常用 Stop 停止 Worker。
func (w *Worker) Start(ctx context.Context) {
	workCtx, stopWork := context.WithCancelCause(ctx)
	ctx, w.stopPolling = context.WithCancel(workCtx)
	w.stopWork = stopWork
	if w.readyErr != nil {
		w.markReady(w.readyErr)
		return
	}
	// 同时携带 Workflow 定义时，先同步上报自身 Activity 能力：定义登记期的
	// Activity 路由校验要能看到本 worker 刚声明的 ActivityType/Queue。
	// 不携带定义时保持原有行为：版本注册失败在后台重试，不阻塞 Start。
	if len(w.options.Workflows) > 0 && w.hasActivityVersionToRegister() {
		if err := w.options.ActivityWorkerVersionRegistry.RegisterActivityWorkerVersion(ctx, NewActivityWorkerVersion(w.options, w.activities)); err != nil {
			w.markReady(fmt.Errorf("register activity worker version: %w", err))
			return
		}
	}
	if err := w.registerWorkflows(ctx); err != nil {
		w.markReady(err)
		return
	}
	if !w.startActivityWorkerVersionRegistry(ctx) {
		w.markReady(nil)
	}
	if (w.options.Role == WorkerRoleBoth || w.options.Role == WorkerRoleWorkflow) && w.workflowAPI != nil {
		for range w.options.WorkflowConcurrency {
			w.wg.Add(1)
			go func() {
				defer w.wg.Done()
				w.consumeWorkflowTasks(ctx, workCtx)
			}()
		}
	}
	if w.executionCache != nil {
		w.wg.Add(1)
		go func() {
			defer w.wg.Done()
			w.sweepExecutionCache(ctx)
		}()
	}
	if (w.options.Role == WorkerRoleBoth || w.options.Role == WorkerRoleActivity) && w.activityQueue != nil && w.activityRuntime != nil {
		for _, taskQueue := range w.options.ActivityTaskQueues {
			for range w.options.ActivityConcurrency {
				w.wg.Add(1)
				go func() {
					defer w.wg.Done()
					w.consumeActivityTasks(ctx, workCtx, taskQueue)
				}()
			}
		}
	}
}

// WaitReady 等待 worker 完成启动前置注册。
func (w *Worker) WaitReady(ctx context.Context) error {
	select {
	case <-w.ready:
		w.readyMu.Lock()
		err := w.readyErr
		w.readyMu.Unlock()
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// registerWorkflows 在开始 poll 之前同步登记随二进制发布的 Workflow 定义。
//
// 登记传输从已配置的依赖上推断，作者不需要额外配置；任何失败都不启动 poll，由
// WaitReady 返回。
func (w *Worker) registerWorkflows(ctx context.Context) error {
	if len(w.options.Workflows) == 0 {
		return nil
	}
	registrar, err := w.workflowRegistrar()
	if err != nil {
		return err
	}
	for _, definition := range w.options.Workflows {
		workflowType, workflowJSON, err := definition.Load()
		if err != nil {
			return err
		}
		version, err := registrar.RegisterWorkflowJSON(ctx, w.options.Namespace, workflowJSON)
		if err != nil {
			return fmt.Errorf("register workflow %s: %w", workflowType, err)
		}
		w.logger.Info("workflow definition registered", "workflow_type", workflowType, "version", version, "namespace", w.options.Namespace)
	}
	return nil
}

// workflowRegistrar 选择登记传输：Workflow / Both 角色用 workflow API（*engine.Server 或
// runtime HTTP 客户端）；Activity 角色没有 workflow API，用上报 Activity 路由的 registry。
func (w *Worker) workflowRegistrar() (registration.Registrar, error) {
	if registrar, ok := w.workflowAPI.(registration.Registrar); ok {
		return registrar, nil
	}
	if registrar, ok := w.options.ActivityWorkerVersionRegistry.(registration.Registrar); ok {
		return registrar, nil
	}
	return nil, fmt.Errorf("worker role %s cannot register workflow definitions: workflow api %T and activity worker version registry %T do not implement registration.Registrar",
		w.options.Role, w.workflowAPI, w.options.ActivityWorkerVersionRegistry)
}

func (w *Worker) startActivityWorkerVersionRegistry(ctx context.Context) bool {
	if !w.hasActivityVersionToRegister() {
		return false
	}
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		w.runActivityWorkerVersionRegistry(ctx)
	}()
	return true
}

func (w *Worker) hasActivityVersionToRegister() bool {
	return w.options.ActivityWorkerVersionRegistry != nil &&
		(w.options.Role == WorkerRoleBoth || w.options.Role == WorkerRoleActivity) &&
		len(w.activities.Types()) > 0
}

func (w *Worker) runActivityWorkerVersionRegistry(ctx context.Context) {
	worker := NewActivityWorkerVersion(w.options, w.activities)
	interval := WorkerVersionHeartbeatInterval(
		w.options.ActivityWorkerVersionHeartbeatInterval,
		w.options.ActivityWorkerVersionHeartbeatTTL,
	)
	RunActivityWorkerVersionRegistry(ctx, w.logger, worker, w.options.ActivityWorkerVersionRegistry, interval, w.markReady)
}

func (w *Worker) markReady(err error) {
	w.readyOnce.Do(func() {
		w.readyMu.Lock()
		w.readyErr = err
		w.readyMu.Unlock()
		close(w.ready)
	})
}

// deregisterTimeout 是 Stop 等待注销请求的上限。
const deregisterTimeout = 3 * time.Second

// errWorkerStopped 是宽限期到期时在途任务 context 的取消原因。
var errWorkerStopped = errors.New("worker stopped")

// Stop 优雅停止 Worker：先停止拉取（不再发起新的 poll，请求 Server 结束在途的长轮询，见 endPolls），关闭 activity.GetWorkerStopChannel，
// 再等在途的 Activity 与 Workflow Task 结束，最多等 GracefulStopTimeout。到期后取消仍在运行的任务的
// context：因此结束的 Activity 不上报失败，交还 Server 以同一个 attempt 重新派发；未完成的 Workflow
// Task 不上报，由 Server 的 Workflow Task 超时重新调度。最后向 Server 注销本 Worker 的 Activity 路由，
// 让它立刻退出选路；注销是尽力而为，失败或进程崩溃时路由靠心跳 TTL 到期。
//
// Stop 可以重复、并发调用，每个调用都在停止完成后返回。
func (w *Worker) Stop() {
	w.stopOnce.Do(w.stop)
	<-w.stopped
}

func (w *Worker) stop() {
	defer close(w.stopped)
	close(w.stopping)
	started := w.stopPolling != nil
	if started {
		w.endPolls()
		drained := make(chan struct{})
		go func() {
			w.wg.Wait()
			close(drained)
		}()
		timer := time.NewTimer(w.options.GracefulStopTimeout)
		select {
		case <-drained:
		case <-timer.C:
			w.logger.Warn("worker graceful stop timed out; abandoning in-flight tasks", "timeout", w.options.GracefulStopTimeout)
			w.stopWork(errWorkerStopped)
			<-drained
		}
		timer.Stop()
		w.stopWork(errWorkerStopped)
	}
	if started && w.hasActivityVersionToRegister() {
		ctx, cancel := context.WithTimeout(context.Background(), deregisterTimeout)
		err := w.options.ActivityWorkerVersionRegistry.DeregisterActivityWorkerVersionInNamespace(
			ctx, protocol.NormalizeNamespace(w.options.Namespace), w.options.Identity, w.options.BuildID)
		cancel()
		if err != nil {
			w.logger.Error("deregister activity worker version failed", "err", err)
		}
	}
	if w.executionCache != nil {
		if err := w.executionCache.Close(); err != nil {
			w.logger.Error("close workflow execution cache failed", "err", err)
		}
	}
}

// pollShutdownGrace 是 Server 确认结束长轮询之后，Stop 等在途 poll 返回的上限。接到关闭请求的副本上的长轮询立即返回；
// 落在其他副本上的到期后由 Worker 取消。
const pollShutdownGrace = time.Second

// endPolls 停止拉取。Worker 先断开长轮询会丢投递：Server 察觉断开之前出队的任务写进已关闭的连接，要等可见性超时或
// Workflow Task 超时才重新投递。所以先关闭 polls（不再发起新的 poll），再请求 Server 结束本 session 在途的长轮询：
// 它们在连接仍然连着时以超时结束，已出队的任务照常交给 Worker 处理。之后再取消仍未返回的 poll。
func (w *Worker) endPolls() {
	idle := w.polls.close()
	if w.options.PollShutdown != nil {
		ctx, cancel := context.WithTimeout(context.Background(), deregisterTimeout)
		err := w.options.PollShutdown.ShutdownWorkerPolls(ctx, protocol.NormalizeNamespace(w.options.Namespace), w.options.Identity, w.session)
		cancel()
		if err != nil {
			w.logger.Warn("shutdown worker polls failed; cancelling in-flight polls", "err", err)
		} else {
			timer := time.NewTimer(pollShutdownGrace)
			select {
			case <-idle:
			case <-timer.C:
			}
			timer.Stop()
		}
	}
	w.stopPolling()
}

// trackedPoll 包装 poll：Stop 关闭 polls 之后返回 executor.ErrPollingStopped，否则计入在途 poll。
func (w *Worker) trackedPoll(poll func(context.Context) (protocol.TaskLease, error)) func(context.Context) (protocol.TaskLease, error) {
	return func(ctx context.Context) (protocol.TaskLease, error) {
		if !w.polls.begin() {
			return protocol.TaskLease{}, executor.ErrPollingStopped
		}
		defer w.polls.end()
		return poll(ctx)
	}
}

// pollTracker 统计在途的 poll。close 之后 begin 返回 false，返回的 channel 在在途 poll 全部结束时关闭。
type pollTracker struct {
	mu       sync.Mutex
	inFlight int
	idle     chan struct{}
}

func (t *pollTracker) begin() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.idle != nil {
		return false
	}
	t.inFlight++
	return true
}

func (t *pollTracker) end() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.inFlight--
	if t.idle != nil && t.inFlight == 0 {
		close(t.idle)
	}
}

func (t *pollTracker) close() <-chan struct{} {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.idle = make(chan struct{})
	if t.inFlight == 0 {
		close(t.idle)
	}
	return t.idle
}

func (w *Worker) consumeWorkflowTasks(ctx, workCtx context.Context) {
	pollCtx := protocol.WithWorkerInfo(ctx, w.workerInfo())
	executor.Consume(pollCtx, workCtx, w.logger, "workflow task", w.trackedPoll(w.workflowAPI.PollWorkflowTask), w.HandleWorkflowTask)
}

func (w *Worker) consumeActivityTasks(ctx, workCtx context.Context, taskQueue string) {
	pollCtx := protocol.WithWorkerInfo(protocol.WithNamespace(ctx, w.options.Namespace), w.workerInfo())
	activityCtx := runtimectx.WithWorkerStop(workCtx, w.stopping)
	executor.Consume(pollCtx, activityCtx, w.logger, "activity task",
		w.trackedPoll(func(ctx context.Context) (protocol.TaskLease, error) {
			return w.activityQueue.PollActivityTask(ctx, taskQueue)
		}),
		w.HandleActivityTask)
}

// HandleWorkflowTask 处理一个已租到的 Workflow Task：执行、上报结果并确认或退回租约。
func (w *Worker) HandleWorkflowTask(ctx context.Context, lease protocol.TaskLease) {
	task := lease.Task
	info := w.workerInfo()
	task.WorkerIdentity = info.Identity
	task.WorkerBuildID = info.BuildID
	lease.Task = task
	ctx = w.telemetry.extract(ctx, task.TraceContext)
	ctx, span := w.telemetry.start(ctx, "RunWorkflowTask", tenutotel.TaskSpanAttributes(task)...)
	ctx, observation := withTaskObservation(ctx)
	startedAt := time.Now()
	defer func() {
		elapsed := time.Since(startedAt)
		w.telemetry.recordWorkflowTask(ctx, task, observation.outcome, elapsed)
		w.telemetry.logTask(ctx, "workflow task finished", task, observation, elapsed)
		tenutotel.FinishSpan(span, observation.err)
	}()
	runtimeCtx := protocol.WithWorkflowTaskToken(ctx, task.TaskToken)

	// 执行期的读取（配置、History、状态）也带 task token：HTTP Worker 凭它读取所属 Run。
	taskCtx, cancel := context.WithTimeout(runtimeCtx, w.options.WorkflowTaskTimeout)
	defer cancel()

	opts := executor.WorkerWorkflowTaskOptions{
		WorkflowTaskOptions: executor.WorkflowTaskOptions{
			Capabilities: w.options.WorkflowCapabilities,
			Execute: func(ctx context.Context, workflow *program.WorkflowConfig, events []history.Event) ([]history.Command, error) {
				return interpreter.NewWorkflowExecutorWithContext(ctx, workflow, events).Execute()
			},
			BlockedCause: executor.WorkflowTaskBlockedCause,
		},
		IncrementalABI: history.ExecutorABIVersionV1,
	}
	if incrementalAPI, ok := w.workflowAPI.(IncrementalWorkflowWorkerAPI); ok && w.executionCache != nil {
		opts.RunIncremental = func(ctx, runtimeCtx context.Context, task protocol.Task) executor.WorkflowTaskResult {
			return w.handleWorkflowTaskIncrementally(ctx, runtimeCtx, lease, incrementalAPI)
		}
	}
	result := executor.ExecuteWorkerWorkflowTask(taskCtx, runtimeCtx, task, w.workflowAPI, opts)
	if result.Outcome != "" {
		markTaskOutcome(ctx, result.Outcome, result.Error)
	}
	if result.ReportErr != nil && ctx.Err() != nil {
		// Worker 停止的宽限期已过，上报随 context 一起被取消：放弃这个任务。它已在 Server 上 started，
		// 由 Server 的 Workflow Task 超时重新调度；退回的投递只会被激活 fence 拒绝，所以确认它。
		markTaskOutcome(ctx, "abandoned", result.ReportErr)
		w.ackWorkflow(ctx, lease)
		return
	}
	// Runtime reporting completes before the queue lease is acknowledged.
	w.finishWorkflowLease(ctx, lease, result.ReportErr)
}

// HandleActivityTask 处理一个已租到的 Activity Task。Server 记录 attempt started 后立即确认租约，
// 再执行并上报结果；Server 判定不执行的任务确认丢弃，开始之前出错的任务退回租约。
// 因 Worker 停止而结束的 attempt 交还 Server（outcome released），心跳带回取消请求后结束的
// attempt 不上报（outcome cancelled）。
func (w *Worker) HandleActivityTask(ctx context.Context, lease protocol.TaskLease) {
	task := lease.Task
	info := w.workerInfo()
	task.WorkerIdentity = info.Identity
	task.WorkerBuildID = info.BuildID
	lease.Task = task
	ctx = w.telemetry.extract(ctx, task.TraceContext)
	ctx, span := w.telemetry.start(ctx, "RunActivity", tenutotel.TaskSpanAttributes(task)...)
	ctx, observation := withTaskObservation(ctx)
	startedAt := time.Now()
	defer func() {
		elapsed := time.Since(startedAt)
		w.telemetry.recordActivity(ctx, task, observation.outcome, elapsed)
		w.telemetry.logTask(ctx, "activity finished", task, observation, elapsed)
		tenutotel.FinishSpan(span, observation.err)
	}()
	acked := false
	outcome, executionErr, reportErr := executor.RunActivityTask(ctx, task, w.activityRuntime, w.activities, func() {
		acked = true
		w.ackActivity(ctx, lease)
	})
	switch outcome {
	case "skipped", "pending", "failed", "timed_out":
		markTaskOutcome(ctx, outcome, executionErr)
	case "cancelled", "released":
		markTaskOutcome(ctx, outcome, nil)
	}
	if !acked {
		if errors.Is(reportErr, protocol.ErrInvalidRequest) {
			// Server 以不可重试的 4xx 拒绝开始：立即退回只会让队列无退避地重投同一份投递。按连续拒绝次数退避后再退回。
			w.backoffRejectedBegin(ctx, reportErr)
		}
		w.finishActivityLease(ctx, lease, reportErr)
		return
	}
	w.beginRejections.Store(0)
	// The lease was acknowledged when the attempt started. A result that could not be
	// reported leaves the attempt started; its StartToClose or heartbeat timeout retries it.
	if reportErr != nil {
		markTaskOutcome(ctx, "error", reportErr)
		w.logger.Error("report activity result failed; the attempt's timeout will retry it", "run_id", lease.Task.RunID, "err", reportErr)
	}
}

// backoffRejectedBegin 记一次 Begin 被拒绝，等待 min(1s·2^(n-1), 1min)（n 是连续拒绝次数）；ctx 结束时提前返回。
func (w *Worker) backoffRejectedBegin(ctx context.Context, err error) {
	n := w.beginRejections.Add(1)
	delay := min(beginRejectionInitialBackoff<<min(n-1, 6), beginRejectionMaxBackoff)
	w.logger.Warn("server rejected the activity begin request; returning the task after a backoff",
		"consecutive_rejections", n, "backoff", delay, "err", err)
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-ctx.Done():
	}
}

func (w *Worker) workerInfo() protocol.WorkerInfo {
	info := protocol.WorkerInfo{
		Namespace:            protocol.NormalizeNamespace(w.options.Namespace),
		Identity:             w.options.Identity,
		BuildID:              w.options.BuildID,
		WorkflowCapabilities: append([]string(nil), w.options.WorkflowCapabilities...),
		WorkflowTaskTimeout:  w.options.WorkflowTaskTimeout,
		Session:              w.session,
	}
	if w.executionCache != nil {
		snapshot := w.executionCache.Snapshot()
		info.StickyCache = &snapshot
	}
	return info
}

func (w *Worker) finishWorkflowLease(ctx context.Context, lease protocol.TaskLease, err error) {
	var unsupported *program.UnsupportedWLangCapabilityError
	if errors.As(err, &unsupported) {
		// 本 Worker 不具备程序要求的能力：立即退回，由具备能力的 Worker 领取。
		w.nackWorkflow(ctx, lease, err)
		return
	}
	if errors.Is(err, protocol.ErrInvalidRequest) {
		// Server 以不可重试的 4xx 拒绝上报：立即退回只会让同一份投递无退避地重放。不确认也不退回，投递留到
		// 队列的可见性超时；已 started 的 WFT 由 Server 的 Workflow Task 超时重新调度。
		markTaskOutcome(ctx, "rejected", err)
		w.logger.Error("server rejected the workflow task report; leaving the delivery to the queue's visibility timeout",
			"run_id", lease.Task.RunID, "scheduled_event_id", lease.Task.ScheduledEventID, "err", err)
		return
	}
	if err != nil {
		w.nackWorkflow(ctx, lease, err)
		return
	}
	w.ackWorkflow(ctx, lease)
}

func (w *Worker) finishActivityLease(ctx context.Context, lease protocol.TaskLease, err error) {
	if err != nil {
		w.nackActivity(ctx, lease, err)
		return
	}
	w.ackActivity(ctx, lease)
}

func (w *Worker) ackWorkflow(ctx context.Context, lease protocol.TaskLease) {
	ackCtx, cancel := leaseAckContext(ctx)
	defer cancel()
	if err := w.workflowAPI.AckWorkflowTask(ackCtx, lease); err != nil {
		w.logger.Error("ack workflow task failed", "run_id", lease.Task.RunID, "err", err)
	}
}

func (w *Worker) nackWorkflow(ctx context.Context, lease protocol.TaskLease, err error) {
	markTaskOutcome(ctx, "error", err)
	// 使用独立 context，避免 worker stop 后 ctx cancel 导致 nack 失败。
	nackCtx, cancel := leaseAckContext(ctx)
	defer cancel()
	if nackErr := w.workflowAPI.NackWorkflowTask(nackCtx, lease); nackErr != nil {
		w.logger.Error("nack workflow task failed", "run_id", lease.Task.RunID, "err", nackErr)
	}
}

func (w *Worker) ackActivity(ctx context.Context, lease protocol.TaskLease) {
	ackCtx, cancel := leaseAckContext(ctx)
	defer cancel()
	if err := w.activityQueue.AckActivityTask(ackCtx, lease); err != nil {
		w.logger.Error("ack activity task failed", "run_id", lease.Task.RunID, "err", err)
	}
}

func (w *Worker) nackActivity(ctx context.Context, lease protocol.TaskLease, err error) {
	markTaskOutcome(ctx, "error", err)
	// 使用独立 context，避免 worker stop 后 ctx cancel 导致 nack 失败。
	nackCtx, cancel := leaseAckContext(ctx)
	defer cancel()
	if nackErr := w.activityQueue.NackActivityTask(nackCtx, lease); nackErr != nil {
		w.logger.Error("nack activity task failed", "run_id", lease.Task.RunID, "err", nackErr)
	}
}

func leaseAckContext(parent context.Context) (context.Context, context.CancelFunc) {
	// 历史已推进后仍需完成 ack，避免 worker 停止让旧 lease 阻塞队列。
	return context.WithTimeout(context.WithoutCancel(parent), 5*time.Second)
}
