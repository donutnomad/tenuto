package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/donutnomad/tenuto/internal/executor"
	internalworker "github.com/donutnomad/tenuto/internal/worker"
	"github.com/donutnomad/tenuto/sdk/activity"
	"github.com/donutnomad/tenuto/sdk/client"
	"github.com/donutnomad/tenuto/sdk/internal/runtimeclient"
	"github.com/donutnomad/tenuto/sdk/registration"
	tenutotel "github.com/donutnomad/tenuto/telemetry"
)

const (
	// DefaultWorkflowTaskTimeout 是 Options.WorkflowTaskTimeout 的缺省值。
	DefaultWorkflowTaskTimeout = internalworker.DefaultWorkflowTaskTimeout
	// DefaultGracefulStopTimeout 是 Options.GracefulStopTimeout 的缺省值：覆盖一个缺省超时的
	// Workflow Task，并留在常见编排器（Kubernetes 缺省 30s）的终止宽限期之内。
	DefaultGracefulStopTimeout = internalworker.DefaultGracefulStopTimeout
)

// Role 决定 Worker 处理哪类任务。
type Role string

const (
	// RoleBoth 同时处理 Workflow Task 与 Activity Task。
	RoleBoth Role = Role(internalworker.WorkerRoleBoth)
	// RoleWorkflow 只处理 Workflow Task。
	RoleWorkflow Role = Role(internalworker.WorkerRoleWorkflow)
	// RoleActivity 只处理 Activity Task。
	RoleActivity Role = Role(internalworker.WorkerRoleActivity)
)

// Options 配置 Worker。
type Options struct {
	// Role 为空时：Activities 登记了 Activity 取 RoleBoth，否则取 RoleWorkflow。
	Role Role
	// Workflows 是随二进制发布的 Workflow 定义（生成代码里的 WorkflowDefinition()）。Start 在开始拉取任务
	// 之前登记它们：新内容首次出现时成为 latest，同一内容重复登记保持 latest；登记失败时不拉取任务，
	// 错误由 WaitReady 返回。
	Workflows []registration.Definition
	// Activities 是本 Worker 执行的 Activity（生成代码里的 RegisterActivities 登记到它）。
	// 契约随 Worker 的 Activity 路由一起上报。
	Activities *activity.Registry
	// ActivityTaskQueues 是拉取 Activity Task 的队列；为空时取 "default"。
	ActivityTaskQueues []string
	// WorkflowConcurrency 与 ActivityConcurrency 是并发处理的任务数（Activity 按每个队列计），缺省 1。
	WorkflowConcurrency int
	ActivityConcurrency int
	// WorkflowTaskTimeout 是一个 Workflow Task 的执行上限，缺省 DefaultWorkflowTaskTimeout。
	WorkflowTaskTimeout time.Duration
	// Identity 标识这个 Worker 进程，Activity 路由与粘性执行按它寻址；缺省取 client.Options.Identity。
	// 进程重启后保持不变的 Identity 能让 Server 把它当作同一个 Worker。
	Identity string
	// BuildID 是这个二进制的构建标识，随 Activity 路由上报。
	BuildID string
	// ActivityWorkerVersionDefault 与 ActivityWorkerVersionWeight 只在 Activity 没有指定队列、多个队列都承接它的类型时
	// 参与选择队列：Default 的登记优先，其余按 Weight 加权。它们不决定由哪个 build 执行，任务投到队列后由领取它的 Worker 执行。
	ActivityWorkerVersionDefault bool
	ActivityWorkerVersionWeight  int
	// ActivityWorkerVersionHeartbeatInterval 是 Activity 路由的心跳间隔，缺省取 TTL 的一半。
	ActivityWorkerVersionHeartbeatInterval time.Duration
	// ActivityWorkerVersionHeartbeatTTL 是 Server 在没有心跳时保留路由的时长；零值用 Server 的缺省值。
	ActivityWorkerVersionHeartbeatTTL time.Duration
	// GracefulStopTimeout 是 Stop 等待在途 Activity 与 Workflow Task 结束的上限，<=0 时取
	// DefaultGracefulStopTimeout。到期后仍在运行的 Activity 的 context 被取消，它们和未完成的
	// Workflow Task 都不上报失败，交还 Server 重新派发。
	GracefulStopTimeout time.Duration
	// ExecutionCache 非 nil 时启用粘性执行：Workflow 的执行现场留在本进程，后续 Workflow Task 只重放
	// 新增事件。需要稳定的 Identity。
	ExecutionCache *ExecutionCacheOptions
	// Telemetry 给任务执行加 trace、指标与日志；零值时沿用 client.Options.Telemetry。Worker 的日志（任务结束记录，
	// 以及 poll、ack/nack、路由登记与心跳、结果上报的失败）写入其中的 Logger，Logger 为 nil 时写入 slog.Default()。
	Telemetry tenutotel.Providers
}

// ExecutionCacheOptions 是粘性执行缓存的容量。零值字段取括号里的缺省值。
type ExecutionCacheOptions struct {
	// MaxEntries 是缓存的执行现场个数上限（1000）。
	MaxEntries int
	// MaxEstimatedBytes 是估算内存占用上限（256 MiB）。
	MaxEstimatedBytes int64
	// IdleTTL 是执行现场空闲多久后被淘汰（5m）。
	IdleTTL time.Duration
}

// Worker 从 Server 拉取 Workflow Task 与 Activity Task，执行并上报结果。
type Worker struct {
	inner *internalworker.Worker
}

// New 创建经 c 连接 Server 的 Worker，namespace 取自 c。c 必须由 client.Dial 创建。
//
// 与 Temporal 不同，New 不接收 task queue 参数：Workflow Task 不分队列，由 Server 派给任意 Workflow Worker；
// Activity 的队列由 Options.ActivityTaskQueues 指定。Workflow 与 Activity 经 Options 登记，没有
// RegisterWorkflow / RegisterActivity 方法：Workflow 是生成器打进二进制的定义，Activity 由生成的
// RegisterActivities 登记到 activity.Registry。
func New(c client.Client, options Options) (*Worker, error) {
	var cache *executor.ExecutionCache
	if options.ExecutionCache != nil {
		var err error
		if cache, err = newExecutionCache(*options.ExecutionCache); err != nil {
			return nil, err
		}
	}
	return newWorker(c, options, cache)
}

// newWorker 用已创建的粘性执行缓存（可为 nil）组装 Worker。
func newWorker(c client.Client, options Options, cache *executor.ExecutionCache) (*Worker, error) {
	provider, ok := c.(runtimeclient.ConnProvider)
	if !ok || provider == nil {
		return nil, errors.New("worker: client must be created by client.Dial")
	}
	conn := provider.RuntimeConn()
	inner := internalworker.WorkerOptions{
		Namespace:                              conn.Namespace,
		WorkflowConcurrency:                    options.WorkflowConcurrency,
		ActivityConcurrency:                    options.ActivityConcurrency,
		Role:                                   internalworker.WorkerRole(options.Role),
		ActivityTaskQueues:                     options.ActivityTaskQueues,
		WorkflowTaskTimeout:                    options.WorkflowTaskTimeout,
		Identity:                               options.Identity,
		BuildID:                                options.BuildID,
		ActivityWorkerVersionDefault:           options.ActivityWorkerVersionDefault,
		ActivityWorkerVersionWeight:            options.ActivityWorkerVersionWeight,
		ActivityWorkerVersionRegistry:          conn.Workflow,
		ActivityWorkerVersionHeartbeatInterval: options.ActivityWorkerVersionHeartbeatInterval,
		ActivityWorkerVersionHeartbeatTTL:      options.ActivityWorkerVersionHeartbeatTTL,
		GracefulStopTimeout:                    options.GracefulStopTimeout,
		Telemetry:                              options.Telemetry,
		Workflows:                              options.Workflows,
		ExecutionCache:                         cache,
	}
	if inner.Identity == "" {
		inner.Identity = conn.Identity
	}
	if inner.Telemetry == (tenutotel.Providers{}) {
		inner.Telemetry = conn.Telemetry
	}
	if inner.Telemetry.Logger == nil {
		inner.Telemetry.Logger = slog.Default()
	}
	switch options.Role {
	case "":
		inner.Role = internalworker.WorkerRoleWorkflow
		if options.Activities != nil && len(options.Activities.Types()) > 0 {
			inner.Role = internalworker.WorkerRoleBoth
		}
	case RoleBoth, RoleWorkflow, RoleActivity:
	default:
		return nil, fmt.Errorf("worker: unknown role %q", options.Role)
	}
	return &Worker{inner: internalworker.New(conn.WorkflowTasks, conn.Activities, conn.Activities, options.Activities, inner)}, nil
}

func newExecutionCache(options ExecutionCacheOptions) (*executor.ExecutionCache, error) {
	config := executor.ExecutionCacheConfig{
		MaxEntries: options.MaxEntries, MaxEstimatedBytes: options.MaxEstimatedBytes, IdleTTL: options.IdleTTL,
	}
	if config.MaxEntries <= 0 {
		config.MaxEntries = 1000
	}
	if config.MaxEstimatedBytes <= 0 {
		config.MaxEstimatedBytes = 256 << 20
	}
	if config.IdleTTL <= 0 {
		config.IdleTTL = 5 * time.Minute
	}
	cache, err := executor.NewExecutionCache(config)
	if err != nil {
		return nil, fmt.Errorf("worker: %w", err)
	}
	return cache, nil
}

// Start 登记 Options.Workflows 与 Activity 路由，然后在后台开始拉取任务，立即返回。启动结果由 WaitReady
// 读取。ctx 是在途任务的执行 context：它被取消时在途任务立即中止（与 Stop 的宽限期到期相同）；平常用 Stop 停止。
func (w *Worker) Start(ctx context.Context) {
	w.inner.Start(ctx)
}

// WaitReady 等待 Start 的登记完成，返回登记错误；ctx 先结束时返回 ctx 的错误。
func (w *Worker) WaitReady(ctx context.Context) error {
	return w.inner.WaitReady(ctx)
}

// Run 启动 Worker 并阻塞到 ctx 结束，然后优雅停止。登记失败时停止并返回错误；ctx 结束后返回 nil。
func (w *Worker) Run(ctx context.Context) error {
	w.inner.Start(context.WithoutCancel(ctx))
	defer w.inner.Stop()
	if err := w.inner.WaitReady(ctx); err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return err
	}
	<-ctx.Done()
	return nil
}

// Stop 优雅停止 Worker：停止拉取新任务，关闭 activity.GetWorkerStopChannel，等在途任务结束（最多
// GracefulStopTimeout），再向 Server 注销本 Worker 的 Activity 路由。可以重复、并发调用，每个调用都在
// 停止完成后返回。
func (w *Worker) Stop() {
	w.inner.Stop()
}
