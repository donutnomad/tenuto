// Package assembly 为 engine.Server 选择依赖的具体实现并完成装配。
// 应用入口、示例与包外测试经这里构造 Server；未指定的仓储取内存实现。
package assembly

import (
	"time"

	"github.com/donutnomad/tenuto/internal/executor/interpreter"
	"github.com/donutnomad/tenuto/internal/worker"
	"github.com/donutnomad/tenuto/sdk/activity"
	"github.com/donutnomad/tenuto/server/biz/domain/batch"
	"github.com/donutnomad/tenuto/server/biz/domain/clock"
	"github.com/donutnomad/tenuto/server/biz/domain/definition"
	"github.com/donutnomad/tenuto/server/biz/domain/dispatch"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/biz/domain/schedule"
	"github.com/donutnomad/tenuto/server/biz/service/engine"
	"github.com/donutnomad/tenuto/server/infra/memory"
	"github.com/donutnomad/tenuto/server/infra/persistence"
	"github.com/donutnomad/tenuto/server/infra/systemclock"
)

// Stores 是装配一个 Server 所需的仓储。Executions 必填，其余留空时使用内存实现。
type Stores struct {
	Executions     execution.ExecutionManager
	Configs        definition.ConfigRepository
	Dispatches     dispatch.Repository
	WorkerVersions dispatch.WorkerVersionRepository
	Schedules      schedule.Repository
	Batches        batch.Repository
}

// Persistent 取 GORM Store 的各仓储，它们共用同一个数据库连接。
func Persistent(store *persistence.Store) Stores {
	return Stores{
		Executions:     store.Executions,
		Configs:        store.Definitions,
		Dispatches:     store.Dispatches,
		WorkerVersions: store.Dispatches,
		Schedules:      store.Schedules,
		Batches:        store.Batches,
	}
}

// Options 是装配一个 Server 所需的全部依赖。
type Options struct {
	Stores        Stores
	WorkflowQueue dispatch.TaskQueue
	// ActivityRouter 按 task queue 投递与领取 Activity 任务：内存部署用 memory.NewActivityQueueRouter，
	// TaskQ 部署用 taskq.NewLazyActivityQueueRouter。
	ActivityRouter dispatch.ActivityTaskQueueRouter
	// TaskTokens 签发与校验 task token。留空时使用本进程随机密钥：token 只在这个 Server 进程内有效，
	// 多副本部署必须传入由共享密钥构造的 codec。
	TaskTokens *dispatch.TaskTokenCodec
	// Clock 是 Server 的时间来源，留空时使用系统时钟；测试传入 clocktest.Fake 手动推进时间。
	Clock clock.Clock
	// ReconcileInterval 与 ReconcileFullScanInterval 是 Engine 周期补偿与全量补偿的间隔，留空时取
	// engine.DefaultReconcileInterval 与 engine.DefaultReconcileFullScanInterval。NewServer 不使用它们。
	ReconcileInterval         time.Duration
	ReconcileFullScanInterval time.Duration
}

// NewServer 按 opts 选定各依赖的具体实现并构造 Server；未给出的仓储使用内存实现。
func NewServer(opts Options, options ...engine.ServerOption) *engine.Server {
	stores := opts.Stores
	tokens := opts.TaskTokens
	if tokens == nil {
		tokens = dispatch.NewRandomTaskTokenCodec()
	}
	clk := opts.Clock
	if clk == nil {
		clk = systemclock.Clock{}
	}
	deps := engine.ServerDeps{
		Executions:     stores.Executions,
		WorkflowQueue:  opts.WorkflowQueue,
		ActivityRouter: opts.ActivityRouter,
		TaskTokens:     tokens,
		Configs:        stores.Configs,
		Dispatches:     stores.Dispatches,
		WorkerVersions: stores.WorkerVersions,
		Schedules:      stores.Schedules,
		Batches:        stores.Batches,
		QueryEvaluator: interpreter.QueryEvaluator{},
		Clock:          clk,
	}
	if deps.Configs == nil {
		// 内存 ExecutionManager 自带配置仓储（RunStore 内嵌 DefinitionRepository），测试经它读回配置。
		configs, ok := stores.Executions.(definition.ConfigRepository)
		if !ok {
			configs = memory.NewDefinitionRepository()
		}
		deps.Configs = configs
	}
	if deps.Dispatches == nil {
		deps.Dispatches = memory.NewDispatchRepository()
	}
	if deps.WorkerVersions == nil {
		deps.WorkerVersions = memory.NewWorkerVersionRepository()
	}
	if deps.Schedules == nil {
		deps.Schedules = memory.NewScheduleRepository()
	}
	if deps.Batches == nil {
		deps.Batches = memory.NewBatchRepository()
	}
	return engine.NewServer(deps, options...)
}

// NewEngine 构造一个 Server 与同进程 Worker 的组合。
func NewEngine(opts Options, registry *activity.Registry, workerOpts worker.WorkerOptions) *Engine {
	return newEngine(NewServer(opts), opts.Stores.Executions, registry, workerOpts, opts)
}
