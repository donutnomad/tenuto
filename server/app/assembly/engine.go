package assembly

import (
	"context"
	"sync"

	"github.com/donutnomad/tenuto/internal/worker"
	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/sdk/activity"
	"github.com/donutnomad/tenuto/server/biz/domain/definition"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/biz/service/engine"
)

// Engine 把一个 Server 与同进程的 Worker 组合起来，供示例与测试使用。Start 之后与 Server 应用一样运行周期补偿
// （engine.Reconciler），重启后晚于启动恢复窗口的定时器由它在到期前挂上；Stop 时停止。
type Engine struct {
	server     *engine.Server
	worker     *worker.Worker
	mgr        execution.ExecutionManager
	reconciler engine.Reconciler

	reconcileCancel context.CancelFunc
	reconcileWG     sync.WaitGroup
}

func newEngine(server *engine.Server, mgr execution.ExecutionManager, registry *activity.Registry, workerOpts worker.WorkerOptions, opts Options) *Engine {
	reconciler := engine.Reconciler{Server: server, Interval: opts.ReconcileInterval, FullScanInterval: opts.ReconcileFullScanInterval}
	if reconciler.Interval <= 0 {
		reconciler.Interval = engine.DefaultReconcileInterval
	}
	if reconciler.FullScanInterval <= 0 {
		reconciler.FullScanInterval = engine.DefaultReconcileFullScanInterval
	}
	return &Engine{server: server, worker: NewWorker(server, registry, workerOpts), mgr: mgr, reconciler: reconciler}
}

func (e *Engine) RegisterWorkflow(wf *program.WorkflowConfig) (definition.WorkflowConfigRef, error) {
	return e.server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, wf)
}

func (e *Engine) Start(ctx context.Context) {
	if err := e.server.RecoverRunningWorkflows(ctx, e.reconciler.Lookahead()); err != nil {
		e.server.Logger().Error("recover running workflows failed", "err", err)
	}
	reconcileCtx, cancel := context.WithCancel(ctx)
	e.reconcileCancel = cancel
	e.reconcileWG.Go(func() { _ = e.reconciler.Start(reconcileCtx) })
	e.worker.Start(ctx)
}

func (e *Engine) Stop() {
	e.worker.Stop()
	if e.reconcileCancel != nil {
		e.reconcileCancel()
	}
	e.reconcileWG.Wait()
	e.server.Stop()
}

func (e *Engine) Run(ctx context.Context, workflowType string, input map[string]any) (string, error) {
	run, err := e.server.StartWorkflow(ctx, "", workflowType, "", "", input)
	if err != nil {
		return "", err
	}
	return run.ID, nil
}

func (e *Engine) GetRun(ctx context.Context, runID string) (*execution.Run, error) {
	return e.mgr.Get(runID)
}

func (e *Engine) GetEvents(ctx context.Context, runID string) ([]history.Event, error) {
	return e.mgr.GetEvents(runID)
}
