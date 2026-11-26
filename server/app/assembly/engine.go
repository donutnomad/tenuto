package assembly

import (
	"context"

	"github.com/donutnomad/tenuto/internal/worker"
	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/sdk/activity"
	"github.com/donutnomad/tenuto/server/biz/domain/definition"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/biz/service/engine"
)

// Engine 把一个 Server 与同进程的 Worker 组合起来，供示例与测试使用。
type Engine struct {
	server *engine.Server
	worker *worker.Worker
	mgr    execution.ExecutionManager
}

func newEngine(server *engine.Server, mgr execution.ExecutionManager, registry *activity.Registry, opts worker.WorkerOptions) *Engine {
	return &Engine{server: server, worker: NewWorker(server, registry, opts), mgr: mgr}
}

func (e *Engine) RegisterWorkflow(wf *program.WorkflowConfig) (definition.WorkflowConfigRef, error) {
	return e.server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, wf)
}

func (e *Engine) Start(ctx context.Context) {
	if err := e.server.RecoverRunningWorkflows(ctx, engine.DefaultReconcileLookahead); err != nil {
		e.server.Logger().Error("recover running workflows failed", "err", err)
	}
	e.worker.Start(ctx)
}

func (e *Engine) Stop() {
	e.worker.Stop()
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
