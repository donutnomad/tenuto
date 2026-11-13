package worker

import (
	"context"
	"time"

	"github.com/donutnomad/tenuto/internal/executor"
	"github.com/donutnomad/tenuto/internal/executor/interpreter"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
)

func (w *Worker) handleWorkflowTaskIncrementally(
	ctx context.Context,
	runtimeCtx context.Context,
	lease protocol.TaskLease,
	api IncrementalWorkflowWorkerAPI,
) executor.WorkflowTaskResult {
	task := lease.Task
	key, err := executor.ExecutionCacheKeyForTask(task, history.ExecutorABIVersionV1)
	if err != nil {
		return executor.WorkflowTaskResult{Outcome: "error", Error: err, ReportErr: err}
	}
	return executor.RunIncrementalWorkflowTask(ctx, runtimeCtx, task, w.workflowAPI, executor.IncrementalTaskOptions{
		Cache:        w.executionCache,
		Key:          key,
		Capabilities: w.options.WorkflowCapabilities,
		NewSession:   interpreter.NewExecutionSession,
		NewHistory:   interpreter.NewSliceHistoryIterator,
		ReadEvents: func(ctx context.Context, runID string) ([]history.Event, error) {
			return executor.ReadWorkflowEvents(ctx, w.workflowAPI, runID)
		},
		GetActivation: func(ctx context.Context, task protocol.Task, afterEventID, generation int64, activationID string) (history.WorkflowActivation, error) {
			return api.GetWorkflowActivation(ctx, history.WorkflowActivationRequest{
				Task: task, AfterEventID: afterEventID, ExecutionGeneration: generation, ActivationID: activationID,
			})
		},
		Complete: func(ctx context.Context, task protocol.Task, activation history.ActivationResult) (history.WorkflowTaskCompletionAck, error) {
			return api.CompleteWorkflowActivation(ctx, history.WorkflowTaskCompletionRequest{
				Task: task, Activation: activation, Commands: activation.Commands,
			})
		},
		ActivationID: history.WorkflowActivationID(task),
	}, executor.WorkflowTaskOptions{
		BlocksOnError: func(err error) bool { return executor.ClassifyWorkflowError(err) == executor.WorkflowErrorBlock },
		BlockedCause:  history.WorkflowTaskFailedCauseNonDeterministic,
	})
}

func (w *Worker) sweepExecutionCache(ctx context.Context) {
	interval := w.executionCache.IdleTTL() / 2
	if interval > time.Minute {
		interval = time.Minute
	}
	if interval < time.Second {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	// cache 的 hits/misses 是进程内累计值，而 OTel counter 要的是增量，
	// 故记住上一轮快照、每轮只上报差值。搭在既有的 sweep 循环上，不新起 goroutine。
	var lastHits, lastMisses uint64
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := w.executionCache.SweepExpired(); err != nil {
				w.logger.Error("sweep workflow execution cache failed", "err", err)
			}
			snapshot := w.executionCache.Snapshot()
			w.telemetry.recordCacheLookups(ctx, snapshot.Hits-lastHits, snapshot.Misses-lastMisses)
			lastHits, lastMisses = snapshot.Hits, snapshot.Misses
		}
	}
}
