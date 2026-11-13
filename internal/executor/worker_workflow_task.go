package executor

import (
	"context"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
)

type WorkerWorkflowBackend interface {
	WorkflowTaskBackend
	GetRunStatus(runID string) (history.Status, error)
}

type workflowStatusContextReader interface {
	GetRunStatusContext(ctx context.Context, runID string) (history.Status, error)
}

type WorkerWorkflowTaskOptions struct {
	WorkflowTaskOptions
	IncrementalABI string
	// RunIncremental 执行增量路径：实现用 ExecutionCacheKeyForTask 从 task 得到 cache key，
	// RunIncrementalWorkflowTask 在 cache miss 时读取 definition。
	RunIncremental func(context.Context, context.Context, protocol.Task) WorkflowTaskResult
}

// ExecuteWorkerWorkflowTask chooses a full-history or incremental execution,
// then reports its result to the Run runtime. Queue settlement stays outside.
func ExecuteWorkerWorkflowTask(taskCtx, runtimeCtx context.Context, task protocol.Task, backend WorkerWorkflowBackend, opts WorkerWorkflowTaskOptions) WorkflowTaskResult {
	status, err := readWorkflowRunStatus(taskCtx, backend, task.RunID)
	if err != nil {
		return workflowReadFailure(taskCtx, runtimeCtx, task, backend, err)
	}
	if history.IsTerminalStatus(status) {
		return WorkflowTaskResult{Outcome: "skipped"}
	}
	if opts.RunIncremental != nil && task.ExecutorABIVersion == opts.IncrementalABI {
		// cache key 完全来自 task。cache hit 直接复用 session；definition 只在
		// cache miss 时由 RunIncrementalWorkflowTask 读取、校验和解析。
		if result := opts.RunIncremental(taskCtx, runtimeCtx, task); !result.fullHistory {
			return result
		}
	}
	return RunWorkflowTask(taskCtx, runtimeCtx, task, backend, opts.WorkflowTaskOptions)
}

func readWorkflowRunStatus(ctx context.Context, backend WorkerWorkflowBackend, runID string) (history.Status, error) {
	if reader, ok := backend.(workflowStatusContextReader); ok {
		return reader.GetRunStatusContext(ctx, runID)
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	return backend.GetRunStatus(runID)
}
