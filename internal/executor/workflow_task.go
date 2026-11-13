package executor

import (
	"context"
	"errors"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
)

// WorkflowTaskBackend is the Run runtime used by a workflow worker. Queue
// settlement stays with the caller, after the runtime has accepted the result.
type WorkflowTaskBackend interface {
	GetEvents(runID string) ([]history.Event, error)
	// GetWorkflowConfig 读取 task.ProgramVersion 这一版 Program 的配置：AutoUpgrade 在 WFT 边界选定版本后，
	// 它就是本次任务的一致快照。
	GetWorkflowConfig(ctx context.Context, runID, programVersion string) (*program.WorkflowConfig, error)
	HandleWorkflowTaskCompleted(ctx context.Context, runID string, scheduledEventID int64, commands []history.Command, workerIdentity string) error
	HandleWorkflowTaskTimedOut(ctx context.Context, runID string, scheduledEventID int64, errMsg string) error
	HandleWorkflowTaskFailed(ctx context.Context, runID string, scheduledEventID int64, cause, reason string) error
}

type workflowTaskContextReader interface {
	GetEventsContext(ctx context.Context, runID string) ([]history.Event, error)
}

type WorkflowTaskOptions struct {
	Capabilities  []string
	Execute       func(context.Context, *program.WorkflowConfig, []history.Event) ([]history.Command, error)
	BlocksOnError func(error) bool
	BlockedCause  string
}

type WorkflowTaskResult struct {
	Outcome   string
	Error     error
	ReportErr error
	// fullHistory 表示增量路径没有执行这个任务，ExecuteWorkerWorkflowTask 改走 full-history 路径。
	fullHistory bool
}

// RunWorkflowTask handles the full-history execution path. The caller owns
// the task deadline, incremental-cache choice, telemetry, and queue lease.
func RunWorkflowTask(taskCtx, runtimeCtx context.Context, task protocol.Task, backend WorkflowTaskBackend, opts WorkflowTaskOptions) WorkflowTaskResult {
	if opts.Execute == nil {
		err := errors.New("workflow task executor is required")
		return WorkflowTaskResult{Outcome: "error", Error: err, ReportErr: err}
	}
	events, err := ReadWorkflowEvents(taskCtx, backend, task.RunID)
	if err != nil {
		return workflowReadFailure(taskCtx, runtimeCtx, task, backend, err)
	}
	if workflowTaskClosed(events, task.ScheduledEventID) {
		return WorkflowTaskResult{Outcome: "skipped"}
	}
	wfConfig, err := backend.GetWorkflowConfig(taskCtx, task.RunID, task.ProgramVersion)
	if err != nil {
		return workflowReadFailure(taskCtx, runtimeCtx, task, backend, err)
	}
	if err := wfConfig.Program.ValidateCapabilities(opts.Capabilities); err != nil {
		return WorkflowTaskResult{Outcome: "error", Error: err, ReportErr: err}
	}

	type executionResult struct {
		commands []history.Command
		err      error
	}
	resultCh := make(chan executionResult, 1)
	go func() {
		commands, executeErr := opts.Execute(taskCtx, wfConfig, events)
		resultCh <- executionResult{commands: commands, err: executeErr}
	}()

	select {
	case result := <-resultCh:
		if workflowTaskTimedOut(taskCtx, result.err) {
			return reportWorkflowTimeout(taskCtx, runtimeCtx, task, backend, result.err)
		}
		if result.err != nil {
			if opts.BlocksOnError != nil && opts.BlocksOnError(result.err) {
				err = backend.HandleWorkflowTaskFailed(runtimeCtx, task.RunID, task.ScheduledEventID, opts.BlockedCause, result.err.Error())
			} else {
				err = backend.HandleWorkflowTaskCompleted(runtimeCtx, task.RunID, task.ScheduledEventID,
					[]history.Command{{Type: history.CmdFailWorkflow, Error: result.err.Error()}}, task.WorkerIdentity)
			}
			return WorkflowTaskResult{Outcome: "failed", Error: result.err, ReportErr: err}
		}
		err = backend.HandleWorkflowTaskCompleted(runtimeCtx, task.RunID, task.ScheduledEventID, result.commands, task.WorkerIdentity)
		return WorkflowTaskResult{ReportErr: err}
	case <-taskCtx.Done():
		return reportWorkflowTimeout(taskCtx, runtimeCtx, task, backend, taskCtx.Err())
	}
}

func workflowReadFailure(taskCtx, runtimeCtx context.Context, task protocol.Task, backend WorkflowTaskBackend, err error) WorkflowTaskResult {
	if workflowTaskTimedOut(taskCtx, err) {
		return reportWorkflowTimeout(taskCtx, runtimeCtx, task, backend, err)
	}
	return WorkflowTaskResult{Outcome: "error", Error: err, ReportErr: err}
}

func reportWorkflowTimeout(taskCtx, runtimeCtx context.Context, task protocol.Task, backend WorkflowTaskBackend, err error) WorkflowTaskResult {
	if errors.Is(taskCtx.Err(), context.Canceled) {
		// 取消而不是到期：Worker 停止的宽限期已过。放弃这个任务、不上报，
		// 由 Server 的 Workflow Task 超时重新调度。
		return WorkflowTaskResult{Outcome: "abandoned", Error: context.Cause(taskCtx)}
	}
	message := context.DeadlineExceeded.Error()
	if taskCtx.Err() != nil {
		message = taskCtx.Err().Error()
	} else if err != nil {
		message = err.Error()
	}
	reportErr := backend.HandleWorkflowTaskTimedOut(context.WithoutCancel(runtimeCtx), task.RunID, task.ScheduledEventID, message)
	return WorkflowTaskResult{Outcome: "timeout", Error: errors.New(message), ReportErr: reportErr}
}

func workflowTaskTimedOut(taskCtx context.Context, err error) bool {
	return taskCtx.Err() != nil || errors.Is(err, context.DeadlineExceeded)
}

func workflowTaskClosed(events []history.Event, scheduledEventID int64) bool {
	for _, event := range events {
		if event.WorkflowTaskCompleted != nil && event.WorkflowTaskCompleted.ScheduledEventID == scheduledEventID {
			return true
		}
		if event.WorkflowTaskTimedOut != nil && event.WorkflowTaskTimedOut.ScheduledEventID == scheduledEventID {
			return true
		}
	}
	return false
}

// ReadWorkflowEvents 读取 Run 的完整 History；backend 提供带 context 的读取时用它传递取消。
func ReadWorkflowEvents(ctx context.Context, backend WorkflowTaskBackend, runID string) ([]history.Event, error) {
	if reader, ok := backend.(workflowTaskContextReader); ok {
		return reader.GetEventsContext(ctx, runID)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return backend.GetEvents(runID)
}
