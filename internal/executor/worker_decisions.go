package executor

import (
	"context"
	"errors"
	"fmt"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
)

func ValidateWorkerCache(cache *ExecutionCache, identity string) error {
	if cache != nil && identity == "" {
		return fmt.Errorf("sticky execution cache requires a stable worker identity")
	}
	return nil
}

func AppendWorkflowCapabilities(capabilities []string, additions ...string) []string {
	seen := make(map[string]struct{}, len(capabilities)+len(additions))
	result := make([]string, 0, len(capabilities)+len(additions))
	for _, capability := range append(append([]string(nil), capabilities...), additions...) {
		if _, ok := seen[capability]; ok {
			continue
		}
		seen[capability] = struct{}{}
		result = append(result, capability)
	}
	return result
}

// SelectIncrementalWorkflow selects the cached execution path after capability
// validation. A task built for another executor ABI uses the full-history path.
func SelectIncrementalWorkflow(task protocol.Task, workflow *program.WorkflowConfig, capabilities []string, expectedABI string) (bool, error) {
	if task.ExecutorABIVersion != expectedABI {
		return false, nil
	}
	if err := workflow.Program.ValidateCapabilities(capabilities); err != nil {
		return false, err
	}
	return true, nil
}

// RunIncrementalWorkflowTask reports execution failures to the Run runtime.
// The caller settles the queue lease after receiving ReportErr.
// 调用方没有给出 Workflow 时，配置推迟到 cache miss 才从 backend 读取并校验。
func RunIncrementalWorkflowTask(taskCtx, runtimeCtx context.Context, task protocol.Task, backend WorkflowTaskBackend, incremental IncrementalTaskOptions, opts WorkflowTaskOptions) WorkflowTaskResult {
	if incremental.Workflow == nil && incremental.LoadWorkflow == nil {
		incremental.LoadWorkflow = func(ctx context.Context) (*program.WorkflowConfig, error) {
			workflow, err := backend.GetWorkflowConfig(ctx, task.RunID, task.ProgramVersion)
			if err != nil {
				return nil, err
			}
			selected, err := SelectIncrementalWorkflow(task, workflow, incremental.Capabilities, task.ExecutorABIVersion)
			if err != nil {
				return nil, err
			}
			if !selected {
				return nil, ErrIncrementalNotSelected
			}
			return workflow, nil
		}
	}
	result := RunIncrementalTask(taskCtx, runtimeCtx, task, incremental)
	return reportIncrementalTaskResult(taskCtx, runtimeCtx, task, backend, result, opts)
}

func reportIncrementalTaskResult(taskCtx, runtimeCtx context.Context, task protocol.Task, backend WorkflowTaskBackend, result IncrementalTaskResult, opts WorkflowTaskOptions) WorkflowTaskResult {
	switch result.Failure {
	case IncrementalTaskReadFailure:
		if workflowTaskTimedOut(taskCtx, result.Err) {
			return reportWorkflowTimeout(taskCtx, runtimeCtx, task, backend, result.Err)
		}
		return WorkflowTaskResult{Outcome: "error", Error: result.Err, ReportErr: result.Err}
	case IncrementalTaskExecutionFailure:
		if workflowTaskTimedOut(taskCtx, result.Err) {
			return reportWorkflowTimeout(taskCtx, runtimeCtx, task, backend, result.Err)
		}
		var reportErr error
		if cause := opts.blockedCause(result.Err); cause != "" {
			reportErr = backend.HandleWorkflowTaskFailed(runtimeCtx, task.RunID, task.ScheduledEventID, cause, result.Err.Error())
		} else {
			reportErr = backend.HandleWorkflowTaskCompleted(runtimeCtx, task.RunID, task.ScheduledEventID,
				[]history.Command{{Type: history.CmdFailWorkflow, Error: result.Err.Error()}}, task.WorkerIdentity)
		}
		return WorkflowTaskResult{Outcome: "failed", Error: result.Err, ReportErr: reportErr}
	case IncrementalTaskSessionFailure:
		// session 已被丢弃。写入 WorkflowTaskFailed 后由 Server 退避重调度，下一次走 cold restore；
		// history 本身损坏时每次重试都在 history 中留下一条带原因的 WorkflowTaskFailed。
		reportErr := backend.HandleWorkflowTaskFailed(runtimeCtx, task.RunID, task.ScheduledEventID,
			history.WorkflowTaskFailedCauseSessionInvalid, result.Err.Error())
		return WorkflowTaskResult{Outcome: "error", Error: result.Err, ReportErr: reportErr}
	case IncrementalTaskNotSelected:
		return WorkflowTaskResult{fullHistory: true}
	case IncrementalTaskCompletionFailure:
		if errors.Is(result.Err, protocol.ErrPayloadTooLarge) {
			return reportCommandsTooLarge(runtimeCtx, task, backend, result.Err)
		}
		return WorkflowTaskResult{ReportErr: result.Err}
	case IncrementalTaskCacheFailure:
		return WorkflowTaskResult{Outcome: "error", Error: result.Err, ReportErr: result.Err}
	default:
		return WorkflowTaskResult{}
	}
}
