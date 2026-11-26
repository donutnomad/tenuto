package executionview

import (
	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/definition"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
)

// RunWithBlocked 是 get-run 的响应：Run、execution state 投影的 blocked 信息，
// 以及失败 Run 的结构化终态失败。blocked 与终态失败只存在于响应层，Run 的存储结构保持原样。
type RunWithBlocked struct {
	Run     *execution.Run        `json:"run"`
	Blocked execution.BlockedInfo `json:"blocked"`
	// TerminalFailure 取自历史里的 WorkflowFailed 事件（message/type/nonRetryable/details/cause）。
	TerminalFailure *protocol.Failure `json:"terminal_failure,omitempty"`
}

// GetRunWithState 返回请求寻址的 Run 及其 execution state，寻址规则见 execution.ResolveRun；Run.Status 取 execution
// state 的投影。
func (v *View) GetRunWithState(namespace, workflowID, runID string) (*execution.Run, *execution.WorkflowExecutionState, error) {
	current, err := execution.ResolveRun(v.runs, namespace, workflowID, runID)
	if err != nil {
		return nil, nil, err
	}
	state, err := v.runs.GetWorkflowExecutionState(current.ID)
	if err != nil {
		return nil, nil, err
	}
	current.Status = state.Status
	return current, state, nil
}

// GetRunWithBlocked 读取 Run 并附加 blocked 信息；Run 与 execution state 用同一个解析出的 runID 读取。
func (v *View) GetRunWithBlocked(namespace, workflowID, runID string) (*RunWithBlocked, error) {
	current, state, err := v.GetRunWithState(namespace, workflowID, runID)
	if err != nil {
		return nil, err
	}
	result := &RunWithBlocked{Run: current, Blocked: state.BlockedInfo()}
	if current.Status == history.StatusFailed {
		// 只读末尾几个事件：WorkflowFailed 是 Run 的结束事件，之后不再追加事件。History 很大的 Run 不整段读出。
		events, err := v.runs.GetEventsRange(current.ID, max(0, state.LastEventID-execution.CloseEventScan), state.LastEventID)
		if err != nil {
			return nil, err
		}
		result.TerminalFailure = terminalFailure(events)
	}
	return result, nil
}

// terminalFailure 取历史里最后一个 WorkflowFailed 的结构化失败载荷；没有 WorkflowFailed 事件时返回 nil。
// 终态失败的权威载荷是事件的 Failure（构造事件时已 Normalize），Error 是它的 message 投影。
func terminalFailure(events []history.Event) *protocol.Failure {
	for i := len(events) - 1; i >= 0; i-- {
		if failed := events[i].WorkflowFailed; failed != nil {
			return failed.Failure
		}
	}
	return nil
}

// GetEvents 逐页读出 Run 的完整历史。
func (v *View) GetEvents(namespace, workflowID, runID string) ([]history.Event, error) {
	current, err := execution.ResolveRun(v.runs, namespace, workflowID, runID)
	if err != nil {
		return nil, err
	}
	var events []history.Event
	request := execution.HistoryPageRequest{RunID: current.ID, PageSize: execution.MaxHistoryPageSize}
	for {
		page, err := v.runs.GetEventPage(request)
		if err != nil {
			return nil, err
		}
		events = append(events, page.Events...)
		if page.NextPageToken == "" {
			return events, nil
		}
		request.PageToken = page.NextPageToken
	}
}

// GetEventPage 读取一页历史；request.RunID 为空时按 workflowID 解析 current Run，寻址规则见 execution.ResolveRun。
func (v *View) GetEventPage(namespace, workflowID string, request execution.HistoryPageRequest) (execution.HistoryPage, error) {
	current, err := execution.ResolveRun(v.runs, namespace, workflowID, request.RunID)
	if err != nil {
		return execution.HistoryPage{}, err
	}
	request.RunID = current.ID
	return v.runs.GetEventPage(request)
}

// GetActivityStates 读取请求寻址的 Run 的全部 Activity 状态，寻址规则见 execution.ResolveRun。
func (v *View) GetActivityStates(namespace, runID string) ([]*execution.ActivityState, error) {
	if _, err := execution.ResolveRun(v.runs, namespace, "", runID); err != nil {
		return nil, err
	}
	return v.runs.ListActivityStates(runID)
}

// GetWorkflowConfig 读取请求寻址的 Run 在 programVersion 这一版 Program 下的配置；programVersion 为空时取 Run 当前
// 采用的版本。寻址规则见 execution.ResolveRun。
func (v *View) GetWorkflowConfig(namespace, runID, programVersion string) (*program.WorkflowConfig, error) {
	run, err := execution.ResolveRun(v.runs, namespace, "", runID)
	if err != nil {
		return nil, err
	}
	if programVersion == "" {
		programVersion = run.WorkflowConfigVersion
	}
	return v.configs.GetWorkflowConfig(definition.WorkflowConfigRef{Namespace: run.Namespace, WorkflowType: run.WorkflowType, Version: programVersion})
}

// ListRuns 按过滤条件列出 Run，过滤与分页规则见 execution.ListRuns。
func (v *View) ListRuns(filter execution.RunListFilter) (execution.RunListResult, error) {
	return execution.ListRuns(v.runs, filter)
}

// GetRunStats 按状态聚合 namespace 内的 Run 计数。
func (v *View) GetRunStats(namespace string) (execution.RunStats, error) {
	return execution.GetRunStats(v.runs, namespace)
}
