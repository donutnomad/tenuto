package client

import (
	"context"
	"errors"
	"time"

	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/protocol/runtimeapi"
	"github.com/donutnomad/tenuto/sdk/workflow"
)

// WorkflowExecutionDescription 是 DescribeWorkflowExecution 读到的 Run 状态。
type WorkflowExecutionDescription struct {
	WorkflowID   string
	RunID        string
	WorkflowType string
	Status       WorkflowExecutionStatus
	// FirstRunID 是执行链的第一个 Run；PreviousRunID 是 continue-as-new、重试或 Reset 之前的 Run，首个 Run 为空。
	FirstRunID    string
	PreviousRunID string
	// ParentRunID 非空表示这是子 Workflow 的 Run。
	ParentRunID string
	// Attempt 是 Workflow 重试的尝试序号，从 1 起。
	Attempt   int
	StartTime time.Time
	// ExecutionDeadline 与 RunDeadline 是执行超时与 Run 超时的到期时刻；没有配置时为 nil。
	ExecutionDeadline *time.Time
	RunDeadline       *time.Time
	SearchAttributes  map[string]string
	// Policy 是 Run 的有效版本策略，ProgramVersion 是它当前采用的 Program 内容版本。
	Policy         workflow.Policy
	ProgramVersion string
	// ConfigError 非空表示确定性的配置冲突，Run 暂停推进，等修复后用
	// UpdateWorkflowExecutionOptions 恢复。
	ConfigError string
	// UpgradeError 是最近一次自动升级失败的原因；升级成功后清空。
	UpgradeError string
	// IsCurrent 报告这是不是 Workflow ID 的当前 Run。
	IsCurrent bool
	// Blocked 非 nil 表示 Workflow Task 连续失败，Run 停滞等待修复。
	Blocked *WorkflowTaskBlocked
}

// WorkflowTaskBlocked 描述 Workflow Task 连续失败而停滞的 Run。
type WorkflowTaskBlocked struct {
	ConsecutiveFailures int
	Cause               string
	Reason              string
	Since               *time.Time
}

func (c *workflowClient) DescribeWorkflowExecution(ctx context.Context, workflowID, runID string) (*WorkflowExecutionDescription, error) {
	resp, err := c.getRun(ctx, workflowID, runID)
	if err != nil {
		return nil, err
	}
	run := resp.Run
	description := &WorkflowExecutionDescription{
		WorkflowID: run.WorkflowID, RunID: run.ID, WorkflowType: run.WorkflowType,
		Status: WorkflowExecutionStatus(run.Status), FirstRunID: run.FirstRunID, PreviousRunID: run.ContinuedRunID,
		ParentRunID: run.ParentRunID, Attempt: run.Attempt, StartTime: run.CreatedAt,
		ExecutionDeadline: run.DeadlineAt, RunDeadline: run.RunDeadlineAt, SearchAttributes: run.SearchAttributes,
		Policy: workflow.Policy(run.Policy), ProgramVersion: run.WorkflowConfigVersion,
		ConfigError: run.ConfigError, UpgradeError: run.UpgradeError, IsCurrent: run.IsCurrent,
	}
	if resp.Blocked.Blocked {
		description.Blocked = &WorkflowTaskBlocked{
			ConsecutiveFailures: resp.Blocked.ConsecutiveFailures, Cause: resp.Blocked.Cause,
			Reason: resp.Blocked.Reason, Since: resp.Blocked.Since,
		}
	}
	return description, nil
}

// HistoryEventIterator 逐个返回 Run 的历史事件，需要时读取下一页。
type HistoryEventIterator interface {
	// HasNext 报告是否还有事件；读取下一页失败时也返回 true，错误由 Next 返回。
	HasNext() bool
	// Next 返回下一个事件。
	Next() (*history.Event, error)
}

// historyPageSize 是 GetWorkflowHistory 每页读取的事件数，等于 Server 的缺省页大小。
const historyPageSize = 200

func (c *workflowClient) GetWorkflowHistory(ctx context.Context, workflowID, runID string) HistoryEventIterator {
	return &historyIterator{ctx: ctx, client: c, workflowID: workflowID, runID: runID, pageSize: historyPageSize}
}

type historyIterator struct {
	ctx        context.Context
	client     *workflowClient
	workflowID string
	runID      string
	pageSize   int

	events    []history.Event
	nextToken string
	started   bool
	err       error
}

func (it *historyIterator) HasNext() bool {
	if len(it.events) > 0 || it.err != nil {
		return true
	}
	if it.started && it.nextToken == "" {
		return false
	}
	it.fetch()
	return len(it.events) > 0 || it.err != nil
}

func (it *historyIterator) Next() (*history.Event, error) {
	if !it.HasNext() {
		return nil, errors.New("client: history iterator has no more events")
	}
	if it.err != nil {
		err := it.err
		it.err = nil
		return nil, err
	}
	event := it.events[0]
	it.events = it.events[1:]
	return &event, nil
}

// fetch 读取下一页。第一页之前先把空 runID 解析成当前 Run，后续页与它保持一致。
func (it *historyIterator) fetch() {
	if !it.started {
		runID, err := it.client.resolveRunID(it.ctx, it.workflowID, it.runID)
		if err != nil {
			it.err = err
			return
		}
		it.runID = runID
	}
	page, err := it.client.workflows().GetEventPage(it.ctx, runtimeapi.WorkflowRuntimeHistoryPageRequest{
		Namespace: it.client.namespace, WorkflowID: it.workflowID,
		HistoryPageRequest: runtimeapi.HistoryPageRequest{RunID: it.runID, PageSize: it.pageSize, PageToken: it.nextToken},
	})
	if err != nil {
		it.err = mapError(callOther, err)
		return
	}
	it.started = true
	it.events = page.Events
	it.nextToken = page.NextPageToken
}

// ListWorkflowOptions 是 ListWorkflow 的过滤与分页条件，条件之间是「与」。
type ListWorkflowOptions struct {
	// WorkflowID 只列出这个 Workflow ID 的 Run（含 continue-as-new、重试产生的 Run）。
	WorkflowID string
	// Statuses 只列出这些状态的 Run；为空时不按状态过滤。
	Statuses []WorkflowExecutionStatus
	// SearchAttributes 要求每个键的值完全相等。
	SearchAttributes map[string]string
	// Limit 是页大小；为 0 时用 Server 的缺省页大小 50，最大 1000，超出时 Server 返回 400。
	Limit int
	// NextPageToken 是上一页 ListWorkflowResult.NextPageToken，为空时从第一页开始；它绑定签发时的过滤条件（WorkflowID、
	// Statuses、SearchAttributes、NewestFirst），条件变了再用返回 ErrInvalidArgument。
	NextPageToken string
	// NewestFirst 按启动时间倒序；缺省升序。
	NewestFirst bool
	// SkipTotal 为 true 时 Server 不计算满足条件的 Run 总数（Total 为 0），翻页看 NextPageToken；Run 很多时这一页更快。
	SkipTotal bool
}

// WorkflowExecutionInfo 是 ListWorkflow 返回的一行。
type WorkflowExecutionInfo struct {
	WorkflowID       string
	RunID            string
	WorkflowType     string
	Status           WorkflowExecutionStatus
	StartTime        time.Time
	CloseTime        *time.Time
	ParentRunID      string
	SearchAttributes map[string]string
	HistoryLength    int
	// FailureReason 是失败 Run 的失败文案。
	FailureReason string
}

// ListWorkflowResult 是一页结果；Total 是满足条件的 Run 总数（SkipTotal 时为 0）。还有下一页时 NextPageToken 非空，
// 放进 ListWorkflowOptions.NextPageToken 取下一页（同 Temporal ListWorkflow 的 next page token）。
type ListWorkflowResult struct {
	Executions    []WorkflowExecutionInfo
	Total         int
	NextPageToken string
}

func (c *workflowClient) ListWorkflow(ctx context.Context, options ListWorkflowOptions) (*ListWorkflowResult, error) {
	statuses := make([]history.Status, len(options.Statuses))
	for i, status := range options.Statuses {
		statuses[i] = history.Status(status)
	}
	result, err := c.workflows().ListRuns(ctx, runtimeapi.RunListFilter{
		Namespace: c.namespace, WorkflowID: options.WorkflowID, Statuses: statuses,
		SearchAttributes: options.SearchAttributes, IncludeTerminal: true,
		PageToken: options.NextPageToken, Limit: options.Limit, OrderDesc: options.NewestFirst, SkipTotal: options.SkipTotal,
	})
	if err != nil {
		return nil, mapError(callOther, err)
	}
	out := &ListWorkflowResult{Total: result.Total, NextPageToken: result.NextPageToken, Executions: make([]WorkflowExecutionInfo, len(result.Items))}
	for i, item := range result.Items {
		out.Executions[i] = WorkflowExecutionInfo{
			WorkflowID: item.WorkflowID, RunID: item.RunID, WorkflowType: item.WorkflowType,
			Status: WorkflowExecutionStatus(item.Status), StartTime: item.CreatedAt, CloseTime: item.ClosedAt,
			ParentRunID: item.ParentRunID, SearchAttributes: item.SearchAttributes,
			HistoryLength: item.HistoryLength, FailureReason: item.FailureReason,
		}
	}
	return out, nil
}

// ResetType 选择 Reset 的分叉点。
type ResetType string

const (
	// ResetTypeFirstWorkflowTask 从第一个 Workflow Task 处分叉。
	ResetTypeFirstWorkflowTask ResetType = "first_workflow_task"
	// ResetTypeLastWorkflowTask 从最后一个已完成的 Workflow Task 处分叉。
	ResetTypeLastWorkflowTask ResetType = "last_workflow_task"
)

// ResetWorkflowExecutionOptions 描述一次 Reset。ResetType 与 EventID 二选一；分叉点之前调度的 Activity 与
// timer 必须都已结束。
type ResetWorkflowExecutionOptions struct {
	// WorkflowID 与 RunID 寻址被 Reset 的 Run；RunID 为空时取 Workflow ID 的当前 Run。
	WorkflowID string
	RunID      string
	Reason     string
	ResetType  ResetType
	// EventID 是分叉点：一个 Workflow Task 的 Started、Completed、Failed 或 TimedOut 事件的 ID。
	EventID int64
	// ReapplyExcludeTypes 是不重新应用到新 Run 的外部输入；缺省把分叉点之后的 Signal 与 Update 重新应用到新 Run。
	ReapplyExcludeTypes []ResetReapplyExcludeType
}

// ResetReapplyExcludeType 是 Reset 时可以不重新应用的外部输入类型。
type ResetReapplyExcludeType string

const (
	// ResetReapplyExcludeTypeSignal 不重新应用分叉点之后的 Signal。
	ResetReapplyExcludeTypeSignal ResetReapplyExcludeType = "signal"
	// ResetReapplyExcludeTypeUpdate 不重新应用分叉点之后已接受的 Update。
	ResetReapplyExcludeTypeUpdate ResetReapplyExcludeType = "update"
)

func (c *workflowClient) ResetWorkflowExecution(ctx context.Context, options ResetWorkflowExecutionOptions) (string, error) {
	runID, err := c.resolveRunID(ctx, options.WorkflowID, options.RunID)
	if err != nil {
		return "", err
	}
	exclude := make([]string, len(options.ReapplyExcludeTypes))
	for i, typ := range options.ReapplyExcludeTypes {
		exclude[i] = string(typ)
	}
	newRunID, err := c.workflows().ResetWorkflow(ctx, runtimeapi.WorkflowRuntimeResetRequest{
		Namespace: c.namespace, RunID: runID, Reason: options.Reason, ResetType: string(options.ResetType),
		ResetEventID: options.EventID, ReapplyExcludeTypes: exclude,
	})
	return newRunID, mapError(callOther, err)
}

func (c *workflowClient) UpsertSearchAttributes(ctx context.Context, workflowID, runID string, attributes map[string]string) error {
	return mapError(callOther, c.workflows().UpsertSearchAttributes(ctx, runtimeapi.WorkflowRuntimeSearchAttributesRequest{
		Namespace: c.namespace, WorkflowID: workflowID, RunID: runID, SearchAttributes: attributes,
	}))
}

// UpdateWorkflowExecutionOptions 修改一个运行中 Run 的执行选项。
type UpdateWorkflowExecutionOptions struct {
	// WorkflowID 与 RunID 寻址 Run；RunID 为空时取 Workflow ID 的当前 Run。
	WorkflowID string
	RunID      string
	// Policy 覆盖 Run 的版本策略：workflow.PolicyPinned 固定在当前 Program 内容，workflow.PolicyAutoUpgrade
	// 在之后的 Workflow Task 换到 latest。只作用于运行中的 Run；对因 ConfigError 暂停的 Run 调用即恢复它。
	Policy workflow.Policy
}

func (c *workflowClient) UpdateWorkflowExecutionOptions(ctx context.Context, options UpdateWorkflowExecutionOptions) error {
	if options.Policy == "" {
		return errors.New("client: UpdateWorkflowExecutionOptions.Policy is required")
	}
	runID, err := c.resolveRunID(ctx, options.WorkflowID, options.RunID)
	if err != nil {
		return err
	}
	_, err = c.workflows().SetRunPolicy(ctx, runtimeapi.WorkflowRuntimeSetRunPolicyRequest{
		Namespace: c.namespace, RunID: runID, Policy: string(options.Policy),
	})
	return mapError(callOther, err)
}
