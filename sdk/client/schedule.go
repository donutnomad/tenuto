package client

import (
	"context"
	"errors"
	"time"

	"github.com/donutnomad/tenuto/protocol/runtimeapi"
)

// ScheduleOverlapPolicy 决定上一次触发的 Run 仍在运行时，这次触发是否启动新 Run。
type ScheduleOverlapPolicy string

const (
	// ScheduleOverlapPolicySkip 跳过这次触发（缺省）。
	ScheduleOverlapPolicySkip ScheduleOverlapPolicy = "SKIP"
	// ScheduleOverlapPolicyAllowAll 每次触发都启动，与仍在运行的 Run 并行。
	ScheduleOverlapPolicyAllowAll ScheduleOverlapPolicy = "ALLOW_ALL"
)

// ScheduleClient 管理按 cron 表达式周期启动 Workflow 的 Schedule。
type ScheduleClient interface {
	// Create 创建 Schedule 并返回它的句柄；Schedule ID 由 Server 生成。
	Create(ctx context.Context, options ScheduleOptions) (ScheduleHandle, error)
	// GetHandle 返回已有 Schedule 的句柄，不访问 Server。
	GetHandle(ctx context.Context, scheduleID string) ScheduleHandle
	// List 分页列出本 namespace 的 Schedule。
	List(ctx context.Context, options ScheduleListOptions) (*ScheduleListResult, error)
}

// ScheduleOptions 描述新建的 Schedule。
type ScheduleOptions struct {
	// WorkflowType 是每次触发启动的 Workflow Type。
	WorkflowType string
	// Args 至多一个，编码成 JSON 对象作为每次启动的输入。
	Args []any
	// CronExpression 是标准五段 cron 表达式。
	CronExpression string
	// TimeZone 是 cron 的时区（IANA 名字），空值为 UTC。
	TimeZone    string
	Description string
	// OverlapPolicy 为空时取 ScheduleOverlapPolicySkip。
	OverlapPolicy ScheduleOverlapPolicy
	// Paused 让 Schedule 创建后先处于暂停状态。
	Paused bool
	// CatchupWindow 是到期点之后仍会补触发的最长延迟，Server 逐个到期点判定（与 Temporal SchedulePolicies.CatchupWindow 相同）。
	// 0 取 365 天，小于 10 秒时 Server 返回 400。
	CatchupWindow time.Duration
}

// ScheduleUpdateOptions 修改 Schedule；只改写非 nil 的字段。
type ScheduleUpdateOptions struct {
	Description    *string
	CronExpression *string
	TimeZone       *string
	// Args 替换每次启动的输入；指向空切片表示清空输入。
	Args          *[]any
	OverlapPolicy *ScheduleOverlapPolicy
	// CatchupWindow 不小于 10 秒。
	CatchupWindow *time.Duration
}

// ScheduleHandle 操作一个 Schedule。
type ScheduleHandle interface {
	// GetID 返回 Schedule ID。
	GetID() string
	// Describe 读取 Schedule 的配置与触发状态。
	Describe(ctx context.Context) (*ScheduleDescription, error)
	// Update 修改 Schedule。
	Update(ctx context.Context, options ScheduleUpdateOptions) error
	// Pause 暂停触发。
	Pause(ctx context.Context) error
	// Unpause 恢复触发。
	Unpause(ctx context.Context) error
	// Trigger 立即触发一次（遵守 OverlapPolicy），返回启动的 WorkflowRun。
	Trigger(ctx context.Context) (WorkflowRun, error)
	// Delete 删除 Schedule；已经启动的 Run 不受影响。
	Delete(ctx context.Context) error
}

// ScheduleDescription 是 Schedule 的配置与触发状态。
type ScheduleDescription struct {
	ID             string
	WorkflowType   string
	CronExpression string
	TimeZone       string
	Description    string
	OverlapPolicy  ScheduleOverlapPolicy
	Paused         bool
	CatchupWindow  time.Duration
	// Input 是每次启动的输入。
	Input map[string]any
	// LastRunAt、LastRunID 是最近一次触发；NextRunAt 是下一次触发时刻，暂停时为 nil。
	LastRunAt *time.Time
	LastRunID string
	NextRunAt *time.Time
	// RunCount 是累计触发次数。
	RunCount int
	// MissedCatchupWindow 是因超出 CatchupWindow 没有触发的到期点数；BufferDropped 是停机恢复后待补的到期点超过
	// 1000 个时，保留最早的 1000 个之后丢弃的到期点数；StartFailed 是启动失败而丢弃的到期点数（配置错误立即丢弃，
	// 暂时性错误的重试期限取 min(CatchupWindow, 1 小时)，超过 1 小时丢弃，超过更短的 CatchupWindow 计入 MissedCatchupWindow）。
	MissedCatchupWindow int64
	BufferDropped       int64
	StartFailed         int64
	CreatedAt           time.Time
	UpdatedAt           time.Time
}

// ScheduleListOptions 是 List 的过滤与分页条件。
type ScheduleListOptions struct {
	WorkflowType string
	// Paused 非 nil 时只列出暂停（true）或未暂停（false）的 Schedule。
	Paused *bool
	Offset int
	Limit  int
}

// ScheduleListResult 是一页 Schedule；Total 是满足条件的总数。
type ScheduleListResult struct {
	Schedules []ScheduleDescription
	Total     int
}

func (c *workflowClient) ScheduleClient() ScheduleClient { return scheduleClient{client: c} }

type scheduleClient struct {
	client *workflowClient
}

func (s scheduleClient) Create(ctx context.Context, options ScheduleOptions) (ScheduleHandle, error) {
	if options.WorkflowType == "" {
		return nil, errors.New("client: ScheduleOptions.WorkflowType is required")
	}
	input, err := encodeObject("schedule input", options.Args)
	if err != nil {
		return nil, err
	}
	created, err := s.client.workflows().CreateSchedule(ctx, runtimeapi.ScheduleCreateRequest{
		Namespace: s.client.namespace, WorkflowType: options.WorkflowType, Description: options.Description,
		CronExpression: options.CronExpression, Timezone: options.TimeZone, Input: input,
		OverlapPolicy: string(options.OverlapPolicy), Paused: options.Paused, CatchupWindow: options.CatchupWindow,
	})
	if err != nil {
		return nil, mapError(callOther, err)
	}
	return scheduleHandle{client: s.client, id: created.ID}, nil
}

func (s scheduleClient) GetHandle(_ context.Context, scheduleID string) ScheduleHandle {
	return scheduleHandle{client: s.client, id: scheduleID}
}

func (s scheduleClient) List(ctx context.Context, options ScheduleListOptions) (*ScheduleListResult, error) {
	result, err := s.client.workflows().ListSchedules(ctx, runtimeapi.ScheduleListFilter{
		Namespace: s.client.namespace, WorkflowType: options.WorkflowType, Paused: options.Paused,
		Offset: options.Offset, Limit: options.Limit,
	})
	if err != nil {
		return nil, mapError(callOther, err)
	}
	out := &ScheduleListResult{Total: result.Total, Schedules: make([]ScheduleDescription, len(result.Items))}
	for i := range result.Items {
		out.Schedules[i] = scheduleDescription(result.Items[i])
	}
	return out, nil
}

type scheduleHandle struct {
	client *workflowClient
	id     string
}

func (h scheduleHandle) GetID() string { return h.id }

func (h scheduleHandle) request() runtimeapi.WorkflowRuntimeScheduleRequest {
	return runtimeapi.WorkflowRuntimeScheduleRequest{Namespace: h.client.namespace, ScheduleID: h.id}
}

func (h scheduleHandle) Describe(ctx context.Context) (*ScheduleDescription, error) {
	schedule, err := h.client.workflows().GetSchedule(ctx, h.request())
	if err != nil {
		return nil, mapError(callOther, err)
	}
	description := scheduleDescription(schedule)
	return &description, nil
}

func (h scheduleHandle) Update(ctx context.Context, options ScheduleUpdateOptions) error {
	update := runtimeapi.ScheduleUpdate{
		Description: options.Description, CronExpression: options.CronExpression, Timezone: options.TimeZone,
		CatchupWindow: options.CatchupWindow,
	}
	if options.OverlapPolicy != nil {
		policy := string(*options.OverlapPolicy)
		update.OverlapPolicy = &policy
	}
	if options.Args != nil {
		input, err := encodeObject("schedule input", *options.Args)
		if err != nil {
			return err
		}
		if input == nil {
			input = map[string]any{}
		}
		update.Input = &input
	}
	_, err := h.client.workflows().UpdateSchedule(ctx, runtimeapi.WorkflowRuntimeUpdateScheduleRequest{
		Namespace: h.client.namespace, ScheduleID: h.id, Update: update,
	})
	return mapError(callOther, err)
}

func (h scheduleHandle) Pause(ctx context.Context) error {
	return mapError(callOther, h.client.workflows().PauseSchedule(ctx, h.request()))
}

func (h scheduleHandle) Unpause(ctx context.Context) error {
	return mapError(callOther, h.client.workflows().ResumeSchedule(ctx, h.request()))
}

func (h scheduleHandle) Trigger(ctx context.Context) (WorkflowRun, error) {
	runID, err := h.client.workflows().TriggerSchedule(ctx, h.request())
	if err != nil {
		return nil, mapError(callOther, err)
	}
	run, err := h.client.getRun(ctx, "", runID)
	if err != nil {
		return nil, err
	}
	return &workflowRun{client: h.client, workflowID: run.Run.WorkflowID, runID: runID}, nil
}

func (h scheduleHandle) Delete(ctx context.Context) error {
	return mapError(callOther, h.client.workflows().DeleteSchedule(ctx, h.request()))
}

func scheduleDescription(s runtimeapi.Schedule) ScheduleDescription {
	return ScheduleDescription{
		ID: s.ID, WorkflowType: s.WorkflowType, CronExpression: s.CronExpression, TimeZone: s.Timezone,
		Description: s.Description, OverlapPolicy: ScheduleOverlapPolicy(s.OverlapPolicy), Paused: s.Paused,
		CatchupWindow: s.CatchupWindow, Input: s.Input, LastRunAt: s.LastRunAt, LastRunID: s.LastRunID, NextRunAt: s.NextRunAt,
		RunCount: s.RunCount, MissedCatchupWindow: s.MissedCatchupWindow, BufferDropped: s.BufferDropped, StartFailed: s.StartFailed,
		CreatedAt: s.CreatedAt, UpdatedAt: s.UpdatedAt,
	}
}
