package schedule

import (
	"errors"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
)

// CatchupWindow 是到期后仍会补触发的最长延迟；超过它的到期点只推进 NextRunAt。
const CatchupWindow = 1 * time.Hour

// OverlapPolicy 决定上一次触发的 Run 仍在运行时，这次触发是否启动新 Run，语义与 Temporal 的同名策略一致。
type OverlapPolicy string

var OverlapPolicyEnums = struct {
	// AllowAll 每次触发都启动，与仍在运行的 Run 并行。
	AllowAll OverlapPolicy
	// Skip 在上一次触发的 Run 仍在运行时跳过这次触发。
	Skip OverlapPolicy
}{
	AllowAll: "ALLOW_ALL",
	Skip:     "SKIP",
}

// ParseOverlapPolicy 校验 Overlap Policy：空值取 SKIP（Temporal 的缺省），ALLOW_ALL 与 SKIP 原样返回，其他值返回 InvalidRequest。
func ParseOverlapPolicy(value string) (OverlapPolicy, error) {
	switch policy := OverlapPolicy(value); policy {
	case "":
		return OverlapPolicyEnums.Skip, nil
	case OverlapPolicyEnums.AllowAll, OverlapPolicyEnums.Skip:
		return policy, nil
	}
	return "", protocol.InvalidRequestf("invalid overlap_policy %q: want %s or %s", value, OverlapPolicyEnums.AllowAll, OverlapPolicyEnums.Skip)
}

// Schedule
// @Setter
type Schedule struct {
	ID             string         `json:"id"`
	Namespace      string         `json:"namespace"`
	WorkflowType   string         `json:"workflow_type"`
	Description    string         `json:"description,omitempty"`
	CronExpression string         `json:"cron_expression"`
	Timezone       string         `json:"timezone,omitempty"`
	Input          map[string]any `json:"input,omitempty"`
	Paused         bool           `json:"paused"`
	OverlapPolicy  string         `json:"overlap_policy,omitempty"`
	LastRunAt      *time.Time     `json:"last_run_at,omitempty"`
	NextRunAt      *time.Time     `json:"next_run_at,omitempty"`
	LastRunID      string         `json:"last_run_id,omitempty"`
	RunCount       int            `json:"run_count"`
	CreatedAt      time.Time      `json:"created_at"`
	UpdatedAt      time.Time      `json:"updated_at"`
	patch          SchedulePatch
}

type NewParams struct {
	Namespace      string
	WorkflowType   string
	Description    string
	CronExpression string
	Timezone       string
	Input          map[string]any
	OverlapPolicy  string
	Paused         bool
}

// New 校验参数并创建 Schedule；未暂停时按 cron 计算首个 NextRunAt。
func New(id string, params NewParams, now time.Time) (*Schedule, error) {
	if err := errors.Join(
		protocol.CheckLength("namespace", params.Namespace, protocol.MaxNamespaceLength),
		protocol.CheckLength("workflow_type", params.WorkflowType, protocol.MaxWorkflowTypeLength),
		checkScheduleText(params.Description, params.CronExpression, params.Timezone),
	); err != nil {
		return nil, err
	}
	if params.WorkflowType == "" {
		return nil, protocol.InvalidRequestf("workflow_type is required")
	}
	if params.CronExpression == "" {
		return nil, protocol.InvalidRequestf("cron_expression is required")
	}
	var loc *time.Location
	if params.Timezone != "" {
		var err error
		loc, err = time.LoadLocation(params.Timezone)
		if err != nil {
			return nil, protocol.InvalidRequestf("invalid timezone %q: %w", params.Timezone, err)
		}
	}
	if err := ValidateCronExpression(params.CronExpression); err != nil {
		return nil, protocol.InvalidRequestf("invalid cron_expression: %w", err)
	}
	policy, err := ParseOverlapPolicy(params.OverlapPolicy)
	if err != nil {
		return nil, err
	}
	schedule := &Schedule{
		ID: id, Namespace: params.Namespace, WorkflowType: params.WorkflowType,
		Description: params.Description, CronExpression: params.CronExpression,
		Timezone: params.Timezone, Input: params.Input, Paused: params.Paused,
		OverlapPolicy: string(policy), CreatedAt: now, UpdatedAt: now,
	}
	if !schedule.Paused {
		schedule.NextRunAt = CronNextRunAt(schedule.CronExpression, now, loc)
	}
	return schedule, nil
}

// RunWorkflowID 返回 at 这次触发启动的 Run 的 Workflow ID：schedule-<Schedule ID>-<at 的 UTC RFC 3339 秒级时间>。
// 每次触发各有自己的 Workflow ID，与 Temporal 一致：ALLOW_ALL 下并行的 Run 互不接替；同一个到期点重试启动得到同一个
// Workflow ID，它的 Run 已在运行时不会再启动第二个。at 是 cron 到期点，手动触发取触发时刻。
func (s *Schedule) RunWorkflowID(at time.Time) string {
	return "schedule-" + s.ID + "-" + at.UTC().Truncate(time.Second).Format(time.RFC3339)
}

// RunSearchAttributes 返回 at 这次触发启动的 Run 的搜索属性：Schedule ID 与名义触发时间（UTC RFC 3339 秒级），
// 键与 Temporal 相同。at 的取值与 RunWorkflowID 相同。
func (s *Schedule) RunSearchAttributes(at time.Time) map[string]string {
	return map[string]string{
		history.ScheduledByIDSearchAttribute:      s.ID,
		history.ScheduledStartTimeSearchAttribute: at.UTC().Truncate(time.Second).Format(time.RFC3339),
	}
}

// Changes 是一次更新要改的字段；nil 字段保持原值。
type Changes struct {
	Description    *string
	CronExpression *string
	Timezone       *string
	Input          *map[string]any
	OverlapPolicy  *string
}

// Update 应用字段变更；cron 或时区变化且未暂停时重算 NextRunAt。
func (s *Schedule) Update(changes Changes, now time.Time) error {
	if err := checkScheduleText(deref(changes.Description), deref(changes.CronExpression), deref(changes.Timezone)); err != nil {
		return err
	}
	var loc *time.Location
	cronChanged := false
	if changes.Description != nil {
		s.setDescription(*changes.Description)
	}
	if changes.CronExpression != nil {
		if err := ValidateCronExpression(*changes.CronExpression); err != nil {
			return protocol.InvalidRequestf("invalid cron_expression: %w", err)
		}
		s.setCronExpression(*changes.CronExpression)
		cronChanged = true
	}
	if changes.Timezone != nil {
		if *changes.Timezone != "" {
			var err error
			loc, err = time.LoadLocation(*changes.Timezone)
			if err != nil {
				return protocol.InvalidRequestf("invalid timezone %q: %w", *changes.Timezone, err)
			}
		}
		s.setTimezone(*changes.Timezone)
		cronChanged = true
	}
	if changes.Input != nil {
		s.setInput(*changes.Input)
	}
	if changes.OverlapPolicy != nil {
		policy, err := ParseOverlapPolicy(*changes.OverlapPolicy)
		if err != nil {
			return err
		}
		s.setOverlapPolicy(string(policy))
	}
	if cronChanged && !s.Paused {
		if loc == nil {
			loc = s.location()
		}
		s.setNextRunAt(CronNextRunAt(s.CronExpression, now, loc))
	}
	s.setUpdatedAt(now)
	return nil
}

// Pause 暂停调度并清空 NextRunAt。
func (s *Schedule) Pause(now time.Time) {
	s.setPaused(true)
	s.setNextRunAt(nil)
	s.setUpdatedAt(now)
}

// Resume 恢复调度并从 now 起重算 NextRunAt。
func (s *Schedule) Resume(now time.Time) {
	s.setPaused(false)
	s.setNextRunAt(CronNextRunAt(s.CronExpression, now, s.location()))
	s.setUpdatedAt(now)
}

// AdvanceNextRunAt 以 now 为基准把 NextRunAt 推进到下一个 cron 时刻，返回推进前的值供 CAS 比较。
// 暂停中的 Schedule 保持 NextRunAt 不变。
func (s *Schedule) AdvanceNextRunAt(now time.Time) (previous *time.Time) {
	previous = s.NextRunAt
	if !s.Paused {
		s.setNextRunAt(CronNextRunAt(s.CronExpression, now, s.location()))
	}
	s.setUpdatedAt(now)
	return previous
}

// RestoreNextRunAt 把 NextRunAt 改回推进前的值 previous，返回被替换掉的值供 CAS 比较。
// 启动 Run 失败时使用：这次到期点留给下一轮扫描重新触发。
func (s *Schedule) RestoreNextRunAt(previous *time.Time, now time.Time) (advanced *time.Time) {
	advanced = s.NextRunAt
	s.setNextRunAt(previous)
	s.setUpdatedAt(now)
	return advanced
}

// MissedCatchupWindow 报告到期点是否已超出补触发窗口。
func (s *Schedule) MissedCatchupWindow(now time.Time) bool {
	return s.NextRunAt != nil && now.Sub(*s.NextRunAt) > CatchupWindow
}

// ChecksPreviousRun 报告触发前是否要读取上一次触发的 Run 的状态：SKIP 策略且触发过。
func (s *Schedule) ChecksPreviousRun() bool {
	return OverlapPolicy(s.OverlapPolicy) != OverlapPolicyEnums.AllowAll && s.LastRunID != ""
}

// AllowsStart 按 Overlap Policy 判定这次触发能否启动 Run。previous 是上一次触发的 Run（LastRunID）所在 Workflow ID 的
// current Run 的状态：上一次的 Run 经 continue-as-new、重试或 reset 接续时是接续的那一代。previousErr 是读取它的错误。
//   - ALLOW_ALL 总是启动；
//   - SKIP 在那一代仍在运行时跳过；上一次的 Run 已不存在（execution.ErrRunNotFound）视为已结束；其他读取错误无法判定，跳过。
func (s *Schedule) AllowsStart(previous history.Status, previousErr error) bool {
	if !s.ChecksPreviousRun() {
		return true
	}
	if previousErr != nil {
		return errors.Is(previousErr, execution.ErrRunNotFound)
	}
	return history.IsTerminalStatus(previous)
}

// RecordRun 记录一次成功触发。
func (s *Schedule) RecordRun(runID string, at time.Time) {
	s.setLastRunAt(&at)
	s.setLastRunID(runID)
	s.setRunCount(s.RunCount + 1)
}

// Touch 刷新 UpdatedAt。
func (s *Schedule) Touch(at time.Time) {
	s.setUpdatedAt(at)
}

// location 返回 Timezone 对应的时区；空或无法解析时为 nil（按 UTC 计算）。
func (s *Schedule) location() *time.Location {
	if s.Timezone == "" {
		return nil
	}
	loc, _ := time.LoadLocation(s.Timezone)
	return loc
}

// checkScheduleText 检查 Schedule 文本字段的长度（tenuto_schedules 的列宽）。
func checkScheduleText(description, cronExpression, timezone string) error {
	return errors.Join(
		protocol.CheckLength("description", description, protocol.MaxScheduleDescriptionLength),
		protocol.CheckLength("cron_expression", cronExpression, protocol.MaxCronExpressionLength),
		protocol.CheckLength("timezone", timezone, protocol.MaxTimezoneLength),
	)
}

func deref(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}
