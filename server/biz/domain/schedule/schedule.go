package schedule

import (
	"errors"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
)

// 补触发策略，与 Temporal scheduler 的同名默认值一致（service/worker/scheduler 的 DefaultCatchupWindow、
// MinCatchupWindow、MaxBufferSize）。
const (
	// DefaultCatchupWindow 是创建时没有指定 catchup window 的 Schedule 使用的窗口。
	DefaultCatchupWindow = 365 * 24 * time.Hour
	// MinCatchupWindow 是 catchup window 的下限，更小的值返回 InvalidRequest。
	MinCatchupWindow = 10 * time.Second
	// MaxBufferSize 是待补触发的到期点上限：窗口内已到期、还没处理的到期点超过它时，保留最早的 MaxBufferSize 个，
	// 更晚的计入 BufferDropped（同 Temporal 的 buffer 满时丢弃新到的触发）。
	MaxBufferSize = 1000
)

// ParseCatchupWindow 校验 catchup window：0 取 DefaultCatchupWindow，小于 MinCatchupWindow 返回 InvalidRequest。
func ParseCatchupWindow(window time.Duration) (time.Duration, error) {
	if window == 0 {
		return DefaultCatchupWindow, nil
	}
	if window < MinCatchupWindow {
		return 0, protocol.InvalidRequestf("catchup_window must be at least %s, got %s", MinCatchupWindow, window)
	}
	return window, nil
}

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
	// CatchupWindow 是到期点之后仍会补触发的最长延迟，逐个到期点判定。
	CatchupWindow time.Duration `json:"catchup_window"`
	LastRunAt     *time.Time    `json:"last_run_at,omitempty"`
	NextRunAt     *time.Time    `json:"next_run_at,omitempty"`
	LastRunID     string        `json:"last_run_id,omitempty"`
	RunCount      int           `json:"run_count"`
	// MissedCatchupWindow 是因超出 catchup window 而没有触发的到期点数（Temporal ScheduleInfo.MissedCatchupWindow）。
	MissedCatchupWindow int64 `json:"missed_catchup_window"`
	// BufferDropped 是补触发时因超出 MaxBufferSize 而丢弃的到期点数（Temporal ScheduleInfo.BufferDropped）。
	BufferDropped int64 `json:"buffer_dropped"`
	// BufferDropFrom 与 BufferDropUntil 记录 buffer 满时丢弃的到期点区间 [BufferDropFrom, BufferDropUntil]：
	// 保留的 MaxBufferSize 个到期点处理完、NextRunAt 走到 BufferDropFrom 时跳到 BufferDropUntil 之后。没有丢弃时为 nil。
	BufferDropFrom  *time.Time `json:"-"`
	BufferDropUntil *time.Time `json:"-"`
	CreatedAt       time.Time  `json:"created_at"`
	UpdatedAt       time.Time  `json:"updated_at"`
	patch           SchedulePatch
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
	CatchupWindow  time.Duration
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
	catchupWindow, err := ParseCatchupWindow(params.CatchupWindow)
	if err != nil {
		return nil, err
	}
	schedule := &Schedule{
		ID: id, Namespace: params.Namespace, WorkflowType: params.WorkflowType,
		Description: params.Description, CronExpression: params.CronExpression,
		Timezone: params.Timezone, Input: params.Input, Paused: params.Paused,
		OverlapPolicy: string(policy), CatchupWindow: catchupWindow, CreatedAt: now, UpdatedAt: now,
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
	CatchupWindow  *time.Duration
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
	if changes.CatchupWindow != nil {
		window, err := ParseCatchupWindow(*changes.CatchupWindow)
		if err != nil {
			return err
		}
		s.setCatchupWindow(window)
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

// AdvanceNextRunAt 把 NextRunAt 推进到 after 之后的下一个 cron 时刻，返回推进前的值供 CAS 比较。
// 定时触发传这次处理的到期点，错过的后续到期点留给下一次处理；手动触发传 now。暂停中的 Schedule 保持 NextRunAt 不变。
func (s *Schedule) AdvanceNextRunAt(after, now time.Time) (previous *time.Time) {
	previous = s.NextRunAt
	if !s.Paused {
		s.setNextRunAt(CronNextRunAt(s.CronExpression, after, s.location()))
	}
	s.setUpdatedAt(now)
	return previous
}

// SkipUnstartable 按 Temporal scheduler 的补触发规则跳过此刻不会触发的到期点，把 NextRunAt 推进到下一个要触发的到期点：
//   - 落后 now 超过 CatchupWindow 的到期点计入 MissedCatchupWindow：直接推进到窗口内的第一个到期点，被跳过的只计数；
//   - NextRunAt 走到之前 buffer 满时记下的丢弃区间时，跳到区间之后；
//   - 窗口内已到期的到期点超过 MaxBufferSize 个时，保留最早的 MaxBufferSize 个，更晚的（到 now 为止）计入 BufferDropped，
//     并记下丢弃区间。
//
// 返回推进前的 NextRunAt 供 CAS 比较；skipped 为 false 表示 Schedule 不变。cron 只解析一次。
func (s *Schedule) SkipUnstartable(now time.Time) (previous *time.Time, skipped bool) {
	previous = s.NextRunAt
	if s.NextRunAt == nil || s.Paused {
		return previous, false
	}
	cron, err := ParseCron(s.CronExpression, s.location())
	if err != nil {
		return previous, false
	}
	next := s.NextRunAt
	if windowStart := now.Add(-s.CatchupWindow); next.Before(windowStart) {
		missed := 1 + cron.CountBetween(*next, windowStart)
		next = cron.Next(windowStart.Add(-time.Nanosecond))
		s.setMissedCatchupWindow(s.MissedCatchupWindow + missed)
		skipped = true
	}
	if s.BufferDropFrom != nil && (next == nil || !next.Before(*s.BufferDropFrom)) {
		if next != nil && !next.After(*s.BufferDropUntil) {
			next = cron.Next(*s.BufferDropUntil)
		}
		s.setBufferDropFrom(nil)
		s.setBufferDropUntil(nil)
		skipped = true
	}
	if s.BufferDropFrom == nil && next != nil && !next.After(now) {
		if pending := 1 + cron.CountBetween(*next, now.Add(time.Nanosecond)); pending > MaxBufferSize {
			dropFrom := next
			for range MaxBufferSize {
				dropFrom = cron.Next(*dropFrom)
			}
			until := now
			s.setBufferDropped(s.BufferDropped + pending - MaxBufferSize)
			s.setBufferDropFrom(dropFrom)
			s.setBufferDropUntil(&until)
			skipped = true
		}
	}
	if !skipped {
		return previous, false
	}
	s.setNextRunAt(next)
	s.setUpdatedAt(now)
	return previous, true
}

// DueForFire 报告 NextRunAt 是此刻要处理的到期点：不晚于 now，且不在 buffer 满时丢弃的区间里。
func (s *Schedule) DueForFire(now time.Time) bool {
	if s.NextRunAt == nil || s.NextRunAt.After(now) {
		return false
	}
	return s.BufferDropFrom == nil || s.NextRunAt.Before(*s.BufferDropFrom)
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
