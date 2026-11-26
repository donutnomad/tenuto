// Package batch 定义批量操作：对一组 Run 逐个执行取消、终止或 Reset 的持久化任务。
package batch

import (
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
)

// Type 是批量操作对每个 Run 执行的动作。
type Type string

var TypeEnums = struct {
	// Cancel 对每个 Run 发出取消请求；Run 是否以及何时结束取决于 Workflow 对取消的处理。
	Cancel Type
	// Terminate 强制终止每个 Run。
	Terminate Type
	// Reset 把每个 Run 重置到 ResetType 指定的位置，产生新的 Run。
	Reset Type
}{Cancel: "cancel", Terminate: "terminate", Reset: "reset"}

// Status 是批量操作本身的状态，与它处理的 Run 的状态无关。
type Status string

var StatusEnums = struct {
	// Pending 已受理，后台驱动尚未开始处理。
	Pending Status
	// Running 后台驱动正在分页处理。
	Running Status
	// Completed 全部选中的 Run 都已处理：每个 Run 计入 succeeded、failed 或 skipped 之一，至少一个成功或没有失败。
	Completed Status
	// Failed 全部选中的 Run 都已处理，有失败且没有一个成功。
	Failed Status
	// Cancelled 用户在处理完之前取消了批量操作；已处理的计数保留，其余 Run 不再处理。
	Cancelled Status
}{Pending: "pending", Running: "running", Completed: "completed", Failed: "failed", Cancelled: "cancelled"}

// transitions 是状态机允许的迁移。
var transitions = map[Status][]Status{
	StatusEnums.Pending: {StatusEnums.Running, StatusEnums.Cancelled},
	StatusEnums.Running: {StatusEnums.Completed, StatusEnums.Failed, StatusEnums.Cancelled},
}

// CanTransition 报告状态 from 能否迁移到 to。
func CanTransition(from, to Status) bool {
	return slices.Contains(transitions[from], to)
}

// IsTerminal 报告 status 是否是终态。
func IsTerminal(status Status) bool {
	return len(transitions[status]) == 0
}

// MaxErrorSamples 是一个批量操作最多保存的逐 Run 错误条数；更多的失败只计数。
const MaxErrorSamples = 20

// RunError 是一个 Run 处理失败的原因。
type RunError struct {
	RunID   string `json:"run_id"`
	Message string `json:"message"`
}

// Operation 是一个批量操作。选中的 Run 由 Filter 决定，按 (created_at, id) 升序分页处理，只包含 SelectedBefore 之前创建的 Run；
// 每页处理完后计数与 Cursor 一起保存，驱动重启后从 Cursor 继续。
// @Setter
type Operation struct {
	ID        string                  `json:"id"`
	Namespace string                  `json:"namespace"`
	Type      Type                    `json:"type"`
	Filter    execution.RunListFilter `json:"filter"`
	ResetType execution.ResetType     `json:"reset_type,omitempty"`
	Reason    string                  `json:"reason,omitempty"`
	Status    Status                  `json:"status"`
	// TotalCount 在处理第一页时取当时匹配的 Run 数；结束时改为实际处理的 Run 数（翻页期间离开过滤条件的 Run 不处理）。
	TotalCount     int        `json:"total_count"`
	SucceededCount int        `json:"succeeded_count"`
	FailedCount    int        `json:"failed_count"`
	SkippedCount   int        `json:"skipped_count"`
	Errors         []RunError `json:"errors,omitempty"`
	// Cursor 是最后一个已保存结果的 Run 的位置；nil 表示还没有处理任何一页。
	Cursor *execution.RunCursor `json:"-"`
	// Version 每次保存加一，仓储据此拒绝基于过期读取的写入。
	Version    int64      `json:"-"`
	CreatedAt  time.Time  `json:"created_at"`
	StartedAt  *time.Time `json:"started_at,omitempty"`
	FinishedAt *time.Time `json:"finished_at,omitempty"`
	UpdatedAt  time.Time  `json:"updated_at"`
	patch      OperationPatch
}

type NewParams struct {
	Namespace string
	Type      Type
	Filter    execution.RunListFilter
	ResetType execution.ResetType
	Reason    string
}

// New 校验参数并创建 pending 的批量操作。Filter 的 Offset 与 Limit 不参与：批量操作覆盖全部匹配的 Run；
// 显式给出 RunIDs 时已结束的 Run 也在选择之内。
func New(id string, params NewParams, now time.Time) (*Operation, error) {
	switch params.Type {
	case TypeEnums.Cancel, TypeEnums.Terminate:
	case TypeEnums.Reset:
		if err := (execution.ResetWorkflowRequest{ResetType: params.ResetType}).Validate(); err != nil {
			return nil, err
		}
	default:
		return nil, protocol.InvalidRequestf("unknown batch operation type: %q", params.Type)
	}
	if err := errors.Join(
		protocol.CheckLength("namespace", params.Namespace, protocol.MaxNamespaceLength),
		protocol.CheckLength("reason", params.Reason, protocol.MaxBatchReasonLength),
	); err != nil {
		return nil, err
	}
	namespace := protocol.NormalizeNamespace(params.Namespace)
	filter := params.Filter
	filter.Namespace = namespace
	filter.Offset, filter.Limit, filter.OrderDesc = 0, 0, false
	if len(filter.RunIDs) > 0 {
		filter.IncludeTerminal = true
	}
	return &Operation{
		ID: id, Namespace: namespace, Type: params.Type, Filter: filter, ResetType: params.ResetType,
		Reason: params.Reason, Status: StatusEnums.Pending, Version: 1, CreatedAt: now, UpdatedAt: now,
	}, nil
}

// SelectedBefore 是选择的上界：只处理 created_at 早于它的 Run，即批量操作创建时那一毫秒及之前创建的 Run。
// 按毫秒取整与存储的时间精度一致；批量操作自己的动作（Reset）此后产生的 Run 都不在选择之内。
func (o *Operation) SelectedBefore() time.Time {
	return o.CreatedAt.Truncate(time.Millisecond).Add(time.Millisecond)
}

// IsActive 报告批量操作是否还要由驱动处理。
func (o *Operation) IsActive() bool {
	return o.Status == StatusEnums.Pending || o.Status == StatusEnums.Running
}

// Begin 由驱动认领：pending → running。
func (o *Operation) Begin(now time.Time) error {
	if err := o.transition(StatusEnums.Running, now); err != nil {
		return err
	}
	o.setStartedAt(&now)
	return nil
}

// Page 是驱动处理完一页 Run 的结果。
type Page struct {
	// Total 是读取这一页时匹配的 Run 总数，只在第一页（Cursor 为 nil）时采用。
	Total     int
	Succeeded int
	Failed    int
	Skipped   int
	Errors    []RunError
	// Cursor 是这一页最后一个 Run 的位置。
	Cursor execution.RunCursor
}

// RecordPage 累加一页的结果并推进 Cursor。running 之外只接受 cancelled：取消可能在这一页处理期间落库，
// 已经执行的动作仍要计入。
func (o *Operation) RecordPage(page Page, now time.Time) error {
	if o.Status != StatusEnums.Running && o.Status != StatusEnums.Cancelled {
		return fmt.Errorf("%w: batch operation %s is %s, cannot record progress", ErrInvalidTransition, o.ID, o.Status)
	}
	if o.Cursor == nil {
		o.setTotalCount(page.Total)
	}
	o.setSucceededCount(o.SucceededCount + page.Succeeded)
	o.setFailedCount(o.FailedCount + page.Failed)
	o.setSkippedCount(o.SkippedCount + page.Skipped)
	if room := MaxErrorSamples - len(o.Errors); room > 0 && len(page.Errors) > 0 {
		o.setErrors(append(slices.Clone(o.Errors), page.Errors[:min(room, len(page.Errors))]...))
	}
	cursor := page.Cursor
	o.setCursor(&cursor)
	o.touch(now)
	return nil
}

// Processed 是已处理（计入 succeeded、failed 或 skipped）的 Run 数。
func (o *Operation) Processed() int {
	return o.SucceededCount + o.FailedCount + o.SkippedCount
}

// Finish 在全部选中的 Run 都处理完后结束：有失败且没有一个成功为 failed，否则为 completed。
// TotalCount 改为实际处理的 Run 数。
func (o *Operation) Finish(now time.Time) error {
	status := StatusEnums.Completed
	if o.FailedCount > 0 && o.SucceededCount == 0 {
		status = StatusEnums.Failed
	}
	if err := o.transition(status, now); err != nil {
		return err
	}
	o.setTotalCount(o.Processed())
	o.setFinishedAt(&now)
	return nil
}

// Cancel 取消批量操作：pending 或 running → cancelled。已取消时不变；已结束时返回 ErrInvalidTransition。
func (o *Operation) Cancel(now time.Time) error {
	if o.Status == StatusEnums.Cancelled {
		return nil
	}
	if err := o.transition(StatusEnums.Cancelled, now); err != nil {
		return err
	}
	o.setFinishedAt(&now)
	return nil
}

func (o *Operation) transition(to Status, now time.Time) error {
	if !CanTransition(o.Status, to) {
		return fmt.Errorf("%w: batch operation %s is %s, cannot become %s", ErrInvalidTransition, o.ID, o.Status, to)
	}
	o.setStatus(to)
	o.touch(now)
	return nil
}

// touch 刷新 UpdatedAt，并在本次待保存的变更里把 Version 加一（无论改了几次，一次保存只加一）。
func (o *Operation) touch(now time.Time) {
	if o.patch.Version.IsAbsent() {
		o.setVersion(o.Version + 1)
	}
	o.setUpdatedAt(now)
}

// ExpectedVersion 是保存这些变更时存储中应有的 Version：没有待保存的 Version 变更时就是当前值。
func (o *Operation) ExpectedVersion() int64 {
	if o.patch.Version.IsPresent() {
		return o.Version - 1
	}
	return o.Version
}
