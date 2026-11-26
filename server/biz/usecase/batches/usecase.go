// Package batches 编排批量操作：受理请求时只保存 pending 记录，由后台驱动分页处理选中的 Run。
package batches

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/batch"
	"github.com/donutnomad/tenuto/server/biz/domain/clock"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/google/uuid"
)

// PageSize 是驱动每页处理的 Run 数；每页处理完后保存一次进度。
const PageSize = 100

// cancelAttempts 是取消与驱动保存进度冲突时重新读取再取消的次数。
const cancelAttempts = 3

// Runs 是选择与核对 Run 用到的读取。消费方是 UseCase，execution.ExecutionManager 满足它。
type Runs interface {
	Get(runID string) (*execution.Run, error)
	List(filter execution.StoreListFilter) (execution.StoreListResult, error)
}

// Actions 是逐个 Run 执行的动作。消费方是 UseCase，执行核心实现它。
type Actions interface {
	CancelWorkflow(ctx context.Context, namespace, workflowID, runID, reason string) error
	TerminateWorkflow(ctx context.Context, namespace, workflowID, runID, reason string) error
	ResetWorkflow(ctx context.Context, req execution.ResetWorkflowRequest) (string, error)
}

// UseCase 受理、查询与取消批量操作，并由驱动执行。
type UseCase struct {
	repo    batch.Repository
	runs    Runs
	actions Actions
	clock   clock.Clock
	logger  *slog.Logger
}

func NewUseCase(repo batch.Repository, runs Runs, actions Actions, clk clock.Clock, logger *slog.Logger) *UseCase {
	return &UseCase{repo: repo, runs: runs, actions: actions, clock: clk, logger: logger}
}

// Start 校验请求并保存 pending 记录后立即返回；选中的 Run 由驱动在后台处理。
func (u *UseCase) Start(req StartRequest) (*batch.Operation, error) {
	op, err := batch.New(uuid.NewString(), batch.NewParams{
		Namespace: req.Namespace, Type: req.Type, Filter: req.Filter, ResetType: req.ResetType, Reason: req.Reason,
	}, u.clock.Now())
	if err != nil {
		return nil, err
	}
	if err := u.repo.CreateOperation(op); err != nil {
		return nil, err
	}
	return op, nil
}

func (u *UseCase) Get(namespace, operationID string) (*batch.Operation, error) {
	return u.repo.GetOperation(namespace, operationID)
}

// List 按创建时间从新到旧分页返回 namespace 下的批量操作。
func (u *UseCase) List(filter ListFilter) (ListResult, error) {
	ops, total, err := u.repo.ListOperations(batch.ListFilter(filter))
	if err != nil {
		return ListResult{}, err
	}
	items := make([]batch.Operation, 0, len(ops))
	for _, op := range ops {
		items = append(items, *op)
	}
	return ListResult{Items: items, Total: total}, nil
}

// Cancel 把 pending 或 running 的批量操作标为 cancelled 并返回最新记录；驱动在下一页之前停下，已处理的计数保留。
// 已结束的批量操作返回 batch.ErrInvalidTransition。与驱动保存进度冲突时重新读取后再取消。
func (u *UseCase) Cancel(namespace, operationID string) (*batch.Operation, error) {
	for attempt := 1; ; attempt++ {
		op, err := u.repo.GetOperation(namespace, operationID)
		if err != nil {
			return nil, err
		}
		if op.Status == batch.StatusEnums.Cancelled {
			return op, nil
		}
		if err := op.Cancel(u.clock.Now()); err != nil {
			return nil, err
		}
		err = u.repo.UpdateOperation(op)
		if err == nil {
			return op, nil
		}
		if !errors.Is(err, batch.ErrConcurrentUpdate) || attempt == cancelAttempts {
			return nil, err
		}
	}
}

// ProcessActive 按创建顺序处理全部 pending 与 running 的批量操作，每个处理到结束、被取消或 ctx 取消为止。
// 驱动每个 tick 调用一次；多副本部署下只有 leader 副本运行驱动。
func (u *UseCase) ProcessActive(ctx context.Context) {
	ops, err := u.repo.ListActiveOperations()
	if err != nil {
		u.logger.Error("batch driver: list active batch operations failed", "err", err)
		return
	}
	for _, op := range ops {
		if ctx.Err() != nil {
			return
		}
		if err := u.process(ctx, op); err != nil {
			u.logger.Error("batch driver: process batch operation failed", "namespace", op.Namespace, "operation_id", op.ID, "err", err)
		}
	}
}

// process 认领 op 后逐页处理。选择是活的、但有上界：每页按 (created_at, id) 游标从存储读取当时仍匹配过滤条件的 Run，
// 只取批量操作创建时（SelectedBefore）之前创建的 Run。Reset 等动作产生的新 Run 不在选择之内，游标单调前进，处理必然结束；
// 翻页期间离开过滤条件的 Run 不再处理。
//
// 每页的计数与游标在这一页的动作全部执行之后一起保存。保存之前崩溃（或 ctx 取消）时，下一次从上一个游标重做这一页，
// 重做对每种动作都是安全的，见 apply。
func (u *UseCase) process(ctx context.Context, op *batch.Operation) error {
	if op.Status == batch.StatusEnums.Pending {
		if err := op.Begin(u.clock.Now()); err != nil {
			return err
		}
		if err := u.repo.UpdateOperation(op); err != nil {
			return ignoreConflict(err)
		}
	}
	for op.Status == batch.StatusEnums.Running {
		if ctx.Err() != nil {
			return nil
		}
		filter := pageFilter(op)
		page, err := u.runs.List(filter)
		if err != nil {
			return fmt.Errorf("list runs: %w", err)
		}
		if len(page.Runs) == 0 {
			if err := op.Finish(u.clock.Now()); err != nil {
				return err
			}
			return ignoreConflict(u.repo.UpdateOperation(op))
		}
		result := u.applyPage(ctx, op, page)
		if ctx.Err() != nil {
			// 动作可能因 ctx 取消而失败；这一页不保存，下次从上一个游标重做。
			return nil
		}
		if op, err = u.savePage(op, result); err != nil {
			return err
		}
	}
	return nil
}

// pageFilter 是 op 下一页的存储查询：游标之后、SelectedBefore 之前、按 (created_at, id) 升序。
func pageFilter(op *batch.Operation) execution.StoreListFilter {
	f := op.Filter
	createdBefore := op.SelectedBefore()
	return execution.StoreListFilter{
		Namespace: f.Namespace, RunIDs: f.RunIDs, WorkflowID: f.WorkflowID, Statuses: f.Statuses,
		SearchAttributes: f.SearchAttributes, BlockedOnly: f.BlockedOnly, IncludeTerminal: f.IncludeTerminal,
		CreatedBefore: &createdBefore, After: op.Cursor, Limit: PageSize,
	}
}

// errPageTaken 表示另一个驱动已经保存了这一页之后的进度。
var errPageTaken = errors.New("progress was saved by another driver; stopping")

// savePage 保存一页的结果。与取消冲突时重新读取，把这一页记到已取消的记录上；记录的游标已被推进时说明另一个驱动
// （租约切换瞬间的两个 leader）处理了同一页，丢弃本页结果并停止。
func (u *UseCase) savePage(op *batch.Operation, page batch.Page) (*batch.Operation, error) {
	start := op.Cursor
	for {
		if err := op.RecordPage(page, u.clock.Now()); err != nil {
			return op, err
		}
		err := u.repo.UpdateOperation(op)
		if !errors.Is(err, batch.ErrConcurrentUpdate) {
			return op, err
		}
		reloaded, err := u.repo.GetOperation(op.Namespace, op.ID)
		if err != nil {
			return op, err
		}
		if !sameCursor(reloaded.Cursor, start) {
			return reloaded, errPageTaken
		}
		op = reloaded
	}
}

func sameCursor(a, b *execution.RunCursor) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.RunID == b.RunID && a.CreatedAt.Equal(b.CreatedAt)
}

// ignoreConflict 把并发修改当作正常结果：记录已被取消或另一个驱动改过，下一个 tick 重新读取。
func ignoreConflict(err error) error {
	if errors.Is(err, batch.ErrConcurrentUpdate) {
		return nil
	}
	return err
}

type outcome int

const (
	succeeded outcome = iota
	failed
	skipped
)

func (u *UseCase) applyPage(ctx context.Context, op *batch.Operation, page execution.StoreListResult) batch.Page {
	result := batch.Page{Total: page.Total, Cursor: execution.CursorOf(page.Runs[len(page.Runs)-1])}
	for _, run := range page.Runs {
		switch outcome, err := u.apply(ctx, op, run); outcome {
		case succeeded:
			result.Succeeded++
		case skipped:
			result.Skipped++
		case failed:
			result.Failed++
			result.Errors = append(result.Errors, batch.RunError{RunID: run.ID, Message: err.Error()})
		}
	}
	return result
}

// apply 对一个 Run 执行动作。重做一页时每种动作都不会重复生效：
//   - cancel：已结束的 Run 跳过；已请求过取消的 Run 再次请求不追加事件，计为成功（取消已请求）。
//   - terminate：已结束的 Run（包括上一次已终止的）跳过。
//   - reset：Run 的 ResetRunID 指向批量操作创建之后产生的 Run 时视为已 Reset，跳过；读到之后才被 Reset 的
//     （两个重叠的驱动处理同一页）由 Reset 的 RequestID（batch ID 与 Run ID）在 Server 上去重，返回第一次的结果。
//
// 动作失败而 Run 已经结束（与其他关闭路径竞争）时，cancel 与 terminate 计为跳过。
func (u *UseCase) apply(ctx context.Context, op *batch.Operation, run *execution.Run) (outcome, error) {
	if op.Type == batch.TypeEnums.Reset {
		if u.resetSinceCreated(op, run) {
			return skipped, nil
		}
		_, err := u.actions.ResetWorkflow(ctx, execution.ResetWorkflowRequest{
			Namespace: run.Namespace, RunID: run.ID, Reason: op.Reason, ResetType: op.ResetType,
			RequestID: op.ResetRequestID(run.ID),
		})
		if err != nil {
			return failed, err
		}
		return succeeded, nil
	}
	if history.IsTerminalStatus(run.Status) {
		return skipped, nil
	}
	var err error
	if op.Type == batch.TypeEnums.Cancel {
		err = u.actions.CancelWorkflow(ctx, run.Namespace, run.WorkflowID, run.ID, op.Reason)
	} else {
		reason := op.Reason
		if reason == "" {
			reason = "batch terminate"
		}
		err = u.actions.TerminateWorkflow(ctx, run.Namespace, run.WorkflowID, run.ID, reason)
	}
	if err == nil {
		return succeeded, nil
	}
	if current, getErr := u.runs.Get(run.ID); getErr == nil && history.IsTerminalStatus(current.Status) {
		return skipped, nil
	}
	return failed, err
}

// resetSinceCreated 报告 run 是否已在批量操作创建之后被 Reset 过。
func (u *UseCase) resetSinceCreated(op *batch.Operation, run *execution.Run) bool {
	if run.ResetRunID == "" {
		return false
	}
	resetRun, err := u.runs.Get(run.ResetRunID)
	return err == nil && !resetRun.CreatedAt.Before(op.CreatedAt)
}
