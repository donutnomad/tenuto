package schedules

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/server/biz/domain/clock"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/biz/domain/schedule"
	"github.com/google/uuid"
)

// UseCase 编排 Schedule 的增删改查与触发。
type UseCase struct {
	repo   schedule.Repository
	firer  schedule.Firer
	clock  clock.Clock
	logger *slog.Logger
}

func NewUseCase(repo schedule.Repository, firer schedule.Firer, clk clock.Clock, logger *slog.Logger) *UseCase {
	return &UseCase{repo: repo, firer: firer, clock: clk, logger: logger}
}

func (u *UseCase) Create(req CreateRequest) (*schedule.Schedule, error) {
	req.Namespace = protocol.NormalizeNamespace(req.Namespace)
	entity, err := schedule.New(uuid.NewString(), schedule.NewParams(req), u.clock.Now())
	if err != nil {
		return nil, err
	}
	if err := u.repo.CreateSchedule(entity); err != nil {
		return nil, err
	}
	return entity, nil
}

func (u *UseCase) Get(namespace, scheduleID string) (*schedule.Schedule, error) {
	return u.repo.GetSchedule(protocol.NormalizeNamespace(namespace), scheduleID)
}

func (u *UseCase) Update(namespace, scheduleID string, req UpdateRequest) (*schedule.Schedule, error) {
	entity, err := u.Get(namespace, scheduleID)
	if err != nil {
		return nil, err
	}
	if err := entity.Update(schedule.Changes(req), u.clock.Now()); err != nil {
		return nil, err
	}
	if err := u.repo.UpdateSchedule(entity); err != nil {
		return nil, err
	}
	return entity, nil
}

func (u *UseCase) Delete(namespace, scheduleID string) error {
	return u.repo.DeleteSchedule(protocol.NormalizeNamespace(namespace), scheduleID)
}

func (u *UseCase) Pause(namespace, scheduleID string) error {
	entity, err := u.Get(namespace, scheduleID)
	if err != nil {
		return err
	}
	entity.Pause(u.clock.Now())
	return u.repo.UpdateSchedule(entity)
}

func (u *UseCase) Resume(namespace, scheduleID string) error {
	entity, err := u.Get(namespace, scheduleID)
	if err != nil {
		return err
	}
	entity.Resume(u.clock.Now())
	return u.repo.UpdateSchedule(entity)
}

func (u *UseCase) List(filter ListFilter) (ListResult, error) {
	filter.Namespace = protocol.NormalizeNamespace(filter.Namespace)
	all, err := u.repo.ListSchedules(filter.Namespace)
	if err != nil {
		return ListResult{}, err
	}
	items := make([]schedule.Schedule, 0, len(all))
	for _, entity := range all {
		if filter.WorkflowType != "" && entity.WorkflowType != filter.WorkflowType {
			continue
		}
		if filter.Paused != nil && entity.Paused != *filter.Paused {
			continue
		}
		items = append(items, *entity)
	}
	sort.Slice(items, func(i, j int) bool { return items[i].CreatedAt.Before(items[j].CreatedAt) })
	total := len(items)
	limit := filter.Limit
	if limit <= 0 {
		limit = 50
	}
	offset := filter.Offset
	if offset > total {
		offset = total
	}
	end := offset + limit
	if end > total {
		end = total
	}
	return ListResult{Items: items[offset:end], Total: total}, nil
}

// Trigger 立即触发一次，同样遵守 Overlap Policy：SKIP 下上一次触发的 Run 仍在运行时返回 schedule.ErrOverlapSkipped。
// 先用 CAS 推进 NextRunAt 占位，再启动 Run 并记录结果；启动失败时 NextRunAt 恢复为触发前的值。
func (u *UseCase) Trigger(ctx context.Context, namespace, scheduleID string) (string, error) {
	namespace = protocol.NormalizeNamespace(namespace)
	entity, err := u.Get(namespace, scheduleID)
	if err != nil {
		return "", err
	}
	if !u.overlapAllowsStart(entity) {
		return "", fmt.Errorf("%w: schedule %s/%s last run %s", schedule.ErrOverlapSkipped, namespace, scheduleID, entity.LastRunID)
	}
	now := u.clock.Now()
	oldNextRunAt := entity.AdvanceNextRunAt(now)
	advanced, err := u.repo.UpdateScheduleNextRunAtCAS(entity, oldNextRunAt)
	if err != nil {
		return "", err
	}
	if !advanced {
		return "", fmt.Errorf("%w: %s/%s, try again", schedule.ErrConcurrentUpdate, namespace, scheduleID)
	}
	runID, err := u.firer.StartWorkflowForCron(ctx, entity.Namespace, entity.WorkflowType, entity.RunWorkflowID(now), entity.Input, entity.RunSearchAttributes(now))
	if err != nil {
		u.restoreNextRunAt(entity, oldNextRunAt)
		return "", err
	}
	entity.RecordRun(runID, now)
	if err := u.repo.UpdateSchedule(entity); err != nil {
		u.logger.Error("schedule triggered but updating its last run failed", "namespace", entity.Namespace, "schedule_id", entity.ID, "run_id", runID, "err", err)
	}
	return runID, nil
}

// FireDue 扫描到期的 Schedule 并逐一触发；入口和循环内均检查 ctx。
func (u *UseCase) FireDue(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}
	due, err := u.repo.ListDueSchedules(u.clock.Now())
	if err != nil {
		u.logger.Error("schedule driver: list due schedules failed", "err", err)
		return
	}
	for _, entity := range due {
		if ctx.Err() != nil {
			return
		}
		u.fire(ctx, entity)
	}
}

// fire 处理单个到期 Schedule。
// 流程：catchup 检查 → CAS 推进 NextRunAt → OverlapPolicy 检查 → 触发 → 更新 LastRun 信息。
// 启动 Run 失败时恢复 NextRunAt，这次到期点由下一轮扫描重新触发，直到超出 catchup window。这次到期点的 Workflow ID
// 已有运行中的 Run（上一轮启动已提交、应答丢失）时这次触发已经完成，不恢复。
func (u *UseCase) fire(ctx context.Context, entity *schedule.Schedule) {
	now := u.clock.Now()

	// 超出 catchup window：跳过触发，但仍重算 NextRunAt
	if entity.MissedCatchupWindow(now) {
		u.advanceNextRunAt(entity, now)
		return
	}

	// 先 CAS 推进 NextRunAt（占位），CAS 失败说明已被其他实例处理
	oldNextRunAt, advanced := u.advanceNextRunAt(entity, now)
	if !advanced {
		return
	}

	if !u.overlapAllowsStart(entity) {
		u.logger.Info("schedule driver: last run is still running or its status is unknown, skipping", "namespace", entity.Namespace, "schedule_id", entity.ID, "last_run_id", entity.LastRunID, "overlap_policy", entity.OverlapPolicy)
		return
	}

	// 触发 workflow（标记 Initiator=Cron），Workflow ID 取这次的到期点
	runID, err := u.firer.StartWorkflowForCron(ctx, entity.Namespace, entity.WorkflowType, entity.RunWorkflowID(*oldNextRunAt), entity.Input, entity.RunSearchAttributes(*oldNextRunAt))
	if errors.Is(err, execution.ErrCurrentRunConflict) {
		u.logger.Info("schedule driver: run for this fire time is already running", "namespace", entity.Namespace, "schedule_id", entity.ID, "fire_time", oldNextRunAt.UTC().Format(time.RFC3339))
		return
	}
	if err != nil {
		u.logger.Error("schedule driver: start workflow failed", "namespace", entity.Namespace, "schedule_id", entity.ID, "err", err)
		u.restoreNextRunAt(entity, oldNextRunAt)
		return
	}

	// 触发成功后更新 LastRunAt/LastRunID/RunCount（失败无害，NextRunAt 已通过 CAS 推进）
	entity.RecordRun(runID, now)
	entity.Touch(now)
	if err := u.repo.UpdateSchedule(entity); err != nil {
		u.logger.Warn("schedule driver: update last run failed", "namespace", entity.Namespace, "schedule_id", entity.ID, "err", err)
	}
}

// overlapAllowsStart 按 Overlap Policy 判定能否启动：需要时读取上一次触发所在 Workflow ID 的 current Run，规则见
// schedule.(*Schedule).AllowsStart。
func (u *UseCase) overlapAllowsStart(entity *schedule.Schedule) bool {
	if !entity.ChecksPreviousRun() {
		return true
	}
	status, err := u.firer.CurrentRunStatus(entity.LastRunID)
	if err != nil && !errors.Is(err, execution.ErrRunNotFound) {
		u.logger.Warn("schedule driver: read last run failed", "namespace", entity.Namespace, "schedule_id", entity.ID, "err", err)
	}
	return entity.AllowsStart(status, err)
}

// advanceNextRunAt 以 now 为基准把 NextRunAt 推进到未来并落库，返回推进前的值。
// advanced 为 false 表示被抢占或出错。
func (u *UseCase) advanceNextRunAt(entity *schedule.Schedule, now time.Time) (previous *time.Time, advanced bool) {
	previous = entity.AdvanceNextRunAt(now)
	advanced, err := u.repo.UpdateScheduleNextRunAtCAS(entity, previous)
	if err != nil {
		u.logger.Error("schedule driver: advance next_run_at failed", "namespace", entity.Namespace, "schedule_id", entity.ID, "err", err)
		return previous, false
	}
	return previous, advanced
}

// restoreNextRunAt 在启动 Run 失败后把 NextRunAt 改回推进前的值。
func (u *UseCase) restoreNextRunAt(entity *schedule.Schedule, previous *time.Time) {
	advanced := entity.RestoreNextRunAt(previous, u.clock.Now())
	if _, err := u.repo.UpdateScheduleNextRunAtCAS(entity, advanced); err != nil {
		u.logger.Error("schedule driver: restore next_run_at failed", "namespace", entity.Namespace, "schedule_id", entity.ID, "err", err)
	}
}
