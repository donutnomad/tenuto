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
// Workflow ID 取触发时刻（秒级），同一秒内重复触发返回同一个 Run。
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
	// 同一秒内重复触发落在同一个 Workflow ID 上，返回已启动的 Run，不重复记录。
	if runID == entity.LastRunID {
		return runID, nil
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
// 流程：catchup 检查 → OverlapPolicy 检查 → 以到期点的 Workflow ID 启动 Run → CAS 推进 NextRunAt → 更新 LastRun 信息。
// 先启动再推进：两步之间进程退出时 NextRunAt 仍是这次到期点，下一轮扫描以同一个 Workflow ID 重新启动。Cron 启动按
// Workflow ID 去重（见 Firer.StartWorkflowForCron），重新启动返回已启动的 Run，不会重复执行；多个实例同时触发同一个
// 到期点时同样只启动一个 Run，CAS 胜出的实例记录它。启动失败时 NextRunAt 不动，下一轮扫描重试，直到超出 catchup window。
func (u *UseCase) fire(ctx context.Context, entity *schedule.Schedule) {
	now := u.clock.Now()

	// 超出 catchup window：跳过触发，但仍重算 NextRunAt
	if entity.MissedCatchupWindow(now) {
		u.advanceNextRunAt(entity, now)
		return
	}

	if !u.overlapAllowsStart(entity) {
		u.logger.Info("schedule driver: last run is still running or its status is unknown, skipping", "namespace", entity.Namespace, "schedule_id", entity.ID, "last_run_id", entity.LastRunID, "overlap_policy", entity.OverlapPolicy)
		u.advanceNextRunAt(entity, now)
		return
	}

	// 触发 workflow（标记 Initiator=Cron），Workflow ID 取这次的到期点
	fireTime := *entity.NextRunAt
	runID, err := u.firer.StartWorkflowForCron(ctx, entity.Namespace, entity.WorkflowType, entity.RunWorkflowID(fireTime), entity.Input, entity.RunSearchAttributes(fireTime))
	if errors.Is(err, execution.ErrCurrentRunConflict) {
		u.logger.Info("schedule driver: run for this fire time is already running", "namespace", entity.Namespace, "schedule_id", entity.ID, "fire_time", fireTime.UTC().Format(time.RFC3339))
		u.advanceNextRunAt(entity, now)
		return
	}
	if err != nil {
		u.logger.Error("schedule driver: start workflow failed", "namespace", entity.Namespace, "schedule_id", entity.ID, "err", err)
		return
	}

	// CAS 失败说明另一个实例已推进这次到期点，由它记录 Run
	if _, advanced := u.advanceNextRunAt(entity, now); !advanced {
		return
	}
	// 记录 LastRunAt/LastRunID/RunCount（失败无害，NextRunAt 已通过 CAS 推进）
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
