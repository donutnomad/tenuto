package persistence

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/biz/domain/schedule"
	"github.com/donutnomad/tenuto/server/biz/domain/schedule/scheduletest"
	"github.com/donutnomad/tenuto/server/biz/usecase/schedules"
	"github.com/donutnomad/tenuto/server/infra/systemclock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ============================================
// Schedule gorm 持久化测试
// ============================================
// 测试策略:
// - 仓储契约（round-trip、ListDueSchedules、更新删除、NextRunAt CAS）：与内存实现共用 scheduletest
// - Integration: gorm 按 patch 更新的行为（快照更新、触发期间的并发暂停）
// ============================================

func TestScheduleRepositoryContract(t *testing.T) {
	scheduletest.RunRepositoryContract(t, func(t *testing.T) schedule.Repository { return newTestStore(t).Schedules })
}

// startFunc 把启动函数适配成 schedule.Firer。
type startFunc func(context.Context, string, string, map[string]any) (string, error)

func (f startFunc) StartWorkflowForCron(ctx context.Context, namespace, workflowType, workflowID string, input map[string]any, _ map[string]string) (string, error) {
	return f(ctx, namespace, workflowType, input)
}

func (startFunc) CurrentRunStatus(string) (history.Status, error) {
	return "", execution.ErrRunNotFound
}

func TestScheduleUpdateWithoutChangesIsNoOp(t *testing.T) {
	// Integration: 没有经领域方法改动的 UpdateSchedule 不写库，也不撤销随后的 CAS 推进。
	store := newTestStore(t)
	service := schedules.NewUseCase(store.Schedules, nil, systemclock.Clock{}, slog.New(slog.DiscardHandler))
	entity, err := service.Create(schedules.CreateRequest{
		WorkflowType: "wf-snapshot", Description: "original", CronExpression: "* * * * *",
		Input: map[string]any{"original": true},
	})
	require.NoError(t, err)

	advancer, err := store.Schedules.GetSchedule(entity.Namespace, entity.ID)
	require.NoError(t, err)
	previous := advancer.AdvanceNextRunAt(advancer.NextRunAt.Add(time.Hour), advancer.NextRunAt.Add(time.Hour))
	advanced, err := store.Schedules.UpdateScheduleNextRunAtCAS(advancer, previous)
	require.NoError(t, err)
	require.True(t, advanced)

	// 直接改公开字段没有经过 setter，不属于待保存的变更。
	entity.Description = "untracked"
	entity.Paused = true
	entity.NextRunAt = nil
	entity.Input = nil
	require.NoError(t, store.Schedules.UpdateSchedule(entity))

	got, err := store.Schedules.GetSchedule(entity.Namespace, entity.ID)
	require.NoError(t, err)
	assert.Equal(t, "original", got.Description)
	assert.False(t, got.Paused)
	assert.Equal(t, map[string]any{"original": true}, got.Input)
	require.NotNil(t, got.NextRunAt)
	assert.True(t, got.NextRunAt.Equal(*advancer.NextRunAt), "next_run_at = %v, want the CAS-advanced %v", got.NextRunAt, advancer.NextRunAt)
}

func TestScheduleTriggerKeepsPause(t *testing.T) {
	// Integration: 触发期间的暂停保留空 NextRunAt，后续执行记录只保存本次变更。
	store := newTestStore(t)
	var service *schedules.UseCase
	var entity *schedule.Schedule
	service = schedules.NewUseCase(store.Schedules, startFunc(func(context.Context, string, string, map[string]any) (string, error) {
		err := service.Pause(entity.Namespace, entity.ID)
		return "triggered-run", err
	}), systemclock.Clock{}, slog.New(slog.DiscardHandler))
	var err error
	entity, err = service.Create(schedules.CreateRequest{
		WorkflowType: "wf-trigger-pause", CronExpression: "* * * * *",
	})
	require.NoError(t, err)

	runID, err := service.Trigger(context.Background(), entity.Namespace, entity.ID)
	require.NoError(t, err)
	got, err := store.Schedules.GetSchedule(entity.Namespace, entity.ID)
	require.NoError(t, err)
	assert.True(t, got.Paused)
	assert.Nil(t, got.NextRunAt)
	assert.Equal(t, runID, got.LastRunID)
	assert.Equal(t, 1, got.RunCount)
}
