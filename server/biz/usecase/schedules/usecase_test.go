package schedules_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/server/biz/domain/schedule"
	"github.com/donutnomad/tenuto/server/biz/usecase/schedules"
	"github.com/donutnomad/tenuto/server/infra/memory"
	"github.com/donutnomad/tenuto/server/infra/systemclock"
)

// ============================================
// Schedule CRUD + 生命周期测试
// ============================================
// 测试策略:
// - Happy Path: 创建、获取、更新、删除、暂停、恢复、列表、触发
// - Error Case: 无效 cron 表达式、不存在的 schedule
// - Edge Case:  分页、namespace 隔离、paused 过滤、暂停状态手动触发
// ============================================

func TestCreateSchedule_Success(t *testing.T) {
	// Happy Path: 使用有效 cron 表达式创建 schedule，验证所有字段正确设置
	uc := newTestUseCase(newFakeFirer(memory.NewScheduleRepository()))

	req := schedules.CreateRequest{
		Namespace:      "default",
		WorkflowType:   "my-workflow",
		Description:    "every 5 minutes",
		CronExpression: "*/5 * * * *",
		Timezone:       "UTC",
		Input:          map[string]any{"key": "value"},
		OverlapPolicy:  "SKIP",
		Paused:         false,
	}

	schedule, err := uc.Create(req)
	if err != nil {
		t.Fatalf("CreateSchedule failed: %v", err)
	}

	if schedule.ID == "" {
		t.Fatal("expected non-empty schedule ID")
	}
	if schedule.Namespace != "default" {
		t.Fatalf("expected namespace=default, got %s", schedule.Namespace)
	}
	if schedule.WorkflowType != "my-workflow" {
		t.Fatalf("expected workflow_type=my-workflow, got %s", schedule.WorkflowType)
	}
	if schedule.Description != "every 5 minutes" {
		t.Fatalf("expected description='every 5 minutes', got %s", schedule.Description)
	}
	if schedule.CronExpression != "*/5 * * * *" {
		t.Fatalf("expected cron_expression='*/5 * * * *', got %s", schedule.CronExpression)
	}
	if schedule.Timezone != "UTC" {
		t.Fatalf("expected timezone=UTC, got %s", schedule.Timezone)
	}
	if schedule.Input["key"] != "value" {
		t.Fatalf("expected input key=value, got %v", schedule.Input)
	}
	if schedule.OverlapPolicy != "SKIP" {
		t.Fatalf("expected overlap_policy=SKIP, got %s", schedule.OverlapPolicy)
	}
	if schedule.Paused {
		t.Fatal("expected paused=false")
	}
	if schedule.NextRunAt == nil {
		t.Fatal("expected next_run_at to be set for unpaused schedule")
	}
	if schedule.RunCount != 0 {
		t.Fatalf("expected run_count=0, got %d", schedule.RunCount)
	}
	if schedule.CreatedAt.IsZero() {
		t.Fatal("expected non-zero created_at")
	}
	if schedule.UpdatedAt.IsZero() {
		t.Fatal("expected non-zero updated_at")
	}
}

func TestCreateSchedule_Paused(t *testing.T) {
	// Happy Path: 创建暂停状态的 schedule，nextRunAt 应为 nil
	uc := newTestUseCase(newFakeFirer(memory.NewScheduleRepository()))

	schedule, err := uc.Create(schedules.CreateRequest{
		WorkflowType:   "wf-paused",
		CronExpression: "0 * * * *",
		Paused:         true,
	})
	if err != nil {
		t.Fatalf("CreateSchedule failed: %v", err)
	}
	if !schedule.Paused {
		t.Fatal("expected paused=true")
	}
	if schedule.NextRunAt != nil {
		t.Fatalf("expected next_run_at=nil for paused schedule, got %v", schedule.NextRunAt)
	}
}

func TestCreateSchedule_InvalidCron(t *testing.T) {
	// Error Case: 无效 cron 表达式应返回错误
	uc := newTestUseCase(newFakeFirer(memory.NewScheduleRepository()))

	cases := []struct {
		name string
		cron string
	}{
		{"empty", ""},
		{"too few fields", "* * *"},
		{"too many fields", "* * * * * *"},
		{"invalid minute", "99 * * * *"},
		{"invalid hour", "0 25 * * *"},
		{"invalid day", "0 0 32 * *"},
		{"invalid month", "0 0 * 13 *"},
		{"invalid dow", "0 0 * * 8"},
		{"non-numeric", "abc * * * *"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := uc.Create(schedules.CreateRequest{
				WorkflowType:   "wf-test",
				CronExpression: tc.cron,
			})
			if err == nil {
				t.Fatalf("expected error for cron expression %q, got nil", tc.cron)
			}
		})
	}
}

func TestCreateSchedule_MissingWorkflowType(t *testing.T) {
	// Error Case: 缺少 workflow_type 应返回错误
	uc := newTestUseCase(newFakeFirer(memory.NewScheduleRepository()))

	_, err := uc.Create(schedules.CreateRequest{
		CronExpression: "*/5 * * * *",
	})
	if err == nil {
		t.Fatal("expected error for empty workflow_type")
	}
}

func TestCreateSchedule_InvalidTimezone(t *testing.T) {
	// Error Case: 无效时区应返回错误
	uc := newTestUseCase(newFakeFirer(memory.NewScheduleRepository()))

	_, err := uc.Create(schedules.CreateRequest{
		WorkflowType:   "wf-tz",
		CronExpression: "0 0 * * *",
		Timezone:       "Invalid/Timezone_NotExist",
	})
	if err == nil {
		t.Fatal("expected error for invalid timezone")
	}
}

func TestGetSchedule_NotFound(t *testing.T) {
	// Error Case: 获取不存在的 schedule 应返回错误
	uc := newTestUseCase(newFakeFirer(memory.NewScheduleRepository()))

	_, err := uc.Get("default", "non-existent-id")
	if err == nil {
		t.Fatal("expected error for non-existent schedule")
	}
}

func TestGetSchedule_AfterCreate(t *testing.T) {
	// Happy Path: 创建后立即获取，验证数据一致
	uc := newTestUseCase(newFakeFirer(memory.NewScheduleRepository()))

	created, err := uc.Create(schedules.CreateRequest{
		WorkflowType:   "wf-get",
		CronExpression: "30 2 * * *",
		Description:    "daily at 2:30",
	})
	if err != nil {
		t.Fatalf("CreateSchedule failed: %v", err)
	}

	got, err := uc.Get("default", created.ID)
	if err != nil {
		t.Fatalf("GetSchedule failed: %v", err)
	}
	if got.ID != created.ID {
		t.Fatalf("expected id=%s, got %s", created.ID, got.ID)
	}
	if got.WorkflowType != "wf-get" {
		t.Fatalf("expected workflow_type=wf-get, got %s", got.WorkflowType)
	}
	if got.Description != "daily at 2:30" {
		t.Fatalf("expected description='daily at 2:30', got %s", got.Description)
	}
}

func TestUpdateSchedule_CronChange(t *testing.T) {
	// Happy Path: 更新 cron 表达式后，nextRunAt 应重新计算
	uc := newTestUseCase(newFakeFirer(memory.NewScheduleRepository()))

	created, err := uc.Create(schedules.CreateRequest{
		WorkflowType:   "wf-update",
		CronExpression: "0 0 * * *",
	})
	if err != nil {
		t.Fatalf("CreateSchedule failed: %v", err)
	}

	oldNextRunAt := created.NextRunAt
	if oldNextRunAt == nil {
		t.Fatal("expected initial next_run_at to be set")
	}

	newCron := "30 12 * * *"
	updated, err := uc.Update("default", created.ID, schedules.UpdateRequest{
		CronExpression: &newCron,
	})
	if err != nil {
		t.Fatalf("UpdateSchedule failed: %v", err)
	}

	if updated.CronExpression != "30 12 * * *" {
		t.Fatalf("expected cron_expression='30 12 * * *', got %s", updated.CronExpression)
	}
	if updated.NextRunAt == nil {
		t.Fatal("expected next_run_at to be recalculated after cron change")
	}
	if updated.UpdatedAt.Before(created.UpdatedAt) || updated.UpdatedAt.Equal(created.UpdatedAt) {
		t.Fatal("expected updated_at to be after original")
	}
}

func TestUpdateSchedule_DescriptionOnly(t *testing.T) {
	// Happy Path: 仅更新 description，cron 和 nextRunAt 不变
	uc := newTestUseCase(newFakeFirer(memory.NewScheduleRepository()))

	created, err := uc.Create(schedules.CreateRequest{
		WorkflowType:   "wf-desc",
		CronExpression: "0 0 * * *",
		Description:    "old",
	})
	if err != nil {
		t.Fatalf("CreateSchedule failed: %v", err)
	}

	newDesc := "new description"
	updated, err := uc.Update("default", created.ID, schedules.UpdateRequest{
		Description: &newDesc,
	})
	if err != nil {
		t.Fatalf("UpdateSchedule failed: %v", err)
	}
	if updated.Description != "new description" {
		t.Fatalf("expected description='new description', got %s", updated.Description)
	}
	if updated.CronExpression != "0 0 * * *" {
		t.Fatalf("expected cron unchanged, got %s", updated.CronExpression)
	}
}

func TestUpdateSchedule_InvalidCron(t *testing.T) {
	// Error Case: 更新为无效 cron 应返回错误
	uc := newTestUseCase(newFakeFirer(memory.NewScheduleRepository()))

	created, err := uc.Create(schedules.CreateRequest{
		WorkflowType:   "wf-bad-update",
		CronExpression: "0 0 * * *",
	})
	if err != nil {
		t.Fatalf("CreateSchedule failed: %v", err)
	}

	badCron := "invalid"
	_, err = uc.Update("default", created.ID, schedules.UpdateRequest{
		CronExpression: &badCron,
	})
	if err == nil {
		t.Fatal("expected error for invalid cron expression on update")
	}
}

func TestDeleteSchedule(t *testing.T) {
	// Happy Path: 创建后删除，再获取应返回错误
	uc := newTestUseCase(newFakeFirer(memory.NewScheduleRepository()))

	created, err := uc.Create(schedules.CreateRequest{
		WorkflowType:   "wf-delete",
		CronExpression: "0 0 * * *",
	})
	if err != nil {
		t.Fatalf("CreateSchedule failed: %v", err)
	}

	if err := uc.Delete("default", created.ID); err != nil {
		t.Fatalf("DeleteSchedule failed: %v", err)
	}

	_, err = uc.Get("default", created.ID)
	if err == nil {
		t.Fatal("expected error after deleting schedule")
	}
}

func TestDeleteSchedule_NotFound(t *testing.T) {
	// Error Case: 删除不存在的 schedule 应返回错误
	uc := newTestUseCase(newFakeFirer(memory.NewScheduleRepository()))

	err := uc.Delete("default", "non-existent")
	if err == nil {
		t.Fatal("expected error for deleting non-existent schedule")
	}
}

func TestPauseResumeSchedule(t *testing.T) {
	// Happy Path: 创建 → 暂停（nextRunAt 为 nil）→ 恢复（nextRunAt 重新计算）
	uc := newTestUseCase(newFakeFirer(memory.NewScheduleRepository()))

	created, err := uc.Create(schedules.CreateRequest{
		WorkflowType:   "wf-pause",
		CronExpression: "*/10 * * * *",
	})
	if err != nil {
		t.Fatalf("CreateSchedule failed: %v", err)
	}
	if created.NextRunAt == nil {
		t.Fatal("expected initial next_run_at to be set")
	}

	// Pause
	if err := uc.Pause("default", created.ID); err != nil {
		t.Fatalf("PauseSchedule failed: %v", err)
	}

	paused, err := uc.Get("default", created.ID)
	if err != nil {
		t.Fatalf("GetSchedule after pause failed: %v", err)
	}
	if !paused.Paused {
		t.Fatal("expected paused=true after PauseSchedule")
	}
	if paused.NextRunAt != nil {
		t.Fatalf("expected next_run_at=nil after pause, got %v", paused.NextRunAt)
	}

	// Resume
	if err := uc.Resume("default", created.ID); err != nil {
		t.Fatalf("ResumeSchedule failed: %v", err)
	}

	resumed, err := uc.Get("default", created.ID)
	if err != nil {
		t.Fatalf("GetSchedule after resume failed: %v", err)
	}
	if resumed.Paused {
		t.Fatal("expected paused=false after ResumeSchedule")
	}
	if resumed.NextRunAt == nil {
		t.Fatal("expected next_run_at to be recalculated after resume")
	}
}

func TestPauseSchedule_NotFound(t *testing.T) {
	// Error Case: 暂停不存在的 schedule 应返回错误
	uc := newTestUseCase(newFakeFirer(memory.NewScheduleRepository()))

	err := uc.Pause("default", "ghost")
	if err == nil {
		t.Fatal("expected error for pausing non-existent schedule")
	}
}

func TestResumeSchedule_NotFound(t *testing.T) {
	// Error Case: 恢复不存在的 schedule 应返回错误
	uc := newTestUseCase(newFakeFirer(memory.NewScheduleRepository()))

	err := uc.Resume("default", "ghost")
	if err == nil {
		t.Fatal("expected error for resuming non-existent schedule")
	}
}

func TestListSchedules_FilterByNamespace(t *testing.T) {
	// Happy Path: 在不同 namespace 创建 schedule，按 namespace 正确过滤
	uc := newTestUseCase(newFakeFirer(memory.NewScheduleRepository()))

	for i := range 3 {
		_, err := uc.Create(schedules.CreateRequest{
			Namespace:      "ns-alpha",
			WorkflowType:   fmt.Sprintf("wf-alpha-%d", i),
			CronExpression: "0 0 * * *",
		})
		if err != nil {
			t.Fatalf("CreateSchedule ns-alpha failed: %v", err)
		}
	}
	for i := range 2 {
		_, err := uc.Create(schedules.CreateRequest{
			Namespace:      "ns-beta",
			WorkflowType:   fmt.Sprintf("wf-beta-%d", i),
			CronExpression: "0 0 * * *",
		})
		if err != nil {
			t.Fatalf("CreateSchedule ns-beta failed: %v", err)
		}
	}

	alphaResult, err := uc.List(schedules.ListFilter{Namespace: "ns-alpha"})
	if err != nil {
		t.Fatalf("ListSchedules ns-alpha failed: %v", err)
	}
	if alphaResult.Total != 3 {
		t.Fatalf("expected 3 schedules in ns-alpha, got %d", alphaResult.Total)
	}

	betaResult, err := uc.List(schedules.ListFilter{Namespace: "ns-beta"})
	if err != nil {
		t.Fatalf("ListSchedules ns-beta failed: %v", err)
	}
	if betaResult.Total != 2 {
		t.Fatalf("expected 2 schedules in ns-beta, got %d", betaResult.Total)
	}

	defaultResult, err := uc.List(schedules.ListFilter{Namespace: "default"})
	if err != nil {
		t.Fatalf("ListSchedules default failed: %v", err)
	}
	if defaultResult.Total != 0 {
		t.Fatalf("expected 0 schedules in default, got %d", defaultResult.Total)
	}
}

func TestListSchedules_FilterByPaused(t *testing.T) {
	// Happy Path: 创建暂停和未暂停的 schedule，按 paused 状态过滤
	uc := newTestUseCase(newFakeFirer(memory.NewScheduleRepository()))

	for i := range 3 {
		_, err := uc.Create(schedules.CreateRequest{
			WorkflowType:   fmt.Sprintf("wf-active-%d", i),
			CronExpression: "0 0 * * *",
			Paused:         false,
		})
		if err != nil {
			t.Fatalf("CreateSchedule active failed: %v", err)
		}
	}
	for i := range 2 {
		_, err := uc.Create(schedules.CreateRequest{
			WorkflowType:   fmt.Sprintf("wf-paused-%d", i),
			CronExpression: "0 0 * * *",
			Paused:         true,
		})
		if err != nil {
			t.Fatalf("CreateSchedule paused failed: %v", err)
		}
	}

	pausedTrue := true
	pausedResult, err := uc.List(schedules.ListFilter{
		Namespace: "default",
		Paused:    &pausedTrue,
	})
	if err != nil {
		t.Fatalf("ListSchedules paused=true failed: %v", err)
	}
	if pausedResult.Total != 2 {
		t.Fatalf("expected 2 paused schedules, got %d", pausedResult.Total)
	}

	pausedFalse := false
	activeResult, err := uc.List(schedules.ListFilter{
		Namespace: "default",
		Paused:    &pausedFalse,
	})
	if err != nil {
		t.Fatalf("ListSchedules paused=false failed: %v", err)
	}
	if activeResult.Total != 3 {
		t.Fatalf("expected 3 active schedules, got %d", activeResult.Total)
	}

	allResult, err := uc.List(schedules.ListFilter{Namespace: "default"})
	if err != nil {
		t.Fatalf("ListSchedules all failed: %v", err)
	}
	if allResult.Total != 5 {
		t.Fatalf("expected 5 total schedules, got %d", allResult.Total)
	}
}

func TestListSchedules_FilterByWorkflowType(t *testing.T) {
	// Happy Path: 按 workflow_type 过滤
	uc := newTestUseCase(newFakeFirer(memory.NewScheduleRepository()))

	for i := range 2 {
		_, err := uc.Create(schedules.CreateRequest{
			WorkflowType:   "wf-target",
			CronExpression: fmt.Sprintf("%d 0 * * *", i),
		})
		if err != nil {
			t.Fatalf("CreateSchedule wf-target failed: %v", err)
		}
	}
	_, err := uc.Create(schedules.CreateRequest{
		WorkflowType:   "wf-other",
		CronExpression: "0 0 * * *",
	})
	if err != nil {
		t.Fatalf("CreateSchedule wf-other failed: %v", err)
	}

	result, err := uc.List(schedules.ListFilter{
		Namespace:    "default",
		WorkflowType: "wf-target",
	})
	if err != nil {
		t.Fatalf("ListSchedules failed: %v", err)
	}
	if result.Total != 2 {
		t.Fatalf("expected 2 schedules for wf-target, got %d", result.Total)
	}
	for _, item := range result.Items {
		if item.WorkflowType != "wf-target" {
			t.Fatalf("expected all items to have workflow_type=wf-target, got %s", item.WorkflowType)
		}
	}
}

func TestListSchedules_Pagination(t *testing.T) {
	// Happy Path: 创建多个 schedule，验证 offset/limit 分页正确
	uc := newTestUseCase(newFakeFirer(memory.NewScheduleRepository()))

	for i := range 5 {
		_, err := uc.Create(schedules.CreateRequest{
			WorkflowType:   fmt.Sprintf("wf-page-%d", i),
			CronExpression: "0 0 * * *",
		})
		if err != nil {
			t.Fatalf("CreateSchedule failed: %v", err)
		}
	}

	// First page
	result, err := uc.List(schedules.ListFilter{
		Namespace: "default",
		Offset:    0,
		Limit:     2,
	})
	if err != nil {
		t.Fatalf("ListSchedules page 1 failed: %v", err)
	}
	if result.Total != 5 {
		t.Fatalf("expected total=5, got %d", result.Total)
	}
	if len(result.Items) != 2 {
		t.Fatalf("expected 2 items on first page, got %d", len(result.Items))
	}

	// Second page
	result2, err := uc.List(schedules.ListFilter{
		Namespace: "default",
		Offset:    2,
		Limit:     2,
	})
	if err != nil {
		t.Fatalf("ListSchedules page 2 failed: %v", err)
	}
	if result2.Total != 5 {
		t.Fatalf("expected total=5, got %d", result2.Total)
	}
	if len(result2.Items) != 2 {
		t.Fatalf("expected 2 items on second page, got %d", len(result2.Items))
	}

	// Last page (partial)
	result3, err := uc.List(schedules.ListFilter{
		Namespace: "default",
		Offset:    4,
		Limit:     2,
	})
	if err != nil {
		t.Fatalf("ListSchedules page 3 failed: %v", err)
	}
	if result3.Total != 5 {
		t.Fatalf("expected total=5, got %d", result3.Total)
	}
	if len(result3.Items) != 1 {
		t.Fatalf("expected 1 item on last page, got %d", len(result3.Items))
	}

	// Beyond range
	result4, err := uc.List(schedules.ListFilter{
		Namespace: "default",
		Offset:    100,
		Limit:     2,
	})
	if err != nil {
		t.Fatalf("ListSchedules beyond range failed: %v", err)
	}
	if result4.Total != 5 {
		t.Fatalf("expected total=5, got %d", result4.Total)
	}
	if len(result4.Items) != 0 {
		t.Fatalf("expected 0 items beyond range, got %d", len(result4.Items))
	}
}

func TestListSchedules_DefaultLimit(t *testing.T) {
	// Edge Case: limit=0 时使用默认值 50
	uc := newTestUseCase(newFakeFirer(memory.NewScheduleRepository()))

	for i := range 3 {
		_, err := uc.Create(schedules.CreateRequest{
			WorkflowType:   fmt.Sprintf("wf-dl-%d", i),
			CronExpression: "0 0 * * *",
		})
		if err != nil {
			t.Fatalf("CreateSchedule failed: %v", err)
		}
	}

	result, err := uc.List(schedules.ListFilter{
		Namespace: "default",
		Limit:     0,
	})
	if err != nil {
		t.Fatalf("ListSchedules failed: %v", err)
	}
	if result.Total != 3 {
		t.Fatalf("expected total=3, got %d", result.Total)
	}
	if len(result.Items) != 3 {
		t.Fatalf("expected 3 items with default limit, got %d", len(result.Items))
	}
}

func TestTriggerSchedule_NotFound(t *testing.T) {
	// Error Case: 触发不存在的 schedule 应返回错误
	ctx := context.Background()
	uc := newTestUseCase(newFakeFirer(memory.NewScheduleRepository()))

	_, err := uc.Trigger(ctx, "default", "non-existent")
	if err == nil {
		t.Fatal("expected error for triggering non-existent schedule")
	}
}

func TestSchedule_NamespaceNormalization(t *testing.T) {
	// Edge Case: 空 namespace 应归一化为 "default"
	uc := newTestUseCase(newFakeFirer(memory.NewScheduleRepository()))

	created, err := uc.Create(schedules.CreateRequest{
		Namespace:      "",
		WorkflowType:   "wf-ns-norm",
		CronExpression: "0 0 * * *",
	})
	if err != nil {
		t.Fatalf("CreateSchedule failed: %v", err)
	}
	if created.Namespace != "default" {
		t.Fatalf("expected namespace=default, got %s", created.Namespace)
	}

	got, err := uc.Get("", created.ID)
	if err != nil {
		t.Fatalf("GetSchedule with empty namespace failed: %v", err)
	}
	if got.ID != created.ID {
		t.Fatalf("expected same schedule via empty namespace lookup")
	}

	got2, err := uc.Get("default", created.ID)
	if err != nil {
		t.Fatalf("GetSchedule with default namespace failed: %v", err)
	}
	if got2.ID != created.ID {
		t.Fatalf("expected same schedule via default namespace lookup")
	}
}

func TestUpdateSchedule_NotFound(t *testing.T) {
	// Error Case: 更新不存在的 schedule 应返回错误
	uc := newTestUseCase(newFakeFirer(memory.NewScheduleRepository()))

	desc := "new"
	_, err := uc.Update("default", "ghost", schedules.UpdateRequest{
		Description: &desc,
	})
	if err == nil {
		t.Fatal("expected error for updating non-existent schedule")
	}
}

func TestUpdateSchedule_MultipleFields(t *testing.T) {
	// Happy Path: 同时更新多个字段
	uc := newTestUseCase(newFakeFirer(memory.NewScheduleRepository()))

	created, err := uc.Create(schedules.CreateRequest{
		WorkflowType:   "wf-multi",
		CronExpression: "0 0 * * *",
		Description:    "original",
		OverlapPolicy:  "SKIP",
	})
	if err != nil {
		t.Fatalf("CreateSchedule failed: %v", err)
	}

	newDesc := "updated"
	newCron := "30 6 * * 1"
	newPolicy := "ALLOW_ALL"
	newInput := map[string]any{"env": "prod"}

	updated, err := uc.Update("default", created.ID, schedules.UpdateRequest{
		Description:    &newDesc,
		CronExpression: &newCron,
		OverlapPolicy:  &newPolicy,
		Input:          &newInput,
	})
	if err != nil {
		t.Fatalf("UpdateSchedule failed: %v", err)
	}
	if updated.Description != "updated" {
		t.Fatalf("expected description=updated, got %s", updated.Description)
	}
	if updated.CronExpression != "30 6 * * 1" {
		t.Fatalf("expected cron_expression='30 6 * * 1', got %s", updated.CronExpression)
	}
	if updated.OverlapPolicy != "ALLOW_ALL" {
		t.Fatalf("expected overlap_policy=ALLOW_ALL, got %s", updated.OverlapPolicy)
	}
	if updated.Input["env"] != "prod" {
		t.Fatalf("expected input env=prod, got %v", updated.Input)
	}
}

func TestListSchedules_Empty(t *testing.T) {
	// Happy Path: 无 schedule 时返回空列表
	uc := newTestUseCase(newFakeFirer(memory.NewScheduleRepository()))

	result, err := uc.List(schedules.ListFilter{Namespace: "default"})
	if err != nil {
		t.Fatalf("ListSchedules failed: %v", err)
	}
	if result.Total != 0 {
		t.Fatalf("expected total=0, got %d", result.Total)
	}
	if len(result.Items) != 0 {
		t.Fatalf("expected empty items, got %d", len(result.Items))
	}
}

func TestTrigger_RecordsRun(t *testing.T) {
	// Happy Path: 触发把 Schedule 的 namespace、workflow 与 input 交给 Firer，并记录本次 Run
	ctx := context.Background()
	firer := newFakeFirer(memory.NewScheduleRepository())
	uc := newTestUseCase(firer)
	created, err := uc.Create(schedules.CreateRequest{
		Namespace: "ns-trigger", WorkflowType: "wf-trigger", CronExpression: "0 0 * * *",
		Input: map[string]any{"source": "schedule"},
	})
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	runID, err := uc.Trigger(ctx, "ns-trigger", created.ID)
	if err != nil {
		t.Fatalf("Trigger failed: %v", err)
	}

	if runID != "run-wf-trigger" {
		t.Fatalf("expected run id from firer, got %q", runID)
	}
	if len(firer.startCalls) != 1 {
		t.Fatalf("expected 1 start call, got %d", len(firer.startCalls))
	}
	call := firer.startCalls[0]
	if call.namespace != "ns-trigger" || call.workflowType != "wf-trigger" || !strings.HasPrefix(call.workflowID, "schedule-"+created.ID+"-") || call.input["source"] != "schedule" {
		t.Fatalf("unexpected start call %+v", call)
	}
	got, err := uc.Get("ns-trigger", created.ID)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if got.RunCount != 1 || got.LastRunID != runID || got.LastRunAt == nil {
		t.Fatalf("expected last run recorded, got run_count=%d last_run_id=%q last_run_at=%v", got.RunCount, got.LastRunID, got.LastRunAt)
	}
	if got.NextRunAt == nil {
		t.Fatal("expected next_run_at to stay set after trigger")
	}
}

func TestTrigger_CASConflict(t *testing.T) {
	// Error Case: NextRunAt 的 CAS 被抢占时返回并发修改错误，且不启动 Run
	ctx := context.Background()
	store := memory.NewScheduleRepository()
	created, err := newTestUseCase(newFakeFirer(store)).Create(schedules.CreateRequest{
		WorkflowType: "wf-conflict", CronExpression: "0 0 * * *",
	})
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	firer := newFakeFirer(newCASFailStore(store))

	_, err = newTestUseCase(firer).Trigger(ctx, "default", created.ID)

	if err == nil || !strings.Contains(err.Error(), "was concurrently modified") {
		t.Fatalf("expected concurrent modification error, got %v", err)
	}
	if firer.startCallCount() != 0 {
		t.Fatalf("expected no start call after CAS conflict, got %d", firer.startCallCount())
	}
}

func TestTrigger_StartFailure(t *testing.T) {
	// Error Case: 启动 Run 失败时返回该错误，不记录 Run
	ctx := context.Background()
	firer := newFakeFirer(memory.NewScheduleRepository())
	uc := newTestUseCase(firer)
	created, err := uc.Create(schedules.CreateRequest{WorkflowType: "wf-fail", CronExpression: "0 0 * * *"})
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	firer.startErr = errors.New("workflow not registered")

	_, err = uc.Trigger(ctx, "default", created.ID)

	if !errors.Is(err, firer.startErr) {
		t.Fatalf("expected start error, got %v", err)
	}
	got, err := uc.Get("default", created.ID)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if got.RunCount != 0 || got.LastRunID != "" || got.LastRunAt != nil {
		t.Fatalf("expected no run recorded, got run_count=%d last_run_id=%q last_run_at=%v", got.RunCount, got.LastRunID, got.LastRunAt)
	}
	if got.NextRunAt == nil || !got.NextRunAt.Equal(*created.NextRunAt) {
		t.Fatalf("expected next_run_at to stay %v after a failed trigger, got %v", created.NextRunAt, got.NextRunAt)
	}
}

func TestFireDue_StartFailureKeepsDueTime(t *testing.T) {
	// Error Case: 启动 Run 失败时 NextRunAt 保持为到期点，下一轮扫描重新触发
	store := memory.NewScheduleRepository()
	firer := newFakeFirer(store)
	due := time.Now().Add(-1 * time.Minute).Truncate(time.Millisecond)
	if err := store.CreateSchedule(newTestSchedule("sch-retry", "wf-retry", "0 0 * * *", "ALLOW_ALL", &due)); err != nil {
		t.Fatal(err)
	}
	uc := newTestUseCase(firer)

	firer.startErr = errors.New("queue unavailable")
	uc.FireDue(context.Background())

	got, err := store.GetSchedule("default", "sch-retry")
	if err != nil {
		t.Fatal(err)
	}
	if got.NextRunAt == nil || !got.NextRunAt.Equal(due) {
		t.Fatalf("expected next_run_at kept at %v after a failed start, got %v", due, got.NextRunAt)
	}

	firer.mu.Lock()
	firer.startErr = nil
	firer.mu.Unlock()
	uc.FireDue(context.Background())

	got, err = store.GetSchedule("default", "sch-retry")
	if err != nil {
		t.Fatal(err)
	}
	if got.RunCount != 1 || got.NextRunAt == nil || !got.NextRunAt.After(time.Now()) {
		t.Fatalf("expected the retried fire to record the run and advance next_run_at, got run_count=%d next_run_at=%v", got.RunCount, got.NextRunAt)
	}
}

// 启动 Run 时 NextRunAt 仍是这次到期点：启动之后、推进之前进程退出时这次触发不会丢失。
func TestFireDue_StartsBeforeAdvancingNextRunAt(t *testing.T) {
	store := memory.NewScheduleRepository()
	due := time.Now().Add(-1 * time.Minute).Truncate(time.Millisecond)
	if err := store.CreateSchedule(newTestSchedule("sch-order", "wf-order", "0 0 * * *", "ALLOW_ALL", &due)); err != nil {
		t.Fatal(err)
	}
	firer := &nextRunAtObservingFirer{fakeFirer: newFakeFirer(store), store: store, scheduleID: "sch-order"}
	schedules.NewUseCase(store, firer, systemclock.Clock{}, slog.New(slog.DiscardHandler)).FireDue(context.Background())

	if firer.observed == nil || !firer.observed.Equal(due) {
		t.Fatalf("next_run_at when the run started = %v, want the due time %v", firer.observed, due)
	}
}

// 推进 NextRunAt 失败（模拟启动后、推进前进程退出）：下一轮扫描以同一个 Workflow ID 再次启动，Run 只记录一次。
func TestFireDue_RetriesTheSameFireTimeWhenAdvanceFails(t *testing.T) {
	store := memory.NewScheduleRepository()
	due := time.Now().Add(-1 * time.Minute).Truncate(time.Millisecond)
	if err := store.CreateSchedule(newTestSchedule("sch-crash", "wf-crash", "0 0 * * *", "ALLOW_ALL", &due)); err != nil {
		t.Fatal(err)
	}
	failing := &casErrorStore{Repository: store, fail: true}
	firer := newFakeFirer(failing)
	uc := newTestUseCase(firer)

	uc.FireDue(context.Background())
	got, err := store.GetSchedule("default", "sch-crash")
	if err != nil {
		t.Fatal(err)
	}
	if got.NextRunAt == nil || !got.NextRunAt.Equal(due) || got.RunCount != 0 {
		t.Fatalf("after the failed advance: next_run_at=%v run_count=%d, want %v and 0", got.NextRunAt, got.RunCount, due)
	}

	failing.fail = false
	uc.FireDue(context.Background())
	got, err = store.GetSchedule("default", "sch-crash")
	if err != nil {
		t.Fatal(err)
	}
	if got.RunCount != 1 || got.NextRunAt == nil || !got.NextRunAt.After(time.Now()) {
		t.Fatalf("after the retry: run_count=%d next_run_at=%v, want 1 and a future time", got.RunCount, got.NextRunAt)
	}
	firer.mu.Lock()
	defer firer.mu.Unlock()
	if len(firer.startCalls) != 2 || firer.startCalls[0].workflowID != firer.startCalls[1].workflowID {
		t.Fatalf("start calls = %+v, want two with the same workflow id", firer.startCalls)
	}
}

// nextRunAtObservingFirer 在启动 Run 时读取仓储里的 NextRunAt。
type nextRunAtObservingFirer struct {
	*fakeFirer
	store      schedule.Repository
	scheduleID string
	observed   *time.Time
}

func (f *nextRunAtObservingFirer) StartWorkflowForCron(ctx context.Context, namespace, workflowType, workflowID string, input map[string]any, attributes map[string]string) (string, error) {
	stored, err := f.store.GetSchedule(namespace, f.scheduleID)
	if err != nil {
		return "", err
	}
	f.observed = stored.NextRunAt
	return f.fakeFirer.StartWorkflowForCron(ctx, namespace, workflowType, workflowID, input, attributes)
}

// casErrorStore 在 fail 为 true 时让 UpdateScheduleNextRunAtCAS 返回错误。
type casErrorStore struct {
	schedule.Repository
	fail bool
}

func (c *casErrorStore) UpdateScheduleNextRunAtCAS(entity *schedule.Schedule, old *time.Time) (bool, error) {
	if c.fail {
		return false, errors.New("database unavailable")
	}
	return c.Repository.UpdateScheduleNextRunAtCAS(entity, old)
}

func TestFireDue_RecordsRun(t *testing.T) {
	// Happy Path: 自动触发后记录 LastRunAt、LastRunID 与 RunCount
	store := memory.NewScheduleRepository()
	firer := newFakeFirer(store)
	past := time.Now().Add(-1 * time.Minute)
	if err := store.CreateSchedule(newTestSchedule("sch-record", "wf-record", "* * * * *", "ALLOW_ALL", &past)); err != nil {
		t.Fatal(err)
	}

	newTestUseCase(firer).FireDue(context.Background())

	got, err := store.GetSchedule("default", "sch-record")
	if err != nil {
		t.Fatal(err)
	}
	if got.RunCount != 1 || got.LastRunID != "run-wf-record" || got.LastRunAt == nil {
		t.Fatalf("expected last run recorded, got run_count=%d last_run_id=%q last_run_at=%v", got.RunCount, got.LastRunID, got.LastRunAt)
	}
}
