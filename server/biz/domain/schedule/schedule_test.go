package schedule_test

import (
	"errors"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/biz/domain/schedule"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var testNow = time.Date(2026, 3, 10, 10, 7, 30, 0, time.UTC)

func newSchedule(t *testing.T, params schedule.NewParams) *schedule.Schedule {
	t.Helper()
	if params.WorkflowType == "" {
		params.WorkflowType = "wf-1"
	}
	if params.CronExpression == "" {
		params.CronExpression = "*/5 * * * *"
	}
	entity, err := schedule.New("sch-1", params, testNow)
	require.NoError(t, err)
	return entity
}

func TestNew(t *testing.T) {
	// Happy Path: 未暂停的 Schedule 创建时按 cron 算出首个 NextRunAt，且没有待保存的 patch。
	entity, err := schedule.New("sch-1", schedule.NewParams{
		Namespace: "default", WorkflowType: "wf-1", Description: "every 5 minutes",
		CronExpression: "*/5 * * * *", Timezone: "UTC", Input: map[string]any{"k": "v"}, OverlapPolicy: "SKIP",
	}, testNow)

	require.NoError(t, err)
	assert.Equal(t, "sch-1", entity.ID)
	assert.Equal(t, "default", entity.Namespace)
	assert.Equal(t, "wf-1", entity.WorkflowType)
	assert.Equal(t, "every 5 minutes", entity.Description)
	assert.Equal(t, map[string]any{"k": "v"}, entity.Input)
	assert.Equal(t, "SKIP", entity.OverlapPolicy)
	assert.Equal(t, testNow, entity.CreatedAt)
	assert.Equal(t, testNow, entity.UpdatedAt)
	require.NotNil(t, entity.NextRunAt)
	assert.True(t, entity.NextRunAt.Equal(time.Date(2026, 3, 10, 10, 10, 0, 0, time.UTC)))
	assert.Equal(t, schedule.SchedulePatch{}, *entity.ExportPatch())
}

func TestNew_Paused(t *testing.T) {
	// Edge Case: 暂停状态创建的 Schedule 没有 NextRunAt。
	entity := newSchedule(t, schedule.NewParams{Paused: true})

	assert.True(t, entity.Paused)
	assert.Nil(t, entity.NextRunAt)
}

func TestNew_Invalid(t *testing.T) {
	tests := []struct {
		name    string
		params  schedule.NewParams
		wantErr string
	}{
		{name: "Error Case: missing workflow id", params: schedule.NewParams{CronExpression: "* * * * *"}, wantErr: "workflow_type is required"},
		{name: "Error Case: missing cron", params: schedule.NewParams{WorkflowType: "wf"}, wantErr: "cron_expression is required"},
		{name: "Error Case: invalid timezone", params: schedule.NewParams{WorkflowType: "wf", CronExpression: "* * * * *", Timezone: "Invalid/Zone"}, wantErr: `invalid timezone "Invalid/Zone"`},
		{name: "Error Case: invalid cron", params: schedule.NewParams{WorkflowType: "wf", CronExpression: "99 * * * *"}, wantErr: "invalid cron_expression"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			entity, err := schedule.New("sch-1", tt.params, testNow)

			assert.Nil(t, entity)
			assert.ErrorContains(t, err, tt.wantErr)
		})
	}
}

func TestSchedule_Update(t *testing.T) {
	later := testNow.Add(time.Hour)

	t.Run("Happy Path: description only keeps NextRunAt", func(t *testing.T) {
		entity := newSchedule(t, schedule.NewParams{})
		before := *entity.NextRunAt
		description := "new"

		require.NoError(t, entity.Update(schedule.Changes{Description: &description}, later))

		assert.Equal(t, "new", entity.Description)
		assert.True(t, entity.NextRunAt.Equal(before))
		assert.Equal(t, later, entity.UpdatedAt)
		patch := entity.ExportPatch()
		assert.True(t, patch.Description.IsPresent())
		assert.True(t, patch.UpdatedAt.IsPresent())
		assert.False(t, patch.NextRunAt.IsPresent())
	})

	t.Run("Happy Path: cron change recomputes NextRunAt from now", func(t *testing.T) {
		entity := newSchedule(t, schedule.NewParams{})
		cron := "30 2 * * *"

		require.NoError(t, entity.Update(schedule.Changes{CronExpression: &cron}, later))

		assert.Equal(t, cron, entity.CronExpression)
		assert.True(t, entity.NextRunAt.Equal(time.Date(2026, 3, 11, 2, 30, 0, 0, time.UTC)))
	})

	t.Run("Happy Path: timezone change recomputes NextRunAt in the new zone", func(t *testing.T) {
		entity := newSchedule(t, schedule.NewParams{CronExpression: "0 0 * * *"})
		timezone := "Asia/Shanghai"

		require.NoError(t, entity.Update(schedule.Changes{Timezone: &timezone}, later))

		assert.Equal(t, timezone, entity.Timezone)
		assert.True(t, entity.NextRunAt.Equal(time.Date(2026, 3, 10, 16, 0, 0, 0, time.UTC)))
	})

	t.Run("Edge Case: paused schedule keeps nil NextRunAt on cron change", func(t *testing.T) {
		entity := newSchedule(t, schedule.NewParams{Paused: true})
		cron := "30 2 * * *"

		require.NoError(t, entity.Update(schedule.Changes{CronExpression: &cron}, later))

		assert.Nil(t, entity.NextRunAt)
	})

	t.Run("Happy Path: input and overlap policy", func(t *testing.T) {
		entity := newSchedule(t, schedule.NewParams{})
		input := map[string]any{"env": "prod"}
		policy := "ALLOW_ALL"

		require.NoError(t, entity.Update(schedule.Changes{Input: &input, OverlapPolicy: &policy}, later))

		assert.Equal(t, input, entity.Input)
		assert.Equal(t, policy, entity.OverlapPolicy)
		assert.True(t, entity.ExportPatch().Input.IsPresent())
	})

	t.Run("Error Case: invalid cron", func(t *testing.T) {
		entity := newSchedule(t, schedule.NewParams{})
		cron := "bad"

		assert.ErrorContains(t, entity.Update(schedule.Changes{CronExpression: &cron}, later), "invalid cron_expression")
	})

	t.Run("Error Case: invalid timezone", func(t *testing.T) {
		entity := newSchedule(t, schedule.NewParams{})
		timezone := "Invalid/Zone"

		assert.ErrorContains(t, entity.Update(schedule.Changes{Timezone: &timezone}, later), "invalid timezone")
	})
}

func TestSchedule_PauseResume(t *testing.T) {
	// Happy Path: 暂停清空 NextRunAt，恢复后从恢复时刻重算。
	entity := newSchedule(t, schedule.NewParams{})
	pausedAt := testNow.Add(time.Minute)
	resumedAt := testNow.Add(time.Hour)

	entity.Pause(pausedAt)

	assert.True(t, entity.Paused)
	assert.Nil(t, entity.NextRunAt)
	assert.Equal(t, pausedAt, entity.UpdatedAt)

	entity.Resume(resumedAt)

	assert.False(t, entity.Paused)
	require.NotNil(t, entity.NextRunAt)
	assert.True(t, entity.NextRunAt.Equal(time.Date(2026, 3, 10, 11, 10, 0, 0, time.UTC)))
	assert.Equal(t, resumedAt, entity.UpdatedAt)
}

func TestSchedule_AdvanceNextRunAt(t *testing.T) {
	t.Run("Happy Path: advances from now and returns the previous value", func(t *testing.T) {
		entity := newSchedule(t, schedule.NewParams{})
		before := *entity.NextRunAt
		now := testNow.Add(2 * time.Hour)

		previous := entity.AdvanceNextRunAt(now)

		require.NotNil(t, previous)
		assert.True(t, previous.Equal(before))
		assert.True(t, entity.NextRunAt.Equal(time.Date(2026, 3, 10, 12, 10, 0, 0, time.UTC)))
		assert.Equal(t, now, entity.UpdatedAt)
	})

	t.Run("Edge Case: paused schedule keeps nil NextRunAt and still refreshes UpdatedAt", func(t *testing.T) {
		entity := newSchedule(t, schedule.NewParams{Paused: true})
		now := testNow.Add(2 * time.Hour)

		previous := entity.AdvanceNextRunAt(now)

		assert.Nil(t, previous)
		assert.Nil(t, entity.NextRunAt)
		assert.Equal(t, now, entity.UpdatedAt)
		assert.False(t, entity.ExportPatch().NextRunAt.IsPresent())
	})
}

func TestSchedule_MissedCatchupWindow(t *testing.T) {
	entity := newSchedule(t, schedule.NewParams{})
	due := *entity.NextRunAt

	// Edge Case: 恰好等于窗口仍补触发，超过才跳过。
	assert.False(t, entity.MissedCatchupWindow(due.Add(schedule.CatchupWindow)))
	assert.True(t, entity.MissedCatchupWindow(due.Add(schedule.CatchupWindow+time.Nanosecond)))
	// Edge Case: 没有 NextRunAt 的 Schedule 不算超窗。
	assert.False(t, newSchedule(t, schedule.NewParams{Paused: true}).MissedCatchupWindow(due.Add(48*time.Hour)))
}

func TestParseOverlapPolicy(t *testing.T) {
	tests := []struct {
		name    string
		value   string
		want    schedule.OverlapPolicy
		wantErr bool
	}{
		{name: "Happy Path: SKIP", value: "SKIP", want: schedule.OverlapPolicyEnums.Skip},
		{name: "Happy Path: ALLOW_ALL", value: "ALLOW_ALL", want: schedule.OverlapPolicyEnums.AllowAll},
		{name: "Edge Case: empty defaults to SKIP", value: "", want: schedule.OverlapPolicyEnums.Skip},
		{name: "Error Case: values are case-sensitive", value: "skip", wantErr: true},
		{name: "Error Case: unsupported Temporal policy", value: "TERMINATE_OTHER", wantErr: true},
		{name: "Error Case: unknown value", value: "cancel_other", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := schedule.ParseOverlapPolicy(tt.value)
			if tt.wantErr {
				assert.ErrorIs(t, err, protocol.ErrInvalidRequest)
				assert.ErrorContains(t, err, "invalid overlap_policy")
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestNewAndUpdate_ValidateOverlapPolicy(t *testing.T) {
	// Happy Path: 未给出 Overlap Policy 时保存 SKIP。
	assert.Equal(t, "SKIP", newSchedule(t, schedule.NewParams{}).OverlapPolicy)

	// Error Case: 未知值在创建时被拒绝。
	_, err := schedule.New("sch-1", schedule.NewParams{WorkflowType: "wf", CronExpression: "* * * * *", OverlapPolicy: "BUFFER_ONE"}, testNow)
	assert.ErrorIs(t, err, protocol.ErrInvalidRequest)

	// Error Case: 未知值在更新时被拒绝，原值不变。
	entity := newSchedule(t, schedule.NewParams{OverlapPolicy: "ALLOW_ALL"})
	unknown := "allow"
	assert.ErrorIs(t, entity.Update(schedule.Changes{OverlapPolicy: &unknown}, testNow), protocol.ErrInvalidRequest)
	assert.Equal(t, "ALLOW_ALL", entity.OverlapPolicy)

	// Happy Path: 更新为空值等于 SKIP。
	empty := ""
	require.NoError(t, entity.Update(schedule.Changes{OverlapPolicy: &empty}, testNow))
	assert.Equal(t, "SKIP", entity.OverlapPolicy)
}

func TestSchedule_AllowsStart(t *testing.T) {
	storageErr := errors.New("connection refused")
	tests := []struct {
		name        string
		policy      string
		lastRunID   string
		previous    history.Status
		previousErr error
		want        bool
	}{
		{name: "Happy Path: SKIP starts after the chain finished", policy: "SKIP", lastRunID: "run-1", previous: history.StatusCompleted, want: true},
		{name: "Happy Path: SKIP skips while the chain is running", policy: "SKIP", lastRunID: "run-1", previous: history.StatusRunning},
		{name: "Edge Case: SKIP starts when the chain ended by continue-as-new into a failed run", policy: "SKIP", lastRunID: "run-1", previous: history.StatusFailed, want: true},
		{name: "Edge Case: SKIP starts when the last run no longer exists", policy: "SKIP", lastRunID: "run-1", previousErr: execution.ErrRunNotFound, want: true},
		{name: "Error Case: SKIP skips when the chain cannot be read", policy: "SKIP", lastRunID: "run-1", previousErr: storageErr},
		{name: "Edge Case: SKIP starts the first fire", policy: "SKIP", want: true},
		{name: "Happy Path: ALLOW_ALL starts while the chain is running", policy: "ALLOW_ALL", lastRunID: "run-1", previous: history.StatusRunning, want: true},
		{name: "Edge Case: ALLOW_ALL ignores read errors", policy: "ALLOW_ALL", lastRunID: "run-1", previousErr: storageErr, want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			entity := newSchedule(t, schedule.NewParams{OverlapPolicy: tt.policy})
			entity.LastRunID = tt.lastRunID

			assert.Equal(t, tt.lastRunID != "" && tt.policy == "SKIP", entity.ChecksPreviousRun())
			assert.Equal(t, tt.want, entity.AllowsStart(tt.previous, tt.previousErr))
		})
	}
}

func TestSchedule_RunWorkflowID(t *testing.T) {
	entity := newSchedule(t, schedule.NewParams{})
	shanghai := time.FixedZone("CST", 8*3600)

	// Happy Path: Workflow ID 带上 UTC、秒级的触发时间。
	assert.Equal(t, "schedule-sch-1-2026-03-10T10:10:00Z", entity.RunWorkflowID(time.Date(2026, 3, 10, 10, 10, 0, 0, time.UTC)))
	// Edge Case: 同一时刻的不同时区与亚秒部分得到同一个 Workflow ID。
	assert.Equal(t, entity.RunWorkflowID(time.Date(2026, 3, 10, 10, 10, 0, 0, time.UTC)),
		entity.RunWorkflowID(time.Date(2026, 3, 10, 18, 10, 0, 999, shanghai)))
	// Edge Case: 不同的触发时间各有自己的 Workflow ID。
	assert.NotEqual(t, entity.RunWorkflowID(testNow), entity.RunWorkflowID(testNow.Add(time.Minute)))
}

func TestSchedule_RecordRun(t *testing.T) {
	// Happy Path: 记录触发结果只标记三个运行字段，UpdatedAt 由 Touch 单独刷新。
	entity := newSchedule(t, schedule.NewParams{})
	at := testNow.Add(time.Minute)

	entity.RecordRun("run-1", at)

	require.NotNil(t, entity.LastRunAt)
	assert.True(t, entity.LastRunAt.Equal(at))
	assert.Equal(t, "run-1", entity.LastRunID)
	assert.Equal(t, 1, entity.RunCount)
	assert.Equal(t, testNow, entity.UpdatedAt)
	patch := entity.ExportPatch()
	assert.True(t, patch.LastRunAt.IsPresent())
	assert.True(t, patch.LastRunID.IsPresent())
	assert.True(t, patch.RunCount.IsPresent())
	assert.False(t, patch.UpdatedAt.IsPresent())

	entity.Touch(at)

	assert.Equal(t, at, entity.UpdatedAt)
	assert.True(t, entity.ExportPatch().UpdatedAt.IsPresent())
}
