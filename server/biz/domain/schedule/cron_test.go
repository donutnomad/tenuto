package schedule_test

import (
	"testing"
	"time"

	"github.com/donutnomad/tenuto/server/biz/domain/schedule"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateCronExpression(t *testing.T) {
	tests := []struct {
		name    string
		expr    string
		wantErr bool
	}{
		{name: "Happy Path: every minute", expr: "* * * * *"},
		{name: "Happy Path: step", expr: "*/5 * * * *"},
		{name: "Happy Path: range with step and list", expr: "0-30/10 9,17 * * 1-5"},
		{name: "Error Case: empty", expr: "", wantErr: true},
		{name: "Error Case: too few fields", expr: "* * *", wantErr: true},
		{name: "Error Case: too many fields", expr: "* * * * * *", wantErr: true},
		{name: "Error Case: minute out of bounds", expr: "99 * * * *", wantErr: true},
		{name: "Error Case: hour out of bounds", expr: "0 25 * * *", wantErr: true},
		{name: "Error Case: day out of bounds", expr: "0 0 32 * *", wantErr: true},
		{name: "Error Case: month out of bounds", expr: "0 0 * 13 *", wantErr: true},
		{name: "Error Case: weekday out of bounds", expr: "0 0 * * 8", wantErr: true},
		{name: "Error Case: non-numeric", expr: "abc * * * *", wantErr: true},
		{name: "Error Case: zero step", expr: "*/0 * * * *", wantErr: true},
		{name: "Error Case: reversed range", expr: "30-10 * * * *", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := schedule.ValidateCronExpression(tt.expr)
			assert.Equal(t, tt.wantErr, err != nil, "err = %v", err)
		})
	}
}

func TestCronNextRunAt(t *testing.T) {
	shanghai, err := time.LoadLocation("Asia/Shanghai")
	require.NoError(t, err)
	after := time.Date(2026, 3, 10, 10, 7, 30, 0, time.UTC) // 周二
	tests := []struct {
		name string
		expr string
		loc  *time.Location
		want time.Time
	}{
		{name: "Happy Path: next minute boundary", expr: "* * * * *", want: time.Date(2026, 3, 10, 10, 8, 0, 0, time.UTC)},
		{name: "Happy Path: step aligns to multiple", expr: "*/5 * * * *", want: time.Date(2026, 3, 10, 10, 10, 0, 0, time.UTC)},
		{name: "Happy Path: daily rolls to next day", expr: "30 2 * * *", want: time.Date(2026, 3, 11, 2, 30, 0, 0, time.UTC)},
		{name: "Happy Path: weekday filter", expr: "0 9 * * 1", want: time.Date(2026, 3, 16, 9, 0, 0, 0, time.UTC)},
		{name: "Happy Path: month rollover", expr: "0 0 1 * *", want: time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)},
		// 日与星期都受限时满足其一即触发：13 日（周五）先于下一个周一。
		{name: "Edge Case: restricted day-of-month or day-of-week, day-of-month first", expr: "0 0 13 * 1", want: time.Date(2026, 3, 13, 0, 0, 0, 0, time.UTC)},
		{name: "Edge Case: restricted day-of-month or day-of-week, day-of-week first", expr: "0 0 20 * 1", want: time.Date(2026, 3, 16, 0, 0, 0, 0, time.UTC)},
		// 日字段以 * 开头时不算受限：两个字段同时满足才触发（23 日是奇数日且为周一）。
		{name: "Edge Case: star-step day-of-month and day-of-week", expr: "0 0 */2 * 1", want: time.Date(2026, 3, 23, 0, 0, 0, 0, time.UTC)},
		// 上海 0 点是 UTC 前一日 16 点；结果以 UTC 返回。
		{name: "Edge Case: timezone evaluates in location", expr: "0 0 * * *", loc: shanghai, want: time.Date(2026, 3, 10, 16, 0, 0, 0, time.UTC)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := schedule.CronNextRunAt(tt.expr, after, tt.loc)
			require.NotNil(t, got)
			assert.True(t, tt.want.Equal(*got), "want %v, got %v", tt.want, *got)
			assert.Equal(t, time.UTC, got.Location())
		})
	}
}

func TestCronNextRunAt_Unsatisfiable(t *testing.T) {
	after := time.Date(2026, 3, 10, 10, 0, 0, 0, time.UTC)

	// Error Case: 表达式无效时返回 nil。
	assert.Nil(t, schedule.CronNextRunAt("not a cron", after, nil))
	// Edge Case: 2 月 30 日永不出现，四年内找不到匹配时返回 nil。
	assert.Nil(t, schedule.CronNextRunAt("0 0 30 2 *", after, nil))
}
