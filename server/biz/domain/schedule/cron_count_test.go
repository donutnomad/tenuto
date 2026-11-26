package schedule_test

import (
	"testing"
	"time"

	"github.com/donutnomad/tenuto/server/biz/domain/schedule"
	"github.com/stretchr/testify/require"
)

// CountBetween 按整天计数的结果与逐个 Next 的结果相同，包括夏令时切换日（23、25 小时）、半小时偏移的切换与区间两端的半天。
func TestCronCountBetweenMatchesEnumeration(t *testing.T) {
	zones := []string{"UTC", "America/New_York", "Australia/Lord_Howe", "Asia/Shanghai"}
	exprs := []string{"* * * * *", "*/7 * * * *", "30 1,2,3 * * *", "0 0 13 * 5", "15 2 * 3,10,11 0", "0 12 29 2 *"}
	ranges := []struct{ from, to time.Time }{
		{time.Date(2026, 3, 1, 5, 17, 30, 0, time.UTC), time.Date(2026, 4, 12, 9, 3, 0, 0, time.UTC)},
		{time.Date(2026, 9, 28, 0, 0, 0, 0, time.UTC), time.Date(2026, 11, 9, 0, 0, 0, 0, time.UTC)},
		{time.Date(2028, 2, 28, 23, 59, 0, 0, time.UTC), time.Date(2028, 3, 1, 0, 1, 0, 0, time.UTC)},
	}
	for _, zone := range zones {
		loc, err := time.LoadLocation(zone)
		require.NoError(t, err)
		for _, expr := range exprs {
			cron, err := schedule.ParseCron(expr, loc)
			require.NoError(t, err)
			for _, r := range ranges {
				var want int64
				for at := cron.Next(r.from); at != nil && at.Before(r.to); at = cron.Next(*at) {
					want++
				}
				require.Equal(t, want, cron.CountBetween(r.from, r.to), "%s %q (%s, %s)", zone, expr, r.from, r.to)
			}
		}
	}
}
