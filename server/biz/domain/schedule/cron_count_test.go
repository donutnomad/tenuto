package schedule_test

import (
	"math/rand"
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

// 随机区间下 CountBetween 与逐个 Next 一致，覆盖 0 点切换夏令时的时区（America/Havana、America/Santiago：当天 0 点不存在，
// time.Date 把它归一化到前一天）与整天被跳过的 Pacific/Apia 2011-12-30。
func TestCronCountBetweenMatchesNextWhenMidnightIsSkipped(t *testing.T) {
	zones := []string{"UTC", "America/New_York", "Europe/London", "Australia/Lord_Howe", "America/Santiago", "Asia/Tehran", "Pacific/Apia", "America/Havana", "Asia/Kolkata", "Pacific/Chatham"}
	exprs := []string{"0 * * * *", "*/15 * * * *", "30 2 * * *", "0 0 * * *", "15 1,2,3 * * 0", "0 12 1 * *", "* * * * *", "0 0 29 2 *"}
	rng := rand.New(rand.NewSource(7))
	base := time.Date(2011, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, zone := range zones {
		loc, err := time.LoadLocation(zone)
		require.NoError(t, err)
		for _, expr := range exprs {
			cron, err := schedule.ParseCron(expr, loc)
			require.NoError(t, err)
			for range 40 {
				from := base.Add(time.Duration(rng.Int63n(int64(15 * 365 * 24 * time.Hour)))).Truncate(time.Second)
				span := time.Duration(rng.Int63n(int64(20 * 24 * time.Hour)))
				if expr == "* * * * *" {
					span = time.Duration(rng.Int63n(int64(3 * 24 * time.Hour)))
				}
				to := from.Add(span)
				var want int64
				for at := cron.Next(from); at != nil && at.Before(to); at = cron.Next(*at) {
					want++
				}
				require.Equal(t, want, cron.CountBetween(from, to), "%s %q (%s, %s)", zone, expr, from, to)
			}
		}
	}
	// 确定的回归点：Havana 2022-03-13 与 Santiago 2024-09-08 的 0 点不存在，Apia 2011-12-30 整天不存在。
	for _, c := range []struct {
		zone, expr string
		from, to   time.Time
	}{
		{"America/Havana", "0 * * * *", time.Date(2022, 3, 11, 8, 51, 20, 0, time.UTC), time.Date(2022, 3, 20, 14, 23, 20, 0, time.UTC)},
		{"America/Santiago", "0 0 * * *", time.Date(2024, 8, 28, 10, 0, 38, 0, time.UTC), time.Date(2024, 9, 12, 10, 42, 11, 0, time.UTC)},
		{"Pacific/Apia", "30 2 * * *", time.Date(2011, 12, 10, 11, 11, 48, 0, time.UTC), time.Date(2011, 12, 30, 10, 13, 14, 0, time.UTC)},
	} {
		loc, err := time.LoadLocation(c.zone)
		require.NoError(t, err)
		cron, err := schedule.ParseCron(c.expr, loc)
		require.NoError(t, err)
		var want int64
		for at := cron.Next(c.from); at != nil && at.Before(c.to); at = cron.Next(*at) {
			want++
		}
		require.Equal(t, want, cron.CountBetween(c.from, c.to), "%s %q", c.zone, c.expr)
	}
}
