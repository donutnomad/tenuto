package schedule_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/server/biz/domain/schedule"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// 回拨日上一个时刻的下一次触发：美东 2026-11-01 02:00 EDT 回拨到 01:00 EST。计算必须返回，
// 每天 09:00 的下一次是回拨日的 09:00 EST。
func TestCronNextRunAt_DailyAcrossFallBackReturns(t *testing.T) {
	newYork := loadLocation(t, "America/New_York")
	done := make(chan *time.Time, 1)
	go func() {
		done <- schedule.CronNextRunAt("0 9 * * *", time.Date(2026, 10, 31, 13, 0, 0, 0, time.UTC), newYork)
	}()
	select {
	case next := <-done:
		require.NotNil(t, next)
		assert.True(t, time.Date(2026, 11, 1, 9, 0, 0, 0, newYork).Equal(*next), "next = %v", next.In(newYork))
	case <-time.After(3 * time.Second):
		t.Fatal("CronNextRunAt did not return across the DST fall-back")
	}
}

// 回拨日重复的时刻只在第一次出现时触发；前拨日不存在的时刻当天跳过。
func TestCronNextRunAt_DSTRepeatedAndSkippedTimes(t *testing.T) {
	newYork := loadLocation(t, "America/New_York")
	lordHowe := loadLocation(t, "Australia/Lord_Howe")
	edt := time.FixedZone("EDT", -4*3600)
	est := time.FixedZone("EST", -5*3600)
	lhdt := time.FixedZone("+11", 11*3600)
	lhst := time.FixedZone("+1030", 10*3600+1800)
	tests := []struct {
		name  string
		expr  string
		loc   *time.Location
		after time.Time
		want  []time.Time
	}{
		{
			name: "New_York fall back fires 01:30 once", expr: "30 1 * * *", loc: newYork,
			after: time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC),
			want:  []time.Time{time.Date(2026, 11, 1, 1, 30, 0, 0, edt), time.Date(2026, 11, 2, 1, 30, 0, 0, est)},
		},
		{
			name: "New_York spring forward skips 02:30", expr: "30 2 * * *", loc: newYork,
			after: time.Date(2026, 3, 7, 12, 0, 0, 0, time.UTC),
			want:  []time.Time{time.Date(2026, 3, 9, 2, 30, 0, 0, edt)},
		},
		{
			name: "Lord_Howe fall back fires 01:45 once", expr: "45 1 * * *", loc: lordHowe,
			after: time.Date(2026, 4, 4, 0, 0, 0, 0, time.UTC),
			want:  []time.Time{time.Date(2026, 4, 5, 1, 45, 0, 0, lhdt), time.Date(2026, 4, 6, 1, 45, 0, 0, lhst)},
		},
		{
			name: "Lord_Howe spring forward skips 02:15 but keeps 02:45", expr: "15,45 2 * * *", loc: lordHowe,
			after: time.Date(2026, 10, 3, 0, 0, 0, 0, time.UTC),
			want:  []time.Time{time.Date(2026, 10, 4, 2, 45, 0, 0, lhdt), time.Date(2026, 10, 5, 2, 15, 0, 0, lhdt)},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			after := tt.after
			for _, want := range tt.want {
				next := schedule.CronNextRunAt(tt.expr, after, tt.loc)
				require.NotNil(t, next)
				assert.True(t, want.Equal(*next), "after %v: want %v, got %v", after.In(tt.loc), want.In(tt.loc), next.In(tt.loc))
				after = *next
			}
		})
	}
}

// 在每个时区的回拨与前拨前后逐分钟对照：CronNextRunAt 从任一时刻出发得到的下一次，等于逐分钟扫描时
// 第一个匹配且墙上时间第一次出现的时刻；连续的触发时刻严格递增。
func TestCronNextRunAt_MatchesMinuteScanAroundDSTTransitions(t *testing.T) {
	exprs := []string{"0 9 * * *", "30 1 * * *", "0 2 * * *", "45 2 * * *", "*/20 * * * *", "0 0 * * *", "30 23 * * *", "0,30 0-3 * * 0"}
	for _, zone := range []string{"America/New_York", "America/Santiago", "Europe/Berlin", "Australia/Lord_Howe"} {
		loc := loadLocation(t, zone)
		transitions := offsetTransitions(loc, 2026)
		require.Len(t, transitions, 2, zone)
		for _, transition := range transitions {
			for _, expr := range exprs {
				t.Run(fmt.Sprintf("%s %s %s", zone, transition.In(loc).Format("2006-01-02"), expr), func(t *testing.T) {
					checkAgainstMinuteScan(t, expr, loc, transition.Add(-36*time.Hour), transition.Add(36*time.Hour))
				})
			}
		}
	}
}

func checkAgainstMinuteScan(t *testing.T, expr string, loc *time.Location, from, to time.Time) {
	t.Helper()
	// 期望的触发时刻：逐分钟扫描，墙上时间按字段匹配且在扫描中第一次出现。扫描从 from 之前一天开始，
	// 让 from 附近的重复墙上时间能被认出来。
	seen := map[string]bool{}
	var fires []time.Time
	for minute := from.Add(-24 * time.Hour); minute.Before(to.Add(8 * 24 * time.Hour)); minute = minute.Add(time.Minute) {
		local := minute.In(loc)
		wall := local.Format("2006-01-02 15:04")
		first := !seen[wall]
		seen[wall] = true
		if first && !minute.Before(from) && cronMatches(t, expr, local) {
			fires = append(fires, minute)
		}
	}
	require.NotEmpty(t, fires)
	index := 0
	for at := from; at.Before(to); at = at.Add(time.Minute) {
		for !fires[index].After(at) {
			index++
		}
		next := schedule.CronNextRunAt(expr, at, loc)
		require.NotNil(t, next, "after %v", at.In(loc))
		require.True(t, fires[index].Equal(*next), "after %v: want %v, got %v", at.In(loc), fires[index].In(loc), next.In(loc))
	}
	for i := 1; i < len(fires); i++ {
		require.True(t, fires[i].After(fires[i-1]))
	}
}

// cronMatches 只解释测试用到的写法：*、*/N、N、N-M 与逗号列表；日与星期两个字段不同时受限。
func cronMatches(t *testing.T, expr string, local time.Time) bool {
	t.Helper()
	var minute, hour, dom, month, dow string
	_, err := fmt.Sscan(expr, &minute, &hour, &dom, &month, &dow)
	require.NoError(t, err)
	return fieldMatches(t, minute, local.Minute()) && fieldMatches(t, hour, local.Hour()) &&
		fieldMatches(t, dom, local.Day()) && fieldMatches(t, month, int(local.Month())) && fieldMatches(t, dow, int(local.Weekday()))
}

func fieldMatches(t *testing.T, field string, value int) bool {
	t.Helper()
	var step, lo, hi int
	switch {
	case field == "*":
		return true
	case strings.HasPrefix(field, "*/"):
		_, err := fmt.Sscanf(field, "*/%d", &step)
		require.NoError(t, err)
		return value%step == 0
	}
	for _, part := range strings.Split(field, ",") {
		if n, _ := fmt.Sscanf(part, "%d-%d", &lo, &hi); n == 2 {
			if value >= lo && value <= hi {
				return true
			}
			continue
		}
		_, err := fmt.Sscanf(part, "%d", &lo)
		require.NoError(t, err)
		if value == lo {
			return true
		}
	}
	return false
}

// offsetTransitions 返回 loc 在 year 年里偏移量变化的时刻（精确到分钟）。
func offsetTransitions(loc *time.Location, year int) []time.Time {
	var transitions []time.Time
	start := time.Date(year, 1, 1, 0, 0, 0, 0, time.UTC)
	end := start.AddDate(1, 0, 0)
	_, previous := start.In(loc).Zone()
	for at := start; at.Before(end); at = at.Add(time.Hour) {
		if _, offset := at.In(loc).Zone(); offset != previous {
			exact := at.Add(-time.Hour)
			for _, o := exact.In(loc).Zone(); o == previous; _, o = exact.In(loc).Zone() {
				exact = exact.Add(time.Minute)
			}
			transitions = append(transitions, exact)
			previous = offset
		}
	}
	return transitions
}

func loadLocation(t *testing.T, name string) *time.Location {
	t.Helper()
	loc, err := time.LoadLocation(name)
	require.NoError(t, err)
	return loc
}
