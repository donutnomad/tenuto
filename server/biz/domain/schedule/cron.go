package schedule

import (
	"strconv"
	"strings"
	"time"

	"github.com/donutnomad/tenuto/protocol"
)

// CronNextRunAt 返回 after 之后的下一次触发时刻（UTC）；表达式无效或搜索范围内没有匹配时返回 nil。
func CronNextRunAt(expr string, after time.Time, loc *time.Location) *time.Time {
	parsed, err := parseCronExpr(expr)
	if err != nil {
		return nil
	}
	t := cronNext(parsed, after, loc)
	if t.IsZero() {
		return nil
	}
	return &t
}

// cronExpr represents a parsed 5-field cron expression: minute hour dom month dow.
type cronExpr struct {
	minutes     []int // 0-59
	hours       []int // 0-23
	daysOfMonth []int // 1-31
	months      []int // 1-12
	daysOfWeek  []int // 0-6 (0=Sunday)
	// dayFieldsRestricted 表示日与星期两个字段都受限（都不以 * 开头）：按标准 cron，满足其一即匹配。
	dayFieldsRestricted bool
}

func parseCronExpr(expr string) (*cronExpr, error) {
	fields := strings.Fields(expr)
	if len(fields) != 5 {
		return nil, protocol.InvalidRequestf("cron expression must have 5 fields, got %d", len(fields))
	}
	minutes, err := parseCronField(fields[0], 0, 59)
	if err != nil {
		return nil, protocol.InvalidRequestf("minute field: %w", err)
	}
	hours, err := parseCronField(fields[1], 0, 23)
	if err != nil {
		return nil, protocol.InvalidRequestf("hour field: %w", err)
	}
	dom, err := parseCronField(fields[2], 1, 31)
	if err != nil {
		return nil, protocol.InvalidRequestf("day-of-month field: %w", err)
	}
	months, err := parseCronField(fields[3], 1, 12)
	if err != nil {
		return nil, protocol.InvalidRequestf("month field: %w", err)
	}
	dow, err := parseCronField(fields[4], 0, 6)
	if err != nil {
		return nil, protocol.InvalidRequestf("day-of-week field: %w", err)
	}
	dayFieldsRestricted := !strings.HasPrefix(fields[2], "*") && !strings.HasPrefix(fields[4], "*")
	if !dayFieldsRestricted && !dayOfMonthExists(dom, months) {
		return nil, protocol.InvalidRequestf("cron expression %q never matches: no listed month has the listed day of month", expr)
	}
	return &cronExpr{
		minutes:     minutes,
		hours:       hours,
		daysOfMonth: dom,
		months:      months,
		daysOfWeek:  dow,

		dayFieldsRestricted: dayFieldsRestricted,
	}, nil
}

// dayOfMonthExists 报告某个月份里存在某个日：2 月按 29 天计。日与星期按「同时满足」组合时，
// 日期本身存在就会在星期周期内出现。
func dayOfMonthExists(days, months []int) bool {
	maxDays := [13]int{0, 31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31}
	for _, month := range months {
		if days[0] <= maxDays[month] {
			return true
		}
	}
	return false
}

func ValidateCronExpression(expr string) error {
	_, err := parseCronExpr(expr)
	return err
}

// parseCronField parses a single cron field supporting: *, */N, N, N-M, comma-separated values.
func parseCronField(field string, min, max int) ([]int, error) {
	var result []int
	seen := make(map[int]bool)
	parts := strings.Split(field, ",")
	for _, part := range parts {
		part = strings.TrimSpace(part)
		vals, err := parseCronPart(part, min, max)
		if err != nil {
			return nil, err
		}
		for _, v := range vals {
			if !seen[v] {
				seen[v] = true
				result = append(result, v)
			}
		}
	}
	if len(result) == 0 {
		return nil, protocol.InvalidRequestf("empty field")
	}
	sortInts(result)
	return result, nil
}

func parseCronPart(part string, min, max int) ([]int, error) {
	// Handle */N
	if strings.HasPrefix(part, "*/") {
		stepStr := part[2:]
		step, err := strconv.Atoi(stepStr)
		if err != nil || step <= 0 {
			return nil, protocol.InvalidRequestf("invalid step %q", part)
		}
		var vals []int
		for i := min; i <= max; i += step {
			vals = append(vals, i)
		}
		return vals, nil
	}
	// Handle *
	if part == "*" {
		vals := make([]int, 0, max-min+1)
		for i := min; i <= max; i++ {
			vals = append(vals, i)
		}
		return vals, nil
	}
	// Handle N-M or N-M/S
	if strings.Contains(part, "-") {
		rangeParts := strings.SplitN(part, "/", 2)
		bounds := strings.SplitN(rangeParts[0], "-", 2)
		if len(bounds) != 2 {
			return nil, protocol.InvalidRequestf("invalid range %q", part)
		}
		lo, err := strconv.Atoi(strings.TrimSpace(bounds[0]))
		if err != nil {
			return nil, protocol.InvalidRequestf("invalid range start %q", bounds[0])
		}
		hi, err := strconv.Atoi(strings.TrimSpace(bounds[1]))
		if err != nil {
			return nil, protocol.InvalidRequestf("invalid range end %q", bounds[1])
		}
		if lo < min || hi > max || lo > hi {
			return nil, protocol.InvalidRequestf("range %d-%d out of bounds [%d,%d]", lo, hi, min, max)
		}
		step := 1
		if len(rangeParts) == 2 {
			step, err = strconv.Atoi(rangeParts[1])
			if err != nil || step <= 0 {
				return nil, protocol.InvalidRequestf("invalid step in range %q", part)
			}
		}
		var vals []int
		for i := lo; i <= hi; i += step {
			vals = append(vals, i)
		}
		return vals, nil
	}
	// Handle plain number
	n, err := strconv.Atoi(part)
	if err != nil {
		return nil, protocol.InvalidRequestf("invalid value %q", part)
	}
	if n < min || n > max {
		return nil, protocol.InvalidRequestf("value %d out of bounds [%d,%d]", n, min, max)
	}
	return []int{n}, nil
}

func sortInts(a []int) {
	for i := 1; i < len(a); i++ {
		key := a[i]
		j := i - 1
		for j >= 0 && a[j] > key {
			a[j+1] = a[j]
			j--
		}
		a[j+1] = key
	}
}

// cronSearchYears 是 cronNext 的搜索范围。静态校验保证日期组合存在；2 月 29 日这类与星期组合的日期
// 按 28 年的星期周期一定出现。
const cronSearchYears = 28

// cronNext 返回 after 之后第一个匹配 cron 表达式的时刻（UTC），范围内没有时返回零值。
//
// 搜索在绝对时间上前进，每一步至少前进一分钟，夏令时切换不会让它原地打转。墙上时间按 loc 计算：
// 前拨日不存在的时刻当天跳过；回拨日重复的时刻只在第一次出现时触发。
func cronNext(c *cronExpr, after time.Time, loc *time.Location) time.Time {
	if loc == nil {
		loc = time.UTC
	}
	t := after.Truncate(time.Minute).Add(time.Minute)
	deadline := after.AddDate(cronSearchYears, 0, 0)

	monthSet := intSet(c.months)
	domSet := intSet(c.daysOfMonth)
	dowSet := intSet(c.daysOfWeek)
	hourSet := intSet(c.hours)
	minuteSet := intSet(c.minutes)

	for t.Before(deadline) {
		local := t.In(loc)
		var next time.Time
		domMatch, dowMatch := domSet[local.Day()], dowSet[int(local.Weekday())]
		switch {
		case !monthSet[int(local.Month())]:
			next = nextLocalDay(t, local, time.Date(local.Year(), local.Month()+1, 1, 0, 0, 0, 0, loc))
		case !(domMatch && dowMatch || c.dayFieldsRestricted && (domMatch || dowMatch)):
			next = nextLocalDay(t, local, time.Date(local.Year(), local.Month(), local.Day()+1, 0, 0, 0, 0, loc))
		case !hourSet[local.Hour()]:
			next = t.Add(minutesToNextHour(local))
		case !minuteSet[local.Minute()] || repeatedWallClock(local):
			next = t.Add(time.Minute)
		default:
			return t.UTC()
		}
		if !next.After(t) {
			// 每一步都应严格前进；做不到时停止搜索，而不是原地打转。
			return time.Time{}
		}
		t = next
	}
	return time.Time{}
}

// nextLocalDay 返回 target（下一个本地日或下一个月的 0 点）对应的第一个时刻。0 点不存在、
// 或 time.Date 给出的时刻不在 t 之后时，退回到下一个本地整点逐小时前进。
func nextLocalDay(t, local, target time.Time) time.Time {
	target = firstOccurrence(target)
	if target.After(t) {
		return target
	}
	return t.Add(minutesToNextHour(local))
}

// minutesToNextHour 是从 local 到下一个本地整点的分钟数（至少一分钟）。半小时偏移的时区
// 也落在本地整点上。
func minutesToNextHour(local time.Time) time.Duration {
	return time.Duration(60-local.Minute()) * time.Minute
}

// repeatedWallClock 报告 t（已转换到 Schedule 时区）的墙上时间在之前已经出现过（回拨日重复区间的第二次出现）。
func repeatedWallClock(t time.Time) bool {
	return !firstOccurrence(t).Equal(t)
}

// firstOccurrence 返回与 t（已转换到 Schedule 时区）墙上时间相同的最早时刻。回拨让偏移量减小 d（30 分钟、1 小时等）时，
// 切换后 d 之内的时刻与 t-d 的墙上时间相同；3 小时之前的偏移量更大说明 3 小时内发生过回拨。
func firstOccurrence(t time.Time) time.Time {
	_, offset := t.Zone()
	_, before := t.Add(-3 * time.Hour).Zone()
	if before <= offset {
		return t
	}
	shift := time.Duration(before-offset) * time.Second
	earlier := t.Add(-shift)
	if _, earlierOffset := earlier.Zone(); earlierOffset == before {
		return earlier
	}
	return t
}

func intSet(vals []int) map[int]bool {
	m := make(map[int]bool, len(vals))
	for _, v := range vals {
		m[v] = true
	}
	return m
}
