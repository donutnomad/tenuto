package schedule

import (
	"strconv"
	"strings"
	"time"

	"github.com/donutnomad/tenuto/protocol"
)

// CronNextRunAt 返回 after 之后的下一次触发时刻（UTC）；表达式无效或搜索范围内没有匹配时返回 nil。
func CronNextRunAt(expr string, after time.Time, loc *time.Location) *time.Time {
	cron, err := ParseCron(expr, loc)
	if err != nil {
		return nil
	}
	return cron.Next(after)
}

// Cron 是解析过的 cron 表达式与它的时区，多次计算复用同一份解析结果。
type Cron struct {
	expr *cronExpr
	loc  *time.Location
}

// ParseCron 解析 expr；loc 为 nil 时按 UTC 计算。
func ParseCron(expr string, loc *time.Location) (Cron, error) {
	parsed, err := parseCronExpr(expr)
	if err != nil {
		return Cron{}, err
	}
	if loc == nil {
		loc = time.UTC
	}
	return Cron{expr: parsed, loc: loc}, nil
}

// Next 返回 after 之后的下一次触发时刻（UTC）；搜索范围内没有匹配时返回 nil。
func (c Cron) Next(after time.Time) *time.Time {
	t := cronNext(c.expr, after, c.loc)
	if t.IsZero() {
		return nil
	}
	return &t
}

// CountBetween 返回 from 与 to 之间（两端都不含）的触发时刻数。整天落在区间内、且当天没有夏令时切换（正好 24 小时）时
// 按「日期匹配则 小时数 × 分钟数」直接计数，不逐个计算；区间两端的那两天逐个计算。代价与区间的天数成正比。
func (c Cron) CountBetween(from, to time.Time) int64 {
	var n int64
	cursor := from
	for cursor.Before(to) {
		local := cursor.In(c.loc)
		dayStart := startOfLocalDay(local.Year(), local.Month(), local.Day(), c.loc)
		dayEnd := startOfLocalDay(local.Year(), local.Month(), local.Day()+1, c.loc)
		if !dayEnd.After(cursor) {
			dayEnd = cursor.Add(time.Hour)
		}
		if dayStart.After(from) && !dayEnd.After(to) && dayEnd.Sub(dayStart) == 24*time.Hour {
			if c.expr.dayMatches(local) {
				n += int64(len(c.expr.hours) * len(c.expr.minutes))
			}
		} else {
			start := cursor
			if cursor.After(from) {
				start = cursor.Add(-time.Nanosecond) // 第一天之后从当天 0 点算起，0 点本身也计入
			}
			for t := cronNext(c.expr, start, c.loc); !t.IsZero() && t.Before(dayEnd) && t.Before(to); t = cronNext(c.expr, t, c.loc) {
				n++
			}
		}
		cursor = dayEnd
	}
	return n
}

// startOfLocalDay 返回 loc 中 year-month-day（允许溢出，按 time.Date 归一）这一天的第一个时刻。0 点因夏令时前拨不存在时，
// time.Date 可能把它归一化到前一天（如 America/Havana 的 00:00 → 前一天 23:00），这里改取切换时刻；整天被跳过时
// （Pacific/Apia 2011-12-30）取之后第一天的开始。
func startOfLocalDay(year int, month time.Month, day int, loc *time.Location) time.Time {
	want := time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
	t := firstOccurrence(time.Date(year, month, day, 0, 0, 0, 0, loc))
	for {
		y, m, d := t.In(loc).Date()
		if !time.Date(y, m, d, 0, 0, 0, 0, time.UTC).Before(want) {
			return t
		}
		_, end := t.ZoneBounds()
		if !end.After(t) {
			return t
		}
		t = end
	}
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
	// 各字段的取值集合，解析时建好，计算下一次触发时直接查。
	minuteSet [60]bool
	hourSet   [24]bool
	domSet    [32]bool
	monthSet  [13]bool
	dowSet    [7]bool
}

// dayMatches 报告 local（Schedule 时区）这一天是否满足月、日与星期三个字段。
func (c *cronExpr) dayMatches(local time.Time) bool {
	domMatch, dowMatch := c.domSet[local.Day()], c.dowSet[int(local.Weekday())]
	return c.monthSet[int(local.Month())] && (domMatch && dowMatch || c.dayFieldsRestricted && (domMatch || dowMatch))
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
	parsed := &cronExpr{
		minutes:     minutes,
		hours:       hours,
		daysOfMonth: dom,
		months:      months,
		daysOfWeek:  dow,

		dayFieldsRestricted: dayFieldsRestricted,
	}
	for _, v := range minutes {
		parsed.minuteSet[v] = true
	}
	for _, v := range hours {
		parsed.hourSet[v] = true
	}
	for _, v := range dom {
		parsed.domSet[v] = true
	}
	for _, v := range months {
		parsed.monthSet[v] = true
	}
	for _, v := range dow {
		parsed.dowSet[v] = true
	}
	return parsed, nil
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

	for t.Before(deadline) {
		local := t.In(loc)
		var next time.Time
		switch {
		case !c.monthSet[int(local.Month())]:
			next = nextLocalDay(t, local, time.Date(local.Year(), local.Month()+1, 1, 0, 0, 0, 0, loc))
		case !c.dayMatches(local):
			next = nextLocalDay(t, local, time.Date(local.Year(), local.Month(), local.Day()+1, 0, 0, 0, 0, loc))
		case !c.hourSet[local.Hour()]:
			next = t.Add(minutesToNextHour(local))
		case !c.minuteSet[local.Minute()] || repeatedWallClock(local):
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
