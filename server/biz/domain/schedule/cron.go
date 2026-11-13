package schedule

import (
	"strconv"
	"strings"
	"time"

	"github.com/donutnomad/tenuto/protocol"
)

// cronNextRunAt computes the next fire time after 'after' for the given 5-field cron expression.
// Returns nil if the expression is invalid.
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
	return &cronExpr{
		minutes:     minutes,
		hours:       hours,
		daysOfMonth: dom,
		months:      months,
		daysOfWeek:  dow,

		dayFieldsRestricted: !strings.HasPrefix(fields[2], "*") && !strings.HasPrefix(fields[4], "*"),
	}, nil
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

// cronNext finds the next time after 'after' that matches the cron expression.
// Searches up to 4 years to find a match.
func cronNext(c *cronExpr, after time.Time, loc *time.Location) time.Time {
	if loc == nil {
		loc = time.UTC
	}
	t := after.In(loc)
	// Start from the next minute
	t = t.Truncate(time.Minute).Add(time.Minute)

	deadline := after.Add(4 * 365 * 24 * time.Hour)

	monthSet := intSet(c.months)
	domSet := intSet(c.daysOfMonth)
	dowSet := intSet(c.daysOfWeek)
	hourSet := intSet(c.hours)
	minuteSet := intSet(c.minutes)

	for t.Before(deadline) {
		if !monthSet[int(t.Month())] {
			// Advance to next month
			t = time.Date(t.Year(), t.Month()+1, 1, 0, 0, 0, 0, loc)
			continue
		}
		domMatch, dowMatch := domSet[t.Day()], dowSet[int(t.Weekday())]
		if dayMatch := domMatch && dowMatch || c.dayFieldsRestricted && (domMatch || dowMatch); !dayMatch {
			t = t.AddDate(0, 0, 1)
			t = time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, loc)
			continue
		}
		if !hourSet[t.Hour()] {
			t = t.Add(time.Hour)
			t = time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), 0, 0, 0, loc)
			continue
		}
		if !minuteSet[t.Minute()] {
			t = t.Add(time.Minute)
			continue
		}
		return t.In(time.UTC)
	}
	return time.Time{}
}

func intSet(vals []int) map[int]bool {
	m := make(map[int]bool, len(vals))
	for _, v := range vals {
		m[v] = true
	}
	return m
}
