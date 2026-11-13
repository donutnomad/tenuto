package main

import (
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol/history"
)

func TestSummarizePercentiles(t *testing.T) {
	var values []time.Duration
	for i := 100; i >= 1; i-- {
		values = append(values, time.Duration(i)*time.Millisecond)
	}
	stats := summarize(values)
	if stats.Count != 100 || stats.P50 != 50 || stats.P95 != 95 || stats.P99 != 99 || stats.Max != 100 || stats.Mean != 50.5 {
		t.Fatalf("stats = %+v", stats)
	}
	if one := summarize([]time.Duration{7 * time.Millisecond}); one.P50 != 7 || one.P99 != 7 || one.Max != 7 {
		t.Fatalf("single value = %+v", one)
	}
	if empty := summarize(nil); empty != (latencyStats{}) {
		t.Fatalf("empty = %+v", empty)
	}
}

func TestParsePS(t *testing.T) {
	cases := []struct {
		line string
		cpu  float64
		rss  int64
	}{
		{"   0:01.50  20480\n", 1.5, 20480},                    // macOS
		{"00:01:02 1024", 62, 1024},                            // Linux
		{"2-03:04:05 4096", 2*86400 + 3*3600 + 4*60 + 5, 4096}, // Linux, days
		{"12:34.56 1", 12*60 + 34.56, 1},
	}
	for _, tc := range cases {
		cpu, rss, err := parsePS(tc.line)
		if err != nil || cpu != tc.cpu || rss != tc.rss {
			t.Errorf("parsePS(%q) = %v, %d, %v; want %v, %d", tc.line, cpu, rss, err, tc.cpu, tc.rss)
		}
	}
	for _, line := range []string{"", "1:00", "x:y 1", "1:00 x"} {
		if _, _, err := parsePS(line); err == nil {
			t.Errorf("parsePS(%q) accepted", line)
		}
	}
}

func TestHistoryTimings(t *testing.T) {
	base := time.Date(2026, 11, 12, 10, 0, 0, 0, time.UTC)
	at := func(ms int) time.Time { return base.Add(time.Duration(ms) * time.Millisecond) }
	events := []history.Event{
		{ID: 1, Timestamp: at(0), WorkflowStarted: &history.WorkflowStartedEvent{}},
		{ID: 2, Timestamp: at(0), WorkflowTaskScheduled: &history.WorkflowTaskScheduledEvent{}},
		{ID: 3, Timestamp: at(4), WorkflowTaskStarted: &history.WorkflowTaskStartedEvent{ScheduledEventID: 2}},
		{ID: 4, Timestamp: at(6), WorkflowTaskCompleted: &history.WorkflowTaskCompletedEvent{ScheduledEventID: 2}},
		{ID: 5, Timestamp: at(6), ActivityScheduled: &history.ActivityScheduledEvent{ActivityID: "#a1"}},
		{ID: 6, Timestamp: at(9), ActivityStarted: &history.ActivityStartedEvent{ScheduledEventID: 5, Attempt: 1}},
		{ID: 7, Timestamp: at(20), ActivityAttemptFailed: &history.ActivityAttemptFailedEvent{ScheduledEventID: 5, Attempt: 1}},
		{ID: 8, Timestamp: at(1030), ActivityStarted: &history.ActivityStartedEvent{ScheduledEventID: 5, Attempt: 2}},
		{ID: 9, Timestamp: at(1031), ActivityCompleted: &history.ActivityCompletedEvent{ScheduledEventID: 5, Attempt: 2}},
		{ID: 10, Timestamp: at(1031), WorkflowTaskScheduled: &history.WorkflowTaskScheduledEvent{}},
		{ID: 11, Timestamp: at(1033), WorkflowTaskStarted: &history.WorkflowTaskStartedEvent{ScheduledEventID: 10}},
		{ID: 12, Timestamp: at(1035), WorkflowTaskCompleted: &history.WorkflowTaskCompletedEvent{ScheduledEventID: 10}},
		{ID: 13, Timestamp: at(1035), WorkflowCompleted: &history.WorkflowCompletedEvent{}},
	}
	timing := historyTimings(events)
	if timing.completion != 1035*time.Millisecond {
		t.Fatalf("completion = %s", timing.completion)
	}
	if len(timing.workflowTasks) != 2 || timing.workflowTasks[0] != 4*time.Millisecond || timing.workflowTasks[1] != 2*time.Millisecond {
		t.Fatalf("workflow tasks = %v", timing.workflowTasks)
	}
	// 只计第一次 attempt 的调度→开始；重试的退避不算调度延迟。
	if len(timing.activities) != 1 || timing.activities[0] != 3*time.Millisecond {
		t.Fatalf("activities = %v", timing.activities)
	}
}

func TestMySQLDelta(t *testing.T) {
	before := map[string]float64{"Questions": 100, "Com_insert": 10}
	after := map[string]float64{"Questions": 1100, "Com_insert": 210}
	report := mysqlDelta(before, after, 10*time.Second, 50, 42)
	if q := report.Counters["Questions"]; q.Delta != 1000 || q.PerSecond != 100 || q.PerWorkflow != 20 {
		t.Fatalf("Questions = %+v", q)
	}
	if report.Counters["Com_insert"].PerWorkflow != 4 || report.ThreadsConnectedMax != 42 {
		t.Fatalf("report = %+v", report)
	}
}
