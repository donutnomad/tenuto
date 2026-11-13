package execution_test

import (
	"reflect"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
)

func TestConfiguredTimeout(t *testing.T) {
	for _, test := range []struct {
		name  string
		value string
		want  time.Duration
	}{
		{name: "Happy Path: a positive duration", value: "90s", want: 90 * time.Second},
		{name: "Edge Case: not declared", value: "", want: 0},
		{name: "Edge Case: zero", value: "0s", want: 0},
		{name: "Edge Case: negative", value: "-1m", want: 0},
		{name: "Error Case: not a duration", value: "soon", want: 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := execution.ConfiguredTimeout(test.value); got != test.want {
				t.Fatalf("ConfiguredTimeout(%q) = %v, want %v", test.value, got, test.want)
			}
			if got := execution.ExecutionTimeoutMs(test.value); got != test.want.Milliseconds() {
				t.Fatalf("ExecutionTimeoutMs(%q) = %d, want %d", test.value, got, test.want.Milliseconds())
			}
		})
	}
}

func TestWorkflowTaskStartToCloseTimeout(t *testing.T) {
	for _, test := range []struct {
		name     string
		declared time.Duration
		want     time.Duration
	}{
		{name: "Happy Path: the worker's declared timeout", declared: 3 * time.Second, want: 3 * time.Second},
		{name: "Edge Case: not declared", declared: 0, want: protocol.DefaultWorkflowTaskTimeout},
		{name: "Edge Case: negative", declared: -time.Second, want: protocol.DefaultWorkflowTaskTimeout},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := execution.WorkflowTaskStartToCloseTimeout(test.declared); got != test.want {
				t.Fatalf("WorkflowTaskStartToCloseTimeout(%v) = %v, want %v", test.declared, got, test.want)
			}
		})
	}
}

// NewRetryBackoffTimer 的身份是 (重试 Run, now 的纳秒时间戳)，在 now 之后 backoff 到期。
func TestNewRetryBackoffTimer(t *testing.T) {
	now := time.Date(2026, time.November, 11, 9, 0, 0, 123, time.UTC)
	trace := map[string]string{"traceparent": "00-abc"}
	for _, test := range []struct {
		name    string
		backoff time.Duration
	}{
		{name: "Happy Path: a positive backoff", backoff: 5 * time.Second},
		{name: "Edge Case: no backoff fires at once", backoff: 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			got := execution.NewRetryBackoffTimer("retry-run", test.backoff, now, trace)
			want := &execution.TimerState{
				RunID: "retry-run", ScheduledEventID: now.UnixNano(),
				Kind: execution.TimerKindEnums.RetryBackoff, Deadline: now.Add(test.backoff),
				Status: execution.TimerStatusEnums.Scheduled, TraceContext: trace,
				CreatedAt: now, UpdatedAt: now,
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("NewRetryBackoffTimer() = %+v, want %+v", got, want)
			}
		})
	}
}
