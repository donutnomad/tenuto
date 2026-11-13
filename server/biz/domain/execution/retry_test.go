package execution

import (
	"testing"
	"time"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
)

func TestDecideWorkflowRetryPriorityAndDeadline(t *testing.T) {
	now := time.Date(2026, time.October, 27, 10, 0, 0, 0, time.UTC)
	policy := &program.RetryPolicy{
		InitialInterval: "2s", BackoffCoefficient: 2, MaxInterval: "10s",
		MaxAttempts: 3, NonRetryableErrors: []string{"permanent"},
	}
	deadline := now.Add(3 * time.Second)
	run := &Run{Attempt: 2, DeadlineAt: &deadline}
	for _, test := range []struct {
		name    string
		message string
		attempt int
		want    string
	}{
		{name: "non retryable first", message: "permanent failure", attempt: 3, want: "NonRetryableFailure"},
		{name: "attempt cap", message: "transient", attempt: 3, want: "MaximumAttemptsReached"},
		{name: "deadline", message: "transient", attempt: 2, want: "Timeout"},
	} {
		t.Run(test.name, func(t *testing.T) {
			run.Attempt = test.attempt
			if retry, _, state := DecideWorkflowRetry(run, policy, protocol.Failure{Message: test.message}, now); retry || state != test.want {
				t.Fatalf("retry=%v state=%q, want %q", retry, state, test.want)
			}
		})
	}
	deadline = now.Add(4 * time.Second)
	run.Attempt = 2
	if retry, delay, state := DecideWorkflowRetry(run, policy, protocol.Failure{Message: "transient"}, now); !retry || delay != 4*time.Second || state != "InProgress" {
		t.Fatalf("retry=%v delay=%v state=%q", retry, delay, state)
	}
}

func TestActivityRetryDelay(t *testing.T) {
	capped := &history.RetryPolicy{InitialIntervalMs: 1000, BackoffCoefficient: 2, MaxIntervalMs: 10_000}
	for _, test := range []struct {
		name        string
		policy      *history.RetryPolicy
		nextAttempt int
		want        time.Duration
	}{
		{name: "nil policy uses the default", policy: nil, nextAttempt: 2, want: time.Second},
		{name: "first retry waits initial interval", policy: capped, nextAttempt: 2, want: time.Second},
		{name: "second retry", policy: capped, nextAttempt: 3, want: 2 * time.Second},
		{name: "fourth retry", policy: capped, nextAttempt: 5, want: 8 * time.Second},
		{name: "cap", policy: capped, nextAttempt: 6, want: 10 * time.Second},
		{name: "attempt 35", policy: capped, nextAttempt: 35, want: 10 * time.Second},
		{name: "attempt 64 overflows int64", policy: capped, nextAttempt: 64, want: 10 * time.Second},
		{name: "attempt 1000", policy: capped, nextAttempt: 1000, want: 10 * time.Second},
		{name: "attempt 5000 overflows float64", policy: capped, nextAttempt: 5000, want: 10 * time.Second},
		{name: "unnormalized attempt", policy: capped, nextAttempt: 0, want: time.Second},
		{
			name:        "missing max interval caps at 100 times the initial interval",
			policy:      &history.RetryPolicy{InitialIntervalMs: 2000, BackoffCoefficient: 2},
			nextAttempt: 1000, want: 200 * time.Second,
		},
		{
			name:        "missing initial interval defaults to 1s",
			policy:      &history.RetryPolicy{BackoffCoefficient: 2, MaxIntervalMs: 10_000},
			nextAttempt: 2, want: time.Second,
		},
		{
			name:        "missing coefficient defaults to 2",
			policy:      &history.RetryPolicy{InitialIntervalMs: 1000, MaxIntervalMs: 10_000},
			nextAttempt: 3, want: 2 * time.Second,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := activityRetryDelay(test.policy, test.nextAttempt); got != test.want {
				t.Fatalf("delay=%v, want %v", got, test.want)
			}
		})
	}
}

func TestWorkflowRetryBackoffCapsLargeAttempts(t *testing.T) {
	policy := &program.RetryPolicy{InitialInterval: "1s", BackoffCoefficient: 2, MaxInterval: "10s"}
	for attempt, want := range map[int]time.Duration{
		0: time.Second, 1: time.Second, 2: 2 * time.Second, 64: 10 * time.Second, 5000: 10 * time.Second,
	} {
		if got := WorkflowRetryBackoff(policy, attempt); got != want {
			t.Fatalf("attempt %d backoff=%v, want %v", attempt, got, want)
		}
	}
}
