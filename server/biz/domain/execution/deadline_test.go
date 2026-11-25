package execution_test

import (
	"testing"
	"time"

	"github.com/donutnomad/tenuto/server/biz/domain/execution"
)

func TestApplyTimeoutsAndInheritance(t *testing.T) {
	now := time.Date(2026, time.October, 27, 10, 0, 0, 0, time.UTC)
	current := execution.NewRun(execution.NewRunOptions{ID: "run-1", WorkflowID: "flow", WorkflowType: "flow"})
	if current.ApplyTimeouts(0, 0, now) || current.DeadlineAt != nil || current.RunDeadlineAt != nil {
		t.Fatalf("undeclared timeouts set deadlines: %+v", current)
	}
	if !current.ApplyTimeouts(time.Hour, 10*time.Minute, now) {
		t.Fatal("declared timeouts did not change the run")
	}
	workflowDeadline, runDeadline := now.Add(time.Hour), now.Add(10*time.Minute)
	if current.DeadlineAt == nil || !current.DeadlineAt.Equal(workflowDeadline) || current.RunDeadlineAt == nil || !current.RunDeadlineAt.Equal(runDeadline) {
		t.Fatalf("deadlines=%v %v", current.DeadlineAt, current.RunDeadlineAt)
	}
	patch := current.ExportPatch()
	if !patch.DeadlineAt.IsPresent() || !patch.RunDeadlineAt.IsPresent() {
		t.Fatal("deadline changes are not tracked for the patch update")
	}
	if current.ApplyTimeouts(2*time.Hour, 20*time.Minute, now.Add(time.Minute)) || !current.DeadlineAt.Equal(workflowDeadline) || !current.RunDeadlineAt.Equal(runDeadline) {
		t.Fatalf("existing deadlines were replaced: %v %v", current.DeadlineAt, current.RunDeadlineAt)
	}
	next := execution.NewRetryRun(current, "run-2", now.Add(time.Minute), nil)
	if !next.ApplyTimeouts(2*time.Hour, 10*time.Minute, now.Add(time.Minute)) || !next.DeadlineAt.Equal(workflowDeadline) || !next.RunDeadlineAt.Equal(now.Add(11*time.Minute)) {
		t.Fatalf("retry run deadlines=%v %v, want the inherited execution deadline and a fresh run deadline", next.DeadlineAt, next.RunDeadlineAt)
	}
	if !execution.WorkflowDeadlineExpired(current, workflowDeadline) || !execution.RunDeadlineExpired(current, runDeadline) {
		t.Fatal("deadline expiration was not recognized")
	}
}
