package execution_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/server/biz/domain/definition"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
)

func TestClassifyWorkflowTaskPrepareError(t *testing.T) {
	for _, test := range []struct {
		name string
		err  error
		want execution.WorkflowTaskPrepareFailure
	}{
		{name: "Happy Path: a missing Program version pauses the run", err: fmt.Errorf("%w: default/orders@v9", definition.ErrWorkflowConfigNotFound), want: execution.PrepareFailurePauseRun},
		{name: "Happy Path: a corrupt branch token pauses the run", err: fmt.Errorf("run r1: %w", execution.ErrInvalidBranchToken), want: execution.PrepareFailurePauseRun},
		{name: "Edge Case: a deleted run drops the task", err: fmt.Errorf("get run: %w", execution.ErrRunNotFound), want: execution.PrepareFailureDropTask},
		{name: "Edge Case: an incapable worker leaves the task to another worker", err: &program.UnsupportedWLangCapabilityError{Capability: "wlang.v9"}, want: execution.PrepareFailureRetry},
		{name: "Error Case: a storage error is retried", err: errors.New("dial tcp: connection refused"), want: execution.PrepareFailureRetry},
		{name: "Error Case: a timeout is retried", err: context.DeadlineExceeded, want: execution.PrepareFailureRetry},
		{name: "Error Case: a concurrent update is retried", err: execution.ErrCurrentRunConflict, want: execution.PrepareFailureRetry},
		{name: "Error Case: an unknown invalid request is retried", err: protocol.InvalidRequestf("something else"), want: execution.PrepareFailureRetry},
		{name: "Edge Case: no error", err: nil, want: execution.PrepareFailureRetry},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := execution.ClassifyWorkflowTaskPrepareError(test.err); got != test.want {
				t.Fatalf("ClassifyWorkflowTaskPrepareError(%v) = %d, want %d", test.err, got, test.want)
			}
		})
	}
}
