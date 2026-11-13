package engine

import (
	"context"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
)

// Happy Path: GetActivityStates 返回 run 的全部 activity 当前状态（读 state 表）。
func TestWorkflowRuntimeGetActivityStates(t *testing.T) {
	srv, mgr := newStateTestServer(t)
	runID, schedID := scheduleRetryableActivity2(t, srv, mgr)
	_ = srv.HandleActivityStarted(runID, schedID, 1, 1, time.Second, 0)
	_ = srv.HandleActivityFailed(context.Background(), runID, schedID, 1, 1, protocol.Failure{Message: "boom"}) // attempt→2

	states, err := srv.View().GetActivityStates("", runID)
	if err != nil {
		t.Fatal(err)
	}
	if len(states) != 1 || states[0].Attempt != 2 || states[0].Status != execution.ActivityStatusEnums.Scheduled {
		t.Fatalf("bad states: %+v", states)
	}
}
