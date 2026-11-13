package persistence

import (
	"testing"

	"github.com/donutnomad/tenuto/protocol/history"
)

// WorkflowStarted 的搜索属性须区分 nil（旧 History 未记录）与空 map（启动时为空）。
func TestGormWorkflowStartedSearchAttributesRoundTrip(t *testing.T) {
	store := newTestStore(t)
	for _, tc := range []struct {
		runID string
		attrs map[string]string
	}{
		{runID: "run-legacy"},
		{runID: "run-empty", attrs: map[string]string{}},
		{runID: "run-values", attrs: map[string]string{"env": "prod"}},
	} {
		createRunWF(t, store, tc.runID, "wf-"+tc.runID, history.StatusRunning)
		started := history.NewWorkflowStartedEvent(nil, tc.runID, "", "wf-"+tc.runID, "Workflow", 0)
		started.WorkflowStarted.SearchAttributes = tc.attrs
		if _, err := store.Executions.Append(tc.runID, tc.runID, tc.runID, started); err != nil {
			t.Fatal(err)
		}
		events, err := store.Executions.GetEvents(tc.runID)
		if err != nil || len(events) != 1 || events[0].WorkflowStarted == nil {
			t.Fatalf("%s events = %+v err=%v", tc.runID, events, err)
		}
		got := events[0].WorkflowStarted.SearchAttributes
		if (got == nil) != (tc.attrs == nil) || len(got) != len(tc.attrs) || got["env"] != tc.attrs["env"] {
			t.Fatalf("%s search attributes = %#v, want %#v", tc.runID, got, tc.attrs)
		}
	}
}
