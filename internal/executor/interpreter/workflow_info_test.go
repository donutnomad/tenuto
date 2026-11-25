package interpreter

import (
	"testing"
	"time"

	"github.com/donutnomad/tenuto/compiler/sdkgen"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/sdk/workflow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPopulateWorkflowTaskStartedHistoryInfo_ProducesCanonicalSnapshot(t *testing.T) {
	start := history.NewWorkflowStartedEvent(map[string]any{"key": "value"}, "", "", "", "", 0)
	start.ID = 1
	start.Timestamp = time.Unix(1, 0).UTC()
	startSize, err := history.CanonicalEventSize(start)
	if err != nil {
		t.Fatal(err)
	}
	started := history.NewWorkflowTaskStartedEvent(1, "worker", "build", 0)
	started.Timestamp = time.Unix(2, 0).UTC()
	limits := history.HistoryLimits{WarningEvents: 2, WarningBytes: 1, HardEvents: 100, HardBytes: 1 << 20}
	if err := history.PopulateWorkflowTaskStartedHistoryInfo(&started, 2, history.HistoryMetrics{EventCount: 1, SizeBytes: startSize}, limits); err != nil {
		t.Fatal(err)
	}
	started.ID = 2
	startedSize, err := history.CanonicalEventSize(started)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := started.WorkflowTaskStarted
	if snapshot.HistoryEventCount != 2 || snapshot.HistorySizeBytes != startSize+startedSize {
		t.Fatalf("snapshot = %+v, canonical sizes = %d + %d", snapshot, startSize, startedSize)
	}
	wantReasons := []protocol.ContinueAsNewReason{
		history.ContinueAsNewReasonHistoryEventCount,
		history.ContinueAsNewReasonHistorySizeBytes,
	}
	if len(snapshot.ContinueAsNewReasons) != len(wantReasons) {
		t.Fatalf("reasons = %v, want %v", snapshot.ContinueAsNewReasons, wantReasons)
	}
	for i := range wantReasons {
		if snapshot.ContinueAsNewReasons[i] != wantReasons[i] {
			t.Fatalf("reasons = %v, want %v", snapshot.ContinueAsNewReasons, wantReasons)
		}
	}
}

func TestWorkflowGetInfo_WLangEndToEnd(t *testing.T) {
	const source = `package flows
import workflow "github.com/donutnomad/tenuto/sdk/workflow"
func W(ctx workflow.Context, input map[string]any) map[string]any {
	info := workflow.GetInfo(ctx)
	return map[string]any{
		"length": info.GetCurrentHistoryLength(),
		"size": info.GetCurrentHistorySize(),
		"suggested": info.GetContinueAsNewSuggested(),
	}
}`
	generated, err := sdkgen.GenerateFromSource([]byte(source), sdkgen.Options{WorkflowType: "workflow-info", FuncName: "W", Filename: "workflow_info.go"})
	if err != nil {
		t.Fatal(err)
	}
	wfConfig := generated.Workflow
	nodes := wfConfig
	start := history.NewWorkflowStartedEvent(nil, "", "", "", "", 0)
	start.ID = 1
	started := history.NewWorkflowTaskStartedEvent(1, "worker", "build", 0)
	if err := history.PopulateWorkflowTaskStartedHistoryInfo(&started, 2, history.HistoryMetrics{EventCount: 1, SizeBytes: mustEventSize(t, start)}, history.DefaultHistoryLimits()); err != nil {
		t.Fatal(err)
	}
	started.ID = 2
	commands, err := NewWorkflowExecutor(nodes, []history.Event{start, started}).Execute()
	if err != nil {
		t.Fatal(err)
	}
	completed, ok := findCompleteCmd(commands)
	if !ok {
		t.Fatalf("completion command missing: %+v", commands)
	}
	if completed.Output["length"] != int64(2) || completed.Output["size"] != started.WorkflowTaskStarted.HistorySizeBytes || completed.Output["suggested"] != false {
		t.Fatalf("GetInfo output = %+v, snapshot = %+v", completed.Output, started.WorkflowTaskStarted)
	}
}

func TestRuntimeWorkflowAPI_GetInfoReturnsDefensiveCopy(t *testing.T) {
	executor := NewWorkflowExecutor(nil, nil)
	executor.activationInfo = workflow.Info{
		CurrentHistoryLength: 12,
		CurrentHistorySize:   345,
		ContinueAsNewReasons: []workflow.ContinueAsNewReason{workflow.ContinueAsNewReasonHistoryEventCount},
	}
	api := runtimeWorkflowAPI{executor: executor}
	first := api.GetInfo(workflowContext{})
	first.ContinueAsNewReasons[0] = workflow.ContinueAsNewReasonHistorySizeBytes
	second := api.GetInfo(workflowContext{})
	if second.CurrentHistoryLength != 12 || second.CurrentHistorySize != 345 ||
		second.ContinueAsNewReasons[0] != workflow.ContinueAsNewReasonHistoryEventCount {
		t.Fatalf("GetInfo leaked mutable snapshot state: %+v", second)
	}
	copyReasons := second.GetContinueAsNewReasons()
	copyReasons[0] = workflow.ContinueAsNewReasonHistorySizeBytes
	if second.ContinueAsNewReasons[0] != workflow.ContinueAsNewReasonHistoryEventCount {
		t.Fatalf("Info getter leaked mutable reasons: %+v", second)
	}
}

func TestWorkflowInfoSnapshot_AdvancesOnlyAtActivationBoundary(t *testing.T) {
	executor := NewWorkflowExecutor(nil, nil)
	start := history.NewWorkflowStartedEvent(nil, "", "", "", "", 0)
	start.ID = 1
	started := history.NewWorkflowTaskStartedEvent(1, "worker", "build", 0)
	if err := history.PopulateWorkflowTaskStartedHistoryInfo(&started, 2, history.HistoryMetrics{EventCount: 1, SizeBytes: mustEventSize(t, start)}, history.DefaultHistoryLimits()); err != nil {
		t.Fatal(err)
	}
	started.ID = 2
	executor.applyBatch([]history.Event{start, started})
	api := &runtimeWorkflowAPI{executor: executor}
	before := api.GetInfo(workflowContext{})
	completed := history.NewWorkflowTaskCompletedEvent(1, "worker")
	completed.ID = 3
	executor.applyBatch([]history.Event{completed})
	during := api.GetInfo(workflowContext{})
	if before.CurrentHistoryLength != 2 || during.CurrentHistoryLength != before.CurrentHistoryLength || during.CurrentHistorySize != before.CurrentHistorySize {
		t.Fatalf("activation snapshot changed during commit: before=%+v during=%+v", before, during)
	}
}

// Integration: GetInfo 是该次 Workflow Task 在 WorkflowTaskStarted 处看到的快照。重放这个批时，
// 批里还带着它自己的 WorkflowTaskCompleted 与命令事件——这些事件不属于当年的快照。
// 带 Server 统计与不带统计（本地累计）的历史都必须重放出与 live 相同的值。
func TestWorkflowGetInfo_ReplaySeesSnapshotOfItsWorkflowTask(t *testing.T) {
	const source = `package flows
import (
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/app/activities"
)
func W(ctx workflow.Context, input map[string]any) map[string]any {
	length := workflow.GetInfo(ctx).GetCurrentHistoryLength()
	var out map[string]any
	_ = workflow.ExecuteActivity(ctx, activities.A, input).Get(ctx, &out)
	return map[string]any{"length": length}
}`
	nodes := compileWLangSource(t, []byte(source), "workflow-info-replay", "W", "workflow_info_replay.go")
	for _, tc := range []struct {
		name       string
		serverInfo bool
	}{
		{name: "server history totals", serverInfo: true},
		{name: "locally measured history", serverInfo: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			taskStarted := func(id, count int64) history.Event {
				event := executionSessionEvent(id, history.NewWorkflowTaskStartedEvent(0, "worker", "build", 0))
				if tc.serverInfo {
					event.WorkflowTaskStarted.HistoryEventCount = count
					event.WorkflowTaskStarted.HistorySizeBytes = count * 100
				}
				return event
			}
			history := []history.Event{
				executionSessionEvent(1, history.NewWorkflowStartedEvent(nil, "", "", "", "", 0)),
				taskStarted(2, 2),
				executionSessionEvent(3, history.NewWorkflowTaskCompletedEvent(0, "worker")),
				executionSessionEvent(4, history.NewActivityScheduledEvent(history.ActivityScheduledEvent{ActivityID: "#a1", ActivityType: "activities.A"})),
				executionSessionEvent(5, history.NewActivityCompletedEvent(history.ActivityCompletedEvent{ScheduledEventID: 4, Output: map[string]any{}})),
				taskStarted(6, 6),
			}
			commands, err := NewWorkflowExecutor(nodes, history).Execute()
			require.NoError(t, err)
			completed, ok := findCompleteCmd(commands)
			require.True(t, ok, "completion command missing: %+v", commands)
			// 第一个 Workflow Task 开始时历史里只有 2 个事件。
			assert.Equal(t, int64(2), completed.Output["length"])
		})
	}
}

func mustEventSize(t *testing.T, event history.Event) int64 {
	t.Helper()
	size, err := history.CanonicalEventSize(event)
	if err != nil {
		t.Fatal(err)
	}
	return size
}
