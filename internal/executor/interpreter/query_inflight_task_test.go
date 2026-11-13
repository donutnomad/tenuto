package interpreter

import (
	"strings"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol/history"
)

// ============================================
// Workflow Task 在途时的 Query
// ============================================
// 测试策略:
// - Error Case（复现）: Sleep 之后紧跟 SideEffect / RandomUUID / GetVersion 的 workflow，在 TimerFired 已落库、
//   下一个 Workflow Task 尚未完成时被 query：重放停在最后一个已完成的 Workflow Task，query 读到那时的状态
// - Happy Path: 该 Workflow Task 完成（marker 落库）后 query 读到推进后的状态
// - Edge Case: 还没有任何 Workflow Task 完成时 query 被拒，错误说明原因
// - Replay:   main 返回即结束的历史（协程仍阻塞）重放后 query 可读
// ============================================

const queryInflightSrc = `package flows
import (
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
)
func W(ctx workflow.Context, input map[string]any) error {
	stage := "sleeping"
	workflow.SetQueryHandler(ctx, "stage", func(ctx workflow.Context, in map[string]any) map[string]any {
		return map[string]any{"stage": stage}
	})
	workflow.Sleep(ctx, 3600000000000)
	var cfg map[string]any
	if err := workflow.SideEffect(ctx, func(ctx workflow.Context) any { return map[string]any{"region": "sg"} }).Get(ctx, &cfg); err != nil {
		return err
	}
	id := workflow.RandomUUID(ctx)
	version := workflow.GetVersion(ctx, "change-a", 2, 3)
	if id != "" && version >= 2 {
		stage = "woke"
	}
	return workflow.WaitSignal(ctx, "stop").Get(ctx, nil)
}
`

// queryInflightHistory 是「首个 Workflow Task 已完成并调度了 timer，timer 已触发」的历史。
func queryInflightHistory() []history.Event {
	started := history.NewWorkflowStartedEvent(map[string]any{}, "", "", "", "", 0)
	started.ID = 1
	return []history.Event{
		started,
		wftCompletedEvent(2),
		{ID: 3, Timestamp: time.Now(), TimerScheduled: &history.TimerScheduledEvent{TimerID: "#t1", DelayInput: map[string]any{"delay_ns": int64(time.Hour)}}},
		{ID: 4, Timestamp: time.Now(), TimerFired: &history.TimerFiredEvent{TimerID: "#t1", ScheduledEventID: 3}},
	}
}

// Error Case（复现）: TimerFired 之后、下一个 Workflow Task 完成之前 query → 不报 non-deterministic，
// 读到最后一个已完成 Workflow Task 时的状态。
func TestQueryWhileWorkflowTaskInFlight_ReadsCommittedState(t *testing.T) {
	events := append(queryInflightHistory(),
		history.Event{ID: 5, Timestamp: time.Now(), WorkflowTaskScheduled: &history.WorkflowTaskScheduledEvent{}},
		history.Event{ID: 6, Timestamp: time.Now(), WorkflowTaskStarted: &history.WorkflowTaskStartedEvent{ScheduledEventID: 5}},
	)
	got := queryViaReplay(t, queryInflightSrc, events, "stage", map[string]any{})
	if got.replayErr != nil || got.rejected {
		t.Fatalf("query during an in-flight workflow task must succeed, got err=%v rejected=%v msg=%q", got.replayErr, got.rejected, got.errMsg)
	}
	if got.result["stage"] != "sleeping" {
		t.Fatalf("query must read the state as of the last completed workflow task, got %v", got.result["stage"])
	}
}

// Happy Path: 该 Workflow Task 完成、marker 落库后，query 读到推进后的状态。
func TestQueryAfterWorkflowTaskCompleted_ReadsAdvancedState(t *testing.T) {
	events := append(queryInflightHistory(),
		wftCompletedEvent(5),
		history.Event{ID: 6, Timestamp: time.Now(), MarkerRecorded: &history.MarkerRecordedEvent{MarkerName: "SideEffect", MarkerID: "#se1", Value: map[string]any{"value": map[string]any{"region": "sg"}}}},
		history.Event{ID: 7, Timestamp: time.Now(), MarkerRecorded: &history.MarkerRecordedEvent{MarkerName: "RandomUUID", MarkerID: "#ru1", Value: map[string]any{"value": "5b1f6f0e-0000-4000-8000-000000000001"}}},
		history.Event{ID: 8, Timestamp: time.Now(), MarkerRecorded: &history.MarkerRecordedEvent{MarkerName: "GetVersion", MarkerID: "change-a", Value: map[string]any{"version": int64(3)}}},
	)
	got := queryViaReplay(t, queryInflightSrc, events, "stage", map[string]any{})
	if got.replayErr != nil || got.rejected {
		t.Fatalf("query must succeed, got err=%v rejected=%v msg=%q", got.replayErr, got.rejected, got.errMsg)
	}
	if got.result["stage"] != "woke" {
		t.Fatalf("query must read the advanced state, got %v", got.result["stage"])
	}
}

// Edge Case: 还没有任何 Workflow Task 完成 → query 被拒，错误说明原因。
func TestQueryBeforeFirstWorkflowTaskCompleted_Rejected(t *testing.T) {
	started := history.NewWorkflowStartedEvent(map[string]any{}, "", "", "", "", 0)
	nodes := compileWLangSource(t, []byte(queryInflightSrc), "probe-query-first", "W", "w.go")
	_, rejected, message := QueryEvaluator{}.EvaluateQuery(t.Context(), nodes, []history.Event{started}, "stage", nil)
	if !rejected || message != queryBeforeFirstWorkflowTaskMessage {
		t.Fatalf("rejected=%v message=%q, want the first-workflow-task rejection", rejected, message)
	}
}

// Replay: main 返回即完成的历史（协程仍阻塞在 signal 上）重放后 query 可读，Execute 不再产命令。
func TestMainReturn_CompletedHistoryReplaysForQuery(t *testing.T) {
	const src = `package flows
import (
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
)
func W(ctx workflow.Context, input map[string]any) (map[string]any, error) {
	stage := "started"
	workflow.SetQueryHandler(ctx, "stage", func(ctx workflow.Context, in map[string]any) map[string]any {
		return map[string]any{"stage": stage}
	})
	workflow.Go(ctx, func(ctx workflow.Context) {
		workflow.WaitSignal(ctx, "never").Get(ctx, nil)
		stage = "lane-woke"
	})
	stage = "returned"
	return map[string]any{"done": true}, nil
}
`
	events := []history.Event{
		mainReturnStarted(),
		wftCompletedEvent(2),
		{ID: 3, Timestamp: time.Now(), WorkflowCompleted: &history.WorkflowCompletedEvent{Output: map[string]any{"done": true}}},
	}
	got := queryViaReplay(t, src, events, "stage", map[string]any{})
	if got.replayErr != nil || got.rejected || got.result["stage"] != "returned" {
		t.Fatalf("query on the completed run: result=%v err=%v rejected=%v msg=%q", got.result, got.replayErr, got.rejected, got.errMsg)
	}
	cmds, err := probeExecFix(t, src, events)
	if err != nil || len(cmds) != 0 {
		t.Fatalf("a completed run must not produce commands, got %+v err=%v", cmds, err)
	}
}

// Error Case: query handler 里调用阻塞原语或 workflow.Go 时 query 被拒，求值不在调用方 goroutine 上
// Goexit，也不留下协程。
func TestQueryHandlerBlockingCallIsRejected(t *testing.T) {
	for name, call := range map[string]string{
		"await": "workflow.Await(ctx, func() bool { return false })",
		"go":    "workflow.Go(ctx, func(ctx workflow.Context) {})",
	} {
		t.Run(name, func(t *testing.T) {
			src := `package flows
import (
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
)
func W(ctx workflow.Context, input map[string]any) error {
	workflow.SetQueryHandler(ctx, "wait", func(ctx workflow.Context, in map[string]any) map[string]any {
		` + call + `
		return map[string]any{"done": true}
	})
	return workflow.WaitSignal(ctx, "stop").Get(ctx, nil)
}
`
			nodes := compileWLangSource(t, []byte(src), "probe-query-blocking", "W", "w.go")
			started := history.NewWorkflowStartedEvent(map[string]any{}, "", "", "", "", 0)
			started.ID = 1
			type outcome struct {
				rejected bool
				message  string
			}
			done := make(chan outcome, 1)
			go func() {
				_, rejected, message := QueryEvaluator{}.EvaluateQuery(t.Context(), nodes, []history.Event{started, wftCompletedEvent(2)}, "wait", nil)
				done <- outcome{rejected: rejected, message: message}
			}()
			select {
			case got := <-done:
				if !got.rejected || !strings.Contains(got.message, "query handler") {
					t.Fatalf("rejected=%v message=%q, want a query handler rejection", got.rejected, got.message)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("EvaluateQuery did not return: the blocking call ended the caller goroutine")
			}
		})
	}
}
