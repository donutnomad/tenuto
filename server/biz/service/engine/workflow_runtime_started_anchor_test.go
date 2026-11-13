package engine

import (
	"context"
	"testing"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/program/programtest"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

// 验证单 run reset 接受 Started 锚点后，与传相邻 Completed 锚点行为等价：
// 新 run 的截断前缀 + reset marker 结构一致（对齐 Temporal「Started/Completed 平等对待」）。

// wftLifecycleRun：起 run 并追加一个完整 WFT（Scheduled→Started→Completed），返回 runID 及
// Started/Completed 的 event id。
//
// 真实 event id 布局（已核实，执行时不要按旧假设"对齐"）：
//
//	id1 WorkflowStarted          ← StartWorkflow 自动产生
//	id2 WorkflowTaskScheduled    ← StartWorkflow 内 scheduleWorkflowTask 自动产生
//	id3 WorkflowTaskScheduled    ← 本 helper 裸 Append（schedID=3）
//	id4 WorkflowTaskStarted      ← 本 helper 裸 Append（ScheduledEventID=3）→ startedID=4
//	id5 WorkflowTaskCompleted    ← 本 helper 裸 Append（ScheduledEventID=3）→ completedID=5
//
// reset 时该 run 为 Running，ResetWorkflow 会先 auto-terminate 追加 id6 WorkflowTerminated；
// 两个锚点（startedID=4 / completedID=5）都归一化到 Started=id4，fork 前缀为 id1..id4，
// id6 在 fork 点之后被截断。两个新 run 同构。
// 裸 mgr.Append 绕过 open-slot 去重，故能直接续接 id3/4/5（不要改用带去重的封装）。
func wftLifecycleRun(t *testing.T, ctx context.Context, svc *Server, mgr execution.ExecutionManager, wfID string) (runID string, startedID, completedID int64) {
	t.Helper()
	runID, err := startedRunID(svc.StartWorkflow(ctx, "", wfID, wfID, "", nil))
	if err != nil {
		t.Fatal(err)
	}
	schedID, err := mgr.Append(runID, runID, runID, history.NewWorkflowTaskScheduledEvent(0))
	if err != nil {
		t.Fatal(err)
	}
	startedID, err = mgr.Append(runID, runID, runID, history.NewWorkflowTaskStartedEvent(schedID, "w", "b", 0))
	if err != nil {
		t.Fatal(err)
	}
	completedID, err = mgr.Append(runID, runID, runID, history.NewWorkflowTaskCompletedEvent(schedID, ""))
	if err != nil {
		t.Fatal(err)
	}
	return runID, startedID, completedID
}

func eventTypeSeq(events []history.Event) []history.EventType {
	seq := make([]history.EventType, 0, len(events))
	for _, e := range events {
		seq = append(seq, e.EventType())
	}
	return seq
}

func TestResetWorkflow_StartedAnchor_EquivalentToAdjacentCompleted(t *testing.T) {
	// Integration: reset 传 Started(N) 与传相邻 Completed 产生结构一致的新 run history
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	server := assembleServer(mgr, newTestTaskQueue(), newTestTaskQueue())
	// 两条 run 用独立 workflowID：同一 workflowID 至多一个 current/running（对标 Temporal）。
	if _, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{ID: "wf-started-anchor-a", Program: programtest.Noop()}); err != nil {
		t.Fatal(err)
	}
	if _, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{ID: "wf-started-anchor-b", Program: programtest.Noop()}); err != nil {
		t.Fatal(err)
	}

	runA, startedID, _ := wftLifecycleRun(t, ctx, server, mgr, "wf-started-anchor-a")
	runB, _, completedID := wftLifecycleRun(t, ctx, server, mgr, "wf-started-anchor-b")

	newA, err := server.ResetWorkflow(ctx, execution.ResetWorkflowRequest{RunID: runA, Reason: "via-started", ResetEventID: startedID})
	if err != nil {
		t.Fatalf("reset via Started: %v", err)
	}
	newB, err := server.ResetWorkflow(ctx, execution.ResetWorkflowRequest{RunID: runB, Reason: "via-completed", ResetEventID: completedID})
	if err != nil {
		t.Fatalf("reset via Completed: %v", err)
	}

	eventsA, err := mgr.GetEvents(newA)
	if err != nil {
		t.Fatal(err)
	}
	eventsB, err := mgr.GetEvents(newB)
	if err != nil {
		t.Fatal(err)
	}

	seqA, seqB := eventTypeSeq(eventsA), eventTypeSeq(eventsB)

	if len(seqA) != len(seqB) {
		t.Fatalf("Started-anchor and Completed-anchor must yield same event count, got %d vs %d\nseqA=%v\nseqB=%v", len(seqA), len(seqB), seqA, seqB)
	}
	for i := range seqA {
		if seqA[i] != seqB[i] {
			t.Fatalf("event[%d] type mismatch: Started-anchor=%s Completed-anchor=%s", i, seqA[i], seqB[i])
		}
	}

	for _, tc := range []struct {
		name   string
		events []history.Event
	}{
		{"via-started (newA)", eventsA},
		{"via-completed (newB)", eventsB},
	} {
		var markers int
		for _, e := range tc.events {
			if e.WorkflowTaskFailed != nil && e.WorkflowTaskFailed.Cause == "reset_workflow" {
				markers++
			}
		}
		if markers != 1 {
			t.Fatalf("%s: expected exactly 1 reset_workflow marker, got %d", tc.name, markers)
		}
	}
}
