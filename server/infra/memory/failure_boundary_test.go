package memory

import (
	"strings"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/biz/domain/execution/executiontest"
)

func unsortedFailureDetails() protocol.Payloads {
	return protocol.Payloads{{
		Encoding: protocol.PayloadEncodingJSONPlainV1,
		Data:     []byte(`{"b":1,"a":2}`),
	}}
}

func oversizedFailureDetails() protocol.Payloads {
	payloads := make(protocol.Payloads, 0, 1025)
	for i := 0; i < 1025; i++ {
		payloads = append(payloads, protocol.Payload{
			Encoding: protocol.PayloadEncodingJSONPlainV1,
			Data:     []byte(`0`),
		})
	}
	return payloads
}

func newFailureBoundaryRun(t *testing.T, mgr *ExecutionManager, runID string) *execution.Run {
	t.Helper()
	run := &execution.Run{ID: runID, BranchToken: executiontest.BranchToken(runID), WorkflowID: "flow", Status: history.StatusRunning, CreatedAt: time.Now()}
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(run) }); err != nil {
		t.Fatalf("Create: %v", err)
	}
	return run
}

// memory 后端与 gorm 共用 run.NormalizeEventFailures：合法 details 被 canonical 化后落库。
func TestMemoryEventStoreCanonicalizesFailureDetails(t *testing.T) {
	mgr := NewExecutionManager()
	run := newFailureBoundaryRun(t, mgr, "run-canonical")
	failure := protocol.Failure{Message: "card declined", Type: "PaymentDeclined", Details: unsortedFailureDetails()}
	event := history.Event{
		Timestamp:      time.Now(),
		ActivityFailed: &history.ActivityFailedEvent{ActivityID: "step", ScheduledEventID: 1, Error: failure.Message, Failure: &failure},
	}
	if _, err := mgr.Append(run.ID, run.ID, run.ID, event); err != nil {
		t.Fatalf("Append: %v", err)
	}
	events, err := mgr.GetEvents(run.ID)
	if err != nil {
		t.Fatalf("GetEvents: %v", err)
	}
	if len(events) != 1 || events[0].ActivityFailed == nil || events[0].ActivityFailed.Failure == nil {
		t.Fatalf("失败载荷未落库: %+v", events)
	}
	if got := string(events[0].ActivityFailed.Failure.Details[0].Data); got != `{"a":2,"b":1}` {
		t.Fatalf("details 未 canonical 化: %q", got)
	}
}

// 超限载荷在写入前明确失败：事件与 run 状态都不变（不静默截断、不半写）。
func TestMemoryEventStoreRejectsOversizedFailurePayload(t *testing.T) {
	mgr := NewExecutionManager()
	run := newFailureBoundaryRun(t, mgr, "run-oversize")
	oversize := protocol.Failure{Message: "too many", Type: "Bulk", Details: oversizedFailureDetails()}
	event := history.Event{
		Timestamp:      time.Now(),
		WorkflowFailed: &history.WorkflowFailedEvent{Error: oversize.Message, Failure: &oversize},
	}
	if _, err := mgr.Append(run.ID, run.ID, run.ID, event); err == nil {
		t.Fatal("超限载荷必须失败")
	} else if !strings.Contains(err.Error(), "payload count exceeds") {
		t.Fatalf("错误信息应指出上限: %v", err)
	}
	if _, _, err := mgr.AppendIdempotent(run.ID, run.ID, run.ID, event); err == nil {
		t.Fatal("幂等路径也必须失败")
	}
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error {
		_, _, err := tx.AppendIdempotent(run.ID, run.ID, run.ID, event)
		return err
	}); err == nil {
		t.Fatal("事务路径也必须失败")
	}

	events, err := mgr.GetEvents(run.ID)
	if err != nil {
		t.Fatalf("GetEvents: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("超限载荷不应留下事件: %+v", events)
	}
	current, err := mgr.Get(run.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if current.Status != history.StatusRunning {
		t.Fatalf("run 状态被改动: %s", current.Status)
	}
}

// 事务内超限失败必须整体回滚：同一事务里先写的状态不能提交。
func TestMemoryEventStoreTxRollsBackOnOversizedFailurePayload(t *testing.T) {
	mgr := NewExecutionManager()
	run := newFailureBoundaryRun(t, mgr, "run-tx-rollback")
	oversize := protocol.Failure{Message: "too many", Details: oversizedFailureDetails()}
	event := history.Event{
		Timestamp:      time.Now(),
		ActivityFailed: &history.ActivityFailedEvent{ActivityID: "step", Error: oversize.Message, Failure: &oversize},
	}
	err := mgr.RunInTx(func(tx execution.ExecutionTx) error {
		if err := tx.UpsertActivityState(&execution.ActivityState{
			RunID: run.ID, ScheduledEventID: 1, ActivityID: "step", Attempt: 1, Stamp: 1,
			Status: execution.ActivityStatusEnums.Scheduled, CreatedAt: time.Now(), UpdatedAt: time.Now(),
		}); err != nil {
			return err
		}
		_, _, appendErr := tx.AppendIdempotent(run.ID, run.ID, run.ID, event)
		return appendErr
	})
	if err == nil {
		t.Fatal("事务应因超限载荷失败")
	}
	states, err := mgr.ListActivityStates(run.ID)
	if err != nil {
		t.Fatalf("ListActivityStates: %v", err)
	}
	if len(states) != 0 {
		t.Fatalf("事务回滚后不应留下 activity state: %+v", states)
	}
}

// Child 失败事件同样走统一边界：载荷原样保存，Error 列镜像 Failure.Message。
func TestMemoryEventStoreChildFailureRoundTrip(t *testing.T) {
	mgr := NewExecutionManager()
	run := newFailureBoundaryRun(t, mgr, "run-legacy")
	failure := protocol.Failure{Message: "child declined"}
	event := history.NewChildWorkflowFailedEvent("child-1", "run-child", 4, failure)
	event.Timestamp = time.Now()
	if _, err := mgr.Append(run.ID, run.ID, run.ID, event); err != nil {
		t.Fatalf("Append: %v", err)
	}
	events, err := mgr.GetEvents(run.ID)
	if err != nil {
		t.Fatalf("GetEvents: %v", err)
	}
	got := events[0].ChildWorkflowFailed
	if got == nil || got.Failure == nil || got.Failure.Message != "child declined" || got.Failure.Type != "" {
		t.Fatalf("child 失败载荷 = %+v", got)
	}
	if got.Error != "child declined" {
		t.Fatalf("Error 镜像被改写: %q", got.Error)
	}
}
