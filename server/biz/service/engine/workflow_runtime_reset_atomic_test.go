package engine

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/program/programtest"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/clock/clocktest"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

var errInjectedWrite = errors.New("injected write failure")

// failingTxManager 在 fail 打开时让事务里的 SetResetRunID 失败：reset 事务的其余写入已经做完，
// RunInTx 必须整体回滚。
type failingTxManager struct {
	execution.ExecutionManager
	fail atomic.Bool
}

func (m *failingTxManager) RunInTx(fn func(execution.ExecutionTx) error) error {
	return m.ExecutionManager.RunInTx(func(tx execution.ExecutionTx) error {
		if m.fail.Load() {
			tx = failSetResetRunIDTx{ExecutionTx: tx}
		}
		return fn(tx)
	})
}

type failSetResetRunIDTx struct{ execution.ExecutionTx }

func (failSetResetRunIDTx) SetResetRunID(string, string) error { return errInjectedWrite }

// Reset 的事务失败时，被 reset 的 Run 仍在运行、仍是 current，没有新 Run；修复后同一个 reset 照常完成。
func TestResetWorkflowLeavesBaseRunningWhenTransactionFails(t *testing.T) {
	ctx := context.Background()
	mgr := &failingTxManager{ExecutionManager: memory.NewExecutionManager()}
	server := assembleServerWithClock(mgr, &ackRecordingQueue{}, newTestTaskQueue(), clocktest.NewFake(testClockStart))
	t.Cleanup(server.Stop)

	if _, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{ID: "reset-atomic", Program: programtest.Noop()}); err != nil {
		t.Fatal(err)
	}
	baseID, err := startedRunID(server.StartWorkflow(ctx, "", "reset-atomic", "reset-atomic", "", nil))
	if err != nil {
		t.Fatal(err)
	}
	lease := pollWorkflowTaskAs(t, server, "worker-a")
	taskCtx := protocol.WithWorkflowTaskToken(ctx, lease.Task.TaskToken)
	if err := server.HandleWorkflowTaskCompleted(taskCtx, baseID, lease.Task.ScheduledEventID, nil, "worker-a"); err != nil {
		t.Fatal(err)
	}
	request := execution.ResetWorkflowRequest{RunID: baseID, Reason: "bad deploy", ResetType: execution.ResetTypeEnums.LastWorkflowTask}

	mgr.fail.Store(true)
	if _, err := server.ResetWorkflow(ctx, request); !errors.Is(err, errInjectedWrite) {
		t.Fatalf("reset err = %v, want the injected failure", err)
	}
	if status, err := server.GetRunStatus(baseID); err != nil || status != history.StatusRunning {
		t.Fatalf("base status after a failed reset = %s (err=%v), want running", status, err)
	}
	events, err := mgr.GetEvents(baseID)
	if err != nil {
		t.Fatal(err)
	}
	if got := countEngineEvents(events, history.EventWorkflowTerminated); got != 0 {
		t.Fatalf("failed reset left %d WorkflowTerminated events on the base run", got)
	}
	if current, err := mgr.GetCurrentRunID(protocol.DefaultNamespace, "reset-atomic"); err != nil || current != baseID {
		t.Fatalf("current run = %q (err=%v), want the base run", current, err)
	}
	runs, err := mgr.List(execution.StoreListFilter{IncludeTerminal: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(runs.Runs) != 1 {
		t.Fatalf("runs after a failed reset = %d, want only the base run", len(runs.Runs))
	}

	mgr.fail.Store(false)
	newRunID, err := server.ResetWorkflow(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if status, _ := server.GetRunStatus(baseID); status != history.StatusTerminated {
		t.Fatalf("base status after reset = %s, want terminated", status)
	}
	if status, _ := server.GetRunStatus(newRunID); status != history.StatusRunning {
		t.Fatalf("reset run status = %s, want running", status)
	}
	if current, _ := mgr.GetCurrentRunID(protocol.DefaultNamespace, "reset-atomic"); current != newRunID {
		t.Fatalf("current run = %q, want the reset run %q", current, newRunID)
	}
}
