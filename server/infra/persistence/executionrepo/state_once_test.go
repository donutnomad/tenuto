package executionrepo

import (
	"strings"
	"testing"

	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
)

// 一个事务里追加多个事件时，execution state 只加锁读一次、写回一次：Workflow Task 完成（close 加两个命令事件）
// 是 1 条 SELECT ... FOR UPDATE、3 条事件 INSERT、1 条 state UPDATE。此前每个事件各一次加锁读与 UPDATE（共 9 条）。
func TestTransactionReadsAndWritesExecutionStateOnce(t *testing.T) {
	repo, connector := newCannedRepository(t)
	commands := []history.Event{
		history.NewTimerScheduledEvent("t-1", nil, 1000),
		history.NewTimerScheduledEvent("t-2", nil, 1000),
	}
	if err := repo.RunInTx(func(tx execution.ExecutionTx) error {
		result, err := tx.AppendWorkflowTaskCompleted("run-1", "tree-1", "branch-1",
			history.NewWorkflowTaskCompletedEvent(38, "worker"), commands)
		if err != nil {
			return err
		}
		if !result.ClosedInserted || len(result.CommandEvents) != 2 {
			t.Fatalf("result = %+v", result)
		}
		// 本事务里再读 state 得到的是已投影三个事件的那一份，不再查询。
		state, err := tx.GetWorkflowExecutionStateForUpdate("run-1")
		if err != nil {
			return err
		}
		if state.NextEventID != 43 {
			t.Fatalf("NextEventID = %d, want 43", state.NextEventID)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	var lockReads, stateUpdates, eventInserts int
	for _, statement := range connector.statements {
		switch {
		case strings.HasPrefix(statement, "SELECT") && strings.Contains(statement, "tenuto_execution_states") && strings.HasSuffix(statement, "FOR UPDATE"):
			lockReads++
		case strings.HasPrefix(statement, "UPDATE") && strings.Contains(statement, "tenuto_execution_states"):
			stateUpdates++
		case strings.HasPrefix(statement, "INSERT") && strings.Contains(statement, "tenuto_events"):
			eventInserts++
		}
	}
	if lockReads != 1 || stateUpdates != 1 || eventInserts != 3 || len(connector.statements) != 5 {
		t.Fatalf("lock reads %d, state updates %d, event inserts %d, statements %d (want 1, 1, 3, 5):\n%s",
			lockReads, stateUpdates, eventInserts, len(connector.statements), strings.Join(connector.statements, "\n"))
	}
}

// 没有改动 state 的事务不写回：sticky 分配被代数 fence 拒绝时只有一条加锁读。
func TestUnchangedExecutionStateIsNotWritten(t *testing.T) {
	repo, connector := newCannedRepository(t)
	if _, assigned, err := repo.AssignStickyExecution("run-1", 99, 99, "worker", "build", repo.clock.Now()); err != nil || assigned {
		t.Fatalf("assigned = %v, err = %v", assigned, err)
	}
	if len(connector.statements) != 1 {
		t.Fatalf("statements = %q, want only the lock read", connector.statements)
	}
}
