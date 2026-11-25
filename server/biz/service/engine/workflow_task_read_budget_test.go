package engine

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/program/programtest"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/definition"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

// ============================================
// Workflow Task 热路径的事务外读取次数
// ============================================
// - Happy Path: 领取一个 Workflow Task（poll → 准备 → 领取）事务外读 Run 一次、execution state 一次、Program 配置一次。
// - Happy Path: 完成一个 Workflow Task（取锁、token 校验、open 判定、选中版本、提交）事务外读 Run 一次、execution state 一次。
// 每一次事务外读取在 MySQL 上是一条 SELECT；计数回升说明有人把已读到的状态又读了一遍。
// ============================================

// readCountingManager 统计事务外的 Run、execution state 读取；事务内的读取走 ExecutionTx，不计入。
type readCountingManager struct {
	*memory.ExecutionManager
	runReads   atomic.Int64
	stateReads atomic.Int64
}

func (m *readCountingManager) Get(runID string) (*execution.Run, error) {
	m.runReads.Add(1)
	return m.ExecutionManager.Get(runID)
}

func (m *readCountingManager) GetWorkflowExecutionState(runID string) (*execution.WorkflowExecutionState, error) {
	m.stateReads.Add(1)
	return m.ExecutionManager.GetWorkflowExecutionState(runID)
}

// configReadCounter 统计 Program 配置的读取。
type configReadCounter struct {
	definition.ConfigRepository
	reads atomic.Int64
}

func (c *configReadCounter) GetWorkflowConfig(ref definition.WorkflowConfigRef) (*program.WorkflowConfig, error) {
	c.reads.Add(1)
	return c.ConfigRepository.GetWorkflowConfig(ref)
}

type readCounts struct{ runs, states, configs int64 }

func (m *readCountingManager) snapshot(configs *configReadCounter) readCounts {
	return readCounts{runs: m.runReads.Load(), states: m.stateReads.Load(), configs: configs.reads.Load()}
}

func (c readCounts) minus(other readCounts) readCounts {
	return readCounts{runs: c.runs - other.runs, states: c.states - other.states, configs: c.configs - other.configs}
}

func TestWorkflowTaskHotPathReadsRunAndStateOnce(t *testing.T) {
	mgr := &readCountingManager{ExecutionManager: memory.NewExecutionManager()}
	deps := assembleServerDeps(mgr, newTestTaskQueue(), QueueActivityRouter{Queue: newTestTaskQueue()})
	configs := &configReadCounter{ConfigRepository: deps.Configs}
	deps.Configs = configs
	server := NewServer(deps)
	t.Cleanup(server.Stop)

	if _, err := server.Definitions().RegisterWorkflowInNamespace("default", &program.WorkflowConfig{Type: "read-budget", Name: "A", Program: programtest.Noop()}); err != nil {
		t.Fatal(err)
	}
	runID, err := startedRunID(server.StartWorkflow(context.Background(), "", "read-budget", "read-budget", "", nil))
	if err != nil {
		t.Fatal(err)
	}

	before := mgr.snapshot(configs)
	lease, err := server.PollWorkflowTask(workerContext("worker-a"))
	if err != nil {
		t.Fatal(err)
	}
	if got := mgr.snapshot(configs).minus(before); got != (readCounts{runs: 1, states: 1, configs: 1}) {
		t.Fatalf("poll and start a workflow task read %+v, want one Run, one execution state and one config", got)
	}

	before = mgr.snapshot(configs)
	ctx := protocol.WithWorkflowTaskToken(context.Background(), lease.Task.TaskToken)
	complete := []history.Command{{Type: history.CmdCompleteWorkflow, Output: map[string]any{"ok": true}}}
	if err := server.HandleWorkflowTaskCompleted(ctx, runID, lease.Task.ScheduledEventID, complete, "worker-a"); err != nil {
		t.Fatal(err)
	}
	if got := mgr.snapshot(configs).minus(before); got != (readCounts{runs: 1, states: 1}) {
		t.Fatalf("complete a workflow task read %+v, want one Run and one execution state", got)
	}
	if run, err := server.mgr.Get(runID); err != nil || run.Status != history.StatusCompleted {
		t.Fatalf("run = %+v err = %v, want completed", run, err)
	}
}
