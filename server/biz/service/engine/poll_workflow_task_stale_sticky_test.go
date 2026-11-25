package engine_test

import (
	"context"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/program/programtest"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/server/app/assembly"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/biz/service/engine"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

// stackDepthProbeManager 包装 memory 的 ExecutionManager，在 PollWorkflowTask 每轮都会调用的
// GetWorkflowExecutionState 上采样调用栈里 PollWorkflowTask 帧的数量。
//
// 递归实现下，每丢弃一个陈旧 sticky task 就多压一层 PollWorkflowTask 帧，采样值随之线性增长；
// 循环实现下该值恒为 1。这样即可直接断言「没有栈增长」，而不只是断言最终结果正确。
type stackDepthProbeManager struct {
	*memory.ExecutionManager
	maxPollFrames int
}

func (m *stackDepthProbeManager) GetWorkflowExecutionState(runID string) (*execution.WorkflowExecutionState, error) {
	if frames := countPollWorkflowTaskFrames(); frames > m.maxPollFrames {
		m.maxPollFrames = frames
	}
	return m.ExecutionManager.GetWorkflowExecutionState(runID)
}

// countPollWorkflowTaskFrames 数当前调用栈里 PollWorkflowTask 的帧数。
func countPollWorkflowTaskFrames() int {
	buf := make([]byte, 1<<16)
	buf = buf[:runtime.Stack(buf, false)]
	return strings.Count(string(buf), ").PollWorkflowTask(")
}

// TestPollWorkflowTaskDrainsStaleStickyTasksWithoutStackGrowth 覆盖「队列里堆积多个 sticky
// generation 不匹配、所属 WFT 早已关闭的陈旧 task」场景。修复前该分支 `return s.PollWorkflowTask(ctx)` 递归自调，
// 栈深度随陈旧 task 数线性增长直至溢出；修复后改为 for 循环，栈深度恒定。
//
// 断言两件事：
//  1. 陈旧 task 被逐个确认丢弃（不 Nack 回队列、不丢失后续 task），open WFT 的投递被返回；
//  2. PollWorkflowTask 的栈帧数始终为 1，即没有发生递归。
func TestPollWorkflowTaskDrainsStaleStickyTasksWithoutStackGrowth(t *testing.T) {
	const staleTasks = 32

	base := memory.NewExecutionManager()
	mgr := &stackDepthProbeManager{ExecutionManager: base}
	workflowTasks := memory.NewTaskQueue(staleTasks + 8)
	activityTasks := memory.NewTaskQueue(16)
	server := assembly.NewServer(assembly.Options{Stores: assembly.Stores{Executions: mgr}, WorkflowQueue: workflowTasks, ActivityRouter: engine.QueueActivityRouter{Queue: activityTasks}})

	if _, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{
		Type:    "stale-sticky-flow",
		Program: programtest.Noop(),
		Activities: map[string]program.ActivityRef{
			"first": {Symbol: "first", Type: "activity.first"},
		},
	}); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	runID, err := startedRunID(server.StartWorkflow(ctx, "", "stale-sticky-flow", "stale-sticky-flow", "", nil))
	if err != nil {
		t.Fatal(err)
	}

	const workerIdentity = "sticky-worker-1"
	state, err := mgr.GetWorkflowExecutionState(runID)
	if err != nil {
		t.Fatal(err)
	}

	// 把 run 绑定到 workerIdentity，使「同一 worker + 正确 generation」的 task 可被接受。
	currentSticky, assigned, err := mgr.AssignStickyExecution(
		runID, state.ExecutionGeneration, state.StickyGeneration, workerIdentity, "build-1", timeFarFuture())
	if err != nil {
		t.Fatal(err)
	}
	if !assigned {
		t.Fatal("failed to assign sticky execution")
	}

	// StartWorkflow 已投过一个 task，先清空队列，保证后面消费的是本测试注入的 task。
	drainWorkflowQueue(t, workflowTasks)

	// 注入 staleTasks 个 sticky generation 不匹配、所属 WFT 不存在或已关闭的陈旧 task（generation 明显偏旧）。
	for i := range staleTasks {
		task := protocol.Task{
			Type:             protocol.TaskTypeWorkflow,
			RunID:            runID,
			ScheduledEventID: int64(1000 + i),
			StickyGeneration: currentSticky - 1000 - int64(i),
		}
		if err := workflowTasks.EnqueueSticky(ctx, workerIdentity, task); err != nil {
			t.Fatal(err)
		}
	}
	// 最后压入 open WFT 的一份 generation 匹配的有效 task，它应当被正确返回。
	valid := protocol.Task{
		Type:             protocol.TaskTypeWorkflow,
		RunID:            runID,
		ScheduledEventID: state.OpenWorkflowTaskEventID,
		StickyGeneration: currentSticky,
	}
	if err := workflowTasks.EnqueueSticky(ctx, workerIdentity, valid); err != nil {
		t.Fatal(err)
	}

	mgr.maxPollFrames = 0
	lease, err := server.PollWorkflowTask(protocol.WithWorkerInfo(ctx, protocol.WorkerInfo{
		Identity: workerIdentity, BuildID: "build-1",
		WorkflowCapabilities: program.CurrentWLangCapabilities(),
	}))
	if err != nil {
		t.Fatal(err)
	}

	if lease.Task.RunID != runID {
		t.Fatalf("lease run = %q, want %q", lease.Task.RunID, runID)
	}
	if lease.Task.ScheduledEventID != valid.ScheduledEventID {
		t.Fatalf("lease scheduled event = %d, want %d (stale sticky tasks were not drained correctly)",
			lease.Task.ScheduledEventID, valid.ScheduledEventID)
	}

	// 核心断言：循环实现下栈里始终只有一帧 PollWorkflowTask。
	// 递归实现下这里会是 staleTasks+1 量级。
	if mgr.maxPollFrames > 1 {
		t.Fatalf("PollWorkflowTask stack frames = %d, want 1: draining %d stale sticky tasks grew the stack (recursion)",
			mgr.maxPollFrames, staleTasks)
	}
}

func timeFarFuture() time.Time {
	return time.Now().Add(time.Hour)
}

func drainWorkflowQueue(t *testing.T, queue *memory.TaskQueue) {
	t.Helper()
	for {
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // 已取消的 ctx：队列空时立刻返回，不阻塞。
		if _, err := queue.Dequeue(ctx); err != nil {
			return
		}
	}
}
