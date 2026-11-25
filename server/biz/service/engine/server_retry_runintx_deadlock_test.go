package engine_test

import (
	"context"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/program/programtest"
	"github.com/donutnomad/tenuto/protocol"
	historypkg "github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/app/assembly"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

// Integration（死锁回归）：连接池只有一条连接时，startRetryRun 的 RunInTx
// 闭包内原本调 s.configs.GetWorkflowConfig —— 该方法走非事务 s.db 句柄，而事务已占住唯一连接，
// 于是闭包内的读永远等不到连接，形成自死锁。
//
// 触发条件：workflow 配了 RetryPolicy 且失败 → prepareWorkflowFailure → startRetryRun。
// 现有 server_runintx_deadlock_test.go 只覆盖 HandleActivityCompleted/Failed 两条路径，
// 所有 retry 测试都走 memory 后端（memory 无单连接约束），故都测不出本缺陷。
//
// 修复后：配置由 prepareWorkflowFailure 在事务外取好并传入，闭包内不再有任何非事务读。
func TestServerStartRetryRunNoDeadlockUnderSingleConn(t *testing.T) {
	store := newSingleConnStore(t)

	server := assembly.NewServer(assembly.Options{Stores: assembly.Persistent(store), WorkflowQueue: memory.NewTaskQueue(8), ActivityRouter: memory.NewActivityQueueRouter(8)})
	t.Cleanup(server.Stop)

	ref, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{
		Program: programtest.Noop(),
		Type:    "wf-retry-deadlock",
		Name:    "wf-retry-deadlock",
		RetryPolicy: &program.RetryPolicy{
			MaxAttempts:        3,
			InitialInterval:    "50ms",
			BackoffCoefficient: 2.0,
			MaxInterval:        "1s",
		},
	})

	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	run1ID, err := startedRunID(server.StartWorkflow(ctx, "", ref.WorkflowType, ref.WorkflowType, "", nil))
	if err != nil {
		t.Fatal(err)
	}

	// CmdFailWorkflow → prepareWorkflowFailure → startRetryRun（RunInTx，唯一连接被占用）。
	// 修复前：闭包内非事务读配置永久阻塞，此调用不返回。
	done := make(chan error, 1)
	go func() {
		done <- server.HandleWorkflowTaskCompleted(server.WorkflowTaskContext(ctx, run1ID, server.OpenWorkflowTaskID(run1ID)), run1ID, server.OpenWorkflowTaskID(run1ID), []historypkg.Command{
			{Type: historypkg.CmdFailWorkflow, Error: "boom"},
		}, "worker-1")
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("HandleWorkflowTaskCompleted returned error: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("startRetryRun deadlocked under MaxOpenConns(1): RunInTx closure read workflow config via the non-tx handle while holding the only connection")
	}

	// 断言 retry 真的发生了（否则测试会变成恒真：不走 retry 自然也不死锁）。
	var run2 *execution.Run
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		listed, _ := store.Executions.List(execution.StoreListFilter{Statuses: []historypkg.Status{historypkg.StatusRunning}})
		runs := listed.Runs
		for _, r := range runs {
			if r.ID != run1ID && r.WorkflowID == ref.WorkflowType {
				run2 = r
				break
			}
		}
		if run2 != nil {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if run2 == nil {
		t.Fatal("expected a retry run to be created; without it this test would not exercise startRetryRun at all")
	}
	if run2.Attempt != 2 {
		t.Fatalf("expected retry run Attempt=2, got %d", run2.Attempt)
	}
	if run2.Initiator != execution.InitiatorEnums.Retry {
		t.Fatalf("expected retry run Initiator=Retry, got %q", run2.Initiator)
	}
	if run2.ContinuedRunID != run1ID {
		t.Fatalf("expected retry run ContinuedRunID=%q, got %q", run1ID, run2.ContinuedRunID)
	}

	// 新 run 的 WorkflowStarted 事件必须真的落库（证明闭包整体跑完、事务已提交，
	// 而不是在读配置那一步之前就返回）。注意 gormstore 的事件行不持久化
	// WorkflowStarted 的 Name/ExecutionTimeoutMs（见 store.go 的列映射，初始 run 同样如此），
	// 故此处只断言 retry 语义字段。
	history, err := store.Executions.GetEvents(run2.ID)
	if err != nil {
		t.Fatal(err)
	}
	var started *historypkg.WorkflowStartedEvent
	for i := range history {
		if history[i].WorkflowStarted != nil {
			started = history[i].WorkflowStarted
			break
		}
	}
	if started == nil {
		t.Fatal("retry run missing WorkflowStarted event")
	}
	if started.Initiator != execution.InitiatorEnums.Retry || started.Attempt != 2 {
		t.Fatalf("expected WorkflowStarted{Initiator:Retry, Attempt:2}, got %+v", started)
	}

	// 旧 run 必须落终态 WorkflowFailed 且指向新 run（startRetryRun 闭包的第一步）。
	oldHistory, err := store.Executions.GetEvents(run1ID)
	if err != nil {
		t.Fatal(err)
	}
	var failed *historypkg.WorkflowFailedEvent
	for i := range oldHistory {
		if oldHistory[i].WorkflowFailed != nil {
			failed = oldHistory[i].WorkflowFailed
			break
		}
	}
	if failed == nil {
		t.Fatal("old run missing terminal WorkflowFailed event")
	}
	if failed.NewExecutionRunID != run2.ID {
		t.Fatalf("expected WorkflowFailed.NewExecutionRunID=%q, got %q", run2.ID, failed.NewExecutionRunID)
	}
}
