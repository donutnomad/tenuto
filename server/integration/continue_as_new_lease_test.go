package integration

import (
	"context"
	"testing"
	"time"

	historypkg "github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/app/assembly"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/biz/service/engine"
	"github.com/donutnomad/tenuto/server/infra/memory"
	"github.com/donutnomad/tenuto/server/infra/persistence/mysqltest"
)

// 按 Workflow ID 寻址（不带 runID）的写请求在 keyed lock 内、事务外解析 current Run。keyed lock 是进程内的：
// 另一个副本在解析之后提交 continue-as-new（或失败重试、reset）时，事务内读到的旧 Run 已经结束。lease 在事务外
// 重新解析 current，current 换成接续 Run 时重试，Signal 落到接续 Run 上。
//
// 「副本 A」的 continue-as-new 事务由测试直接在存储上执行（与 completeWorkflowTaskWithStore 的事务体相同：
// GetRunForUpdate → WorkflowContinuedAsNew → CommitContinuedRun）；「副本 B」是一个真实的 engine.Server。
// B 的请求在 A 的事务提交前到达，排在旧 Run 的 execution state 行锁上。
func TestSignalByWorkflowIDFollowsContinueAsNewOnAnotherReplica(t *testing.T) {
	for name, send := range map[string]func(*engine.Server, string) error{
		"SignalWorkflow": func(server *engine.Server, workflowID string) error {
			_, err := server.SignalWorkflow(context.Background(), "", workflowID, "", "deposit", "req-1", map[string]any{"amount": 1})
			return err
		},
		"SignalWithStart": func(server *engine.Server, workflowID string) error {
			_, _, err := server.SignalWithStart(context.Background(), "", "entity", workflowID, nil, "deposit", "req-1", map[string]any{"amount": 1})
			return err
		},
	} {
		t.Run(name, func(t *testing.T) { signalAcrossContinueAsNew(t, send) })
	}
}

func signalAcrossContinueAsNew(t *testing.T, send func(*engine.Server, string) error) {
	mysqltest.Require(t)
	store := newMySQLWorkflowStore(t)
	const workflowID = "wf-entity"
	oldRun := execution.NewRun(execution.NewRunOptions{ID: "run-can-old", WorkflowID: workflowID, WorkflowType: "entity", WorkflowConfigVersion: "v1", CreatedAt: time.Now()})
	if err := store.Executions.RunInTx(func(tx execution.ExecutionTx) error {
		return execution.CommitStartedRun(tx, oldRun, "", oldRun.ID, oldRun.ID, historypkg.NewWorkflowStartedEvent(nil, oldRun.ID, "", workflowID, "Entity", 0))
	}); err != nil {
		t.Fatal(err)
	}
	replicaB := assembly.NewServer(assembly.Options{
		Stores: assembly.Persistent(store), WorkflowQueue: memory.NewTaskQueue(16), ActivityRouter: memory.NewActivityQueueRouter(16),
	})
	defer replicaB.Stop()

	next := execution.NewContinuedRun(oldRun, "run-can-new", oldRun.Namespace, "entity", "v1", nil, time.Now())
	locked := make(chan struct{})
	signalSent := make(chan struct{})
	canErr := make(chan error, 1)
	go func() {
		canErr <- store.Executions.RunInTx(func(tx execution.ExecutionTx) error {
			if _, err := tx.GetRunForUpdate(oldRun.ID); err != nil {
				return err
			}
			close(locked)
			<-signalSent
			time.Sleep(300 * time.Millisecond) // 让副本 B 的 Signal 解析出 current 并排到行锁上
			if _, _, err := tx.AppendIdempotent(oldRun.ID, oldRun.ID, oldRun.ID, historypkg.NewWorkflowContinuedAsNewEvent("Entity", next.ID, "v1", nil)); err != nil {
				return err
			}
			return execution.CommitContinuedRun(tx, oldRun.ID, next, next.ID, next.ID, historypkg.NewWorkflowStartedEvent(nil, oldRun.ID, oldRun.ID, workflowID, "Entity", 0))
		})
	}()
	<-locked
	signalErr := make(chan error, 1)
	go func() { signalErr <- send(replicaB, workflowID) }()
	close(signalSent)
	if err := <-canErr; err != nil {
		t.Fatalf("continue-as-new: %v", err)
	}
	err := <-signalErr

	current, currentErr := store.Executions.GetCurrentRunID("default", workflowID)
	if currentErr != nil || current != next.ID {
		t.Fatalf("current run = %q, %v; want %q", current, currentErr, next.ID)
	}
	events, eventsErr := store.Executions.GetEvents(next.ID)
	if eventsErr != nil {
		t.Fatal(eventsErr)
	}
	delivered := false
	for _, event := range events {
		delivered = delivered || event.WorkflowSignaled != nil
	}
	if err != nil || !delivered {
		t.Errorf("按 Workflow ID 发的 Signal 没有落到接续 Run（delivered=%v），调用方收到: %v", delivered, err)
	}
}
