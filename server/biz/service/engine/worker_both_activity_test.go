package engine

import (
	"context"
	"encoding/json"
	"testing"
	"testing/fstest"
	"time"

	startupflow "github.com/donutnomad/tenuto/docs/examples/startup-workflow"
	"github.com/donutnomad/tenuto/internal/worker"
	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/protocol"
	activity "github.com/donutnomad/tenuto/sdk/activity"
	"github.com/donutnomad/tenuto/sdk/registration"
)

// blockingWorkerAPI 委托 Server 的登记/路由能力，并让 poll 阻塞到 ctx 取消：
// 本用例只验证 Start 的先后顺序，不需要真实的空队列 Dequeue。
type blockingWorkerAPI struct {
	*Server
	polled chan struct{}
}

func (a blockingWorkerAPI) PollWorkflowTask(ctx context.Context) (protocol.TaskLease, error) {
	if a.polled != nil {
		select {
		case a.polled <- struct{}{}:
		default:
		}
	}
	<-ctx.Done()
	return protocol.TaskLease{}, ctx.Err()
}

func (a blockingWorkerAPI) PollActivityTask(ctx context.Context, _ string) (protocol.TaskLease, error) {
	<-ctx.Done()
	return protocol.TaskLease{}, ctx.Err()
}

// TestWorkerBothRoleActivityBeforeEnroll 覆盖「正常单次 Start」的混合 Worker：
// 已有路由目录启用，NewWorker（both 角色）同时携带新的本地 ActivityType 和引用它的
// Workflow 定义。Start 必须先上报自身 Activity 能力（路由），再登记定义，最后 poll。
//
// 定义里的 activity 刻意不带 task_queue：登记期路由校验是硬失败，只有本 worker 的
// Activity 路由已经进目录时才能通过。
func TestWorkerBothRoleActivityBeforeEnroll(t *testing.T) {
	const namespace = "startup"
	server := newTestServer()

	registry := activity.NewRegistry()
	activity.RegisterActivity(registry, "startup.say-hello", "v1",
		func(context.Context, startupflow.SayHelloInput) (startupflow.SayHelloOutput, error) {
			return startupflow.SayHelloOutput{Message: "hi"}, nil
		})

	worker := assembleWorker(blockingWorkerAPI{Server: server}, registry, worker.WorkerOptions{
		Namespace:                     namespace,
		ActivityTaskQueues:            []string{"startup"},
		Identity:                      "startup-mixed-1",
		BuildID:                       "startup-v1",
		ActivityWorkerVersionDefault:  true,
		ActivityWorkerVersionRegistry: server,
		Workflows: []registration.Definition{{FS: fstest.MapFS{
			"workflow.json": &fstest.MapFile{Data: definitionWithoutTaskQueue(t)},
		}}},
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	worker.Start(ctx)
	defer worker.Stop()

	readyCtx, readyCancel := context.WithTimeout(ctx, 5*time.Second)
	defer readyCancel()
	if err := worker.WaitReady(readyCtx); err != nil {
		t.Fatalf("WaitReady: %v", err)
	}
	active, err := server.activeWorkflowRefInNamespace(namespace, "startup-greeting")
	if err != nil {
		t.Fatalf("定义未登记: %v", err)
	}
	if active.Version == "" {
		t.Fatal("定义已登记但 latest 为空")
	}
}

// definitionWithoutTaskQueue 取示例定义并清空 activity 的 task_queue，
// 让登记期路由校验只能靠 worker 自己上报的路由通过。
func definitionWithoutTaskQueue(t *testing.T) []byte {
	t.Helper()
	_, data, err := startupflow.WorkflowDefinition().Load()
	if err != nil {
		t.Fatalf("读取示例定义: %v", err)
	}
	var workflow program.WorkflowConfig
	if err := json.Unmarshal(data, &workflow); err != nil {
		t.Fatalf("解析示例定义: %v", err)
	}
	cleared := 0
	for symbol, ref := range workflow.Activities {
		if ref.TaskQueue != "" {
			ref.TaskQueue = ""
			workflow.Activities[symbol] = ref
			cleared++
		}
	}
	if cleared == 0 {
		t.Fatal("示例定义里没有可清空的 task_queue")
	}
	out, err := json.Marshal(&workflow)
	if err != nil {
		t.Fatalf("序列化定义: %v", err)
	}
	return out
}

// TestStale 覆盖启动可用性边界：
// 同内容（已存在且已有 latest）的重复登记只做静态校验后幂等返回，不因远端
// Activity 路由 TTL 过期而拒绝 Worker 启动；新内容首次激活仍要求路由存在。
func TestStale(t *testing.T) {
	const namespace = "startup"
	server := newTestServer()

	// 远端 Activity 在线，先让 A 的定义能通过路由校验完成首次登记。
	if err := server.RegisterActivityWorkerVersion(context.Background(), protocol.ActivityWorkerVersion{
		Namespace:     namespace,
		Identity:      "startup-activity-1",
		BuildID:       "startup-v1",
		TaskQueues:    []string{"startup"},
		ActivityTypes: []string{"startup.say-hello"},
		Default:       true,
		HeartbeatTTL:  "30s",
	}); err != nil {
		t.Fatalf("注册 Activity worker: %v", err)
	}
	contentA := definitionWithoutTaskQueue(t)
	refA, err := server.Definitions().EnrollWorkflowInNamespace(namespace, mustWorkflowConfig(t, contentA))
	if err != nil {
		t.Fatalf("首次登记 A: %v", err)
	}

	// 所有远端 Activity 记录过期：TTL 压到 1ns 后目录重建为空。
	if err := server.RegisterActivityWorkerVersion(context.Background(), protocol.ActivityWorkerVersion{
		Namespace:     namespace,
		Identity:      "startup-activity-1",
		BuildID:       "startup-v1",
		TaskQueues:    []string{"startup"},
		ActivityTypes: []string{"startup.say-hello"},
		Default:       true,
		HeartbeatTTL:  "1ns",
	}); err != nil {
		t.Fatalf("让 Activity 路由过期: %v", err)
	}
	time.Sleep(2 * time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	polled := make(chan struct{}, 1)
	replayWorker := worker.NewWorkflowWorker(blockingWorkerAPI{Server: server, polled: polled}, worker.WorkerOptions{
		Namespace: namespace,
		Workflows: []registration.Definition{{FS: fstest.MapFS{
			"workflow.json": &fstest.MapFile{Data: contentA},
		}}},
	})
	replayWorker.Start(ctx)
	defer replayWorker.Stop()
	readyCtx, readyCancel := context.WithTimeout(ctx, 5*time.Second)
	defer readyCancel()
	if err := replayWorker.WaitReady(readyCtx); err != nil {
		t.Fatalf("同内容重启不应因 Activity 路由过期失败: %v", err)
	}
	select {
	case <-polled:
	case <-time.After(time.Second):
		t.Fatal("同内容重启后没有开始 poll")
	}
	active, err := server.activeWorkflowRefInNamespace(namespace, "startup-greeting")
	if err != nil || active.Version != refA.Version {
		t.Fatalf("同内容重启改动了 latest: %+v err=%v", active, err)
	}

	// 新 hash B 首次激活：路由过期不阻止启动，latest 指向 B。
	contentB := definitionVariant(t, "B")
	newWorker := worker.NewWorkflowWorker(blockingWorkerAPI{Server: server}, worker.WorkerOptions{
		Namespace: namespace,
		Workflows: []registration.Definition{{FS: fstest.MapFS{
			"workflow.json": &fstest.MapFile{Data: contentB},
		}}},
	})
	newWorker.Start(ctx)
	defer newWorker.Stop()
	if err := newWorker.WaitReady(readyCtx); err != nil {
		t.Fatalf("新内容缺路由不应阻止启动: %v", err)
	}
	active, err = server.activeWorkflowRefInNamespace(namespace, "startup-greeting")
	if err != nil || active.Version == refA.Version {
		t.Fatalf("B 登记后 latest 应指向 B: %+v err=%v", active, err)
	}
}

func mustWorkflowConfig(t *testing.T, data []byte) *program.WorkflowConfig {
	t.Helper()
	var workflow program.WorkflowConfig
	if err := json.Unmarshal(data, &workflow); err != nil {
		t.Fatalf("解析定义: %v", err)
	}
	return &workflow
}

func definitionVariant(t *testing.T, name string) []byte {
	t.Helper()
	workflow := mustWorkflowConfig(t, definitionWithoutTaskQueue(t))
	workflow.Name = name
	out, err := json.Marshal(workflow)
	if err != nil {
		t.Fatalf("序列化定义变体: %v", err)
	}
	return out
}
