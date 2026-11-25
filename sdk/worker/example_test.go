package worker_test

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http/httptest"
	"testing/fstest"

	"github.com/donutnomad/tenuto/compiler/sdkgen"
	"github.com/donutnomad/tenuto/sdk/activity"
	"github.com/donutnomad/tenuto/sdk/client"
	"github.com/donutnomad/tenuto/sdk/registration"
	"github.com/donutnomad/tenuto/sdk/worker"
	"github.com/donutnomad/tenuto/server/app/assembly"
	"github.com/donutnomad/tenuto/server/http/runtimehttp"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

// 启动一个同时执行 Workflow 与 Activity 的 Worker，再经同一个 Client 启动 Workflow。
func Example() {
	hostPort, stop := startExampleServer() // 示例用的进程内 Server
	defer stop()

	c, err := client.Dial(client.Options{HostPort: hostPort})
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	// 真实项目里这两行是生成代码：orders.RegisterActivities(registry) 与 orders.WorkflowDefinition()。
	registry := activity.NewRegistry()
	if err := activity.RegisterActivityOnQueue(registry, "orders", "orders.Charge", charge); err != nil {
		log.Fatal(err)
	}
	w, err := worker.New(c, worker.Options{
		Workflows:  []registration.Definition{orderWorkflowDefinition()},
		Activities: registry,
		Identity:   "orders-worker-1",
		BuildID:    "orders-v1",
	})
	if err != nil {
		log.Fatal(err)
	}
	ctx := context.Background()
	w.Start(ctx)
	defer w.Stop()
	if err := w.WaitReady(ctx); err != nil {
		log.Fatal(err)
	}

	run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: "order-42"}, "place-order", map[string]any{"order_id": "42"})
	if err != nil {
		log.Fatal(err)
	}
	var out ChargeOutput
	if err := run.Get(ctx, &out); err != nil {
		log.Fatal(err)
	}
	fmt.Println(out.Receipt)
	// Output: receipt-42
}

type ChargeInput struct {
	OrderID string `json:"order_id"`
}

type ChargeOutput struct {
	Receipt string `json:"receipt"`
}

func charge(_ context.Context, in ChargeInput) (ChargeOutput, error) {
	return ChargeOutput{Receipt: "receipt-" + in.OrderID}, nil
}

const orderWorkflowSrc = `package orders

import (
	"time"

	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/orders"
)

func PlaceOrder(ctx workflow.Context, input map[string]any) (map[string]any, error) {
	var out map[string]any
	err := workflow.ExecuteActivity(ctx, orders.Charge, input, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &out)
	return out, err
}
`

// orderWorkflowDefinition 代替生成代码里的 WorkflowDefinition()：从源码生成 place-order 并包装成定义。
func orderWorkflowDefinition() registration.Definition {
	generated, err := sdkgen.GenerateFromSource([]byte(orderWorkflowSrc), sdkgen.Options{WorkflowType: "place-order", FuncName: "PlaceOrder", Filename: "orders.go"})
	if err != nil {
		log.Fatal(err)
	}
	for symbol, ref := range generated.Workflow.Activities {
		ref.Type, ref.TaskQueue = symbol, "orders"
		generated.Workflow.Activities[symbol] = ref
	}
	data, err := json.Marshal(generated.Workflow)
	if err != nil {
		log.Fatal(err)
	}
	return registration.Definition{FS: fstest.MapFS{registration.ArtifactFile: &fstest.MapFile{Data: data}}}
}

func startExampleServer() (string, func()) {
	server := assembly.NewServer(assembly.Options{
		Stores:        assembly.Stores{Executions: memory.NewExecutionManager()},
		WorkflowQueue: memory.NewTaskQueue(64), ActivityRouter: memory.NewActivityQueueRouter(64),
	})
	httpServer := httptest.NewServer(runtimehttp.NewRuntimeHTTPHandler(server))
	return httpServer.URL, func() {
		httpServer.CloseClientConnections()
		httpServer.Close()
		server.Stop()
	}
}
