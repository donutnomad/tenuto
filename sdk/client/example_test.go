package client_test

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http/httptest"

	"github.com/donutnomad/tenuto/sdk/apperror"
	"github.com/donutnomad/tenuto/sdk/client"
	"github.com/donutnomad/tenuto/sdk/registration"
	"github.com/donutnomad/tenuto/sdk/worker"
	"github.com/donutnomad/tenuto/server/app/assembly"
	"github.com/donutnomad/tenuto/server/http/runtimehttp"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

// 启动一个 Workflow 并等待它的结果。
func Example() {
	hostPort, stop := startExampleServer() // 示例用的进程内 Server 与 Worker
	defer stop()

	c, err := client.Dial(client.Options{HostPort: hostPort, Namespace: "default"})
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	ctx := context.Background()
	run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: "greeting-42"}, "greeting", map[string]any{"name": "tenuto"})
	if err != nil {
		log.Fatal(err)
	}
	var out struct {
		Greeting string `json:"greeting"`
	}
	if err := run.Get(ctx, &out); err != nil {
		log.Fatal(err)
	}
	fmt.Println(out.Greeting)
	// Output: hello tenuto
}

// WorkflowRun.Get 的错误：Workflow 返回的应用错误经 errors.As 读取 Type 与 details。
func ExampleWorkflowRun_Get_applicationError() {
	hostPort, stop := startExampleServer()
	defer stop()
	c, err := client.Dial(client.Options{HostPort: hostPort})
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	ctx := context.Background()
	run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{}, "failing")
	if err != nil {
		log.Fatal(err)
	}
	err = run.Get(ctx, nil)
	var appErr *apperror.Error
	if errors.As(err, &appErr) {
		var detail struct {
			Code string `json:"code"`
		}
		_ = appErr.Details(&detail)
		fmt.Println(appErr.Type(), appErr.NonRetryable(), detail.Code)
	}
	// Output: PaymentDeclined true insufficient_funds
}

// startExampleServer 启动内存后端的 Server 与一个 Worker（登记 greeting 与 failing 两个 Workflow），返回 HTTP 地址。
func startExampleServer() (string, func()) {
	server := assembly.NewServer(assembly.Options{
		Stores:        assembly.Stores{Executions: memory.NewExecutionManager()},
		WorkflowQueue: memory.NewTaskQueue(64), ActivityRouter: memory.NewActivityQueueRouter(64),
	})
	httpServer := httptest.NewServer(runtimehttp.NewRuntimeHTTPHandler(server))
	c, err := client.Dial(client.Options{HostPort: httpServer.URL})
	if err != nil {
		log.Fatal(err)
	}
	var definitions []registration.Definition
	for workflowType, src := range map[string]string{"greeting": greetingSrc, "failing": failingSrc} {
		config, err := workflowConfig(workflowType, src)
		if err != nil {
			log.Fatal(err)
		}
		definition, err := definitionOf(config)
		if err != nil {
			log.Fatal(err)
		}
		definitions = append(definitions, definition)
	}
	w, err := worker.New(c, worker.Options{Workflows: definitions, BuildID: "example"})
	if err != nil {
		log.Fatal(err)
	}
	w.Start(context.Background())
	if err := w.WaitReady(context.Background()); err != nil {
		log.Fatal(err)
	}
	return httpServer.URL, func() {
		w.Stop()
		c.Close()
		httpServer.CloseClientConnections()
		httpServer.Close()
		server.Stop()
	}
}
