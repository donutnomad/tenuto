// Package worker 运行 Workflow 与 Activity：从 Tenuto Server 拉取任务，执行生成的 Workflow 程序与
// 登记的 Activity，把结果上报给 Server。
//
// Worker 经 sdk/client 的 Client 连接 Server。Workflow 是生成器打进二进制的定义（WorkflowDefinition()），
// Activity 由生成的 RegisterActivities 登记：
//
//	c, err := client.Dial(client.Options{HostPort: "localhost:7400", Namespace: "orders"})
//	if err != nil {
//		return err
//	}
//	defer c.Close()
//
//	registry := activity.NewRegistry()
//	if err := orders.RegisterActivities(registry); err != nil {
//		return err
//	}
//	w, err := worker.New(c, worker.Options{
//		Workflows:          []registration.Definition{orders.WorkflowDefinition()},
//		Activities:         registry,
//		ActivityTaskQueues: []string{"orders"},
//		BuildID:            "orders-v1",
//	})
//	if err != nil {
//		return err
//	}
//	return w.Run(ctx)
//
// Run 阻塞到 ctx 结束后优雅停止；需要分开控制时用 Start、WaitReady 与 Stop。
package worker
