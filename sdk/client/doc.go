// Package client 是应用代码访问 Tenuto Server 的客户端：启动 Workflow 并等待结果，发送 Signal、Query、
// Update，取消、终止、重置 Run，读取状态与历史，列出 Run，管理 Schedule 与批量操作。
// 接口与语义对齐 Temporal Go SDK 的 client 包，只包含 Server 支持的操作。
//
// Dial 创建 Client；同一个 Client 可以被多个 goroutine 共用，也交给 sdk/worker 创建 Worker：
//
//	c, err := client.Dial(client.Options{HostPort: "localhost:7400", Namespace: "orders"})
//	if err != nil {
//		return err
//	}
//	defer c.Close()
//
//	run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: "order-42"}, "place-order", OrderInput{ID: "42"})
//	if err != nil {
//		return err
//	}
//	var out OrderOutput
//	err = run.Get(ctx, &out)
//
// Workflow 由 `@Workflow(type=...)` 声明的 Workflow Type 寻址。Workflow 的输入与结果、Signal 与 Query 的参数、
// Query 的结果是 JSON 对象（结构体或 map）；Update 的参数与结果按 json/plain-v1 逐个编码。
//
// 错误：Server 应答映射成 *ServiceError，errors.Is 可匹配 ErrNotFound、ErrWorkflowExecutionAlreadyStarted 等
// 哨兵错误；WorkflowRun.Get 在 Run 没有成功完成时返回 *WorkflowExecutionError，Unwrap 得到 Workflow 的
// 应用错误（*apperror.Error）、取消错误、超时或 *TerminatedError。
package client
