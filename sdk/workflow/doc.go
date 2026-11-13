// Package workflow 是编写 Workflow 的 API。Workflow 是一个普通的 Go 函数，用 `@Workflow` 注解声明
// Workflow Type；tenuto-workflow-gen 把它翻译成 WLang 程序打进二进制，Worker 执行这段程序并按历史重放。
// 本包的函数只在 Workflow 源码里使用：它们是定义标记，在普通 Go 代码里调用会 panic，运行时由 Worker
// 的解释器实现。错误判断 IsCanceledError 例外，它也可以在 Workflow 之外使用。
//
//	// @Workflow(id="place-order",queue="orders",version=v1)
//	func PlaceOrder(ctx workflow.Context, in OrderInput) (OrderOutput, error) {
//		charged, err := CallCharge(ctx, ChargeInput{OrderID: in.OrderID}, workflow.ActivityOptions{
//			StartToCloseTimeout: time.Minute,
//		})
//		if err != nil {
//			return OrderOutput{}, err
//		}
//		return OrderOutput{Receipt: charged.Receipt}, nil
//	}
//
// CallCharge 是生成器为 `@Activity` 写出的类型化调用。启动 Workflow、等待结果用 sdk/client，运行
// Workflow 与 Activity 用 sdk/worker。
package workflow
