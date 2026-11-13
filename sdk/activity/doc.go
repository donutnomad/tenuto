// Package activity 是编写与登记 Activity 的 API。Activity 是普通的 Go 函数
// func(context.Context, In) (Out, error)，In 与 Out 编码成 JSON 对象；它在 Worker 进程里执行，
// 可以有副作用，失败时按 Workflow 指定的重试策略重试。
//
// 生成器为每个包写出 RegisterActivities，把包里全部 `@Activity` 实现登记到 Registry；Registry 交给
// sdk/worker 的 worker.Options.Activities。手写登记用 RegisterActivity。Activity 里用 GetInfo 读取
// 调用上下文，用 RecordHeartbeat 上报进度，用 GetWorkerStopChannel 感知 Worker 停止。
package activity
