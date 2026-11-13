// Package apperror 是 Workflow、Activity、Update 与客户端共用的应用错误。
//
// 带 Type 的应用错误跨进程保留 Type、NonRetryable、Details 与 Cause：Activity 返回的错误到达 Workflow，
// Workflow 返回的错误经 sdk/client 的 WorkflowRun.Get 到达调用方，都可以用 errors.As 取出 *Error。
// 取消错误是 Type 为 CanceledType 的应用错误，用 IsCanceled 判断。
package apperror
