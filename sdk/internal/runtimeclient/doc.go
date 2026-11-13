// Package runtimeclient 是 runtime HTTP API 的底层传输：Workflow、Workflow Task 与 Activity 三组端点的
// 类型化调用。端点路径与请求应答类型在 protocol/runtimeapi，服务端 handler 在 server/http/runtimehttp。
// 只有 sdk/client 与 sdk/worker 使用本包；应用代码经 sdk/client.Dial 得到连接。
package runtimeclient
