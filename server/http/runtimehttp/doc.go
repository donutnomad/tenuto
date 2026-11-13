// Package runtimehttp 把 runtime HTTP 请求映射到执行核心与各用例：路由、handler、
// 长轮询与 task token 的线上校验。请求与响应类型、路径常量在 protocol/runtimeapi，
// 与服务端类型之间的转换在 wire.go；client 在 runtimeclient。
package runtimehttp
