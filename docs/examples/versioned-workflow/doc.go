// Package versionedworkflow 校验 lifecycle 示例的生成产物与版本兼容行为。
//
// 目录 v1 / v2 是同一 workflow 的两代源码；activities 与 remote 是 activity 契约包。
// 运行 `go generate ./...`（或本目录的 tenuto-workflow-gen 命令）刷新各目录的 JSON 与桥文件。
package versionedworkflow
