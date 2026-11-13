---
title: Demo Public HTTP API
description: 使用 Update-With-Start 构建同步业务 HTTP 接口
---

# Demo Public HTTP API

Demo 提供一个可直接调用的 HTTP API，用来演示业务服务与 Tenuto 的同步交互：HTTP 请求发起 `Update-With-Start`，Workflow 注册 Typed Update Handler，Handler 等待内部 Activity 完成后返回结构化结果。

## 启动环境

在仓库根目录执行：

```bash
cd tenuto
make dev
```

Demo API 默认地址是 `http://localhost:7403`。Console 和 Tenuto Server 分别使用 7401、7400 端口。

## 创建记录

```bash
curl -i -X POST http://localhost:7403/api/demo/create-record \
  -H 'Content-Type: application/json' \
  -d '{"record_id":"record-http-001"}'
```

成功响应：

```http
HTTP/1.1 200 OK
Content-Type: application/json
```

```json
{
  "record_id": "record-http-001",
  "status": "created"
}
```

## 调用链

```text
HTTP POST
  → demo app typedCreateUseCase.Create
  → NewWithStartWorkflowOperation
  → UpdateWithStartWorkflow(WaitForStage=Completed)
  → Server 启动或复用 Run
  → Workflow 注册 create-record Handler
  → CreateRecord Activity
  → createDone=true
  → Handler 返回 typedCreateResult
  → handle.Get 解码结果
  → HTTP 200
```

HTTP 请求持续等待 Update Completed。请求 Context 取消时，客户端停止等待；Workflow 的持久化状态仍由 Server 管理，后续可以通过 Run 查询结果。

Demo Workflow 在注册 Handler 后先执行一个 3 秒的 durable Sleep，再执行 CreateRecord Activity。使用 `curl` 时可以观察到请求保持等待，响应会在 Sleep、Activity 和 Handler 完成后返回；这个延迟专门用于展示同步等待链路。

## 幂等与重复调用

Demo 使用 `create-<record_id>` 作为 Workflow ID，并配置 `WorkflowIDConflictPolicyUseExisting`。相同 `record_id` 会访问同一个当前 Run。新的 Update ID 仍会产生一次新的 Update；业务接口应在需要严格幂等时传入稳定的 Update ID。

## 错误场景

| 请求 | 响应 |
| --- | --- |
| 非 POST | `405 Method Not Allowed` |
| JSON 无法解析 | `400 Bad Request` |
| `record_id` 为空或超过 128 字符 | `400 Bad Request` |
| Server、TaskQ 或 Workflow 失败 | `502 Bad Gateway` |
| 客户端超时取消 | HTTP 连接结束，Run 可在 Console 继续查看 |

## 健康检查

```bash
curl http://localhost:7403/api/demo/health
```

```json
{"status":"ok"}
```

## 没有 Handler 的行为

```bash
curl -i -X POST http://localhost:7403/api/demo/no-update-handler \
  -H 'Content-Type: application/json' \
  -d '{"record_id":"record-no-handler-001"}'
```

这个端点启动 `no-update-handler-workflow`，Workflow 内只执行短暂 Timer，没有注册 `create-record` Handler。请求使用 `WaitForStage=Completed` 发送 Update。它用于观察“Update 被接受后缺少 Handler”的行为：Worker 用 `UnknownUpdate` 完成这个 Update。

该请求返回 `200`，响应体的 `outcome.failure.type` 为 `UnknownUpdate`，`outcome.failure.message` 为 `no update handler registered for "create-record"`。Run、Accepted 和 Completed 事件可以在 Console 的 History 中查看。

该 API 用于学习和验收 Typed Update 流程。生产业务应在自己的服务中构造 SDK Client，并为鉴权、限流、幂等键和错误映射增加业务策略。
