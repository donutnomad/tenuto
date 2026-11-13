---
title: Update 一直等待
description: 排查 Handler 缺失、Await 条件、终态竞态和客户端 Context
status: Stable
---

# Update 一直等待

<FeatureStatus status="Stable" />

## 快速定位

在 Run History 搜索 Update ID，并确认当前最高阶段。

| History | 含义 |
| --- | --- |
| 无 Accepted | Validator、目标 Run 或请求校验失败 |
| 只有 Accepted | Handler 尚未返回 |
| Accepted + Completed success | 客户端应能取得结果 |
| Accepted + Completed failure | 客户端应收到 `*apperror.Error`（`Outcome.Failure`） |

## 只有 Accepted

1. 核对调用端 Update Name 与 `SetUpdateHandler` Name。
2. 核对 Handler 注册位于 Workflow 稳定前缀。
3. 在 Trace 查看 Handler 协程和 Await 条件。
4. 检查 Handler 等待的 Signal、Timer 或 Activity。
5. 只有 Accepted 的 Update 所在 Run 不会是终态：Run 关闭时，未完成的 Update 以 Failure Type `AcceptedUpdateCompletedWorkflow` 结束，等待中的调用立即返回。

Handler 缺失时 History 里是 Accepted + Completed failure，Failure Type 为 `UnknownUpdate`，属于「Accepted + Completed failure」一行。只有 Accepted 说明 Handler 已开始执行并停在 Await、Activity 或 Timer 上。

## Client 已超时

Client Context 截止只结束本次等待。使用相同 Workflow ID、Run ID、Update Name 和 Update ID 再次调用，可以复用已有记录并取得最终结果。

## 重复 Update

相同 Update ID 对应同一逻辑命令。比较首次请求与重试请求的参数，业务调用方应固定请求内容。Validator 拒绝的请求不会写入 History，修正输入后可以重试。

## 需要采集的信息

Namespace、Workflow ID、Run ID、Update ID、Update Name、Accepted Event ID、Run Status、Worker Build ID、Program Version、客户端 deadline 和 Handler 等待点。
