---
title: 错误与失败处理
description: Workflow、Activity、Update 与 Runtime 错误分类
status: Stable
---

# 错误与失败处理

<FeatureStatus status="Stable" />

Tenuto 错误可以来自客户端校验、Server、TaskQ、Workflow 代码、Activity 外部依赖和数据恢复。处理错误时先判断错误层次，再决定重试、补偿、取消或终止。

## 错误分类

| 类型 | 处理方式 |
| --- | --- |
| 参数错误 | 修正请求，客户端重试无效 |
| Workflow Type/Program 不兼容 | 发布兼容版本或更新 Worker capability |
| Task Queue 暂时不可用 | 由 Server 恢复投递，观察 backlog |
| Activity 可重试失败 | 由 Retry Policy 创建下一 Attempt |
| Activity 业务拒绝 | 返回稳定错误类型，进入补偿或人工处理 |
| Workflow Bug | 使用 Reset、修复版本并重新部署 |
| 数据库或 TaskQ 故障 | 按恢复手册恢复基础设施 |

## Application Error

```go
return workflow.NewApplicationError(
    "card declined",
    "CardDeclined",
    map[string]any{"provider_code": "insufficient_funds"},
)
```

错误类型用于稳定分支，message 用于诊断，details 用于结构化数据。客户端不要依赖 message 文本判断业务分支。

## Update 错误

Typed Update Handler 的失败通过 `*apperror.Error` 返回（`Outcome.Failure`）。客户端可以读取 `Type()`、`Message()` 和 `Details()`，并根据类型决定是否继续等待、修正输入或展示人工处理入口。

### Update 状态排查

| 现象 | 检查顺序 |
| --- | --- |
| HTTP 一直等待 | Context deadline、Update Name、Handler 注册时机、Await 条件 |
| Accepted 后没有 Completed | History 中是否有 Handler Completed，Run 是否已进入终态 |
| `*apperror.Error` | 读取 `Type()`、`Message()`、`Details()`，按稳定类型分支 |
| 重复请求创建多次业务数据 | 固定 Update ID，并在 Activity 使用稳定幂等键 |

Console 的 Run History 可以确认 Accepted、Completed、失败详情和对应 Workflow Task。Trace 可以定位 Handler 当前停在 Activity、Timer、Await 还是并发 lane。

## 重试边界

重试只适合幂等或具备幂等键的操作。不可重试错误应在 Retry Policy 中明确排除；外部服务返回的限流、网络断开和临时不可用通常属于可重试类别。
