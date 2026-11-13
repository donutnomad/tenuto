---
title: 异步 Activity 完成
description: 使用 Activity Task Token 让外部系统完成长时间 Activity
status: Preview
---

# 异步 Activity 完成

<FeatureStatus status="Preview" note="外部回调适配层仍在演进" />

异步完成允许 Activity Worker 释放执行槽，由 Webhook、人工操作或外部批处理在未来提交结果。

## Activity 端

```go
import "github.com/donutnomad/tenuto/sdk/activity"

func WaitForPayment(ctx context.Context, in PaymentInput) (PaymentResult, error) {
    token := activity.GetInfo(ctx).TaskToken
    if err := callbackStore.Save(in.PaymentID, token); err != nil {
        return PaymentResult{}, err
    }
    return PaymentResult{}, activity.ErrPending
}
```

Worker 识别 `activity.ErrPending` 后保留 Activity 的 open state，并确认当前任务租约。`activity.GetInfo(ctx).TaskToken` 是字符串形式的 Token，任务不带 Token 时为空字符串。

## 外部完成

完成入口有两个：Runtime HTTP 端点 `POST /runtime/activity/complete`，以及 Server 进程内的 `(*engine.Server).CompleteActivity`。

### HTTP

```bash
# 成功
curl -sS -X POST http://127.0.0.1:7400/runtime/activity/complete \
  -H 'Content-Type: application/json' \
  -d '{"task_token":"<saved token>","output":{"paid":true,"payment_id":"pay-1"}}'

# 失败
curl -sS -X POST http://127.0.0.1:7400/runtime/activity/complete \
  -H 'Content-Type: application/json' \
  -d '{"task_token":"<saved token>","failure":{"message":"payment expired","type":"PaymentExpired","nonRetryable":true}}'
```

请求体字段：

| 字段 | 含义 |
| --- | --- |
| `task_token` | 必填，`activity.GetInfo(ctx).TaskToken` 的值；为空时返回 `activity runtime task token is empty` |
| `output` | 成功时的 Activity 输出，JSON object |
| `failure` | 省略表示成功；提供时表示失败，结构为 `{message, type, nonRetryable, details, cause}` |
| `task` | 可选，只读取其中的 Worker Identity 与 Build ID 用于记录 |

成功响应为 `{"ok": true}`。Token 由 Server 验签，并且必须是 Activity Task 的 Token。这个端点属于 Runtime API，与其他端点一样受 `WORKFLOW_HTTP_AUTH_TOKEN` 与 mTLS 保护（开启时）；对外暴露给第三方回调时放在受保护的适配层之后，不要把 Server token 交给第三方。

### 进程内

`(*engine.Server).CompleteActivity` 在包 `server/biz/service/engine`：

```go
// 成功
err := tenutoServer.CompleteActivity(
    ctx,
    engine.ActivityTaskToken(savedToken),
    map[string]any{"paid": true, "payment_id": paymentID},
    nil,
)

// 失败
err = tenutoServer.CompleteActivity(
    ctx,
    engine.ActivityTaskToken(savedToken),
    nil,
    &protocol.Failure{Message: "payment expired", Type: "PaymentExpired"},
)
```

第四个参数 `failure *protocol.Failure` 为 `nil` 表示成功，非 `nil` 表示失败。外部回调服务通过自己的受保护适配层调用它，适配层负责鉴权、回调签名验证、幂等和审计。

## Token 管理

- Token 属于敏感凭证，存储时加密并限制读取权限。
- Token 绑定 Run、Scheduled Event、Attempt 和 Stamp。
- Activity Reset 后旧 Token 对应旧 generation，其完成请求不会覆盖新 Attempt。
- 回调业务表保存 provider event ID，重复 Webhook 复用同一完成结果。

## 行为矩阵

| 场景 | 行为 |
| --- | --- |
| 首次有效完成 | 写入 ActivityCompleted 并调度 Workflow Task |
| 同一已关闭 Activity 重复完成 | 幂等成功 |
| Token 签名错误 | 拒绝请求 |
| Token 对应旧 Stamp | 作为 stale completion 处理 |
| Run 已终态 | 返回终态或 closed 状态 |
| 外部系统永久失联 | Schedule-To-Close 或业务 Timer 推进恢复流程 |
| 完成请求与 Reset 并发 | generation/stamp 决定有效执行 |

生产使用时，为 pending Activity 配置 `ScheduleToCloseTimeout`、补偿流程和人工检索入口。
