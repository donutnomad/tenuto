---
title: 错误处理最佳实践
description: 短暂错误、间歇错误、永久错误和 Saga 补偿
status: Stable
---

# 错误处理最佳实践

<FeatureStatus status="Stable" />

## 错误分类

| 分类 | 示例 | 处理 |
| --- | --- | --- |
| 短暂错误 | 网络抖动、服务重启 | Activity Retry + 退避 |
| 间歇错误 | 限流、资源竞争 | 有界重试、jitter、容量治理 |
| 永久错误 | 参数非法、资源状态冲突 | NonRetryable Error 或业务分支 |
| 确定性错误 | 新代码与旧 History 漂移 | Blocked State、回滚或版本分支 |
| Activity panic | nil map 写入、越界 | Worker 上报 `PanicError` 并按 Retry Policy 重试；修复代码后重新部署 |
| Workflow panic | 越界、nil 解引用、`panic(...)` | Workflow Task 以 `workflow_panic` 失败，Run 保持 Running、显示 Blocked 并退避重试；修复代码后重新部署，从失败处继续 |
| Workflow 返回错误 | `return err` | Run 以 Failed 结束（配置了 Workflow RetryPolicy 时按它重试） |
| 操作错误 | 错误 Reset、错误路由 | 审计、恢复 Runbook |

## Application Error

```go
return Result{}, workflow.NewApplicationError(
    "card has expired",
    "CardExpired",
    cardID,
)
```

Type 参与程序分支与 `NonRetryableErrorTypes` 精确匹配（大小写敏感、完全相等），Message 面向诊断，details 承载结构化上下文。三条出口（Workflow 终态、Activity 失败、Update outcome）保存同一份结构化失败，Go 客户端用 `errors.As` 读取 Type/Details/NonRetryable/Cause。

workflow body 内用 `workflow.IsApplicationError(err, "CardExpired")` 判定类型，用 `workflow.ApplicationErrorDetail(err, 0, &detail)` 读取结构化 details（失败返回明确错误）。Activity 实现是原生 Go，直接返回 `apperror.New` / `apperror.NewNonRetryable`。

## Retry Policy

Activity 默认按 Server 的缺省策略不限次数重试（1s 起按 2 倍增长、最长 100s，与 Temporal 相同）。最大次数、初始间隔、退避系数、最大间隔应依据下游 SLO 设置；Schedule-To-Close 为整个 Call 提供总预算，重试在它到期前停止。永久失败返回 NonRetryable 错误，否则会一直重试到预算用完。重试意味着同一次操作会执行多次，Activity 必须幂等，见 [Activity 幂等](/best-practices/activity-idempotency)。

两条非重试策略互相独立，任一命中即停止重试：

| 策略 | 判定对象 | 匹配方式 |
| --- | --- | --- |
| `NonRetryableErrorTypes` | `Failure.Type` | 完全相等，大小写敏感 |
| `NonRetryableErrors` | `Failure.Message` | 子串包含（既有语义） |

`apperror.NewNonRetryable` 的 `NonRetryable` 布尔优先于两条列表策略。判定只看顶层失败。

## 包装错误

判定只看顶层失败，与 Temporal 一致：`fmt.Errorf("ctx: %w", appErr)` 的顶层 Type 是 `wrapError`、`NonRetryable=false`，内层 Type/flag 落在 cause 里，两条显式策略都不会命中。需要保留可判定的类型时，直接返回应用错误本身，或用 `apperror.NewWithCause` 把原始错误放进 cause。

业务层建立稳定错误目录，日志增加 Run ID、Scheduled Event ID、Attempt 和 Worker Build ID。

## Saga

业务步骤成功后记录补偿输入。失败时执行补偿，补偿本身也是 Activity，同样必须幂等；把补偿失败暴露为可操作状态。补偿重试策略通常比正向调用更保守。

取消也会触发补偿：Run 收到取消请求时根 Context 被取消，正在等待的步骤以取消错误返回。补偿必须发在 `workflow.NewDisconnectedContext` 返回的 Context 上，否则补偿 Activity 一发起就被取消。补偿完成后返回取消错误，Run 以 `cancelled` 结束：

```go
if workflow.IsCanceledError(err) {
    cleanupCtx, _ := workflow.NewDisconnectedContext(ctx)
    if _, releaseErr := CallReleaseInventory(cleanupCtx, ReleaseInventoryInput{OrderID: in.OrderID},
        workflow.ActivityOptions{StartToCloseTimeout: time.Minute}); releaseErr != nil {
        return releaseErr
    }
    return err
}
```

可运行的示例见 `docs/examples/cancellation`，规则见 [取消、超时与终止](/develop/cancellation-timeouts#取消后清理)。
