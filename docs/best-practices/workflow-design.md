---
title: Workflow 设计最佳实践
description: 确定性、状态规模、消息契约、并发和长流程设计原则
status: Stable
---

# Workflow 设计最佳实践

<FeatureStatus status="Stable" />

## 保持 Workflow 确定性

Workflow 使用 `workflow.Now`、`RandomUUID`、`RandomInt`、Side Effect 和 Activity 获取外部或随机结果。map 遍历先排序 key，并发 lane 和 Selector case 保持稳定声明顺序。

## 控制内存状态

Workflow 变量会在重放时重建。保存业务 cursor、聚合值和必要幂等窗口，批量明细进入数据库或对象存储，由 Activity 访问。

## 设计稳定消息契约

Signal Name、Update Name、Query Name、参数 DTO、结果 DTO 和 Application Error Type 都是长期契约。消息携带业务 ID、schema version 和幂等 ID。

## 使用结构化并发

单步 fan-out 使用多个 Future；每条 lane 含顺序依赖时使用 WaitGroup：

```go
failed := 0
wg := workflow.NewWaitGroup(ctx)
for _, item := range items {
    current := item
    wg.Go(ctx, func(ctx workflow.Context) {
        reserved, err := CallReserve(ctx, current, workflow.ActivityOptions{StartToCloseTimeout: 30 * time.Second})
        if err != nil {
            failed = failed + 1
            return
        }
        _, err = CallSign(ctx, reserved, workflow.ActivityOptions{StartToCloseTimeout: 30 * time.Second})
        if err != nil {
            failed = failed + 1
        }
    })
}
wg.Wait(ctx)
if failed > 0 {
    return workflow.NewApplicationError("lane failed", "LaneFailed", failed)
}
```

每条 lane 捕获当前循环值，把错误记入共享变量，join 后统一处理。完整写法见 [并发、Channel 与 Selector](/develop/concurrency)。

## 长流程分段

使用 `workflow.GetInfo` 读取 History 建议，在业务安全点 Continue-As-New。输入携带 cursor、deadline、幂等窗口和必要聚合值。

## 明确终态

Workflow 返回 nil 表示 Completed，返回取消错误表示 Cancelled，返回其他 error 表示 Failed。业务撤销走 Cancel：Workflow 代码收到取消后在 disconnected context 上补偿，再返回取消错误；故障隔离走 Terminate，历史修复走 Reset。终态选择进入业务审计和运维手册。

## 可观察性

为 Activity 使用稳定 Type 和 ActivityID，为 Run 写入 Search Attributes，为错误提供稳定 Type 和结构化 details。关键等待点在 Query 中暴露业务进度。
