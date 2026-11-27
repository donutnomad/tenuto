---
title: Event History 与重放
description: Tenuto 的确定性历史、Workflow Task 与重放机制
status: Stable
---

# Event History 与重放

<FeatureStatus status="Stable" />

Event History 记录 Run 已经发生的事实。Workflow 代码每次执行都根据同一份事实表做出相同决定。

## Workflow Task 周期

```text
外部事件到达
    ↓
WorkflowTaskScheduled
    ↓
Worker poll + WorkflowTaskStarted
    ↓
从缓存增量执行或从头重放
    ↓
提交 WorkflowTaskCompleted + Commands
    ↓
Server 把 Commands 转成 Activity / Timer / Child 等事件
```

Workflow Task 执行期间到达的 Signal、Activity 结果等事实照常落库，排在这次的 `WorkflowTaskCompleted` 之前。`WorkflowTaskCompleted.seenThroughEventId` 记下 Worker 决策时看到的最后一个事件，ID 大于它的事实不属于这次决策：命令照常提交，Server 随即调度下一个 Workflow Task 处理这些事实，重放也把它们归入下一批，与 live 执行看到的事件一致。只有一批含结束 Workflow 的命令（Complete、Fail、Cancel、ContinueAsNew）遇到这类事实时，这次完成以 `WorkflowTaskFailed(unhandled_events)` 作废并立即重做，结束 Workflow 不会丢掉事实。作废之后 Run 进入正在关闭状态（同 Temporal 的 `workflowCloseAttempted`）：重做的 Workflow Task started 之后、关闭之前，新的 Signal 与 Update 不写入这个 Run（同一个 `request_id` 的 Signal、同一个 Update ID 已经记下时，重发照常返回第一次的结果，与 Temporal 先去重、后检查关闭的顺序相同），Server 以 503 `workflow operation can not be applied because workflow is closing`（Go SDK 自动重试）拒绝，重做的结束命令因此一定能提交；按 Workflow ID 重发的请求落到 Continue-As-New 的下一代 Run，Run 已结束时按已结束处理。这次 Workflow Task 关闭（提交、失败或超时）后状态清除。持续到达的 Signal 与 Update 因此不会让 Continue-As-New 无限重做。

首次执行到 `ExecuteActivity` 时，Worker 产生 Schedule Activity command。下一轮重放到同一代码位置时，运行时从历史中找到 Activity 结果并写入输出变量。

## 为什么 Activity 保持安全

Workflow 重放只执行编排逻辑。真实数据库、HTTP、MQ 和文件系统操作放在 Activity 中。历史里已经存在 Activity 结果时，重放直接读取结果，Activity Worker会依据任务和历史幂等屏障执行对应 attempt。

## 确定性来源

- Activity、Timer、Signal、Update 和 Child Workflow 结果来自历史。
- `workflow.Now` 取本次 Workflow Task 的 `WorkflowTaskStarted` 时间戳，live 与重放相同。
- `SideEffect`、`MutableSideEffect`、`RandomUUID`、`RandomInt` 和 `GetVersion` 使用 Marker 固化值。
- 并发协程由确定性协作式调度器按稳定顺序推进。
- 每条 lane 使用稳定 Lane Path 生成 Activity、Timer、Child 等原语的缺省 ID。

## Sticky Execution Cache

Worker 首次接收 Run 时执行 cold restore。缓存命中后，后续 Workflow Task 只消费新增 History，复用已恢复的 execution session。缓存逐出、Worker 重启和 sticky 超时会自动回到 cold replay 路径。

## History 容量

默认容量边界：

| 指标 | Warning | Hard limit | 终态预留 |
| --- | ---: | ---: | ---: |
| Event 数量 | 10,240 | 51,200 | 32 |
| JSON 字节 | 10 MiB | 50 MiB | 64 KiB |

长期运行 Workflow 应读取 `workflow.GetInfo(ctx)`，在 `GetContinueAsNewSuggested()` 为 true 时携带业务状态执行 Continue-As-New。
