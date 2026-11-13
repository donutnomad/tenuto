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

首次执行到 `ExecuteActivity` 时，Worker 产生 Schedule Activity command。下一轮重放到同一代码位置时，运行时从历史中找到 Activity 结果并写入输出变量。

## 为什么 Activity 保持安全

Workflow 重放只执行编排逻辑。真实数据库、HTTP、MQ 和文件系统操作放在 Activity 中。历史里已经存在 Activity 结果时，重放直接读取结果，Activity Worker会依据任务和历史幂等屏障执行对应 attempt。

## 确定性来源

- Activity、Timer、Signal、Update 和 Child Workflow 结果来自历史。
- `workflow.Now` 使用激活快照中的确定性时间。
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
