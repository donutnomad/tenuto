---
title: Command 参考
description: Workflow 重放生成的执行意图及其对应 Event
status: Internal
---

# Command 参考

<FeatureStatus status="Internal" />

Command 是 Workflow Worker 重放代码后提交的意图。Server 校验并把 Command 转换为 Event、mutable state 和可恢复副作用。Command 每轮重新生成，Event 才是持久化事实。

## 异步动作

| Command | 对应 Event | 副作用 |
| --- | --- | --- |
| `ScheduleActivity` | `ActivityScheduled` | 投递 Activity Task |
| `ScheduleTimer` | `TimerScheduled` | 建立 Durable Timer |
| `StartChildWorkflow` | `ChildWorkflowStarted` | 创建 Child Run；`ParentClosePolicy` 为空时记为 `terminate`，未知取值让 Workflow Task 失败 |

## 取消

Workflow 代码取消一个作用域时，为其中已提交、尚未结束的操作产生取消命令，顺序与作用域的取消顺序相同。还没提交的调度命令（同一个 Workflow Task 里发起又被取消）直接撤回，不发给 Server。

| Command | 对应 Event | 副作用 |
| --- | --- | --- |
| `RequestCancelActivity`（ActivityID） | `ActivityCancelRequested` | 尚未开始的 Activity 同事务写 `ActivityCancelled` 并调度 Workflow Task；已开始的经 Heartbeat 得知取消 |
| `CancelTimer`（TimerID） | `TimerCancelled` | 停止 Durable Timer |
| `RequestCancelChildWorkflow`（ChildID） | `ChildWorkflowCancelRequested` | 提交后向子 Workflow 当前 Run 写入 `WorkflowCancellationRequested`；中途退出时由子 Run 的恢复补发 |

## 确定性与消息

| Command | 对应 Event |
| --- | --- |
| `RecordMarker` | `MarkerRecorded` |
| `CompleteWorkflowUpdate` | `WorkflowUpdateCompleted` |

## Run 终态

| Command | 对应 Event | 状态 |
| --- | --- | --- |
| `CompleteWorkflow` | `WorkflowCompleted` | Completed |
| `FailWorkflow` | `WorkflowFailed` | Failed |
| `CancelWorkflow`（Details） | `WorkflowCancelled` | Cancelled；Workflow 返回取消错误时产生 |
| `ContinueAsNew` | `WorkflowContinuedAsNew` | Continued-As-New |

## 匹配与幂等

Server 使用 ActivityID、Scheduled Event ID、Timer ID、Child ID、Update ID 和 Marker ID 将 Command 与 History 匹配。重放生成的 Command 序列必须与已有 Event 保持确定性。

Workflow Task Completed、Command Event 和 mutable state 更新在同一事务边界提交。任务投递由 dispatch 状态恢复。
