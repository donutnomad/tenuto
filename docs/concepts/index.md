---
title: 核心概念
description: Tenuto 的 Workflow、Run、Activity、Worker 与 Event History
status: Stable
---

# 核心概念

<FeatureStatus status="Stable" />

Tenuto 把长时间运行的业务过程建模为 Workflow Execution。业务代码描述顺序、分支、等待和并发；平台负责持久化进度、调度外部工作、记录结果并在故障后恢复。

## Workflow Definition

Workflow Definition 是可版本化的流程定义：带 `@Workflow` 注解的 Go 函数经 `tenuto-workflow-gen` 翻译为 WLang Program，见 [注解与代码生成](/develop/code-generation)。登记到服务端的 `workflow.json`（`program.WorkflowConfig`）携带这段 Program 与它引用的 Activity，Worker 的 WLang 解释器执行它。

每个注册版本都由内容哈希标识。Run 固定引用创建时的配置版本，重放时始终使用同一份程序。

## Workflow ID 与 Run ID

- **Workflow ID**：业务侧稳定标识，例如 `create-order-1001`。
- **Run ID**：一次具体执行的唯一标识。
- **Workflow Type**：部署稳定的流程类型，例如 `approval-workflow`。

Continue-As-New 会保留业务关系并创建新的 Run ID。Reset 会从已选 Workflow Task 边界建立新的分支 Run。

## Activity

Activity 执行数据库、RPC、消息发送、文件处理等外部副作用。Tenuto 为 Activity 提供：

- Task Queue 路由与领取时的契约校验。
- Schedule-To-Start、Start-To-Close、Schedule-To-Close 与 Heartbeat 超时。
- Retry Policy 与 Attempt 历史。
- 取消请求交给 Workflow 代码：可取消的 Context 作用域，Activity 经 Heartbeat 感知取消。
- 异步完成 Task Token。

## Worker

Workflow Worker 重放 Workflow Program 并产生命令。Activity Worker 执行业务副作用并回报结果。两类 Worker 可以独立部署、独立扩缩容，也可以在开发环境中由同一进程承载。

## Event History

Event History 是每个 Run 的 append-only 执行记录。常见事件包括：

```text
WorkflowStarted
WorkflowTaskScheduled
WorkflowTaskStarted
WorkflowTaskCompleted
ActivityScheduled
ActivityStarted
ActivityCompleted
TimerScheduled
TimerFired
WorkflowCompleted
```

Workflow Worker 使用历史中的已提交结果进行重放。重放只恢复内存状态和命令决策，Activity 副作用只由 Activity Worker 执行。

## Namespace

Namespace 是租户与资源隔离边界。Run、Workflow Definition、Task Queue、Worker Version、Schedule 和可见性查询均携带 Namespace。

## Signal、Query 与 Update

| 类型 | 是否写入历史 | 是否返回业务结果 | 典型用途 |
| --- | --- | --- | --- |
| Signal | 是 | 事件 ID | 异步通知、审批结果、外部状态变化 |
| Query | 否 | 当前只读结果 | 查看进度、读取 Workflow 内存状态 |
| Update | 是 | 强类型结果或错误 | 同步业务命令、Update-With-Start |
