---
title: 系统架构
description: Tenuto Server、Worker、TaskQ、MySQL 与 Console 的协作方式
status: Stable
---

# 系统架构

<FeatureStatus status="Stable" />

Tenuto 由控制面（含内嵌 matching）、执行面、持久化层和调试界面组成。

```text
Business Service / Go Client
              │ Runtime HTTP
              ▼
┌──────────────────────────────┐
│        Tenuto Server         │
│ Runtime API · State Machine  │
│ Recovery · Timer · Schedule  │
│ Workflow Worker（默认内嵌）  │
└──────────────┬───────────────┘
               │
      ┌────────┴────────┐
      ▼                 ▼
  MySQL Store     TaskQ Matching (:7235)
  Run / History   Workflow / Activity
  Mutable State   Task Queues
      ▲                 │
      │                 ▼
      │            Activity Workers
      │                 │
      └──── Runtime HTTP┘

Tenuto Console ── Runtime HTTP ── Tenuto Server
Browser WASM ─── Event History ── Trace Replay
```

## Tenuto Server

正式入口是 `../../server/cmd/tenuto-server`。Server 负责：

- Workflow 注册、启动、取消、终止、暂停、恢复和 Reset。
- Workflow Task 与 Activity Task 的状态机推进。
- Event History 与 mutable state 原子提交。
- Durable Timer、Schedule、Child Workflow 与 Continue-As-New。
- Worker Version 路由与 Sticky assignment。
- 崩溃恢复、pending task 补投递与 dispatch 对账。
- Console 所需的可见性和调试 API。
- 默认在进程内执行 Workflow Task（`WORKFLOW_WORKER_ENABLED=false` 时由独立 Workflow Worker 执行）。

## Mutable State 与 Event History

Tenuto 使用双层状态模型：

- **Mutable State** 保存当前执行状态，恢复复杂度与 pending 工作量相关。
- **Event History** 保存不可变执行记录，承担重放、审计和 dispatch outbox 语义。

Activity、Timer 和 Child Workflow 的状态更新与对应事件在同一个 `RunInTx` 事务中提交。

## TaskQ Matching

TaskQ 提供 Workflow Task 和 Activity Task 的队列、分区、lease 与 matching 服务。Tenuto Server 通过 Frontend 投递和领取任务，TaskQ membership 为 matching 节点提供服务发现与一致性哈希路由。MySQL 后端下 matching 默认运行在 Tenuto Server 进程内（`:7235`），本进程同时注册自身的广播地址并观察全部 matching 节点；`TASKQ_MATCHING_ENABLED=false` 时回到外置 matching 拓扑，本进程只观察。

## Worker 执行面

Workflow Worker 默认运行在 Tenuto Server 进程内，直接从本进程领取 Workflow Task，读取 Program 与 History，执行确定性重放并提交 commands；独立部署的 Workflow Worker 经 Runtime API 长轮询完成同样的工作。Activity Worker 从指定 Task Queue 领取 Activity，执行业务代码并上报 started、heartbeat、completed 或 failed。

## Tenuto Console 与 WASM Replay

Console 从 Server 获取 Program 与 Event History。浏览器中的 `replay.wasm` 使用同一 WLang 运行时和安全的 Workflow host 实现重放源码，生成代码行、执行线、栈帧、变量和 Activity invocation 数据。

Trace 调试只消费历史结果，外部 Activity 副作用保持隔离。
