---
title: Namespace、Worker 与 Task Queue
description: 使用 Console 检查资源、版本注册、路由和容量
status: Stable
---

# Namespace、Worker 与 Task Queue

<FeatureStatus status="Stable" />

Console 的资源页用于回答三个问题：业务数据位于哪个 Namespace、任务由哪个队列承载、当前哪些 Worker 可以执行。

## Namespace

Namespace 列表展示 Run、Workflow Definition、Worker 和 Schedule 数量。Worker 数按 (Identity, Build ID) 计，一个 Worker 登记多个 Task Queue 或 Activity Type 仍算一个；只有 Worker、还没有 Run 的 Namespace 同样统计 Workflow Definition 与 Schedule。进入 Namespace 后，后续资源筛选保持相同租户边界。

## Worker

Worker 列表展示：

- Identity、Build ID 和 App。
- Task Queue 与 Activity Type。
- Default、Weight、Last Heartbeat 和 Expires At（相对时间：过去的写成 `5s ago`，将来的写成 `in 25s`）。
- Workflow Worker 的 Sticky Cache entries、bytes、hits、misses 和 evictions。

Activity Worker 来自数据库里的登记，所有副本看到同一份。Workflow Worker 的 Last Heartbeat 与 Sticky Cache 来自应答这次请求的 Server 副本：多副本部署时只列出在这个副本上 poll 过的 Workflow Worker。

过期 Worker 会从 active 路由中移除。发布窗口可以并排检查新旧 Build ID 的注册和 Heartbeat。

## Task Queue

Task Queue 页面聚合 active workers、Worker IDs 和 Activity Types。Activity 长时间 Scheduled 时，从 Run 的 Activity Type 和 Task Queue 跳转到对应资源页。

## 诊断顺序

1. 确认 Run Namespace。
2. 确认 ActivityScheduled 中的 Task Queue。
3. 检查 Task Queue active worker 数量。
4. 检查 Worker Heartbeat TTL，以及各 Build ID 上报的契约与 Run 当前 Program 的声明是否一致。
5. 检查 Server 的 logical/physical queue 映射。

相关内容：[Namespace 与 Task Queue 运维](/operations/namespaces-task-queues)。
