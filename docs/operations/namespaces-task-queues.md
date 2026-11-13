---
title: Namespace 与 Task Queue 运维
description: 资源隔离、队列映射、Worker 注册和路由诊断
status: Stable
---

# Namespace 与 Task Queue 运维

<FeatureStatus status="Stable" />

Namespace 是数据和路由边界，Task Queue 是工作负载边界。生产、测试和业务租户应使用独立 Namespace。

## 资源模型

Tenuto 当前根据已存在的 Run、Workflow Definition、Worker 和 Schedule 汇总 Namespace 列表。Task Queue 列表根据 Worker Version 与路由配置汇总 active worker 和 Activity Type。

```text
Namespace
  ├─ Workflow Definitions
  ├─ Runs and Schedules
  ├─ Workflow Task Queue
  └─ Activity Task Queues → Worker Versions
```

## 物理队列映射

生产 Server 通过 `TASKQ_ACTIVITY_QUEUES_JSON` 配置逻辑路由：

```json
{
  "orders/payments": {"physical": "activity-orders-payments", "partitions": 8},
  "orders/default": {"physical": "activity-orders-default"}
}
```

Key 使用 `namespace/task_queue`，`physical` 是物理队列名，`partitions` 可省略。物理 Activity Queue 与 Workflow Queue 使用独立名称。

## 上线步骤

1. 配置 Namespace 对应的 physical queue。
2. 启动 Worker，等待 Version 注册和 Heartbeat。
3. 在 Console 确认 Activity Type、Task Queue、Build ID 和 TTL。
4. 启动一条 canary Run，检查 schedule-to-start。
5. 开放业务流量并观察队列积压。

## 路由故障

| 症状 | 检查项 |
| --- | --- |
| Activity 长时间 Scheduled | logical/physical queue 映射、active Worker |
| Activity 被反复拒绝领取 | 在线 Worker 上报的契约、Run 当前 Program 的声明、需要的旧 build 是否已下线 |
| 不同租户互相看见资源 | Namespace 传递、Task Token、Worker Options |
| Worker 列表频繁消失 | Heartbeat interval、TTL、网络延迟 |
| 同类型由旧版本执行 | 新旧 build 的契约是否相同（相同时任一 build 都可执行）、`ActivityStarted` 的 `workerBuildId` |

## 容量隔离

高价值和高耗时 Activity 使用不同队列与 Worker 池。Namespace 隔离数据视图，Task Queue 隔离处理容量，两者共同形成多租户边界。
