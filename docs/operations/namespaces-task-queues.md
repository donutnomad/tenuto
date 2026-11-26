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

## Activity 的队列如何决定

Server 在调度一个 Activity 时按下面的顺序确定队列，确定的队列写进 History 的 `ActivityScheduled.taskQueue`：

1. 调用处的 `ActivityOptions.TaskQueue`，只作用于这一次调用。
2. Run 的 Workflow 定义（`workflow.json` 的 `activities[].task_queue`）声明的队列，来自 `@Activity(queue=...)`。队列是 Activity 自己的属性，与调用它的 Workflow、与放在同一个包里的 Workflow 都无关。
3. 前两者都没有时，Server 在路由目录里选：目录是在线 Worker 上报的「拉取的队列 × 登记的 Activity 类型」。有契约（入参出参指纹）与定义一致的路由时先排除不一致的，再优先默认路由（`ActivityWorkerVersionDefault`），最后按权重选择。没有任何 Worker 登记这个类型、或路由歧义（见下文）时，Run 显示 Blocked（`activity_route_unavailable`），Worker 上线或声明队列后按退避继续。

第 1、2 步确定了队列时，调度总是写入 `ActivityScheduled` 并投进这个队列，与此刻队列上有没有在线 Worker、在线 Worker 的契约是否一致无关；`ScheduleToStartTimeout`、`ScheduleToCloseTimeout` 从调度时起算。契约在 Worker 领取时按它自己的上报对照，不一致的领取被拒绝，任务约 1s 后重新投递，契约一致的 Worker 上线后自动领取。

两种写法：

| 写法 | 注解 | Worker | 适用 |
| --- | --- | --- | --- |
| 由 Worker 决定 | `@Activity` 不写 `queue` | `ActivityTaskQueues` 设为这个服务的队列（不写时是 `default`） | 一个服务一个队列。队列只在 Worker 上配置，改队列不产生新的定义版本，之后的调度跟随 Worker 的登记 |
| 在 Activity 上声明 | `@Activity(queue="payments")`；跨服务的 `@RemoteActivity` 契约写在共享契约包里 | 不写 `ActivityTaskQueues`，从 registry 推导；手写登记用 `activity.RegisterActivityOnQueue` | 需要固定队列、独立 Worker 池的 Activity；调度结果可以从 `workflow.json` 读出 |

由 Worker 决定的队列在调度时才确定，事后从 History 与 Console 查看。Temporal 没有路由目录：调用其他服务的 Activity 必须在调用处写对方的队列名，写错时任务投到没有实现的队列，Worker 报 activity type not registered 后按重试策略重试。

### 没有声明队列时的歧义

由 Worker 决定的写法依赖路由目录选队列。同一个 Activity 类型由几个 Worker 提供时：

- 它们的队列有交集（同一个服务的多个实例、滚动发布的新旧 build、一个进程拉多个队列）：在这组队列里选，照常投递。队列按进程（Worker Identity）分组，`BuildID` 相同或为空不会把不同进程各自的队列算作一组。
- 只有一个默认路由所在的那组队列：选它，其他非默认路由不参与。
- 没有默认路由、配置了权重：按权重分流，这是显式的分流配置。
- 默认路由（或既没有默认也没有权重的路由）分布在互不相关的队列上，例如两个服务各自的队列：Server 不在它们之间二选一。这次调度不派发，Run 显示 Blocked，原因是 `activity_route_unavailable`：

```text
activity route unavailable: activity charge type payments.charge declares no task queue and workers serve it on unrelated task queues [["billing"] ["payments"]]; declare the task queue in @Activity(queue=...) or ActivityOptions.TaskQueue, or stop one of them
```

在注解或调用处声明队列、重新生成并发布，或停掉其中一个服务后，Run 按退避继续，History 不变。两个服务提供同名 Activity 本身是允许的：Worker 登记不做这项检查，调用方声明了队列时只在该队列里选。

### 在 Console 查看

- Workers 页的 **Activity Routing** 表按 Activity 类型列出在线 Worker 拉取的队列，默认路由所在的队列高亮，一个类型分布在多个队列上时标出。
- Run 详情展开 Activity 步骤，`Task Queue` 是这次调度实际使用的队列，`Queue Source` 按 Run 当前的定义说明它来自定义声明、调用处改派，还是定义没有声明（调用处选项或路由目录）。

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
| Activity 长时间 Scheduled | logical/physical queue 映射、active Worker；Console Run 详情的 `Task Queue` 与 Workers 页 Activity Routing 里该类型的队列是否一致 |
| Run Blocked，`activity_route_unavailable`，`declares no task queue and workers serve it on unrelated task queues` | 定义与调用处都没有声明队列，几个服务在不同队列上提供同一类型，见[没有声明队列时的歧义](#没有声明队列时的歧义) |
| Activity 被反复拒绝领取（日志 `activity claim refused`），Run 不暂停 | 在线 Worker 上报的契约、Run 当前 Program 的声明、需要的旧 build 是否已下线；让契约一致的 build 上线，或把 Run 改成 `auto_upgrade` |
| 不同租户互相看见资源 | Namespace 传递、Task Token、Worker Options |
| Worker 列表频繁消失 | Heartbeat interval、TTL、网络延迟 |
| 同类型由旧版本执行 | 新旧 build 的契约是否相同（相同时任一 build 都可执行）、`ActivityStarted` 的 `workerBuildId` |

## 容量隔离

高价值和高耗时 Activity 使用不同队列与 Worker 池。Namespace 隔离数据视图，Task Queue 隔离处理容量，两者共同形成多租户边界。
