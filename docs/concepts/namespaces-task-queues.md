---
title: Namespace 与 Task Queue
description: 租户隔离、任务路由和 Worker 匹配模型
status: Stable
---

# Namespace 与 Task Queue

<FeatureStatus status="Stable" />

Namespace 和 Task Queue 是 Tenuto 中两个独立的隔离维度。Namespace 表示租户或业务边界，Task Queue 表示一组 Worker 的任务路由。

## Namespace

Namespace 会出现在 Workflow、Run、Update、Signal、Query 和 Visibility 查询中。相同的 Workflow ID 可以在不同 Namespace 中分别存在。

```go
c, err := client.Dial(client.Options{
    HostPort:  "tenuto-server:7400",
    Namespace: "payments-prod",
})
```

Client 的全部调用作用于这个 Namespace；从它创建的 Worker 在这个 Namespace 登记 Activity 路由与 Workflow 定义。

建议按照环境和业务边界规划 Namespace：

| 场景 | 建议 |
| --- | --- |
| 开发、测试、生产 | 使用独立 Namespace |
| 多个业务线共享集群 | 使用业务线 Namespace |
| 同一业务的灰度版本 | 使用同一 Namespace，通过 Workflow Type 和 Build ID 区分 |
| 临时实验 | 使用带过期策略的实验 Namespace |

Namespace 变更会影响 Run 查询、Task Queue 路由和客户端默认目标。生产客户端应显式配置 Namespace，并在日志中记录。

## Task Queue

Task Queue 决定 Activity Task 被哪一组 Worker 领取。Workflow Task 不分队列，由 Server 派给任意 Workflow Worker。

Activity 的队列是 Activity 自己的属性，有两种写法：

```go
// 在 Activity 上声明：workflow.json 记录这个队列，Worker 从生成的 RegisterActivities 推导出要拉取它
// @Activity(type="payments.charge-card",queue="payments-activities")
func ChargeCard(ctx context.Context, input ChargeInput) (ChargeOutput, error) { ... }

// 不写：由执行它的 Worker 决定（worker.Options.ActivityTaskQueues，不写时是 default），
// Server 调度时按在线 Worker 的登记选队列
// @Activity(type="orders.notify")
func Notify(ctx context.Context, input NotifyInput) (NotifyOutput, error) { ... }
```

单次调用通过 `workflow.ActivityOptions{TaskQueue: "payments-activities"}` 改派队列。决定顺序、没有声明队列时的选路与歧义处理见 [Activity 的队列如何决定](/operations/namespaces-task-queues#activity-的队列如何决定)，注解写法见 [注解与代码生成](/develop/code-generation)。

使用独立队列隔离资源密集型 Activity、低延迟 Activity 和高权限 Activity。队列名称属于运行契约，修改队列后必须同步部署对应 Worker。

## Worker 与队列匹配

Worker 只会领取自己监听的 Task Queue。常见关系如下：

```text
Namespace payments-prod
  ├── workflow-tasks       Workflow Worker
  ├── payments-activities  支付 Activity Worker
  └── risk-activities      风控 Activity Worker
```

以下情况会导致任务长期等待：Worker 未启动、Worker 监听了错误 Namespace、队列名称拼写不同、TaskQ Frontend 地址不可达、Worker capability 不包含 Program 所需能力。

## 规划建议

队列数量以故障隔离和资源隔离为目标。每个队列配置明确的 Worker 身份、并发上限、发布版本和告警阈值。业务调用方只依赖 Workflow Type 与 Workflow ID，队列路由由 Workflow 配置和部署清单共同维护。
