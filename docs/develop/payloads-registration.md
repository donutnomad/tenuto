---
title: Workflow Type 与 Payload
description: 稳定的 Workflow Type、Workflow ID 与启动输入的编码
status: Stable
---

# Workflow Type 与 Payload

<FeatureStatus status="Stable" />

Workflow Type 是部署稳定的名称，即 `@Workflow(id=...)` 的取值；Go 函数是本次构建中的实现。客户端按 Workflow Type 启动 Workflow，生成器把函数翻译成以这个 Type 登记的 Program。

## Workflow Type

```go
// @Workflow(id="orders.approval",version=v1)
func Approval(ctx workflow.Context, in CreateInput) (CreateResult, error) { ... }
```

```go
run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: "approval-order-1042"},
    "orders.approval", CreateInput{OrderID: "order-1042"})
```

Type 推荐使用业务域和流程名，例如 `orders.approval`。重命名 Go 函数不影响 Type；修改 Type 等于发布一个新的 Workflow，进行中的 Run 继续使用旧 Type。

## Workflow ID 与 Workflow Type

| 字段 | 作用 | 示例 |
| --- | --- | --- |
| Workflow Type | 选择代码和 Program | `orders.approval` |
| Workflow ID | 业务实例身份 | `approval-order-1042` |
| Run ID | 一段具体执行 | UUID |
| Namespace | 租户和路由边界 | `orders-prod` |

Workflow ID 可以由业务主键构造，也可以由调用端生成 UUID。幂等创建场景通常使用稳定业务 ID。

## 启动输入

`ExecuteWorkflow`、`SignalWithStartWorkflow`、`NewWithStartWorkflowOperation` 与 Schedule 接受零个或一个启动参数，单参数必须编码为 JSON 对象。结构体 DTO 是推荐形式：

```go
start := c.NewWithStartWorkflowOperation(
    client.StartWorkflowOptions{
        ID:                       workflowID,
        WorkflowIDConflictPolicy: client.WorkflowIDConflictPolicyUseExisting,
    },
    "orders.approval",
    CreateInput{OrderID: orderID},
)
```

## 编码

载荷编码由 Server 与 Worker 固定为 JSON，客户端不可替换：JSON 整数保留精确值，`Get` 的解码目标必须是非空指针。规则见 [Payload 与数据转换](/concepts/payloads#客户端编码)，启动与更新组合见 [Update-With-Start](/sdk/update-with-start)。
