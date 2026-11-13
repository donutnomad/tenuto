---
title: Workflow 消息交互
description: Signal、Query、Update 及 With-Start 的语义与生命周期
status: Stable
---

# Workflow 消息交互

<FeatureStatus status="Stable" />

业务进程通过消息与正在运行的 Workflow 交互。选择入口时先确定调用方需要的交付确认和返回结果。

## 语义选择

| 需求 | 入口 | 调用方获得 | History |
| --- | --- | --- | --- |
| 投递异步事实 | Signal | Signal Event ID | `WorkflowSignaled` |
| 读取当前内存状态 | Query | 只读结果 | 本次 Query 本身不新增事件 |
| 执行业务命令并取得结果 | Update | Accepted Handle 或 Completed Result | Accepted + Completed |
| 创建或复用 Run 后执行命令 | Update-With-Start | Update Handle | Start（按需）+ Update |
| 创建或复用 Run 后投递事实 | Signal-With-Start | Run ID + Signal Event ID | Start（按需）+ Signal |

## 目标 Run

Workflow ID 是业务身份，Run ID 是一次具体执行。调用指定 Run ID 时，消息精确定位该 Run；调用只指定 Workflow ID 时，Server 在对应 Namespace 解析 current Run。

```text
Namespace + Workflow ID
          │
          └─ current Run ID ──> Signal / Query / Update
```

Continue-As-New 会更新 current Run。业务请求通常使用 Workflow ID 跟随当前 segment；调试、审计和补偿操作通常携带 Run ID 固定目标。

## Handler 注册

Query 和 Update 依赖 Workflow 代码注册同名 Handler。注册语句应位于 Workflow 主体的稳定前缀，使每次重放都建立相同的消息契约。

```go
const UpdateName = "change-limit"

func LimitWorkflow(ctx workflow.Context, in Input) error {
    limit := in.InitialLimit

    if err := workflow.SetUpdateHandler(ctx, UpdateName,
        func(ctx workflow.Context, next int64) (Result, error) {
            previous := limit
            limit = next
            return Result{Previous: previous, Current: limit}, nil
        },
    ); err != nil {
        return err
    }

    if err := workflow.SetQueryHandler(ctx, "current-limit",
        func(ctx workflow.Context, _ map[string]any) map[string]any {
            return map[string]any{"limit": limit}
        },
    ); err != nil {
        return err
    }

    return workflow.Sleep(ctx, 3600000000000) // 1 小时，单位纳秒
}
```

## 交付与幂等

- Signal 面向 at-least-once 业务交付，payload 中应携带稳定消息 ID。
- Update ID 标识一条逻辑命令；重复请求复用同一 Accepted 或 Completed 记录。
- Activity 继续使用自身幂等键。Update 去重保护 Handler，Activity 幂等保护外部副作用。
- Query 读取瞬时状态，调用方根据业务需要缓存结果和处理过期读。

## 终态

Run 进入 Completed、Failed、Cancelled、Terminated 或 Continued-As-New 后，Server 会终止该 Run 的新消息处理。使用 Workflow ID 的后续请求会依据 current Run 解析规则定位新 segment 或返回终态错误。

相关内容：[Signal](/develop/signals)、[Query](/develop/queries)、[Update](/develop/updates)、[Update-With-Start](/sdk/update-with-start)。
