---
title: Workflow Execution
description: Workflow ID、Run ID、Run Chain、Reset 与 Continue-As-New
status: Stable
---

# Workflow Execution

<FeatureStatus status="Stable" />

Workflow Execution 由业务身份和具体执行实例共同定位。

## 标识关系

```text
Namespace + Workflow ID
          │
          ├── Run A
          │    └── Continue-As-New → Run B
          │                              └── Continue-As-New → Run C
          │
          └── Reset → Run R
```

`Workflow ID` 表示业务过程，`Run ID` 表示一次执行。调用端通常持有 Workflow ID；诊断、历史和精确控制操作使用 Run ID。

## Run 状态

| 状态 | 含义 |
| --- | --- |
| `running` | Run 正在执行或等待外部事件 |
| `completed` | Workflow 正常返回 |
| `failed` | Workflow 以失败结束 |
| `cancelled` | Workflow 返回取消错误（通常在收到取消请求并清理之后） |
| `terminated` | 被立即终止 |
| `continued_as_new` | 当前 Run 已创建下一代 Run |

## 当前等待点

Run visibility 会派生当前等待点：

- Workflow Task
- Activity
- Timer
- Signal 或 Update
- Child Workflow

Console 的 Run 列表与详情页会展示等待类型、Activity ID、Activity Type、Task Queue 和 Scheduled Event ID。

## Run Chain

Continue-As-New 形成线性 Segment 链。Console 按时间和关系显示上一段、当前段与下一段。前一段状态为 `continued_as_new`，下一段是继续执行业务逻辑的新 Run。

## Reset Branch

Reset 从 Workflow Task 边界建立新的执行分支。Tenuto 使用 branch-aware Event History，让新 Run 共享锚点之前的祖先历史，并从所选决策点重新执行后续逻辑。

Reset 锚点使用 `WorkflowTaskCompleted`、`WorkflowTaskFailed` 或 `WorkflowTaskTimedOut` 等完整决策边界，保证重放上下文完整。
