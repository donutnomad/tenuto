---
title: 运行示例环境
description: Tenuto Demo 中可直接体验的 Workflow 场景
---

# 运行示例环境

`server/demo/app` 同时承担本地 Worker、Workflow 注册和 Run Seeder。它通过 Runtime HTTP API 与 Tenuto Server 通信，执行边界与生产部署保持一致。

## Demo 场景

| Workflow ID | 能力 | 推荐页面 |
| --- | --- | --- |
| `demo-cross-app-flow` | Reserve → Sign 顺序 Activity | Run Detail / Event History |
| `demo-auto-sweep` | `for` 循环、条件分支、同一行多次 Activity | Trace / Calls |
| `demo-activity-bulk-loop` | 240 次循环 Activity | Trace / 虚拟列表 |
| `demo-flaky-retry` | 前两次失败、第三次成功 | Attempts |
| `demo-go-fanout` | 三条并发 lane | Trace / Execution Lines |
| `demo-go-multistep-lane` | 每条 lane 内多步顺序链 | Trace / Stack |
| `demo-go-loop-fanout` | 循环内 `workflow.Go` | Trace / RoutineID |
| `demo-trace-child-debug-suite` | 多种 Child Workflow | Trace / Step Into |
| `demo-trace-can-root` | Continue-As-New Run Segment | Trace / Segment |

## 单独运行 Server

```bash
make dev-server APP_PORT=7400
```

内存模式使用以下配置：

```bash
WORKFLOW_BACKEND=memory
WORKFLOW_HTTP_ADDR=127.0.0.1:7400
```

## 单独运行 Demo Worker

```bash
make dev-demo TENUTO_SERVER_URL=http://127.0.0.1:7400
```

Demo 会等待 `/health/ready` 成功，然后注册 Activity Worker Version、启动 Workflow Worker、注册 Workflow 定义并创建 Run。Server 开启 `WORKFLOW_HTTP_AUTH_TOKEN`（见[安全部署](/deploy/security#bearer-token)）时，用环境变量 `TENUTO_AUTH_TOKEN` 把其中一个 token 交给 Demo。

## 观察 Namespace

```bash
curl -sS \
  -H 'Content-Type: application/json' \
  -d '{}' \
  http://localhost:7400/runtime/workflow/list-namespaces
```

典型响应：

```json
{
  "items": [
    {
      "name": "demo",
      "run_count": 21,
      "worker_count": 18,
      "schedule_count": 3
    }
  ],
  "total": 1
}
```

Demo 数据使用内存后端，Server 进程停止后会清空。生产环境通过 MySQL 保存 Run、Event History、mutable state、Schedule、Worker Version 与 dispatch 状态。
