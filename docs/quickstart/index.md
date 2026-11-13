---
title: 五分钟快速开始
description: 在本地启动 Tenuto Server、Worker、Demo 与 Console
---

# 五分钟快速开始

这条路径会启动一个内存后端的 Tenuto Server、Workflow Worker、Activity Worker、Demo Seeder 和 Tenuto Console。Demo 会创建线性流程、并发 fan-out、Activity 重试、Child Workflow、Continue-As-New 与 Trace 调试样例。

## 环境要求

| 工具 | 建议版本 | 用途 |
| --- | --- | --- |
| Go | `go.mod` 声明的版本 | Server、SDK、Worker 与 WASM 构建 |
| pnpm | `10+` | Console 与文档站依赖管理 |
| Node.js | `20+` | Next.js Console 与 VitePress 文档 |
| make、curl、lsof | 系统自带版本 | 开发命令与健康检查 |

## 1. 获取代码并安装前端依赖

```bash
git clone https://github.com/donutnomad/tenuto && cd tenuto
pnpm --dir console install
```

## 2. 启动完整环境

```bash
make dev
```

默认端口：

| 服务 | 地址 |
| --- | --- |
| Tenuto Server | `http://localhost:7400` |
| Tenuto Console | `http://localhost:7401` |

Demo 日志 `.run/tenuto-demo.log` 会输出多个 Trace URL，例如：

```text
TRACE_TEST_URL=http://localhost:7401/demo/runs/<run-id>/trace
TRACE_LOOP_GO_URL=http://localhost:7401/demo/runs/<run-id>/trace
TRACE_BULK_ACTIVITY_URL=http://localhost:7401/demo/runs/<run-id>/trace
```

## 3. 检查服务状态

```bash
curl http://localhost:7400/health/live
curl http://localhost:7400/health/ready
```

健康响应：

```json
{"status":"ok"}
```

## 4. 打开 Console

访问 `http://localhost:7401`，进入 `demo` Namespace。你可以查看：

- Runs：当前与历史 Workflow Execution。
- Workers：Workflow Worker 与 Activity Worker 的 Identity、Build ID 和 Task Queue。
- Schedules：Cron Schedule、暂停状态和重叠策略。
- Event History：服务端持久化的事件序列。
- Trace：源码级重放调试、断点、调用栈、变量与 Activity Attempt。

## 5. 停止环境

```bash
make stop
```

下一步按[接入已有项目](/quickstart/integrate)在自己的 Go 模块里引入 SDK、写注解并注册 Workflow 与 Activity；[编写 Workflow](/develop/workflows) 说明 Go 源码如何生成 WLang Program。
