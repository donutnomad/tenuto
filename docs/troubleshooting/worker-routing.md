---
title: Worker 路由与 Sticky 故障
description: 排查 Build ID、Task Queue、Heartbeat、缓存命中和 owner fallback
status: Stable
---

# Worker 路由与 Sticky 故障

<FeatureStatus status="Stable" />

## Worker 无法领取任务

1. 核对 Namespace 和 Task Queue。
2. 核对 Activity Type 已注册。
3. 核对 Worker 这个 Build ID 上报的契约与 Run 当前 Program 的声明一致：不一致的领取被拒绝，Server 日志记 `activity claim refused: worker contract is incompatible`，任务约 1s 后重新投递。
4. 核对 Version Heartbeat 尚在 TTL 内。
5. 核对 logical queue 到 TaskQ physical queue 的映射。
6. 查看 poll、ack、nack 和 TaskQ membership 日志。

## Sticky 命中率低

| 指标 | 常见原因 |
| --- | --- |
| misses 高 | Worker 滚动、Identity 变化、TTL 短、容量小 |
| capacity eviction 高 | MaxEntries 或 MaxEstimatedBytes 偏小 |
| generation mismatch 高 | Reset、Program/ABI/Build ID 变化 |
| rejected 高 | active entry 全部 pinned |
| fallback 高 | owner 停止、sticky schedule-to-start 超时 |

## Owner Worker 下线

Server 在 sticky assignment 到期后提升 Sticky Generation、清除 owner，并将 open Workflow Task 回退普通队列。新 Worker cold restore History 后建立新的 session。

## Capability 不匹配

检查 Server capability endpoint、Worker 声明、Program 所需 WLang capability、Executor ABI 和 Console WASM 版本。升级顺序见 [Server 与 Worker 升级](/operations/upgrades)。

## 指定 Activity Worker

工作流配置的 `target_worker_identity` 指定业务 Activity 的执行实例。WorkflowTask 继续使用编排 worker 的队列和 sticky 机制。目标 identity 在同一 namespace 内应稳定且唯一；相同 identity 的进程共同代表同一个目标。

Console 的 Workflows 页面可选择在线 Activity Worker，选取后保存新的配置版本。**新运行生效，已有运行及重试保留原目标**。定时触发的新运行使用最新配置；显式启动历史版本继续使用该版本目标。选择 Default scheduling 清除目标，恢复按 task queue 调度。

API 设置目标：

```http
POST /runtime/workflow/set-worker-target
Content-Type: application/json

{"namespace":"default","workflow_type":"payment","target_worker_identity":"worker-B"}
```

返回新版本的 `WorkflowConfigRef`。空字符串清除目标。完整配置注册 `/runtime/workflow/register` 也支持 WorkflowConfig 顶层的 `target_worker_identity`。设置时目标需在线，并能处理配置中的 Activity 类型及显式 queue/build；未知、离线或能力不匹配的目标返回说明性错误。`/runtime/workflow/list-configs` 返回 `target_worker_identity` 和 `target_worker_status`（`default`、`online`、`offline`）。

目标在保存后离线时，已调度 Activity 保持 `scheduled`，等待同身份且匹配 build 的 worker 恢复，或按已有 Schedule To Start / Schedule To Close 超时结束。目标字段保存在 ActivityState 的 `targetWorkerIdentity`；实际执行者记录在 ActivityStarted 的 `workerIdentity`。运行详情显示该运行固定的目标；日志通过 run ID、scheduled event ID 和 attempt 关联。

目标列 `target_worker_identity` 在 `server/infra/sql/schema.sql` 的 `tenuto_activity_states` 表里，默认空值。
